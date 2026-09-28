# Databricks notebook source
# TDX tracking/location bronze writer. Default is a bounded dev sample; sample_mode=false runs a full compare.

# COMMAND ----------

# MAGIC %run /Workspace/Shared/ADC-DB/Prod/Pipelines/Bronze/_bronze_common

# COMMAND ----------

import json
import traceback
from datetime import datetime, timezone
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

for _name, _default in {
    "target_schema": "8_dev.tdx_bronze",
    "sample_mode": "true",
    "sample_rows": "50000",
    "reuse_sample_episode": "false",
    "force_sample_targets": "",
    "allow_production_write": "false",
    "pipeline_run_id": "",
}.items():
    try:
        dbutils.widgets.get(_name)
    except Exception:
        dbutils.widgets.text(_name, _default)

TARGET_SCHEMA = dbutils.widgets.get("target_schema")
SAMPLE_MODE = dbutils.widgets.get("sample_mode").lower() == "true"
SAMPLE_ROWS = int(dbutils.widgets.get("sample_rows"))
REUSE_SAMPLE_EPISODE = dbutils.widgets.get("reuse_sample_episode").lower() == "true"
FORCE_SAMPLE_TARGETS = {x.strip() for x in dbutils.widgets.get("force_sample_targets").split(",") if x.strip()}
ALLOW_PRODUCTION_WRITE = dbutils.widgets.get("allow_production_write").lower() == "true"
RUN_ID = dbutils.widgets.get("pipeline_run_id") or f"tdx-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
assert not TARGET_SCHEMA.lower().startswith("4_prod") or ALLOW_PRODUCTION_WRITE
assert TARGET_SCHEMA.startswith("8_dev.") or ALLOW_PRODUCTION_WRITE
if SAMPLE_MODE:
    assert TARGET_SCHEMA.startswith("8_dev."), "sample builds are dev-only"

PIPELINE_LOGIC_VERSION = "2026.09.tdx2.sample-first"
LOGIC_VERSION_INT = 2026091302
RAW = "4_prod.raw"
CODE_VALUE = "3_lookup.mill.mill_code_value"
SRC_ITEM = f"{RAW}.mill_tracking_item"
SRC_CHECKIN = f"{RAW}.mill_tracking_checkin"
SRC_LOCATOR = f"{RAW}.mill_tracking_locator"
SRC_EVENT = f"{RAW}.mill_tracking_event"
SRC_PREARRIVAL = f"{RAW}.mill_tracking_prearrival"
SRC_PENDING = f"{RAW}.mill_encntr_pending"
SRC_TRACK_EVENT = f"{RAW}.mill_track_event"
SRC_NURSE_UNIT = f"{RAW}.mill_nurse_unit"
SRC_ROOM = f"{RAW}.mill_room"
SRC_BED = f"{RAW}.mill_bed"
SRC_LOC_ATTRIB = f"{RAW}.mill_pm_loc_attrib"
SRC_LOC_ATTRIB_HIST = f"{RAW}.mill_pm_loc_attrib_hist"
SRC_THEATRE_CASE = "4_prod.bronze.map_theatre_case"
SRC_ENCOUNTER = "4_prod.bronze.map_encounter"

EPISODE = f"{TARGET_SCHEMA}.map_tracking_episode"
ATTENDANCE = f"{TARGET_SCHEMA}.map_tracking_attendance"
STAY = f"{TARGET_SCHEMA}.map_tracking_location_stay"
MILESTONE = f"{TARGET_SCHEMA}.map_tracking_milestone"
PREARRIVAL = f"{TARGET_SCHEMA}.map_prearrival"
PENDING = f"{TARGET_SCHEMA}.map_pending_movement"
LOC_UNIT = f"{TARGET_SCHEMA}.map_location_unit"
LOC_ATTRIB = f"{TARGET_SCHEMA}.map_location_attribute_history"

DECODE_LOOKUP = spark.table(CODE_VALUE).select(
    F.col("CODE_VALUE").cast("long").alias("__CODE_VALUE"),
    F.col("DISPLAY").alias("__CODE_DESCRIPTION"),
).dropDuplicates(["__CODE_VALUE"])

ANON_STATE = [
    "anon_status", "anon_redactor_version", "anon_source_text_sha", "anon_identity_fingerprint",
    "anon_context_fingerprint", "anon_redaction_count", "anon_processed_at",
]


def qname(name):
    return ".".join(f"`{p.replace('`', '``')}`" for p in name.split("."))


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qname(TARGET_SCHEMA)}")


def table_exists(name):
    return spark.catalog.tableExists(name)


def add_decode(df, code_column, output_column, code_set=None):
    lookup = DECODE_LOOKUP
    if code_set is not None:
        lookup = spark.table(CODE_VALUE).where(F.col("CODE_SET") == code_set).select(
            F.col("CODE_VALUE").cast("long").alias("__CODE_VALUE"), F.col("DISPLAY").alias("__CODE_DESCRIPTION")
        ).dropDuplicates(["__CODE_VALUE"])
    key = f"__{output_column}_code"
    lk = F.broadcast(lookup.select(F.col("__CODE_VALUE").alias(key), F.col("__CODE_DESCRIPTION").alias(output_column)))
    return df.join(lk, F.col(code_column).cast("long") == F.col(key), "left").drop(key)


def with_anon_state(df, output_columns):
    for name in output_columns:
        df = df.withColumn(name, F.lit(None).cast("string"))
    for name in ANON_STATE:
        dtype = "bigint" if name == "anon_redaction_count" else ("timestamp" if name == "anon_processed_at" else "string")
        df = df.withColumn(name, F.lit(None).cast(dtype))
    return df


def finalise(df, keys, source_table, source_row_col):
    df = (df.withColumn("SOURCE_TABLE", F.lit(source_table))
            .withColumn("SOURCE_ROW_ID", F.col(source_row_col).cast("string"))
            .withColumn("PIPELINE_RUN_ID", F.lit(RUN_ID))
            .withColumn("SOURCE_PRESENT_IND", F.lit(True))
            .withColumn("SOURCE_ABSENT_DETECTED_TS", F.lit(None).cast("timestamp")))
    excluded = {"ROW_HASH", "ADC_UPDT", "PIPELINE_RUN_ID", "SOURCE_ABSENT_DETECTED_TS", *ANON_STATE}
    payload_cols = sorted(c for c in df.columns if c not in excluded and not c.startswith("anon_"))
    df = df.withColumn("ROW_HASH", F.sha2(F.to_json(F.struct(*[F.col(c) for c in payload_cols])), 256))
    return df.withColumn("ADC_UPDT", F.current_timestamp())


def publish(df, target, keys, full_compare=True, cluster_by=None):
    def validate(candidate):
        bad_null = reduce(lambda a, b: a | b, [F.col(k).isNull() for k in keys])
        assert candidate.where(bad_null).limit(1).count() == 0, f"{target}: NULL key"
        assert candidate.groupBy(*keys).count().where("count > 1").limit(1).count() == 0, f"{target}: duplicate key"

    if SAMPLE_MODE:
        # Materialise the bounded dev sample once, then validate that small Delta table.
        # Re-running the lazy source plan for each assertion is disproportionately costly.
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
           .option("delta.enableChangeDataFeed", "true").option("delta.enableRowTracking", "true")
           .saveAsTable(target))
        validate(spark.table(target))
        operation = "SAMPLE_OVERWRITE"
    elif not table_exists(target):
        validate(df)
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
           .option("delta.enableChangeDataFeed", "true").option("delta.enableRowTracking", "true")
           .saveAsTable(target))
        operation = "CREATE"
    else:
        validate(df)
        dt = DeltaTable.forName(spark, target)
        cond = " AND ".join([f"t.`{k}` <=> s.`{k}`" for k in keys])
        mutable = [c for c in df.columns if not c.startswith("anon_") and c not in keys]
        builder = (dt.alias("t").merge(df.alias("s"), cond)
            .whenMatchedUpdate(condition="NOT (t.ROW_HASH <=> s.ROW_HASH)", set={c: f"s.`{c}`" for c in mutable})
            .whenNotMatchedInsertAll())
        if full_compare:
            builder = builder.whenNotMatchedBySourceUpdate(
                condition="t.SOURCE_PRESENT_IND = true",
                set={"SOURCE_PRESENT_IND": "false", "SOURCE_ABSENT_DETECTED_TS": "current_timestamp()", "ADC_UPDT": "current_timestamp()"},
            )
        builder.execute()
        operation = "FULL_MERGE"
    spark.sql(f"ALTER TABLE {qname(target)} SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true','delta.enableRowTracking'='true','delta.enableDeletionVectors'='true')")
    if cluster_by:
        try:
            spark.sql(f"ALTER TABLE {qname(target)} CLUSTER BY ({','.join(f'`{c}`' for c in cluster_by)})")
        except Exception as exc:
            print(f"cluster_by deferred for {target}: {exc}")
    apply_governance(target)
    return {"target": target, "operation": operation, "rows": spark.table(target).count()}


def publish_or_reuse_sample(df, target, keys, full_compare=True, cluster_by=None):
    if SAMPLE_MODE and REUSE_SAMPLE_EPISODE and table_exists(target) and target.split(".")[-1] not in FORCE_SAMPLE_TARGETS:
        return {"target": target, "operation": "REUSED_SAMPLE", "rows": spark.table(target).count()}
    return publish(df, target, keys, full_compare=full_compare, cluster_by=cluster_by)


def apply_governance(table):
    catalog, schema, name = table.split(".")
    spark.sql(f"COMMENT ON TABLE {qname(table)} IS 'TDX curated bronze research input. Grain and interpretation are documented in column comments; sample builds are development evidence only.'")
    identifier_tokens = ("PERSON", "ENCNTR", "NHS", "MRN", "NAME", "DOB", "BIRTH", "POSTCODE", "ADDRESS", "PHONE", "TEL", "FAX", "EMAIL", "PRSNL", "USER_ID")
    text_tokens = ("COMMENT", "COMPLAINT", "REASON", "DESCRIPTION", "VALUE_STRING")
    for col in spark.table(table).columns:
        uc = col.upper()
        comment = f"TDX bronze field {col}; source-faithful unless its name documents a derived value."
        spark.sql(f"ALTER TABLE {qname(table)} ALTER COLUMN `{col.replace('`','``')}` COMMENT '{comment}'")
        if any(token in uc for token in identifier_tokens): risk, severity = "4", "2"
        elif any(token in uc for token in text_tokens) or uc.startswith("ANON_"): risk, severity = "3", "2"
        else: risk, severity = "1", "1"
        spark.sql(f"ALTER TABLE {qname(table)} ALTER COLUMN `{col.replace('`','``')}` SET TAGS ('ig_risk'='{risk}','ig_severity'='{severity}')")


def head(df, n=None):
    return df.limit(n or SAMPLE_ROWS) if SAMPLE_MODE else df

# COMMAND ----------

MILESTONE_RULES = [
    ("TDX-M01", r"^ARRIVAL$|^AE TIME$|^PT ARRIVED INTO DEPA", "arrival", "ed"),
    ("TDX-M02", r"^REGISTRATION$", "registration", "ed"),
    ("TDX-M03", r"^TRIAGE( C)?$", "triage", "ed"),
    ("TDX-M04", r"^(PAED )?ASSESS(MENT|ENT)$|^SEEN$|^REASSESSMENT$", "clinician_assessment", "ed"),
    ("TDX-M05", r"^TREATMENT ?START$", "treatment_start", "ed"),
    ("TDX-M06", r"^SNR REVIEW (REQUIRED|COMPLETED)$|^OBS REVIEW$", "senior_review", "ed"),
    ("TDX-M07", r"^(RED|AMBER) FLAG SEPSIS$|^CONFIRMED SEPSIS$", "sepsis_flag", "ed"),
    ("TDX-M08", r"^REF ", "specialty_referral", "ed"),
    ("TDX-M09", r"^BED REQUEST$|^BED (AVAILABLE|ALLOCATED|ASSIGN)$", "bed_request_assign", "admission"),
    ("TDX-M10", r"^ADMITTED TO WARD$|^WARD TO SURGICAL HAN", "admitted_to_ward", "admission"),
    ("TDX-M11", r"^RDY FOR (STPDN )?DISCH$", "ready_for_discharge", "discharge"),
    ("TDX-M12", r"^DISCHARGE$|^TRANSFER$", "discharge_or_transfer", "discharge"),
    ("TDX-M13", r"^X ?RAY$|^CT READY$|^BLOOD$|^POCT VBG$|^VBG TO REVIEW$|^BLOOD GLUCOSE$|^PREGNANCY TEST$", "investigation", "ed"),
    ("TDX-M14", r"^PT RDY FOR SURG$|^FIT FOR SURGERY$|^TRANSPORT TO ANAES R|^(PT )?IN ANAES ROOM$", "theatre_pre", "theatre"),
    ("TDX-M15", r"^ANAES START$", "anaesthesia_start", "theatre"),
    ("TDX-M16", r"^PT IN THEATRE$", "patient_in_theatre", "theatre"),
    ("TDX-M17", r"^SURG START$", "surgery_start", "theatre"),
    ("TDX-M18", r"^SURG STOP$", "surgery_stop", "theatre"),
    ("TDX-M19", r"^PT OUT THEATRE$", "patient_out_theatre", "theatre"),
    ("TDX-M20", r"^ANAES STOP$", "anaesthesia_stop", "theatre"),
    ("TDX-M21", r"^PT IN REC", "in_recovery", "theatre"),
]
EXCLUDED_DISPLAY_RE = r"^INCOMPLETE |^CARE ROUNDING$|^RECHART OBS$|^REVISION CONVERSATIO|^ACUITY$|^FRAILTY SCORE$|^PRESENTING COMPLAINT$"


def classified_track_events():
    out = spark.table(SRC_TRACK_EVENT).withColumn(
        "NORMALISED_DISPLAY", F.regexp_replace(F.regexp_replace(F.upper(F.trim("DISPLAY")), "[^A-Z0-9 ]", ""), r"\s+", " ")
    ).withColumn("MILESTONE_RULE_ID", F.lit(None).cast("string")) \
     .withColumn("MILESTONE_CLASS", F.lit(None).cast("string")) \
     .withColumn("MILESTONE_PHASE", F.lit(None).cast("string"))
    for rule_id, pattern, klass, phase in reversed(MILESTONE_RULES):
        matches = F.col("NORMALISED_DISPLAY").rlike(pattern)
        out = out.withColumn("MILESTONE_RULE_ID", F.when(matches, rule_id).otherwise(F.col("MILESTONE_RULE_ID"))) \
                 .withColumn("MILESTONE_CLASS", F.when(matches, klass).otherwise(F.col("MILESTONE_CLASS"))) \
                 .withColumn("MILESTONE_PHASE", F.when(matches, phase).otherwise(F.col("MILESTONE_PHASE")))
    return out.where(F.col("MILESTONE_RULE_ID").isNotNull() & ~F.col("NORMALISED_DISPLAY").rlike(EXCLUDED_DISPLAY_RE))

# COMMAND ----------

def build_episode():
    src = spark.table(SRC_ITEM)
    if SAMPLE_MODE:
        per = max(1, SAMPLE_ROWS // 4)
        src = reduce(lambda a, b: a.unionByName(b), [
            src.where("ENCNTR_ID > 0").limit(per),
            src.where("PARENT_ENTITY_NAME = 'SURGICAL_CASE'").limit(per),
            src.where("PARENT_ENTITY_NAME = 'TRACKING_PREARRIVAL'").limit(per),
            src.where("coalesce(ENCNTR_ID,0)=0 AND (PARENT_ENTITY_NAME IS NULL OR trim(PARENT_ENTITY_NAME)='')").limit(per),
        ]).dropDuplicates(["TRACKING_ID"])
    i = src.alias("i")
    tc = spark.table(SRC_THEATRE_CASE).where("SOURCE_PRESENT_IND").select(
        F.col("SURG_CASE_ID").alias("_case_id"), F.col("PERSON_ID").alias("_case_person"), F.col("ENCNTR_ID").alias("_case_encntr")
    ).alias("tc")
    pre = spark.table(SRC_PREARRIVAL).select(
        F.col("TRACKING_PREARRIVAL_ID").alias("_pre_id"), F.col("ATTACHED_PERSON_ID").alias("_pre_person"), F.col("ATTACHED_ENCNTR_ID").alias("_pre_encntr")
    ).alias("p")
    joined = (i.join(tc, (F.col("i.PARENT_ENTITY_NAME") == "SURGICAL_CASE") & (F.col("i.PARENT_ENTITY_ID") == F.col("tc._case_id")), "left")
               .join(pre, (F.col("i.PARENT_ENTITY_NAME") == "TRACKING_PREARRIVAL") & (F.col("i.PARENT_ENTITY_ID") == F.col("p._pre_id")), "left"))
    own = F.when(F.col("i.ENCNTR_ID") > 0, F.col("i.ENCNTR_ID").cast("long"))
    route = F.when(own.isNotNull(), "encounter").when(F.col("_case_person") > 0, "surgical_case").when(F.col("_pre_person") > 0, "prearrival").otherwise("none")
    person = F.when(route == "encounter", F.col("i.PERSON_ID")).when(route == "surgical_case", F.col("_case_person")).when(route == "prearrival", F.col("_pre_person"))
    encntr = F.when(route == "encounter", own).when(route == "surgical_case", F.when(F.col("_case_encntr") > 0, F.col("_case_encntr"))).when(route == "prearrival", F.when(F.col("_pre_encntr") > 0, F.col("_pre_encntr")))
    open_ind = F.col("i.END_TRACKING_DT_TM") >= F.lit("2100-01-01").cast("timestamp")
    out = joined.select(
        F.col("i.TRACKING_ID").cast("long").alias("TRACKING_ID"), person.cast("long").alias("PERSON_ID"), encntr.cast("long").alias("ENCNTR_ID"), route.alias("LINKAGE_ROUTE"),
        F.when(route == "surgical_case", F.col("_case_id")).cast("long").alias("SURG_CASE_ID"),
        F.when(F.col("i.PARENT_ENTITY_NAME") == "TRACKING_PREARRIVAL", F.col("i.PARENT_ENTITY_ID")).cast("long").alias("TRACKING_PREARRIVAL_ID"),
        F.col("i.PARENT_ENTITY_NAME").alias("PARENT_ENTITY_NAME"), F.col("i.PARENT_ENTITY_ID").cast("long").alias("PARENT_ENTITY_ID"),
        F.col("i.ORGANIZATION_ID").cast("long").alias("ORGANIZATION_ID"), F.col("i.Trust").alias("TRUST"),
        "START_TRACKING_DT_TM", "END_TRACKING_DT_TM", F.when(~open_ind, F.col("i.END_TRACKING_DT_TM")).alias("END_TRACKING_DT_TM_CLEAN"), open_ind.alias("TRACKING_OPEN_IND"),
        F.col("i.BASE_LOC_CD").cast("long").alias("BASE_LOC_CD"), "BASE_LOC_DT_TM", F.col("i.CUR_TRACKING_LOCATOR_ID").cast("long").alias("CUR_TRACKING_LOCATOR_ID"),
        F.col("i.TRACKING_STATUS_FLAG").cast("int").alias("TRACKING_STATUS_FLAG"), F.col("i.TRACKING_TYPE_FLAG").cast("int").alias("TRACKING_TYPE_FLAG"),
        (F.col("i.ACTIVE_IND") == 1).alias("ITEM_ACTIVE_IND"), F.col("i.ADC_UPDT").alias("SOURCE_ADC_UPDT"),
    )
    return finalise(add_decode(out, "BASE_LOC_CD", "BASE_LOC_DISPLAY"), ["TRACKING_ID"], SRC_ITEM, "TRACKING_ID")


if SAMPLE_MODE and REUSE_SAMPLE_EPISODE and table_exists(EPISODE):
    results = [{"target": EPISODE, "operation": "REUSED_SAMPLE", "rows": spark.table(EPISODE).count()}]
else:
    try:
        episode_df = build_episode()
        results = [publish(episode_df, EPISODE, ["TRACKING_ID"], cluster_by=["PERSON_ID", "START_TRACKING_DT_TM"])]
    except BaseException:
        spark.createDataFrame(
            [(RUN_ID, "map_tracking_episode", traceback.format_exc(), datetime.now(timezone.utc).replace(tzinfo=None))],
            "run_id string, stage string, detail string, recorded_at timestamp",
        ).write.mode("append").saveAsTable("8_dev.tdx_evidence.tracking_pipeline_errors")
        raise
episode_keys = spark.table(EPISODE).where("SOURCE_PRESENT_IND").select("TRACKING_ID", "PERSON_ID", "ENCNTR_ID", "LINKAGE_ROUTE", "SURG_CASE_ID")

# COMMAND ----------

try:
    checkin = spark.table(SRC_CHECKIN).alias("s").join(F.broadcast(episode_keys.alias("e")), "TRACKING_ID", "inner")
    checkout_open = F.col("s.CHECKOUT_DT_TM") >= F.lit("2100-01-01").cast("timestamp")
    attendance = checkin.select(
        F.col("s.TRACKING_CHECKIN_ID").cast("long").alias("TRACKING_CHECKIN_ID"), F.col("TRACKING_ID").cast("long"),
        F.col("e.PERSON_ID").alias("PERSON_ID"), F.col("e.ENCNTR_ID").alias("ENCNTR_ID"), F.col("e.LINKAGE_ROUTE").alias("LINKAGE_ROUTE"), F.col("e.SURG_CASE_ID").alias("SURG_CASE_ID"),
        "CHECKIN_DT_TM", "CHECKOUT_DT_TM", F.when(~checkout_open, F.col("s.CHECKOUT_DT_TM")).alias("CHECKOUT_DT_TM_CLEAN"), checkout_open.alias("ATTENDANCE_OPEN_IND"),
        *[F.col(f"s.{c}").cast("long").alias(c) for c in ["CHECKOUT_DISPOSITION_CD", "TRACKING_GROUP_CD", "SPECIALTY_ID", "PRIMARY_DOC_ID", "PRIMARY_NURSE_ID", "SECONDARY_DOC_ID", "SECONDARY_NURSE_ID", "TEAM_ID", "REGISTRATION_STATUS_ID", "FAMILY_PRESENT_CD", "REACTIVATE_USER_ID", "CHECKIN_ID", "CHECKOUT_ID", "TRACKING_EVENT_TYPE_CD", "ORGANIZATION_ID"]],
        (F.col("s.TRAUMA_IND") == 1).alias("TRAUMA_IND"), "REACTIVATION_DT_TM", F.col("s.Trust").alias("TRUST"), F.col("s.ADC_UPDT").alias("SOURCE_ADC_UPDT"),
    )
    for code, display in [("CHECKOUT_DISPOSITION_CD", "CHECKOUT_DISPOSITION_DISPLAY"), ("TRACKING_GROUP_CD", "TRACKING_GROUP_DISPLAY"), ("FAMILY_PRESENT_CD", "FAMILY_PRESENT_DISPLAY")]:
        attendance = add_decode(attendance, code, display)
    attendance = attendance.withColumn("ATTENDANCE_SEQUENCE", F.row_number().over(Window.partitionBy("TRACKING_ID").orderBy(F.col("CHECKIN_DT_TM").asc_nulls_last(), F.col("TRACKING_CHECKIN_ID"))))
    attendance = finalise(attendance, ["TRACKING_CHECKIN_ID"], SRC_CHECKIN, "TRACKING_CHECKIN_ID")
    results.append(publish_or_reuse_sample(attendance, ATTENDANCE, ["TRACKING_CHECKIN_ID"], cluster_by=["TRACKING_ID", "CHECKIN_DT_TM"]))
except BaseException:
    spark.createDataFrame(
        [(RUN_ID, "map_tracking_attendance", traceback.format_exc(), datetime.now(timezone.utc).replace(tzinfo=None))],
        "run_id string, stage string, detail string, recorded_at timestamp",
    ).write.mode("append").saveAsTable("8_dev.tdx_evidence.tracking_pipeline_errors")
    raise

# COMMAND ----------

locator = spark.table(SRC_LOCATOR).alias("s").join(F.broadcast(episode_keys.alias("e")), "TRACKING_ID", "inner")
stay_open = F.col("s.DEPART_DT_TM") >= F.lit("2100-01-01").cast("timestamp")
stay = locator.select(
    F.col("s.TRACKING_LOCATOR_ID").cast("long").alias("TRACKING_LOCATOR_ID"), F.col("TRACKING_ID").cast("long"),
    F.col("e.PERSON_ID").alias("PERSON_ID"), F.col("e.ENCNTR_ID").alias("ENCNTR_ID"), F.col("e.LINKAGE_ROUTE").alias("LINKAGE_ROUTE"), F.col("e.SURG_CASE_ID").alias("SURG_CASE_ID"),
    *[F.col(f"s.{c}").cast("long").alias(c) for c in ["LOCATION_CD", "LOC_NURSE_UNIT_CD", "LOC_ROOM_CD", "LOC_BED_CD", "RANK_SEQUENCE", "TRACKING_REASON_CD", "UNAVAIL_TRACKING_EVENT_ID", "TRACKING_ACUITY_LEVEL_ID", "ORGANIZATION_ID"]],
    "ARRIVE_DT_TM", "DEPART_DT_TM", F.when(~stay_open, F.col("s.DEPART_DT_TM")).alias("DEPART_DT_TM_CLEAN"), stay_open.alias("STAY_OPEN_IND"), "SCHEDULED_DT_TM",
    F.col("s.TRACKING_REASON_COMMENT").alias("TRACKING_REASON_COMMENT"), (F.col("s.ACTIVE_EVENTS_IND") == 1).alias("ACTIVE_EVENTS_IND"), F.col("s.Trust").alias("TRUST"), F.col("s.ADC_UPDT").alias("SOURCE_ADC_UPDT"),
)
for code, display in [("LOCATION_CD", "LOCATION_DISPLAY"), ("LOC_NURSE_UNIT_CD", "LOC_NURSE_UNIT_DISPLAY"), ("LOC_ROOM_CD", "LOC_ROOM_DISPLAY"), ("LOC_BED_CD", "LOC_BED_DISPLAY"), ("TRACKING_REASON_CD", "TRACKING_REASON_DISPLAY")]:
    stay = add_decode(stay, code, display)
stay = with_anon_state(stay, ["anon_tracking_reason_comment"])
stay = finalise(stay, ["TRACKING_LOCATOR_ID"], SRC_LOCATOR, "TRACKING_LOCATOR_ID")
results.append(publish_or_reuse_sample(stay, STAY, ["TRACKING_LOCATOR_ID"], cluster_by=["PERSON_ID", "ARRIVE_DT_TM"]))

# COMMAND ----------

try:
    defs = classified_track_events().select(
        F.col("TRACK_EVENT_ID").cast("long").alias("TRACK_EVENT_ID"), F.col("DISPLAY").alias("TRACK_EVENT_DISPLAY"),
        F.col("DESCRIPTION").alias("TRACK_EVENT_DESCRIPTION"), F.col("TRACKING_EVENT_TYPE_CD").cast("long").alias("TRACKING_EVENT_TYPE_CD"),
        "MILESTONE_CLASS", "MILESTONE_PHASE", "MILESTONE_RULE_ID"
    )
    event = spark.table(SRC_EVENT).alias("s").join(F.broadcast(episode_keys.alias("e")), "TRACKING_ID", "inner").join(F.broadcast(defs.alias("d")), F.col("s.TRACK_EVENT_ID") == F.col("d.TRACK_EVENT_ID"), "inner")
    milestone = event.select(
        F.col("s.TRACKING_EVENT_ID").cast("long").alias("TRACKING_EVENT_ID"), F.col("TRACKING_ID").cast("long"),
        F.col("e.PERSON_ID").alias("PERSON_ID"), F.col("e.ENCNTR_ID").alias("ENCNTR_ID"), F.col("e.LINKAGE_ROUTE").alias("LINKAGE_ROUTE"), F.col("e.SURG_CASE_ID").alias("SURG_CASE_ID"),
        F.col("s.TRACK_EVENT_ID").cast("long").alias("TRACK_EVENT_ID"), "TRACK_EVENT_DISPLAY", "TRACK_EVENT_DESCRIPTION", "MILESTONE_CLASS", "MILESTONE_PHASE", "MILESTONE_RULE_ID",
        F.lit("TDX-M.2026-09-13").alias("MILESTONE_RULE_VERSION"),
        F.col("d.TRACKING_EVENT_TYPE_CD").cast("long").alias("TRACKING_EVENT_TYPE_CD"),
        *[F.col(f"s.{c}").cast("long").alias(c) for c in ["EVENT_STATUS_CD", "TRACKING_GROUP_CD", "REQUESTED_ID", "ONSET_ID", "COMPLETE_ID", "CLINICAL_EVENT_CD", "ORGANIZATION_ID"]],
        "REQUESTED_DT_TM", "ONSET_DT_TM", "COMPLETE_DT_TM", F.col("s.Trust").alias("TRUST"), F.col("s.ADC_UPDT").alias("SOURCE_ADC_UPDT"),
    )
    milestone = milestone.join(F.broadcast(spark.table(SRC_TRACK_EVENT).select(F.col("TRACK_EVENT_ID").cast("long"), "OVERDUE_INTERVAL", "CRITICAL_INTERVAL")), "TRACK_EVENT_ID", "left")
    for code, display in [("TRACKING_EVENT_TYPE_CD", "TRACKING_EVENT_TYPE_DISPLAY"), ("EVENT_STATUS_CD", "EVENT_STATUS_DISPLAY"), ("TRACKING_GROUP_CD", "TRACKING_GROUP_DISPLAY")]:
        milestone = add_decode(milestone, code, display)
    milestone = finalise(milestone, ["TRACKING_EVENT_ID"], SRC_EVENT, "TRACKING_EVENT_ID")
    results.append(publish_or_reuse_sample(milestone, MILESTONE, ["TRACKING_EVENT_ID"], cluster_by=["PERSON_ID", "REQUESTED_DT_TM"]))
except BaseException:
    spark.createDataFrame(
        [(RUN_ID, "map_tracking_milestone", traceback.format_exc(), datetime.now(timezone.utc).replace(tzinfo=None))],
        "run_id string, stage string, detail string, recorded_at timestamp",
    ).write.mode("append").saveAsTable("8_dev.tdx_evidence.tracking_pipeline_errors")
    raise

# COMMAND ----------

pre_src = head(spark.table(SRC_PREARRIVAL))
audit = {"UPDT_ID", "UPDT_DT_TM", "UPDT_TASK", "UPDT_APPLCTX", "UPDT_CNT", "LAST_UTC_TS", "INST_ID", "TXN_ID_TEXT", "ADC_UPDT"}
pre_cols = [c for c in pre_src.columns if c not in audit and c not in {"Trust", "ENCNTR_ID", "ORGANIZATION_ID"}]
pre = pre_src.select(*[F.col(c) for c in pre_cols], F.col("Trust").alias("TRUST"), F.col("ORGANIZATION_ID").cast("long"),
                     F.when(F.col("ATTACHED_PERSON_ID") > 0, F.col("ATTACHED_PERSON_ID")).cast("long").alias("LINKED_PERSON_ID"),
                     F.when(F.col("ATTACHED_ENCNTR_ID") > 0, F.col("ATTACHED_ENCNTR_ID")).cast("long").alias("LINKED_ENCNTR_ID"),
                     F.when(F.col("ATTACHED_PERSON_ID") > 0, "attached").otherwise("none").alias("LINKAGE_ROUTE"), F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT"))
for code, display in [("TRACKING_GROUP_CD", "TRACKING_GROUP_DISPLAY"), ("PREARRIVAL_TYPE_CD", "PREARRIVAL_TYPE_DISPLAY"), ("SEX_CD", "SEX_DISPLAY")]:
    pre = add_decode(pre, code, display)
pre = with_anon_state(pre, ["anon_chief_complaint"])
pre = finalise(pre, ["TRACKING_PREARRIVAL_ID"], SRC_PREARRIVAL, "TRACKING_PREARRIVAL_ID")
results.append(publish_or_reuse_sample(pre, PREARRIVAL, ["TRACKING_PREARRIVAL_ID"], cluster_by=["LINKED_PERSON_ID", "ESTIMATED_ARRIVE_DT_TM"]))

# COMMAND ----------

try:
    pend_src = head(spark.table(SRC_PENDING)).alias("s")
    enc = spark.table(SRC_ENCOUNTER)
    if "SOURCE_PRESENT_IND" in enc.columns:
        enc = enc.where("SOURCE_PRESENT_IND")
    enc = enc.select(F.col("ENCNTR_ID").cast("long").alias("_ENCNTR_ID"), F.col("PERSON_ID").cast("long").alias("_PERSON_ID"))
    pending = pend_src.join(enc, F.col("s.ENCNTR_ID").cast("long") == F.col("_ENCNTR_ID"), "left").select(
        F.col("s.ENCNTR_PENDING_ID").cast("long").alias("ENCNTR_PENDING_ID"), F.col("s.ENCNTR_ID").cast("long").alias("ENCNTR_ID"), F.col("_PERSON_ID").alias("PERSON_ID"),
        *[F.col(f"s.{c}") for c in ["PENDING_DT_TM", "EST_COMPLETE_DT_TM", "PREV_EST_DEPART_DT_TM", "PROCESS_STATUS_DT_TM", "ALT_LVL_CARE_DT_TM", "ALC_DECOMP_DT_TM", "BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM"]],
        F.when(F.col("s.END_EFFECTIVE_DT_TM") < F.lit("2100-01-01").cast("timestamp"), F.col("s.END_EFFECTIVE_DT_TM")).alias("END_EFFECTIVE_DT_TM_CLEAN"),
        *[F.col(f"s.{c}").cast("long").alias(c) for c in ["PROCESS_STATUS_FLAG", "PENDING_STATUS_CD", "PENDING_TYPE_FLAG", "PENDING_PRIORITY_CD", "PRIORITY_SEQ", "PEND_FACILITY_CD", "PEND_BUILDING_CD", "PEND_NURSE_UNIT_CD", "PEND_ROOM_CD", "PEND_BED_CD", "ENCNTR_TYPE_CD", "MED_SERVICE_CD", "SPECIALTY_UNIT_CD", "ACCOMMODATION_CD", "ACCOMMODATION_REASON_CD", "ISOLATION_CD", "ALT_LVL_CARE_CD", "ALC_REASON_CD", "DISCH_DISPOSITION_CD", "DISCH_TO_LOCTN_CD", "TRANSACTION_REASON_CD", "ATTENDDOC_ID", "PENDING_PRSNL_ID", "ORGANIZATION_ID"]],
        F.col("s.TRANSACTION_REASON").alias("TRANSACTION_REASON"), (F.col("s.ACTIVE_IND") == 1).alias("ACTIVE_IND"), F.col("s.Trust").alias("TRUST"), F.col("s.ADC_UPDT").alias("SOURCE_ADC_UPDT"),
    )
    for code in ["PENDING_STATUS_CD", "PENDING_PRIORITY_CD", "PEND_FACILITY_CD", "PEND_BUILDING_CD", "PEND_NURSE_UNIT_CD", "PEND_ROOM_CD", "PEND_BED_CD", "ENCNTR_TYPE_CD", "MED_SERVICE_CD", "ACCOMMODATION_CD", "ISOLATION_CD", "DISCH_DISPOSITION_CD", "DISCH_TO_LOCTN_CD", "TRANSACTION_REASON_CD"]:
        pending = add_decode(pending, code, code.replace("_CD", "_DISPLAY"))
    pending = with_anon_state(pending, ["anon_transaction_reason"])
    pending = finalise(pending, ["ENCNTR_PENDING_ID"], SRC_PENDING, "ENCNTR_PENDING_ID")
    results.append(publish_or_reuse_sample(pending, PENDING, ["ENCNTR_PENDING_ID"], cluster_by=["PERSON_ID", "PENDING_DT_TM"]))
except BaseException:
    spark.createDataFrame(
        [(RUN_ID, "map_pending_movement", traceback.format_exc(), datetime.now(timezone.utc).replace(tzinfo=None))],
        "run_id string, stage string, detail string, recorded_at timestamp",
    ).write.mode("append").saveAsTable("8_dev.tdx_evidence.tracking_pipeline_errors")
    raise

# COMMAND ----------

nu = spark.table(SRC_NURSE_UNIT).select(
    F.col("LOCATION_CD").cast("long"), F.lit("nurse_unit").alias("LOCATION_LEVEL"), F.col("LOC_BUILDING_CD").cast("long").alias("PARENT_LOCATION_CD"),
    F.col("LOC_FACILITY_CD").cast("long").alias("FACILITY_CD"), F.col("LOC_BUILDING_CD").cast("long").alias("BUILDING_CD"), F.col("LOCATION_CD").cast("long").alias("NURSE_UNIT_CD"),
    F.lit(None).cast("long").alias("CLASS_CD"), F.lit(None).cast("boolean").alias("FIXED_BED_IND"), F.lit(None).cast("boolean").alias("DUP_BED_IND"),
    F.lit(None).cast("long").alias("CURRENT_BED_STATUS_CD"), (F.col("ACTIVE_IND") == 1).alias("ACTIVE_IND"), "BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM", F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT"),
)
room_src = spark.table(SRC_ROOM)
room = room_src.select(
    F.col("LOCATION_CD").cast("long"), F.lit("room").alias("LOCATION_LEVEL"), F.col("LOC_NURSE_UNIT_CD").cast("long").alias("PARENT_LOCATION_CD"),
    F.lit(None).cast("long").alias("FACILITY_CD"), F.lit(None).cast("long").alias("BUILDING_CD"), F.col("LOC_NURSE_UNIT_CD").cast("long").alias("NURSE_UNIT_CD"),
    F.col("CLASS_CD").cast("long"), (F.col("FIXED_BED_IND") == 1).alias("FIXED_BED_IND"), F.lit(None).cast("boolean").alias("DUP_BED_IND"),
    F.lit(None).cast("long").alias("CURRENT_BED_STATUS_CD"), (F.col("ACTIVE_IND") == 1).alias("ACTIVE_IND"), "BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM", F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT"),
)
room_parent = room.select(F.col("LOCATION_CD").alias("_ROOM_CD"), "NURSE_UNIT_CD")
bed_src = spark.table(SRC_BED).alias("b")
bed = bed_src.join(F.broadcast(room_parent), F.col("b.LOC_ROOM_CD").cast("long") == F.col("_ROOM_CD"), "left").select(
    F.col("b.LOCATION_CD").cast("long"), F.lit("bed").alias("LOCATION_LEVEL"), F.col("b.LOC_ROOM_CD").cast("long").alias("PARENT_LOCATION_CD"),
    F.lit(None).cast("long").alias("FACILITY_CD"), F.lit(None).cast("long").alias("BUILDING_CD"), F.col("NURSE_UNIT_CD"),
    F.lit(None).cast("long").alias("CLASS_CD"), F.lit(None).cast("boolean").alias("FIXED_BED_IND"), (F.col("b.DUP_BED_IND") == 1).alias("DUP_BED_IND"),
    F.col("b.BED_STATUS_CD").cast("long").alias("CURRENT_BED_STATUS_CD"), (F.col("b.ACTIVE_IND") == 1).alias("ACTIVE_IND"), F.col("b.BEG_EFFECTIVE_DT_TM"), F.col("b.END_EFFECTIVE_DT_TM"), F.col("b.ADC_UPDT").alias("SOURCE_ADC_UPDT"),
)
loc = reduce(lambda a, b: a.unionByName(b), [nu, room, bed]).where(F.col("LOCATION_CD") > 0)
loc = add_decode(loc, "LOCATION_CD", "LOCATION_DISPLAY")
loc = add_decode(loc, "PARENT_LOCATION_CD", "PARENT_LOCATION_DISPLAY")
loc = add_decode(loc, "NURSE_UNIT_CD", "NURSE_UNIT_DISPLAY")
loc = add_decode(loc, "CURRENT_BED_STATUS_CD", "CURRENT_BED_STATUS_DISPLAY")
loc = finalise(loc, ["LOCATION_CD"], "4_prod.raw.mill_nurse_unit|mill_room|mill_bed", "LOCATION_CD")
results.append(publish_or_reuse_sample(loc, LOC_UNIT, ["LOCATION_CD"], cluster_by=["LOCATION_LEVEL", "NURSE_UNIT_CD"]))

# COMMAND ----------

try:
    attrib_cols = ["ATTRIB_TYPE_CD", "PM_LOC_ATTRIB_ID", "LOCATION_CD", "DESCRIPTION", "VALUE_TYPE", "VALUE_CD", "VALUE_ID", "VALUE_STRING", "VALUE_NUM", "VALUE_DT_TM", "ACTIVE_IND", "BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM", "ADC_UPDT"]
    cur = head(spark.table(SRC_LOC_ATTRIB)).select(*attrib_cols).withColumn("ATTRIBUTE_ROW_ID", F.concat(F.lit("C:"), F.col("PM_LOC_ATTRIB_ID").cast("long"))).withColumn("HIST_ACTION", F.lit("CURRENT"))
    hist = head(spark.table(SRC_LOC_ATTRIB_HIST)).select(*attrib_cols, "PM_LOC_ATTRIB_HIST_ID", "HIST_ACTION").withColumn("ATTRIBUTE_ROW_ID", F.concat(F.lit("H:"), F.col("PM_LOC_ATTRIB_HIST_ID").cast("long"))).drop("PM_LOC_ATTRIB_HIST_ID")
    attrib = cur.unionByName(hist, allowMissingColumns=True).select(
        "ATTRIBUTE_ROW_ID", F.col("PM_LOC_ATTRIB_ID").cast("long"), F.col("LOCATION_CD").cast("long"), F.col("ATTRIB_TYPE_CD").cast("long"), "HIST_ACTION", "VALUE_TYPE",
        F.col("VALUE_CD").cast("long"), F.col("VALUE_ID").cast("long"), "VALUE_STRING", "VALUE_NUM", "VALUE_DT_TM", "DESCRIPTION",
        (F.col("ACTIVE_IND") == 1).alias("ACTIVE_IND"), "BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM",
        F.when(F.col("END_EFFECTIVE_DT_TM") < F.lit("2100-01-01").cast("timestamp"), F.col("END_EFFECTIVE_DT_TM")).alias("END_EFFECTIVE_DT_TM_CLEAN"),
        F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT"),
    )
    attrib = add_decode(attrib, "LOCATION_CD", "LOCATION_DISPLAY")
    attrib = add_decode(attrib, "ATTRIB_TYPE_CD", "ATTRIB_TYPE_DISPLAY", code_set=17649)
    attrib = add_decode(attrib, "VALUE_CD", "VALUE_DISPLAY")
    attrib = with_anon_state(attrib, ["anon_value_string", "anon_description"])
    attrib = finalise(attrib, ["ATTRIBUTE_ROW_ID"], "4_prod.raw.mill_pm_loc_attrib|mill_pm_loc_attrib_hist", "ATTRIBUTE_ROW_ID")
    results.append(publish_or_reuse_sample(attrib, LOC_ATTRIB, ["ATTRIBUTE_ROW_ID"], cluster_by=["LOCATION_CD", "BEG_EFFECTIVE_DT_TM"]))
except BaseException:
    spark.createDataFrame(
        [(RUN_ID, "map_location_attribute_history", traceback.format_exc(), datetime.now(timezone.utc).replace(tzinfo=None))],
        "run_id string, stage string, detail string, recorded_at timestamp",
    ).write.mode("append").saveAsTable("8_dev.tdx_evidence.tracking_pipeline_errors")
    raise

# COMMAND ----------

spark.createDataFrame(
    [(RUN_ID, PIPELINE_LOGIC_VERSION, SAMPLE_MODE, SAMPLE_ROWS, json.dumps(results, sort_keys=True), datetime.now(timezone.utc).replace(tzinfo=None))],
    "run_id string, logic_version string, sample_mode boolean, sample_rows long, result_json string, recorded_at timestamp",
).write.mode("append").saveAsTable("8_dev.tdx_evidence.tracking_pipeline_runs")
dbutils.notebook.exit(json.dumps({"sample_mode": SAMPLE_MODE, "sample_rows": SAMPLE_ROWS, "results": results}, sort_keys=True))

