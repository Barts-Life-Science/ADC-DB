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
    _insr_w8_assert_covered(target, df.columns)  # INSR_W8_COMMENTS_V1: before any write
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


# INSR_W8_COMMENTS_V1 (W8.04): canonical bronze comments, generated from the INSR W8.02 dictionary. Edit the dictionary,
# never this literal. Column names are the bronze (upper-case) names.
INSR_W8_BRONZE_COMMENTS = {
 "map_location_attribute_history": {
  "ACTIVE_IND": "Indicator showing whether the location attribute row is active.",
  "ADC_UPDT": "When bronze last wrote the row.",
  "ATTRIBUTE_ROW_ID": "Internal surrogate identifier for a location attribute record, taken from the Millennium location attribute or location attribute history row identifier.",
  "ATTRIB_TYPE_CD": "Millennium code value identifying the type of attribute recorded against the location.",
  "ATTRIB_TYPE_DISPLAY": "Display text of the location attribute type, resolved from the Millennium code value reference.",
  "BEG_EFFECTIVE_DT_TM": "Date and time from which the location attribute row is effective.",
  "DESCRIPTION": "Free-text description associated with the location attribute or its value.",
  "END_EFFECTIVE_DT_TM": "Date and time until which the location attribute row is effective, defaulting to a far-future date for open records.",
  "END_EFFECTIVE_DT_TM_CLEAN": "Cleaned date and time at which the location attribute record stopped being effective, derived from the Millennium end effective date-time.",
  "HIST_ACTION": "Short code indicating the history action recorded for the location attribute, such as an addition or deletion.",
  "LOCATION_CD": "Millennium code value identifying the location to which the attribute applies.",
  "LOCATION_DISPLAY": "Display name of the location to which the attribute applies, resolved from the Millennium code value reference.",
  "PIPELINE_RUN_ID": "Bronze pipeline run that last wrote the row.",
  "PM_LOC_ATTRIB_ID": "Internal surrogate identifier uniquely identifying each location attribute row in the Millennium PM_LOC_ATTRIB table.",
  "ROW_HASH": "SHA-256 of the row's payload columns, used by the bronze MERGE to detect changes.",
  "SOURCE_ABSENT_DETECTED_TS": "When bronze first found the row absent from the source snapshot; NULL while present.",
  "SOURCE_ADC_UPDT": "When the source row was landed in raw (raw ADC_UPDT).",
  "SOURCE_PRESENT_IND": "True while the row is present in the latest full source snapshot.",
  "SOURCE_ROW_ID": "Native source row identifier, as text.",
  "SOURCE_TABLE": "Bronze or raw source table the row came from.",
  "VALUE_CD": "Millennium code value holding the attribute value when the attribute is code-typed.",
  "VALUE_DISPLAY": "Display text of the location attribute value, resolved from the Millennium code value reference where applicable.",
  "VALUE_DT_TM": "Date/time attribute value used when the attribute is date/time-typed.",
  "VALUE_ID": "Identifier holding the attribute value when the attribute is ID-typed.",
  "VALUE_NUM": "Numeric attribute value used when the attribute is number-typed.",
  "VALUE_STRING": "Text holding the attribute value when the attribute is string-typed.",
  "VALUE_TYPE": "Single-character indicator of which typed value column (code, ID, string, number or date/time) holds the attribute value.",
  "anon_context_fingerprint": "Hash of the contextual inputs the redactor used for this row; the anonymised text is released only while it matches the current context.",
  "anon_description": "Anonymised version of description: identifiers detected in the free text are replaced by placeholders; published alongside the source column, which gold releases only when anonymisation succeeded.",
  "anon_identity_fingerprint": "Hash of the patient identity details the record-driven redactor used for this row.",
  "anon_processed_at": "When the redactor last processed this row's text.",
  "anon_redaction_count": "Number of redactions the redactor applied to the source text.",
  "anon_redactor_version": "Version of the redactor that produced the anonymised text.",
  "anon_source_text_sha": "SHA-256 digest of the text the redactor processed; when it no longer matches the current input digest the anonymised text is stale.",
  "anon_status": "Outcome of the anonymisation run for this row's free text (anonymized when it succeeded).",
  "anon_value_string": "Anonymised version of value_string: identifiers detected in the free text are replaced by placeholders; published alongside the source column, which gold releases only when anonymisation succeeded."
 },
 "map_location_unit": {
  "ACTIVE_IND": "Indicator of whether this bed record is currently active.",
  "ADC_UPDT": "When bronze last wrote the row.",
  "BEG_EFFECTIVE_DT_TM": "Date and time from which this bed record is effective.",
  "BUILDING_CD": "Millennium code value identifying the building to which the location unit belongs, as sourced from the Millennium location hierarchy.",
  "CLASS_CD": "Millennium code value for the class assigned to the room.",
  "CURRENT_BED_STATUS_CD": "Code value indicating the current status of the bed location, as sourced from the Millennium bed table.",
  "CURRENT_BED_STATUS_DISPLAY": "Display text of the current bed status for the location unit, taken from the Millennium code value lookup.",
  "DUP_BED_IND": "Indicator flagging whether this bed is a duplicate bed.",
  "END_EFFECTIVE_DT_TM": "Date and time at which this bed record ceases to be effective.",
  "FACILITY_CD": "Millennium code value identifying the facility to which the location unit belongs, as sourced from the Millennium location hierarchy.",
  "FIXED_BED_IND": "Indicator showing whether the room is flagged as having fixed beds.",
  "LOCATION_CD": "Code value identifying the bed location that this row describes.",
  "LOCATION_DISPLAY": "Display name of the location unit, taken from the Millennium code value lookup.",
  "LOCATION_LEVEL": "Hierarchy level or type of the location unit, derived from the Millennium code value CDF meaning.",
  "NURSE_UNIT_CD": "Millennium code value identifying the nurse unit (ward) associated with the location unit, as sourced from the Millennium location hierarchy.",
  "NURSE_UNIT_DISPLAY": "Display name of the nurse unit (ward) associated with the location, taken from the Millennium code value lookup.",
  "PARENT_LOCATION_CD": "Millennium code value of the location directly above this location unit in the Millennium location hierarchy, such as the room that contains a bed.",
  "PARENT_LOCATION_DISPLAY": "Display name of the parent location in the location hierarchy, taken from the Millennium code value lookup.",
  "PIPELINE_RUN_ID": "Bronze pipeline run that last wrote the row.",
  "ROW_HASH": "SHA-256 of the row's payload columns, used by the bronze MERGE to detect changes.",
  "SOURCE_ABSENT_DETECTED_TS": "When bronze first found the row absent from the source snapshot; NULL while present.",
  "SOURCE_ADC_UPDT": "When the source row was landed in raw (raw ADC_UPDT).",
  "SOURCE_PRESENT_IND": "True while the row is present in the latest full source snapshot.",
  "SOURCE_ROW_ID": "Native source row identifier, as text.",
  "SOURCE_TABLE": "Bronze or raw source table the row came from."
 },
 "map_pending_movement": {
  "ACCOMMODATION_CD": "Code value identifying the accommodation type associated with the pending encounter action.",
  "ACCOMMODATION_DISPLAY": "Display text of the accommodation code for the pending patient movement, resolved from the Millennium code value lookup.",
  "ACCOMMODATION_REASON_CD": "Code value identifying the reason for the accommodation associated with the pending encounter action.",
  "ACTIVE_IND": "Indicator showing whether the pending encounter record is currently active in Cerner Millennium.",
  "ADC_UPDT": "When bronze last wrote the row.",
  "ALC_DECOMP_DT_TM": "Date and time associated with the alternate level of care decompensation for the pending encounter action.",
  "ALC_REASON_CD": "Code value identifying the reason for the alternate level of care designation.",
  "ALT_LVL_CARE_CD": "Code value identifying the alternate level of care associated with the pending encounter action.",
  "ALT_LVL_CARE_DT_TM": "Date and time associated with the alternate level of care designation for the pending encounter action.",
  "ATTENDDOC_ID": "Internal personnel identifier of the attending clinician associated with the pending encounter action.",
  "BEG_EFFECTIVE_DT_TM": "Date and time from which the pending encounter record is effective.",
  "DISCH_DISPOSITION_CD": "Code value identifying the discharge disposition recorded for the pending encounter action.",
  "DISCH_DISPOSITION_DISPLAY": "Display text of the discharge disposition code for the pending movement, resolved from the Millennium code value lookup.",
  "DISCH_TO_LOCTN_CD": "Code value identifying the location the patient is to be discharged to for the pending encounter action.",
  "DISCH_TO_LOCTN_DISPLAY": "Display text of the discharge-to location code for the pending movement, resolved from the Millennium code value lookup.",
  "ENCNTR_ID": "Identifier of the encounter to which the pending action relates, referencing the Millennium ENCOUNTER table.",
  "ENCNTR_PENDING_ID": "Unique system-generated identifier for each pending encounter action record in the ENCNTR_PENDING table.",
  "ENCNTR_TYPE_CD": "Code value identifying the encounter type associated with the pending encounter action.",
  "ENCNTR_TYPE_DISPLAY": "Display text of the encounter type code associated with the pending movement, resolved from the Millennium code value lookup.",
  "END_EFFECTIVE_DT_TM": "Millennium END_EFFECTIVE_DT_TM of the pending movement; 2100-12-31 is the source marker for a request that is still open and is kept on purpose (J14). Use end_effective_dt_tm_clean for a NULL-for-open version.",
  "END_EFFECTIVE_DT_TM_CLEAN": "Cleaned end-effective date and time of the pending encounter movement record, derived from the Millennium encounter-pending end-effective timestamp marking when the record ceases to be valid.",
  "EST_COMPLETE_DT_TM": "Estimated date and time at which the pending encounter action is expected to be completed.",
  "ISOLATION_CD": "Code value identifying the isolation requirement associated with the pending encounter action.",
  "ISOLATION_DISPLAY": "Display text of the isolation code recorded for the pending movement, resolved from the Millennium code value lookup.",
  "MED_SERVICE_CD": "Code value identifying the medical service associated with the pending encounter action.",
  "MED_SERVICE_DISPLAY": "Display text of the medical service code for the pending movement, resolved from the Millennium code value lookup.",
  "ORGANIZATION_ID": "Internal Millennium organisation identifier (facility or client organisation) attributed to the pending encounter record during loading, derived via encounter-to-organisation mapping from the linked encounter.",
  "PENDING_DT_TM": "Date and time associated with the pending encounter action.",
  "PENDING_PRIORITY_CD": "Code value identifying the priority of the pending encounter action.",
  "PENDING_PRIORITY_DISPLAY": "Display text of the priority code assigned to the pending movement, resolved from the Millennium code value lookup.",
  "PENDING_PRSNL_ID": "Identifier of the personnel record associated with the pending encounter action.",
  "PENDING_STATUS_CD": "Code value identifying the status of the pending encounter action.",
  "PENDING_STATUS_DISPLAY": "Display text of the status code of the pending movement, resolved from the Millennium code value lookup.",
  "PENDING_TYPE_FLAG": "Numeric flag indicating the type of pending encounter action.",
  "PEND_BED_CD": "Location code value identifying the bed associated with the pending encounter action.",
  "PEND_BED_DISPLAY": "Display text of the bed location code for the pending movement, resolved from the Millennium code value lookup.",
  "PEND_BUILDING_CD": "Location code value identifying the building associated with the pending encounter action.",
  "PEND_BUILDING_DISPLAY": "Display text of the building location code for the pending movement, resolved from the Millennium code value lookup.",
  "PEND_FACILITY_CD": "Location code value identifying the facility associated with the pending encounter action.",
  "PEND_FACILITY_DISPLAY": "Display text of the facility location code for the pending movement, resolved from the Millennium code value lookup.",
  "PEND_NURSE_UNIT_CD": "Location code value identifying the nurse unit associated with the pending encounter action.",
  "PEND_NURSE_UNIT_DISPLAY": "Display text of the nurse unit location code for the pending movement, resolved from the Millennium code value lookup.",
  "PEND_ROOM_CD": "Location code value identifying the room associated with the pending encounter action.",
  "PEND_ROOM_DISPLAY": "Display text of the room location code for the pending movement, resolved from the Millennium code value lookup.",
  "PERSON_ID": "Person whom this encounter is for.",
  "PIPELINE_RUN_ID": "Bronze pipeline run that last wrote the row.",
  "PREV_EST_DEPART_DT_TM": "Previously recorded estimated departure date and time for the encounter.",
  "PRIORITY_SEQ": "Sequence number indicating the priority ordering of the pending encounter action.",
  "PROCESS_STATUS_DT_TM": "Date and time the process status of the pending encounter action was last set.",
  "PROCESS_STATUS_FLAG": "Numeric flag indicating the processing status of the pending encounter action.",
  "ROW_HASH": "SHA-256 of the row's payload columns, used by the bronze MERGE to detect changes.",
  "SOURCE_ABSENT_DETECTED_TS": "When bronze first found the row absent from the source snapshot; NULL while present.",
  "SOURCE_ADC_UPDT": "When the source row was landed in raw (raw ADC_UPDT).",
  "SOURCE_PRESENT_IND": "True while the row is present in the latest full source snapshot.",
  "SOURCE_ROW_ID": "Native source row identifier, as text.",
  "SOURCE_TABLE": "Bronze or raw source table the row came from.",
  "SPECIALTY_UNIT_CD": "Code value identifying the specialty unit associated with the pending encounter action.",
  "TRANSACTION_REASON": "Free-text description of the reason for the transaction associated with the pending encounter action.",
  "TRANSACTION_REASON_CD": "Code value identifying the reason for the transaction associated with the pending encounter action.",
  "TRANSACTION_REASON_DISPLAY": "Display text of the transaction reason code for the pending movement, resolved from the Millennium code value lookup.",
  "TRUST": "Name of the NHS Trust to which the record is attributed, derived during loading via encounter-to-organisation mapping.",
  "anon_context_fingerprint": "Hash of the contextual inputs the redactor used for this row; the anonymised text is released only while it matches the current context.",
  "anon_identity_fingerprint": "Hash of the patient identity details the record-driven redactor used for this row.",
  "anon_processed_at": "When the redactor last processed this row's text.",
  "anon_redaction_count": "Number of redactions the redactor applied to the source text.",
  "anon_redactor_version": "Version of the redactor that produced the anonymised text.",
  "anon_source_text_sha": "SHA-256 digest of the text the redactor processed; when it no longer matches the current input digest the anonymised text is stale.",
  "anon_status": "Outcome of the anonymisation run for this row's free text (anonymized when it succeeded).",
  "anon_transaction_reason": "Anonymised version of transaction_reason: identifiers detected in the free text are replaced by placeholders; published alongside the source column, which gold releases only when anonymisation succeeded."
 },
 "map_prearrival": {
  "ACTIVE_IND": "Indicator showing whether the pre-arrival record is active.",
  "ADC_UPDT": "When bronze last wrote the row.",
  "AGE_TXT": "Free-text age of the expected patient as recorded at pre-arrival.",
  "ATTACHED_ENCNTR_ID": "Identifier of the encounter to which the pre-arrival record was later attached.",
  "ATTACHED_PERSON_ID": "Identifier of the person to whom the pre-arrival record was later attached.",
  "BIRTH_DT_TM": "Date and time of birth recorded for the expected patient.",
  "CHIEF_COMPLAINT": "Free-text chief complaint or presenting problem recorded for the expected patient.",
  "CREATE_DT_TM": "Date and time the pre-arrival record was created.",
  "ESTIMATED_ARRIVE_DT_TM": "Estimated date and time of the expected patient's arrival.",
  "FIRST_NAME": "Free-text first name entered for the expected patient on the pre-arrival record.",
  "LAST_NAME": "Free-text last name entered for the expected patient on the pre-arrival record.",
  "LINKAGE_ROUTE": "How the row was linked to its person or encounter.",
  "LINKED_ENCNTR_ID": "Internal surrogate identifier of the encounter to which the pre-arrival tracking record has been attached, sourced from the Millennium tracking pre-arrival attached encounter ID.",
  "LINKED_PERSON_ID": "Internal surrogate identifier of the person to whom the pre-arrival tracking record has been attached, sourced from the Millennium tracking pre-arrival attached person ID.",
  "ORGANIZATION_ID": "Pipeline-added internal organisation identifier (facility or client organisation of the attached encounter) used to map the record to a trust.",
  "PCP_PROVIDER_ID": "Identifier of the expected patient's primary care provider recorded on the pre-arrival record.",
  "PCP_PROVIDER_NAME": "Name of the expected patient's primary care provider recorded on the pre-arrival record.",
  "PIPELINE_RUN_ID": "Bronze pipeline run that last wrote the row.",
  "PREARRIVAL_TYPE_CD": "Code value identifying the type of pre-arrival record.",
  "PREARRIVAL_TYPE_DISPLAY": "Display text describing the type of pre-arrival notification, resolved from the Millennium code value lookup.",
  "REFERRING_SOURCE_ID": "Identifier of the referring source associated with the pre-arrival record.",
  "REFERRING_SOURCE_NAME": "Name of the referring source recorded on the pre-arrival record.",
  "REG_PRSNL_ID": "Identifier of the personnel member who registered the pre-arrival record.",
  "REG_PRSNL_NAME": "Name of the personnel member who registered the pre-arrival record.",
  "ROW_HASH": "SHA-256 of the row's payload columns, used by the bronze MERGE to detect changes.",
  "SEX_CD": "Code value for the recorded sex of the expected patient.",
  "SEX_DISPLAY": "Display text for the sex recorded on the pre-arrival record, resolved from the Millennium code value lookup.",
  "SOURCE_ABSENT_DETECTED_TS": "When bronze first found the row absent from the source snapshot; NULL while present.",
  "SOURCE_ADC_UPDT": "When the source row was landed in raw (raw ADC_UPDT).",
  "SOURCE_PRESENT_IND": "True while the row is present in the latest full source snapshot.",
  "SOURCE_ROW_ID": "Native source row identifier, as text.",
  "SOURCE_TABLE": "Bronze or raw source table the row came from.",
  "TRACKING_GROUP_CD": "Code value identifying the tracking group to which the pre-arrival record is assigned.",
  "TRACKING_GROUP_DISPLAY": "Display text for the tracking group associated with the pre-arrival record, resolved from the Millennium code value lookup.",
  "TRACKING_PREARRIVAL_ID": "Internal Millennium surrogate identifier uniquely identifying each pre-arrival tracking record.",
  "TRUST": "Pipeline-derived NHS trust name attributed to the pre-arrival record via organisation-to-trust mapping, null where no mapping was resolved.",
  "anon_chief_complaint": "Anonymised version of chief_complaint: identifiers detected in the free text are replaced by placeholders; published alongside the source column, which gold releases only when anonymisation succeeded.",
  "anon_context_fingerprint": "Hash of the contextual inputs the redactor used for this row; the anonymised text is released only while it matches the current context.",
  "anon_identity_fingerprint": "Hash of the patient identity details the record-driven redactor used for this row.",
  "anon_processed_at": "When the redactor last processed this row's text.",
  "anon_redaction_count": "Number of redactions the redactor applied to the source text.",
  "anon_redactor_version": "Version of the redactor that produced the anonymised text.",
  "anon_source_text_sha": "SHA-256 digest of the text the redactor processed; when it no longer matches the current input digest the anonymised text is stale.",
  "anon_status": "Outcome of the anonymisation run for this row's free text (anonymized when it succeeded)."
 },
 "map_tracking_attendance": {
  "ADC_UPDT": "When bronze last wrote the row.",
  "ATTENDANCE_OPEN_IND": "Boolean flag showing whether the tracking attendance is still open, derived from the absence of a check-out date and time on the underlying Millennium tracking check-in record.",
  "ATTENDANCE_SEQUENCE": "Integer ordinal position of the tracking attendance, assigned by ordering on check-in date and time with the tracking check-in identifier as tie-breaker.",
  "CHECKIN_DT_TM": "Date and time at which the tracked item was checked in.",
  "CHECKIN_ID": "Identifier of the personnel member who performed the check-in.",
  "CHECKOUT_DISPOSITION_CD": "Millennium code value identifying the disposition recorded at check-out from tracking.",
  "CHECKOUT_DISPOSITION_DISPLAY": "Display text for the checkout disposition recorded for the tracking attendance.",
  "CHECKOUT_DT_TM": "Date and time at which the tracked item was checked out.",
  "CHECKOUT_DT_TM_CLEAN": "Cleaned check-out date and time for the tracking attendance, derived from the Millennium tracking check-in check-out timestamp.",
  "CHECKOUT_ID": "Identifier of the personnel member who performed the check-out.",
  "ENCNTR_ID": "Millennium encounter identifier for the encounter associated with the tracking item, which may be populated from an encounter value resolved during ingestion.",
  "FAMILY_PRESENT_CD": "Code value indicating whether family was present for the tracked check-in.",
  "FAMILY_PRESENT_DISPLAY": "Display text indicating whether family was present during the tracking attendance.",
  "LINKAGE_ROUTE": "How the row was linked to its person or encounter.",
  "ORGANIZATION_ID": "Millennium organisation identifier added by the landing process and used to resolve Trust attribution.",
  "PERSON_ID": "Millennium person identifier for the person associated with the tracking item, where the tracked item references a person.",
  "PIPELINE_RUN_ID": "Bronze pipeline run that last wrote the row.",
  "PRIMARY_DOC_ID": "Identifier of the primary doctor assigned to the tracked check-in.",
  "PRIMARY_NURSE_ID": "Identifier of the primary nurse assigned to the tracked check-in.",
  "REACTIVATE_USER_ID": "Identifier of the Millennium user who reactivated the tracking check-in.",
  "REACTIVATION_DT_TM": "Date and time at which the tracking check-in was reactivated.",
  "REGISTRATION_STATUS_ID": "Identifier of the registration status associated with the tracked check-in.",
  "ROW_HASH": "SHA-256 of the row's payload columns, used by the bronze MERGE to detect changes.",
  "SECONDARY_DOC_ID": "Identifier of the secondary doctor assigned to the tracked check-in.",
  "SECONDARY_NURSE_ID": "Identifier of the secondary nurse assigned to the tracked check-in.",
  "SOURCE_ABSENT_DETECTED_TS": "When bronze first found the row absent from the source snapshot; NULL while present.",
  "SOURCE_ADC_UPDT": "When the source row was landed in raw (raw ADC_UPDT).",
  "SOURCE_PRESENT_IND": "True while the row is present in the latest full source snapshot.",
  "SOURCE_ROW_ID": "Native source row identifier, as text.",
  "SOURCE_TABLE": "Bronze or raw source table the row came from.",
  "SPECIALTY_ID": "Identifier of the specialty assigned to the tracked check-in.",
  "SURG_CASE_ID": "Identifier of the surgical case associated with the tracking attendance, carried through from the silver tracking attendance data and populated only for a minority of rows.",
  "TEAM_ID": "Identifier of the care team assigned to the tracked check-in.",
  "TRACKING_CHECKIN_ID": "Unique Millennium identifier for the tracking check-in record and primary key of the TRACKING_CHECKIN table.",
  "TRACKING_EVENT_TYPE_CD": "Code value identifying the type of tracking event associated with the check-in.",
  "TRACKING_GROUP_CD": "Code value identifying the tracking group in which the check-in was recorded.",
  "TRACKING_GROUP_DISPLAY": "Display text for the tracking group associated with the tracking attendance.",
  "TRACKING_ID": "Identifier of the parent tracking record to which this check-in belongs.",
  "TRAUMA_IND": "Indicator flag recording whether the tracked check-in is flagged as trauma.",
  "TRUST": "NHS Trust attributed to the record by the landing process via organisation-to-trust mapping."
 },
 "map_tracking_episode": {
  "ADC_UPDT": "When bronze last wrote the row.",
  "BASE_LOC_CD": "Millennium location code value for the base location of the tracking item.",
  "BASE_LOC_DISPLAY": "Display name of the base location associated with the clinical tracking episode, resolved from the Millennium code value lookup.",
  "BASE_LOC_DT_TM": "Date and time associated with the base location assignment of the tracking item.",
  "CUR_TRACKING_LOCATOR_ID": "Identifier of the current tracking locator record for the tracking item.",
  "ENCNTR_ID": "Millennium encounter identifier for the encounter associated with the tracking item, which may be populated from an encounter value resolved during ingestion.",
  "END_TRACKING_DT_TM": "Date and time at which tracking of the item ended.",
  "END_TRACKING_DT_TM_CLEAN": "Cleaned date and time at which tracking of the item ended, derived from the Millennium tracking item end tracking timestamp.",
  "ITEM_ACTIVE_IND": "Boolean indicator showing whether the underlying Millennium tracking item record is active.",
  "LINKAGE_ROUTE": "How the row was linked to its person or encounter.",
  "ORGANIZATION_ID": "Millennium organisation surrogate identifier resolved during ingestion, partly via encounter data, and used to derive the Trust.",
  "PARENT_ENTITY_ID": "Identifier of the parent entity record, in the table named by PARENT_ENTITY_NAME, associated with the tracking item.",
  "PARENT_ENTITY_NAME": "Name of the parent entity table referenced by PARENT_ENTITY_ID for the tracking item.",
  "PERSON_ID": "Millennium person identifier for the person associated with the tracking item, where the tracked item references a person.",
  "PIPELINE_RUN_ID": "Bronze pipeline run that last wrote the row.",
  "ROW_HASH": "SHA-256 of the row's payload columns, used by the bronze MERGE to detect changes.",
  "SOURCE_ABSENT_DETECTED_TS": "When bronze first found the row absent from the source snapshot; NULL while present.",
  "SOURCE_ADC_UPDT": "When the source row was landed in raw (raw ADC_UPDT).",
  "SOURCE_PRESENT_IND": "True while the row is present in the latest full source snapshot.",
  "SOURCE_ROW_ID": "Native source row identifier, as text.",
  "SOURCE_TABLE": "Bronze or raw source table the row came from.",
  "START_TRACKING_DT_TM": "Date and time at which tracking of the item started.",
  "SURG_CASE_ID": "The primary key, uniquely identifying a surgical case",
  "TRACKING_ID": "Millennium identifier for the tracking item record, appearing to uniquely identify each row in TRACKING_ITEM.",
  "TRACKING_OPEN_IND": "Boolean indicator showing whether the tracking episode is still open, derived from the tracking item end tracking date and time.",
  "TRACKING_PREARRIVAL_ID": "Internal surrogate identifier of the tracking pre-arrival record linked to the episode, derived from the tracking item parent entity reference.",
  "TRACKING_STATUS_FLAG": "Numeric flag indicating the tracking status of the item.",
  "TRACKING_TYPE_FLAG": "Numeric flag indicating the type of tracking item, such as whether it relates to a person, encounter, inventory item or personnel member.",
  "TRUST": "NHS trust name or code assigned at ingestion by mapping the row's organisation to a trust, partly resolved via encounter data."
 },
 "map_tracking_location_stay": {
  "ACTIVE_EVENTS_IND": "Indicator flag denoting whether active tracking events exist for this locator entry.",
  "ADC_UPDT": "When bronze last wrote the row.",
  "ARRIVE_DT_TM": "Date and time of arrival at the location recorded on this tracking locator entry.",
  "DEPART_DT_TM": "Date and time of departure from the location recorded on this tracking locator entry.",
  "DEPART_DT_TM_CLEAN": "Cleaned date and time the tracked patient departed the tracking location, derived from the Millennium tracking locator departure timestamp.",
  "ENCNTR_ID": "Millennium encounter identifier for the encounter associated with the tracking item, which may be populated from an encounter value resolved during ingestion.",
  "LINKAGE_ROUTE": "How the row was linked to its person or encounter.",
  "LOCATION_CD": "Millennium code value identifying the tracked location for this locator entry.",
  "LOCATION_DISPLAY": "Display name of the tracked location for the location stay, resolved from the Millennium code value lookup.",
  "LOC_BED_CD": "Millennium code value identifying the bed location for this locator entry.",
  "LOC_BED_DISPLAY": "Display name of the bed associated with the tracked location stay, resolved from the Millennium code value lookup.",
  "LOC_NURSE_UNIT_CD": "Millennium code value identifying the nurse unit location for this locator entry.",
  "LOC_NURSE_UNIT_DISPLAY": "Display name of the nurse unit (ward) associated with the tracked location stay, resolved from the Millennium code value lookup.",
  "LOC_ROOM_CD": "Millennium code value identifying the room location for this locator entry.",
  "LOC_ROOM_DISPLAY": "Display name of the room associated with the tracked location stay, resolved from the Millennium code value lookup.",
  "ORGANIZATION_ID": "Pipeline-added Millennium organisation identifier resolved during landing and used to map the row to a Trust.",
  "PERSON_ID": "Millennium person identifier for the person associated with the tracking item, where the tracked item references a person.",
  "PIPELINE_RUN_ID": "Bronze pipeline run that last wrote the row.",
  "RANK_SEQUENCE": "Numeric sequence indicating the rank or ordering of this locator entry.",
  "ROW_HASH": "SHA-256 of the row's payload columns, used by the bronze MERGE to detect changes.",
  "SCHEDULED_DT_TM": "Date and time at which the tracked item was scheduled to be at this location.",
  "SOURCE_ABSENT_DETECTED_TS": "When bronze first found the row absent from the source snapshot; NULL while present.",
  "SOURCE_ADC_UPDT": "When the source row was landed in raw (raw ADC_UPDT).",
  "SOURCE_PRESENT_IND": "True while the row is present in the latest full source snapshot.",
  "SOURCE_ROW_ID": "Native source row identifier, as text.",
  "SOURCE_TABLE": "Bronze or raw source table the row came from.",
  "STAY_OPEN_IND": "Boolean flag indicating whether the tracking location stay is still open, derived from the absence or state of the departure timestamp.",
  "SURG_CASE_ID": "Identifier of the surgical case associated with this tracking location stay, sourced from the tracking location stay and tracking episode records.",
  "TRACKING_ACUITY_LEVEL_ID": "Internal Millennium identifier of the tracking acuity level associated with this locator entry.",
  "TRACKING_ID": "Internal Millennium identifier of the tracking record to which this locator entry belongs.",
  "TRACKING_LOCATOR_ID": "Internal Millennium identifier that uniquely identifies each tracking locator entry.",
  "TRACKING_REASON_CD": "Millennium code value for the tracking reason recorded on this locator entry.",
  "TRACKING_REASON_COMMENT": "Free-text comment describing the tracking reason for this locator entry.",
  "TRACKING_REASON_DISPLAY": "Display text of the reason recorded for the tracking location stay, resolved from the Millennium code value lookup.",
  "TRUST": "Pipeline-added NHS Trust attribution for the row, derived during landing through organisation and encounter mapping.",
  "UNAVAIL_TRACKING_EVENT_ID": "Internal Millennium identifier of the tracking event associated with unavailability for this locator entry.",
  "anon_context_fingerprint": "Hash of the contextual inputs the redactor used for this row; the anonymised text is released only while it matches the current context.",
  "anon_identity_fingerprint": "Hash of the patient identity details the record-driven redactor used for this row.",
  "anon_processed_at": "When the redactor last processed this row's text.",
  "anon_redaction_count": "Number of redactions the redactor applied to the source text.",
  "anon_redactor_version": "Version of the redactor that produced the anonymised text.",
  "anon_source_text_sha": "SHA-256 digest of the text the redactor processed; when it no longer matches the current input digest the anonymised text is stale.",
  "anon_status": "Outcome of the anonymisation run for this row's free text (anonymized when it succeeded).",
  "anon_tracking_reason_comment": "Anonymised version of tracking_reason_comment: identifiers detected in the free text are replaced by placeholders; published alongside the source column, which gold releases only when anonymisation succeeded."
 },
 "map_tracking_milestone": {
  "ADC_UPDT": "When bronze last wrote the row.",
  "CLINICAL_EVENT_CD": "Code value identifying the clinical event associated with the tracking event.",
  "COMPLETE_DT_TM": "Date and time the tracking event was completed.",
  "COMPLETE_ID": "Identifier of the personnel associated with completing the tracking event.",
  "CRITICAL_INTERVAL": "Interval after which the tracking event is treated as critical.",
  "ENCNTR_ID": "Millennium encounter identifier for the encounter associated with the tracking item, which may be populated from an encounter value resolved during ingestion.",
  "EVENT_STATUS_CD": "Code value representing the status of the tracking event.",
  "EVENT_STATUS_DISPLAY": "Display text for the status of the tracking milestone event, resolved from the Millennium code value lookup.",
  "LINKAGE_ROUTE": "How the row was linked to its person or encounter.",
  "MILESTONE_CLASS": "Classification of the clinical tracking milestone, derived from the tracking milestone mapping using the Millennium track event display name.",
  "MILESTONE_PHASE": "Phase of the clinical tracking pathway that the milestone belongs to, derived from the tracking milestone mapping using the Millennium track event display name.",
  "MILESTONE_RULE_ID": "Identifier of the mapping rule used to assign the track event to this milestone, derived from the tracking milestone mapping.",
  "MILESTONE_RULE_VERSION": "Version identifier of the milestone rule applied to the tracking event, taken directly from the source Millennium tracking milestone record.",
  "ONSET_DT_TM": "Date and time of onset (start) of the tracking event.",
  "ONSET_ID": "Identifier of the personnel associated with the onset of the tracking event.",
  "ORGANIZATION_ID": "Landing-added Millennium organisation identifier resolved for the tracking event, used for organisation-to-trust mapping.",
  "OVERDUE_INTERVAL": "Interval after which the tracking event is treated as overdue.",
  "PERSON_ID": "Millennium person identifier for the person associated with the tracking item, where the tracked item references a person.",
  "PIPELINE_RUN_ID": "Bronze pipeline run that last wrote the row.",
  "REQUESTED_DT_TM": "Date and time the tracking event was requested.",
  "REQUESTED_ID": "Identifier of the personnel associated with requesting the tracking event.",
  "ROW_HASH": "SHA-256 of the row's payload columns, used by the bronze MERGE to detect changes.",
  "SOURCE_ABSENT_DETECTED_TS": "When bronze first found the row absent from the source snapshot; NULL while present.",
  "SOURCE_ADC_UPDT": "When the source row was landed in raw (raw ADC_UPDT).",
  "SOURCE_PRESENT_IND": "True while the row is present in the latest full source snapshot.",
  "SOURCE_ROW_ID": "Native source row identifier, as text.",
  "SOURCE_TABLE": "Bronze or raw source table the row came from.",
  "SURG_CASE_ID": "Internal surrogate identifier of the surgical case associated with the clinical tracking milestone, populated only for surgery-related milestones.",
  "TRACKING_EVENT_ID": "Unique Millennium identifier for the tracking event row in the TRACKING_EVENT table.",
  "TRACKING_EVENT_TYPE_CD": "Code value identifying the type of tracking event.",
  "TRACKING_EVENT_TYPE_DISPLAY": "Display text for the type of tracking event associated with the milestone, resolved from the Millennium code value lookup.",
  "TRACKING_GROUP_CD": "Code value identifying the tracking group associated with the tracking event.",
  "TRACKING_GROUP_DISPLAY": "Display text for the tracking group to which the milestone belongs, resolved from the Millennium code value lookup.",
  "TRACKING_ID": "Identifier of the tracking record to which this tracking event belongs.",
  "TRACK_EVENT_DESCRIPTION": "Description of the Millennium tracking event associated with the milestone.",
  "TRACK_EVENT_DISPLAY": "Display name of the Millennium tracking event associated with the milestone.",
  "TRACK_EVENT_ID": "Identifier of the track event definition that this tracking event instantiates.",
  "TRUST": "Landing-added NHS trust name or label resolved for the row through encounter-to-organisation and organisation-to-trust mapping."
 }
}
INSR_W8_BRONZE_TABLE_COMMENTS = {
 "map_location_attribute_history": "One row per recorded change of a Millennium location attribute, keyed by the source attribute row id. Curated bronze input to silver reference_location_attribute_history; source-faithful columns, decodes in silver.",
 "map_location_unit": "One row per Millennium location (bed, room, unit or ward), keyed by LOCATION_CD. Curated bronze input to silver reference_location_unit; source-faithful columns, decodes in silver.",
 "map_pending_movement": "One row per Millennium pending encounter movement (bed request or transfer awaiting completion), keyed by ENCNTR_PENDING_ID. Curated bronze input to silver clinical_pending_movement; source-faithful columns, decodes in silver.",
 "map_prearrival": "One row per FirstNet pre-arrival notice (expected patient), keyed by TRACKING_PREARRIVAL_ID. Curated bronze input to silver clinical_prearrival; source-faithful columns, decodes in silver.",
 "map_tracking_attendance": "One row per FirstNet tracking check-in, keyed by Millennium TRACKING_CHECKIN_ID. Curated bronze input to silver clinical_tracking_attendance; source-faithful columns, decodes in silver.",
 "map_tracking_episode": "One row per FirstNet (emergency department) tracking episode, keyed by Millennium TRACKING_ID. Curated bronze input to silver clinical_tracking_episode; source-faithful columns, decodes in silver.",
 "map_tracking_location_stay": "One row per FirstNet tracking location stay (arrival to departure at one tracked location), keyed by TRACKING_LOCATOR_ID. Curated bronze input to silver clinical_tracking_location_stay; source-faithful columns, decodes in silver.",
 "map_tracking_milestone": "One row per FirstNet tracking event (milestone such as triage or seen by clinician), keyed by TRACKING_EVENT_ID. Curated bronze input to silver clinical_tracking_milestone; source-faithful columns, decodes in silver."
}


def _insr_w8_sql(text):
    """Escape a comment for a single-quoted SQL literal."""
    return text.replace("\\", "\\\\").replace("'", "\\'")


def _insr_w8_comment(table, column):
    """Dictionary comment for one bronze column; KeyError for a column the dictionary does not know."""
    return INSR_W8_BRONZE_COMMENTS[table.split('.')[-1]][column]


def _insr_w8_assert_covered(table, columns):
    """Refuse to publish a table with a column the comment dictionary does not cover (checked before any write)."""
    known = INSR_W8_BRONZE_COMMENTS[table.split('.')[-1]]
    missing = [c for c in columns if c not in known]
    if missing:
        raise KeyError(f"INSR_W8_COMMENTS_V1: {table} columns missing from the comment dictionary: {missing}")


def apply_governance(table):
    catalog, schema, name = table.split(".")
    spark.sql(f"COMMENT ON TABLE {qname(table)} IS '{_insr_w8_sql(INSR_W8_BRONZE_TABLE_COMMENTS[table.split('.')[-1]])}'")
    # INSR_W1_TAG_HEURISTIC_RETIRED_V1: ig_risk/ig_severity come from Table Metadata Compliance (estate-copied), never from column names.
    for col in spark.table(table).columns:
        uc = col.upper()
        comment = _insr_w8_sql(_insr_w8_comment(table, col))
        spark.sql(f"ALTER TABLE {qname(table)} ALTER COLUMN `{col.replace('`','``')}` COMMENT '{comment}'")


# INSR_W1_TRACKING_CLEAN_V1: one rule for every *_CLEAN timestamp. At or after 2100-01-01 is Millennium's open
# sentinel (the row is still open; the *_OPEN_IND flags keep that meaning); before 1901-01-01 is a placeholder, not a
# time (58 values in the 2026-10 data). Both become NULL; the raw column beside it is untouched.
TRACKING_OPEN_SENTINEL = "2100-01-01"
TRACKING_CLEAN_FLOOR = "1901-01-01"


def _clean_tracking_ts(column):
    """Return the timestamp, or NULL when it is the open sentinel (>= 2100) or a pre-1901 placeholder."""
    stamped = column.cast("timestamp")
    usable = (stamped < F.lit(TRACKING_OPEN_SENTINEL).cast("timestamp")) & (stamped >= F.lit(TRACKING_CLEAN_FLOOR).cast("timestamp"))
    return F.when(usable, stamped)


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
        "START_TRACKING_DT_TM", "END_TRACKING_DT_TM", _clean_tracking_ts(F.col("i.END_TRACKING_DT_TM")).alias("END_TRACKING_DT_TM_CLEAN"), open_ind.alias("TRACKING_OPEN_IND"),
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
        ).write.mode("append").saveAsTable("6_mgmt.bronze.tracking_pipeline_errors")  # OGR_NO_DEV_V1/tracking
        raise
episode_keys = spark.table(EPISODE).where("SOURCE_PRESENT_IND").select("TRACKING_ID", "PERSON_ID", "ENCNTR_ID", "LINKAGE_ROUTE", "SURG_CASE_ID")

# COMMAND ----------

try:
    checkin = spark.table(SRC_CHECKIN).alias("s").join(F.broadcast(episode_keys.alias("e")), "TRACKING_ID", "inner")
    checkout_open = F.col("s.CHECKOUT_DT_TM") >= F.lit("2100-01-01").cast("timestamp")
    attendance = checkin.select(
        F.col("s.TRACKING_CHECKIN_ID").cast("long").alias("TRACKING_CHECKIN_ID"), F.col("TRACKING_ID").cast("long"),
        F.col("e.PERSON_ID").alias("PERSON_ID"), F.col("e.ENCNTR_ID").alias("ENCNTR_ID"), F.col("e.LINKAGE_ROUTE").alias("LINKAGE_ROUTE"), F.col("e.SURG_CASE_ID").alias("SURG_CASE_ID"),
        "CHECKIN_DT_TM", "CHECKOUT_DT_TM", _clean_tracking_ts(F.col("s.CHECKOUT_DT_TM")).alias("CHECKOUT_DT_TM_CLEAN"), checkout_open.alias("ATTENDANCE_OPEN_IND"),
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
    ).write.mode("append").saveAsTable("6_mgmt.bronze.tracking_pipeline_errors")
    raise

# COMMAND ----------

locator = spark.table(SRC_LOCATOR).alias("s").join(F.broadcast(episode_keys.alias("e")), "TRACKING_ID", "inner")
stay_open = F.col("s.DEPART_DT_TM") >= F.lit("2100-01-01").cast("timestamp")
stay = locator.select(
    F.col("s.TRACKING_LOCATOR_ID").cast("long").alias("TRACKING_LOCATOR_ID"), F.col("TRACKING_ID").cast("long"),
    F.col("e.PERSON_ID").alias("PERSON_ID"), F.col("e.ENCNTR_ID").alias("ENCNTR_ID"), F.col("e.LINKAGE_ROUTE").alias("LINKAGE_ROUTE"), F.col("e.SURG_CASE_ID").alias("SURG_CASE_ID"),
    *[F.col(f"s.{c}").cast("long").alias(c) for c in ["LOCATION_CD", "LOC_NURSE_UNIT_CD", "LOC_ROOM_CD", "LOC_BED_CD", "RANK_SEQUENCE", "TRACKING_REASON_CD", "UNAVAIL_TRACKING_EVENT_ID", "TRACKING_ACUITY_LEVEL_ID", "ORGANIZATION_ID"]],
    "ARRIVE_DT_TM", "DEPART_DT_TM", _clean_tracking_ts(F.col("s.DEPART_DT_TM")).alias("DEPART_DT_TM_CLEAN"), stay_open.alias("STAY_OPEN_IND"), "SCHEDULED_DT_TM",
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
    ).write.mode("append").saveAsTable("6_mgmt.bronze.tracking_pipeline_errors")
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
        _clean_tracking_ts(F.col("s.END_EFFECTIVE_DT_TM")).alias("END_EFFECTIVE_DT_TM_CLEAN"),
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
    ).write.mode("append").saveAsTable("6_mgmt.bronze.tracking_pipeline_errors")
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
# SDI_LOCATION_UNIT_ALL_V1: the nurse-unit/room/bed arms miss every other Millennium location a patient is booked or
# tracked at (theatres/ancillary, radiology, ambulatory, waiting rooms, facilities and buildings with no nurse unit), so
# 13 encounter and 59 appointment location codes (1.59M appointments) could not decode. Land those from LOCATION (type =
# code set 222 CDF_MEANING, parent = active root-0 LOCATION_GROUP edge), then code-set-220 codes absent from LOCATION.
# Inventory, HIM, specimen-collection, charge-services, blood-bank and tracking-root types are not patient locations and
# are not landed. SOURCE_TABLE names each row's own source.
NON_PATIENT_LOCATION_TYPES = ["INVLOCATOR", "INVLOC", "INVVIEW", "HIM", "HIMROOT", "COLLRTE", "COLLRUN", "COLLROOT",
                              "STORAGECOMP", "CSLOGIN", "CSTRACK", "BBINVAREA", "BBOWNERROOT", "PTTRACKROOT"]
SRC_LOCATION = f"{RAW}.mill_location"
SRC_LOCATION_GROUP = f"{RAW}.mill_location_group"
nu, room, bed = [df.withColumn("__SOURCE_TABLE", F.lit(src)) for df, src in ((nu, SRC_NURSE_UNIT), (room, SRC_ROOM), (bed, SRC_BED))]
arm_cd = reduce(lambda a, b: a.unionByName(b), [df.select(F.col("LOCATION_CD").alias("_ARM_CD")) for df in (nu, room, bed)])
loc_type = spark.table(CODE_VALUE).where(F.col("CODE_SET") == 222).select(
    F.col("CODE_VALUE").cast("long").alias("_TYPE_CD"), F.col("CDF_MEANING").alias("_TYPE")).dropDuplicates(["_TYPE_CD"])
group_parent = (spark.table(SRC_LOCATION_GROUP)
    .where((F.col("ACTIVE_IND") == 1) & (F.col("ROOT_LOC_CD") == 0) & (F.col("PARENT_LOC_CD") > 0))
    .withColumn("_rn", F.row_number().over(Window.partitionBy(F.col("CHILD_LOC_CD").cast("long")).orderBy(
        F.col("BEG_EFFECTIVE_DT_TM").desc_nulls_last(), F.col("PARENT_LOC_CD").cast("long").asc_nulls_last())))
    .where(F.col("_rn") == 1)
    .select(F.col("CHILD_LOC_CD").cast("long").alias("_CHILD_CD"), F.col("PARENT_LOC_CD").cast("long").alias("_PARENT_CD")))


def location_arm(df, code, meaning, active, beg, end, updt, src):
    m = F.when(F.trim(meaning) != "", F.upper(F.trim(meaning)))
    return df.join(group_parent, code == F.col("_CHILD_CD"), "left").select(
        code.alias("LOCATION_CD"),
        F.when(m == "NURSEUNIT", F.lit("nurse_unit")).otherwise(F.lower(F.coalesce(m, F.lit("unclassified")))).alias("LOCATION_LEVEL"),
        F.col("_PARENT_CD").alias("PARENT_LOCATION_CD"),
        F.when(m == "FACILITY", code).alias("FACILITY_CD"), F.when(m == "BUILDING", code).alias("BUILDING_CD"),
        F.when(m == "NURSEUNIT", code).alias("NURSE_UNIT_CD"),
        F.lit(None).cast("long").alias("CLASS_CD"), F.lit(None).cast("boolean").alias("FIXED_BED_IND"), F.lit(None).cast("boolean").alias("DUP_BED_IND"),
        F.lit(None).cast("long").alias("CURRENT_BED_STATUS_CD"), (active == 1).alias("ACTIVE_IND"), beg.alias("BEG_EFFECTIVE_DT_TM"),
        end.alias("END_EFFECTIVE_DT_TM"), updt.alias("SOURCE_ADC_UPDT"), F.lit(src).alias("__SOURCE_TABLE"),
    )


loc_src = (spark.table(SRC_LOCATION).join(F.broadcast(loc_type), F.col("LOCATION_TYPE_CD").cast("long") == F.col("_TYPE_CD"), "left")
    .where(~F.upper(F.trim(F.coalesce(F.col("_TYPE"), F.lit("")))).isin(NON_PATIENT_LOCATION_TYPES))
    .join(arm_cd, F.col("LOCATION_CD").cast("long") == F.col("_ARM_CD"), "left_anti"))
loc_other = location_arm(loc_src, F.col("LOCATION_CD").cast("long"), F.col("_TYPE"), F.col("ACTIVE_IND"),
                         F.col("BEG_EFFECTIVE_DT_TM"), F.col("END_EFFECTIVE_DT_TM"), F.col("ADC_UPDT"), SRC_LOCATION)
cv_src = (spark.table(CODE_VALUE).where(F.col("CODE_SET") == 220)
    .where(~F.upper(F.trim(F.coalesce(F.col("CDF_MEANING"), F.lit("")))).isin(NON_PATIENT_LOCATION_TYPES))
    .join(spark.table(SRC_LOCATION).select(F.col("LOCATION_CD").cast("long").alias("_L_CD")),
          F.col("CODE_VALUE").cast("long") == F.col("_L_CD"), "left_anti")
    .join(arm_cd, F.col("CODE_VALUE").cast("long") == F.col("_ARM_CD"), "left_anti"))
loc_code_only = location_arm(cv_src, F.col("CODE_VALUE").cast("long"), F.col("CDF_MEANING"), F.col("ACTIVE_IND"),
                             F.col("BEGIN_EFFECTIVE_DT_TM"), F.col("END_EFFECTIVE_DT_TM"), F.col("ADC_UPDT"), CODE_VALUE)
loc = reduce(lambda a, b: a.unionByName(b), [nu, room, bed, loc_other, loc_code_only]).where(F.col("LOCATION_CD") > 0)
loc = add_decode(loc, "LOCATION_CD", "LOCATION_DISPLAY")
loc = add_decode(loc, "PARENT_LOCATION_CD", "PARENT_LOCATION_DISPLAY")
loc = add_decode(loc, "NURSE_UNIT_CD", "NURSE_UNIT_DISPLAY")
loc = add_decode(loc, "CURRENT_BED_STATUS_CD", "CURRENT_BED_STATUS_DISPLAY")
loc = finalise(loc, ["LOCATION_CD"], SRC_LOCATION, "LOCATION_CD").withColumn("SOURCE_TABLE", F.col("__SOURCE_TABLE")).drop("__SOURCE_TABLE")  # SDI_LOCATION_UNIT_ALL_V1
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
        _clean_tracking_ts(F.col("END_EFFECTIVE_DT_TM")).alias("END_EFFECTIVE_DT_TM_CLEAN"),
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
    ).write.mode("append").saveAsTable("6_mgmt.bronze.tracking_pipeline_errors")
    raise

# COMMAND ----------

spark.createDataFrame(
    [(RUN_ID, PIPELINE_LOGIC_VERSION, SAMPLE_MODE, SAMPLE_ROWS, json.dumps(results, sort_keys=True), datetime.now(timezone.utc).replace(tzinfo=None))],
    "run_id string, logic_version string, sample_mode boolean, sample_rows long, result_json string, recorded_at timestamp",
).write.mode("append").saveAsTable("6_mgmt.bronze.tracking_pipeline_runs")
dbutils.notebook.exit(json.dumps({"sample_mode": SAMPLE_MODE, "sample_rows": SAMPLE_ROWS, "results": results}, sort_keys=True))
