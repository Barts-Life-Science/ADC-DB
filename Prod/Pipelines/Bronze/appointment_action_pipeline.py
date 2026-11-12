# Databricks notebook source
# TDX appointment-action bronze writer. Default path is a bounded dev sample.

# COMMAND ----------

from datetime import datetime, timezone
import json
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import functions as F

for _name, _default in {"target_schema":"8_dev.tdx_bronze", "sample_mode":"true", "sample_rows":"100000", "allow_production_write":"false", "pipeline_run_id":""}.items():
    try: dbutils.widgets.get(_name)
    except Exception: dbutils.widgets.text(_name, _default)

TARGET_SCHEMA = dbutils.widgets.get("target_schema")
SAMPLE_MODE = dbutils.widgets.get("sample_mode").lower() == "true"
SAMPLE_ROWS = int(dbutils.widgets.get("sample_rows"))
ALLOW_PRODUCTION_WRITE = dbutils.widgets.get("allow_production_write").lower() == "true"
RUN_ID = dbutils.widgets.get("pipeline_run_id") or f"tdx-appt-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
assert not TARGET_SCHEMA.lower().startswith("4_prod") or ALLOW_PRODUCTION_WRITE
if SAMPLE_MODE: assert TARGET_SCHEMA.startswith("8_dev.")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {'.'.join(f'`{p}`' for p in TARGET_SCHEMA.split('.'))}")

SOURCE = "4_prod.raw.mill_sch_event_action"
APPOINTMENT = "4_prod.bronze.map_appointment"
CODE_VALUE = "3_lookup.mill.mill_code_value"
TARGET = f"{TARGET_SCHEMA}.map_appointment_action"
ALLOWLIST = ["SCHEDULE","CONFIRM","CHECKIN","CHECKOUT","NOSHOW","CANCEL","CANCELREQ","RESCHEDULE","REQUEST","COMPLETEREQ","CONTACT","SEENBYGEN1","SEENBYGEN2","SEENBYGEN3","SEENBYGEN4","PATSEEN","PTPATINROOM","PTPATFINISH","UNDOCHECKIN","UNDONOSHOW","UNDOCANCEL"]

def qname(name): return ".".join(f"`{p.replace('`','``')}`" for p in name.split("."))

lookup = spark.table(CODE_VALUE).select(F.col("CODE_VALUE").cast("long").alias("_cv"), F.col("DISPLAY").alias("_display")).dropDuplicates(["_cv"])
def decode(df, code, display):
    key = f"__{display}"
    return df.join(F.broadcast(lookup.select(F.col("_cv").alias(key), F.col("_display").alias(display))), F.col(code).cast("long") == F.col(key), "left").drop(key)

source = spark.table(SOURCE).where(F.upper(F.trim("ACTION_MEANING")).isin(ALLOWLIST))
if SAMPLE_MODE: source = source.limit(SAMPLE_ROWS)
s = source.alias("s")
a = spark.table(APPOINTMENT).where("SOURCE_PRESENT_IND").select(
    F.col("SCH_EVENT_ID").cast("long").alias("_sch_event_id"), F.col("PERSON_ID").cast("long").alias("_person_id"), F.col("ENCNTR_ID").cast("long").alias("_encntr_id")
).dropDuplicates(["_sch_event_id"])
out = s.join(a, F.col("s.SCH_EVENT_ID").cast("long") == F.col("_sch_event_id"), "left").select(
    F.col("s.SCH_ACTION_ID").cast("long").alias("SCH_ACTION_ID"), F.col("s.SCH_EVENT_ID").cast("long").alias("SCH_EVENT_ID"),
    F.col("s.SCHEDULE_ID").cast("long").alias("SCHEDULE_ID"), F.coalesce(F.col("_person_id"), F.lit(None).cast("long")).alias("PERSON_ID"),
    F.coalesce(F.col("s.ENCNTR_ID").cast("long"), F.col("_encntr_id")).alias("ENCNTR_ID"), F.when(F.col("_sch_event_id").isNotNull(), "appointment").otherwise("none").alias("LINKAGE_ROUTE"),
    F.upper(F.trim("s.ACTION_MEANING")).alias("ACTION_MEANING"), F.col("s.SCH_ACTION_CD").cast("long").alias("SCH_ACTION_CD"),
    F.col("s.REQ_ACTION_MEANING"), F.col("s.REASON_MEANING"), F.col("s.SCH_REASON_CD").cast("long").alias("SCH_REASON_CD"),
    F.col("s.ACTION_DT_TM"), F.col("s.PERFORM_DT_TM"), F.col("s.CONTACT_OUTCOME_CD").cast("long").alias("CONTACT_OUTCOME_CD"), F.col("s.CONTACT_FOLLOW_UP_DT_TM"),
    *[F.col(f"s.{c}").cast("long").alias(c) for c in ["ACTION_PRSNL_ID","ACTION_SOURCE_CD","RESOURCE_CD","APPT_SYNONYM_CD","ORIG_ACTION_ID","REQ_ACTION_ID","BREACH_OFFSET_DAYS","ORGANIZATION_ID"]],
    (F.col("s.PATIENT_DECEASED_IND") == 1).alias("PATIENT_DECEASED_IND"), (F.col("s.ACTIVE_IND") == 1).alias("ACTIVE_IND"),
    F.col("s.VER_STATUS_MEANING"), F.col("s.Trust").alias("TRUST"), F.col("s.ADC_UPDT").alias("SOURCE_ADC_UPDT"),
)
for code, display in [("SCH_ACTION_CD","SCH_ACTION_DISPLAY"),("SCH_REASON_CD","SCH_REASON_DISPLAY"),("CONTACT_OUTCOME_CD","CONTACT_OUTCOME_DISPLAY"),("ACTION_SOURCE_CD","ACTION_SOURCE_DISPLAY"),("RESOURCE_CD","RESOURCE_DISPLAY"),("APPT_SYNONYM_CD","APPT_SYNONYM_DISPLAY")]:
    out = decode(out, code, display)
out = (out.withColumn("SOURCE_TABLE", F.lit(SOURCE)).withColumn("SOURCE_ROW_ID", F.col("SCH_ACTION_ID").cast("string"))
    .withColumn("PIPELINE_RUN_ID", F.lit(RUN_ID)).withColumn("SOURCE_PRESENT_IND", F.lit(True)).withColumn("SOURCE_ABSENT_DETECTED_TS", F.lit(None).cast("timestamp")))
# INSR_W8_COMMENTS_V1 (W8.04): canonical bronze comments, generated from the INSR W8.02 dictionary. Edit the dictionary,
# never this literal. Column names are the bronze (upper-case) names.
INSR_W8_BRONZE_COMMENTS = {
 "map_appointment_action": {
  "ACTION_DT_TM": "Date and time the scheduling action was taken.",
  "ACTION_MEANING": "Meaning string corresponding to the scheduling action code.",
  "ACTION_PRSNL_ID": "Identifier of the personnel member who performed the scheduling action.",
  "ACTION_SOURCE_CD": "Code value identifying the source from which the scheduling action originated.",
  "ACTION_SOURCE_DISPLAY": "Display text describing the source of the appointment action, resolved from a Millennium code value.",
  "ACTIVE_IND": "Indicator showing whether the scheduling event action record is active.",
  "ADC_UPDT": "When bronze last wrote the row.",
  "APPT_SYNONYM_CD": "Code value identifying the appointment synonym associated with the scheduling action.",
  "APPT_SYNONYM_DISPLAY": "Display text of the appointment type synonym associated with the appointment action, resolved from a Millennium code value.",
  "BREACH_OFFSET_DAYS": "Number of days of breach offset recorded for this scheduling event action.",
  "CONTACT_FOLLOW_UP_DT_TM": "Date and time recorded for a follow-up of a patient contact associated with the scheduling action.",
  "CONTACT_OUTCOME_CD": "Code value identifying the recorded outcome of a patient contact associated with the scheduling action.",
  "CONTACT_OUTCOME_DISPLAY": "Display text of the contact outcome recorded for the appointment action, resolved from a Millennium code value.",
  "ENCNTR_ID": "Internal surrogate identifier of the encounter linked to the scheduling event action, resolved during ingestion.",
  "LINKAGE_ROUTE": "How the row was linked to its person or encounter.",
  "ORGANIZATION_ID": "Identifier of the organization associated with this scheduling event action, populated with a resolved organization during trust mapping.",
  "ORIG_ACTION_ID": "Identifier of the original scheduling action from which this action was derived.",
  "PATIENT_DECEASED_IND": "Indicator flagging whether the patient associated with the scheduling event was recorded as deceased at the time of the action.",
  "PERFORM_DT_TM": "Date and time the scheduling action was performed.",
  "PERSON_ID": "This is the value of the unique primary identifier of the person table.  It is an internal system assigned number.",
  "PIPELINE_RUN_ID": "Bronze pipeline run that last wrote the row.",
  "REASON_MEANING": "Meaning string corresponding to the scheduling action reason code.",
  "REQ_ACTION_ID": "Identifier of the request action linked to this scheduling event action.",
  "REQ_ACTION_MEANING": "Cerner code meaning text corresponding to REQ_ACTION_CD for the requested action.",
  "RESOURCE_CD": "Code value identifying the scheduling resource associated with this scheduling event action.",
  "RESOURCE_DISPLAY": "Display text of the scheduling resource associated with the appointment action, resolved from a Millennium code value.",
  "ROW_HASH": "SHA-256 of the row's payload columns, used by the bronze MERGE to detect changes.",
  "SCHEDULE_ID": "Identifier of the schedule associated with the scheduling event action.",
  "SCH_ACTION_CD": "Code value identifying the type of scheduling action performed on the event.",
  "SCH_ACTION_DISPLAY": "Display text of the scheduling action performed on the appointment, resolved from a Millennium code value.",
  "SCH_ACTION_ID": "Internal surrogate identifier uniquely identifying a scheduling event action record.",
  "SCH_EVENT_ID": "Identifier of the scheduling event (appointment) to which this action applies.",
  "SCH_REASON_CD": "Code value identifying the reason recorded for the scheduling action.",
  "SCH_REASON_DISPLAY": "Display text of the scheduling reason recorded for the appointment action, resolved from a Millennium code value.",
  "SOURCE_ABSENT_DETECTED_TS": "When bronze first found the row absent from the source snapshot; NULL while present.",
  "SOURCE_ADC_UPDT": "When the source row was landed in raw (raw ADC_UPDT).",
  "SOURCE_PRESENT_IND": "True while the row is present in the latest full source snapshot.",
  "SOURCE_ROW_ID": "Native source row identifier, as text.",
  "SOURCE_TABLE": "Bronze or raw source table the row came from.",
  "TRUST": "Name of the NHS trust to which the record has been attributed by the ingestion trust-resolution process.",
  "VER_STATUS_MEANING": "Cerner code meaning text corresponding to VER_STATUS_CD for the verification status."
 }
}
INSR_W8_BRONZE_TABLE_COMMENTS = {
 "map_appointment_action": "One row per Millennium scheduling action on an appointment (book, confirm, check in, cancel, undo), keyed by SCH_ACTION_ID. Curated bronze input to silver clinical_appointment_action; source-faithful columns, decodes in silver."
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


_insr_w8_assert_covered(TARGET, out.columns + ["ROW_HASH", "ADC_UPDT"])  # INSR_W8_COMMENTS_V1: before any write
payload = sorted(c for c in out.columns if c not in {"PIPELINE_RUN_ID","SOURCE_ABSENT_DETECTED_TS"})
out = out.withColumn("ROW_HASH", F.sha2(F.to_json(F.struct(*[F.col(c) for c in payload])),256)).withColumn("ADC_UPDT",F.current_timestamp())
if SAMPLE_MODE:
    out.write.format("delta").mode("overwrite").option("overwriteSchema","true").option("delta.enableChangeDataFeed","true").option("delta.enableRowTracking","true").saveAsTable(TARGET)
    materialised = spark.table(TARGET)
    assert materialised.where("SCH_ACTION_ID IS NULL").limit(1).count() == 0
    assert materialised.groupBy("SCH_ACTION_ID").count().where("count>1").limit(1).count() == 0
    operation = "SAMPLE_OVERWRITE"
elif not spark.catalog.tableExists(TARGET):
    assert out.where("SCH_ACTION_ID IS NULL").limit(1).count() == 0
    assert out.groupBy("SCH_ACTION_ID").count().where("count>1").limit(1).count() == 0
    out.write.format("delta").mode("overwrite").option("overwriteSchema","true").option("delta.enableChangeDataFeed","true").option("delta.enableRowTracking","true").saveAsTable(TARGET)
    operation = "CREATE"
else:
    assert out.where("SCH_ACTION_ID IS NULL").limit(1).count() == 0
    assert out.groupBy("SCH_ACTION_ID").count().where("count>1").limit(1).count() == 0
    dt = DeltaTable.forName(spark,TARGET)
    mutable = [c for c in out.columns if c != "SCH_ACTION_ID"]
    (dt.alias("t").merge(out.alias("s"),"t.SCH_ACTION_ID <=> s.SCH_ACTION_ID")
      .whenMatchedUpdate(condition="NOT (t.ROW_HASH <=> s.ROW_HASH)",set={c:f"s.`{c}`" for c in mutable})
      .whenNotMatchedInsertAll()
      .whenNotMatchedBySourceUpdate(condition="t.SOURCE_PRESENT_IND=true",set={"SOURCE_PRESENT_IND":"false","SOURCE_ABSENT_DETECTED_TS":"current_timestamp()","ADC_UPDT":"current_timestamp()"}).execute())
    operation = "FULL_MERGE"
spark.sql(f"ALTER TABLE {qname(TARGET)} SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true','delta.enableRowTracking'='true','delta.enableDeletionVectors'='true')")
spark.sql(f"COMMENT ON TABLE {qname(TARGET)} IS '{_insr_w8_sql(INSR_W8_BRONZE_TABLE_COMMENTS[TARGET.split('.')[-1]])}'")  # INSR_W8_COMMENTS_V1
for col in spark.table(TARGET).columns:
    spark.sql(f"ALTER TABLE {qname(TARGET)} ALTER COLUMN `{col}` COMMENT '{_insr_w8_sql(_insr_w8_comment(TARGET, col))}'")
    # INSR_W1_TAG_HEURISTIC_RETIRED_V1: ig_risk/ig_severity come from Table Metadata Compliance (estate-copied), never from column names.
spark.sql(f"ALTER TABLE {qname(TARGET)} CLUSTER BY (SCH_EVENT_ID, ACTION_DT_TM)")
result = {"target":TARGET,"operation":operation,"rows":spark.table(TARGET).count(),"sample_mode":SAMPLE_MODE,"sample_rows":SAMPLE_ROWS}
spark.createDataFrame([(RUN_ID,json.dumps(result,sort_keys=True),datetime.now(timezone.utc).replace(tzinfo=None))],"run_id string, result_json string, recorded_at timestamp").write.mode("append").saveAsTable("6_mgmt.bronze.appointment_action_pipeline_runs")  # OGR_NO_DEV_V1/appointment_action
dbutils.notebook.exit(json.dumps(result,sort_keys=True))
