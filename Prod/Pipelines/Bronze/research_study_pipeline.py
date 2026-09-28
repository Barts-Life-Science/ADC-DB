# Databricks notebook source
# research_study_pipeline - S1/A2. map_research_study + map_research_subject from PowerTrials raw.
# Tiny tables (1,018 / 34,348 rows): deterministic full replacement each run is the deliberate
# master-plan §2.3 trade for sub-100k tables; revisit only if CDF consumers appear.
# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 1122747012694814)
import json

# Prod-idiom target resolution (house pattern: jac_pipeline/endobase_pipeline).
def _widget_text(name, default):
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass
    try:
        v = dbutils.widgets.get(name)
    except Exception:
        v = default
    return (v or default).strip()

TARGET_SCHEMA = _widget_text("target_schema", "8_dev.bronze")
ALLOW_PROD_WRITE = _widget_text("allow_production_write", "false").lower() == "true"
assert TARGET_SCHEMA.startswith("8_dev.") or ALLOW_PROD_WRITE, (
    f"Refusing to write {TARGET_SCHEMA} without allow_production_write=true")

def _control_schema(target):
    return "6_mgmt.bronze" if target == "4_prod.bronze" else target

CONTROL_SCHEMA = _control_schema(TARGET_SCHEMA)

SCHEMA = TARGET_SCHEMA

STATE_PROPERTY = "bronze_completeness.source_versions_json"
SOURCE_TABLES = [
    "3_lookup.mill.mill_code_value",
    "4_prod.raw.mill_prot_master",
    "4_prod.raw.mill_pt_prot_reg",
    "3_lookup.mill.mill_nomenclature",
]
TARGET_TABLES = [
    f"{SCHEMA}.map_research_study",
    f"{SCHEMA}.map_research_subject",
]

def _table_version(table):
    return int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])

def _source_versions():
    return {table: _table_version(table) for table in SOURCE_TABLES}

def _target_state(table):
    if not spark.catalog.tableExists(table):
        return {}
    props = spark.sql(f"DESCRIBE DETAIL {table}").first()["properties"] or {}
    return json.loads(props.get(STATE_PROPERTY, "{}"))

CURRENT_SOURCE_VERSIONS = _source_versions()
if all(
    spark.catalog.tableExists(table)
    and _target_state(table) == CURRENT_SOURCE_VERSIONS
    for table in TARGET_TABLES
):
    dbutils.notebook.exit(json.dumps({
        "result": "NO_OP", "target": TARGET_SCHEMA, "target_schema": TARGET_SCHEMA,
        "source_versions": CURRENT_SOURCE_VERSIONS,
    }, sort_keys=True))

# COMMAND ----------

# ==== COMMON BLOCK v1 (SYNC-WITH _completeness_common) ====
from pyspark.sql import functions as F

SENTINEL_FLOOR = "1901-01-01"

def dq_columns(df, date_cols):
    """Master plan §2.2 date-quality standard block.
    For each timestamp column C adds:
      C_FUTURE_IND   - value is after now()
      C_SENTINEL_IND - value is before 1901-01-01
      C_CLEAN        - value, or NULL when either flag is set
    Source column is retained untouched (bronze keeps source values; silver chooses).
    """
    out = df
    for c in date_cols:
        fut = F.col(c) > F.current_timestamp()
        sen = F.col(c) < F.lit(SENTINEL_FLOOR).cast("timestamp")
        out = (out
               .withColumn(f"{c}_FUTURE_IND", F.when(F.col(c).isNull(), F.lit(None)).otherwise(fut))
               .withColumn(f"{c}_SENTINEL_IND", F.when(F.col(c).isNull(), F.lit(None)).otherwise(sen))
               .withColumn(f"{c}_CLEAN", F.when(fut | sen, F.lit(None).cast("timestamp")).otherwise(F.col(c))))
    return out

def get_watermark(control_table, source_name, default="1980-01-01"):
    """Per-source watermark (master plan §2.3 rule 5 - one row per source, never GREATEST across sources)."""
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {control_table} (
        source_name STRING, watermark TIMESTAMP, updated_at TIMESTAMP)""")
    rows = spark.sql(f"""SELECT watermark FROM {control_table}
                         WHERE source_name = '{source_name}'""").collect()
    return rows[0]["watermark"] if rows else spark.sql(
        f"SELECT CAST('{default}' AS TIMESTAMP) w").collect()[0]["w"]

def set_watermark(control_table, source_name, new_wm):
    """new_wm must be the SOURCE MAX(ADC_UPDT) observed this run (source-change clock, never build clock)."""
    if new_wm is None:
        return
    spark.sql(f"""MERGE INTO {control_table} t
        USING (SELECT '{source_name}' source_name, CAST('{new_wm}' AS TIMESTAMP) watermark) s
        ON t.source_name = s.source_name
        WHEN MATCHED AND s.watermark > t.watermark
             THEN UPDATE SET t.watermark = s.watermark, t.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (source_name, watermark, updated_at)
             VALUES (s.source_name, s.watermark, current_timestamp())""")
# ==== END COMMON BLOCK v1 ====

# COMMAND ----------

from pyspark.sql import functions as F

cv = (spark.table("3_lookup.mill.mill_code_value")
      .filter(F.col("ACTIVE_IND") > 0)
      .select(
          F.col("CODE_VALUE").cast("bigint").alias("CV_CD"),
          F.col("DESCRIPTION").alias("CV_DESC"),
      ))

def dec(df, code_col, out_col):
    lookup = (cv.withColumnRenamed("CV_CD", f"_k_{out_col}")
                .withColumnRenamed("CV_DESC", out_col))
    return (df.join(
                lookup,
                F.col(code_col).cast("bigint") == F.col(f"_k_{out_col}"),
                "left",
            )
            .drop(f"_k_{out_col}"))

pm = spark.table("4_prod.raw.mill_prot_master")
study = pm
for c, o in [
    ("PROT_TYPE_CD", "PROT_TYPE_DESC"),
    ("PROT_PHASE_CD", "PROT_PHASE_DESC"),
    ("PROT_STATUS_CD", "PROT_STATUS_DESC"),
    ("PROT_PURPOSE_CD", "PROT_PURPOSE_DESC"),
    ("PROGRAM_CD", "PROGRAM_DESC"),
    ("PARTICIPATION_TYPE_CD", "PARTICIPATION_TYPE_DESC"),
]:
    study = dec(study, c, o)

study = (study.select(
    F.col("PROT_MASTER_ID").cast("bigint"),
    F.col("PRIMARY_MNEMONIC").alias("STUDY_MNEMONIC"),
    F.col("PRIMARY_MNEMONIC_KEY").alias("STUDY_MNEMONIC_KEY"),
    "PROT_TYPE_CD", "PROT_TYPE_DESC",
    "PROT_PHASE_CD", "PROT_PHASE_DESC",
    "PROT_STATUS_CD", "PROT_STATUS_DESC",
    "PROT_PURPOSE_CD", "PROT_PURPOSE_DESC",
    "PROGRAM_CD", "PROGRAM_DESC",
    "PARTICIPATION_TYPE_CD", "PARTICIPATION_TYPE_DESC",
    F.col("RESEARCH_SPONSOR_ORG_ID").cast("bigint"),
    F.col("COLLAB_SITE_ORG_ID").cast("bigint"),
    "INITIATING_SERVICE_CD", "INITIATING_SERVICE_DESC", "SUB_INITIATING_SERVICE_DESC",
    F.col("PARENT_PROT_MASTER_ID").cast("bigint"),
    F.col("PREV_PROT_MASTER_ID").cast("bigint"),
    "DISPLAY_IND", "SCREENER_IND", "NETWORK_FLAG",
    "BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM", "ADC_UPDT",
).withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp()))

(study.write.format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{SCHEMA}.map_research_study"))

# COMMAND ----------

reg = spark.table("4_prod.raw.mill_pt_prot_reg")
for c, o in [
    ("BEST_RESPONSE_CD", "BEST_RESPONSE_DESC"),
    ("REMOVAL_REASON_CD", "REMOVAL_REASON_DESC_CV"),
    ("REASON_OFF_TX_CD", "REASON_OFF_TX_DESC_CV"),
    ("DIAGNOSIS_TYPE_CD", "DIAGNOSIS_TYPE_DESC"),
    ("FIRST_DIS_REL_EVENT_DEATH_CD", "FIRST_DIS_REL_EVENT_DEATH_DESC"),
]:
    reg = dec(reg, c, o)

nom = (spark.table("3_lookup.mill.mill_nomenclature")
       .select(
           F.col("NOMENCLATURE_ID").alias("_NOM_ID"),
           F.col("SOURCE_STRING").alias("ENROLL_DIAGNOSIS_STRING"),
           F.col("SOURCE_IDENTIFIER").alias("ENROLL_DIAGNOSIS_IDENTIFIER"),
           F.col("CONCEPT_CKI").alias("ENROLL_DIAGNOSIS_CKI"),
       ))
reg = (reg.join(
    nom,
    F.col("NOMENCLATURE_ID").cast("bigint") == F.col("_NOM_ID"),
    "left",
).drop("_NOM_ID"))

reg = reg.withColumn(
    "STATUS_DESC",
    F.expr(
        "CASE CAST(STATUS_ENUM AS INT) "
        "WHEN 1 THEN 'On Study' "
        "WHEN 2 THEN 'On Treatment' "
        "WHEN 3 THEN 'Off Treatment' "
        "WHEN 4 THEN 'On Followup' "
        "WHEN 5 THEN 'Off Study' END"
    ),
)
reg = dq_columns(
    reg,
    [
        "ON_STUDY_DT_TM",
        "OFF_STUDY_DT_TM",
        "TX_START_DT_TM",
        "TX_COMPLETION_DT_TM",
        "FIRST_CR_DT_TM",
        "FIRST_PD_DT_TM",
        "FIRST_PD_FAILURE_DT_TM",
    ],
)

subject = (reg.select(
    F.col("PT_PROT_REG_ID").cast("bigint"),
    F.col("REG_ID").cast("bigint"),
    F.col("PROT_MASTER_ID").cast("bigint"),
    F.col("PERSON_ID").cast("bigint"),
    F.col("ENCNTR_ID").cast("bigint"),
    F.col("EPISODE_ID").cast("bigint"),
    "PROT_ACCESSION_NBR",
    F.col("PROT_ARM_ID").cast("bigint"),
    "STATUS_ENUM", "STATUS_DESC",
    "ON_STUDY_DT_TM", "ON_STUDY_DT_TM_FUTURE_IND", "ON_STUDY_DT_TM_SENTINEL_IND", "ON_STUDY_DT_TM_CLEAN",
    "OFF_STUDY_DT_TM", "OFF_STUDY_DT_TM_FUTURE_IND", "OFF_STUDY_DT_TM_SENTINEL_IND", "OFF_STUDY_DT_TM_CLEAN",
    "TX_START_DT_TM", "TX_START_DT_TM_FUTURE_IND", "TX_START_DT_TM_SENTINEL_IND", "TX_START_DT_TM_CLEAN",
    "TX_COMPLETION_DT_TM", "TX_COMPLETION_DT_TM_FUTURE_IND", "TX_COMPLETION_DT_TM_SENTINEL_IND", "TX_COMPLETION_DT_TM_CLEAN",
    "FIRST_CR_DT_TM", "FIRST_CR_DT_TM_FUTURE_IND", "FIRST_CR_DT_TM_SENTINEL_IND", "FIRST_CR_DT_TM_CLEAN",
    "FIRST_PD_DT_TM", "FIRST_PD_DT_TM_FUTURE_IND", "FIRST_PD_DT_TM_SENTINEL_IND", "FIRST_PD_DT_TM_CLEAN",
    "FIRST_PD_FAILURE_DT_TM", "FIRST_PD_FAILURE_DT_TM_FUTURE_IND", "FIRST_PD_FAILURE_DT_TM_SENTINEL_IND", "FIRST_PD_FAILURE_DT_TM_CLEAN",
    "FIRST_DIS_REL_EVENT_DEATH_CD", "FIRST_DIS_REL_EVENT_DEATH_DESC",
    "BEST_RESPONSE_CD", "BEST_RESPONSE_DESC",
    "REMOVAL_REASON_CD", "REMOVAL_REASON_DESC_CV",
    F.col("REMOVAL_REASON_DESC").alias("REMOVAL_REASON_FT"),
    "REASON_OFF_TX_CD", "REASON_OFF_TX_DESC_CV",
    F.col("REASON_OFF_TX_DESC").alias("REASON_OFF_TX_FT"),
    "NOMENCLATURE_ID", "ENROLL_DIAGNOSIS_STRING", "ENROLL_DIAGNOSIS_IDENTIFIER", "ENROLL_DIAGNOSIS_CKI",
    "DIAGNOSIS_TYPE_CD", "DIAGNOSIS_TYPE_DESC",
    F.col("ENROLLING_ORGANIZATION_ID").cast("bigint"),
    "Trust", "BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM", "ADC_UPDT",
).withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp()))

# Consent linkage seam: PT_CONSENT columns join here when S7/B6 lands.
(subject.write.format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{SCHEMA}.map_research_subject"))

# COMMAND ----------

spark.sql(f"""
    COMMENT ON TABLE {SCHEMA}.map_research_study IS
    'PowerTrials study foundation. Grain: one row per PROT_MASTER_ID. STUDY_MNEMONIC is PRIMARY_MNEMONIC. Bare code-value FKs are decoded against active mill_code_value rows. Refresh strategy is deterministic full replacement because the source is sub-100k.'
""")
spark.sql(f"""
    COMMENT ON TABLE {SCHEMA}.map_research_subject IS
    'PowerTrials research-subject foundation. Grain: every PT_PROT_REG_ID registration is retained. The legacy RDE latest-per-PROT_MASTER_ID-and-PERSON_ID collapse is reproducible by ordering BEG_EFFECTIVE_DT_TM descending with NULLS LAST. Date-quality columns retain source values and add future, sentinel, and clean variants. Consent linkage is an additive S7/B6 seam. Refresh strategy is deterministic full replacement because the source is sub-100k.'
""")
print("map_research_study + map_research_subject replaced")
_state_payload = json.dumps(
    CURRENT_SOURCE_VERSIONS, sort_keys=True, separators=(",", ":")
).replace("'", "''")
for _target in TARGET_TABLES:
    spark.sql(
        f"ALTER TABLE {_target} SET TBLPROPERTIES "
        f"('{STATE_PROPERTY}'='{_state_payload}')"
    )

# COMMAND ----------

# PROMOTION RUNBOOK (human-gated)
# 1. Before any prod write, integrate this code into
#    /Workspace/Shared/ADC-DB/Prod/Pipelines/Bronze/completeness_pipeline
#    (or an existing domain pipeline) and register retained-column contracts through
#    _bronze_common / bronze_assert_primary_contract.
# 2. Set target_schema=4_prod.bronze in the integrated prod notebook.
# 3. Run the deterministic build once there, then rerun every A2 gate against prod.
# 4. Register the step in bronze_pipeline after its dependencies and set task retries deliberately.
# 5. Verify the next weekly run against the measured S1 steady-state runtime.

dbutils.notebook.exit(json.dumps({"result": "BUILT", "target": TARGET_SCHEMA, "target_schema": TARGET_SCHEMA}, sort_keys=True))


