# Databricks notebook source
# allergy_pipeline - S1/A1. Bronze map_allergy from 4_prod.raw.mill_allergy.
# Grain: one row per ALLERGY_INSTANCE_ID (all revisions; CURRENT_IND marks latest per ALLERGY_ID).
# Lossless: no Trust/active/encounter filtering. Absence assertions are flagged, never dropped.
# Named exclusions: UPDT_*, *_TZ, CMB_* except CMB_FLAG, TXN_ID_TEXT, INST_ID,
# ACTIVE_STATUS_PRSNL_ID and DATA_STATUS_PRSNL_ID. SUB_CONCEPT_CKI is retained because
# the live 2026-08-11 probe found 1,709,807 populated rows (CERNER!NKMA on 12,321 distinct records).
# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 468658226031816)
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
TARGET = f"{TARGET_SCHEMA}.map_allergy"
CONTROL = f"{CONTROL_SCHEMA}.s1_watermarks"
FULL = (
    _widget_text("full_rebuild", "false").lower() in ("1", "true", "yes")
    or _widget_text("force_full_refresh", "false").lower() == "true"
)

SOURCE_STATE_PROPERTY = "bronze_completeness.source_versions_json"

def _source_versions(sources):
    return {
        source: int(
            spark.sql(f"DESCRIBE HISTORY {source} LIMIT 1").collect()[0]["version"]
        )
        for source in sources
    }

def _target_state_current(target, versions):
    if not spark.catalog.tableExists(target):
        return False
    properties = spark.sql(f"DESCRIBE DETAIL {target}").first()["properties"] or {}
    previous = {
        key: int(value)
        for key, value in json.loads(
            properties.get(SOURCE_STATE_PROPERTY, "{}")
        ).items()
    }
    return previous == versions

def _record_source_versions(target, versions):
    payload = json.dumps(
        versions, sort_keys=True, separators=(",", ":")
    ).replace("'", "''")
    spark.sql(
        f"ALTER TABLE {target} SET TBLPROPERTIES "
        f"('{SOURCE_STATE_PROPERTY}'='{payload}')"
    )

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

from pyspark.sql import functions as F, Window

ABSENCE_STRINGS = [
    "NO KNOWN ALLERGIES",
    "NO KNOWN DRUG ALLERGIES",
    "NO KNOWN MEDICATION ALLERGIES",
    "NKA",
    "NKDA",
    "NO KNOWN FOOD ALLERGIES",
]

def build_frame(since=None):
    a = spark.table("4_prod.raw.mill_allergy")
    if since is not None:
        changed = (a.filter(F.col("ADC_UPDT") > F.lit(since))
                     .select("ALLERGY_ID").distinct())
        a = a.join(changed, "ALLERGY_ID", "inner")

    n = (spark.table("3_lookup.mill.mill_nomenclature")
         .select(
             F.col("NOMENCLATURE_ID"),
             F.col("SOURCE_STRING").alias("SUBSTANCE_SOURCE_STRING"),
             F.col("SHORT_STRING").alias("SUBSTANCE_SHORT_STRING"),
             F.col("SOURCE_IDENTIFIER").alias("SUBSTANCE_SOURCE_IDENTIFIER"),
             F.col("SOURCE_VOCABULARY_CD").alias("SUBSTANCE_SOURCE_VOCABULARY_CD"),
             F.col("CONCEPT_CKI").alias("SUBSTANCE_CONCEPT_CKI"),
         ))
    cv = (spark.table("3_lookup.mill.mill_code_value")
          .filter(F.col("ACTIVE_IND") > 0)
          .select(
              F.col("CODE_VALUE").cast("bigint").alias("CV_CD"),
              F.col("DESCRIPTION").alias("CV_DESC"),
          ))

    def decode(df, code_col, out_col):
        lookup = (cv.withColumnRenamed("CV_CD", f"_k_{out_col}")
                    .withColumnRenamed("CV_DESC", out_col))
        return (df.join(
                    lookup,
                    F.col(code_col).cast("bigint") == F.col(f"_k_{out_col}"),
                    "left",
                )
                .drop(f"_k_{out_col}"))

    df = a.join(
        n,
        F.col("SUBSTANCE_NOM_ID").cast("bigint") == F.col("NOMENCLATURE_ID"),
        "left",
    )
    for c, o in [
        ("SUBSTANCE_TYPE_CD", "SUBSTANCE_TYPE_DESC"),
        ("REACTION_CLASS_CD", "REACTION_CLASS_DESC"),
        ("SEVERITY_CD", "SEVERITY_DESC"),
        ("SOURCE_OF_INFO_CD", "SOURCE_OF_INFO_DESC"),
        ("REACTION_STATUS_CD", "REACTION_STATUS_DESC"),
        ("CANCEL_REASON_CD", "CANCEL_REASON_DESC"),
        ("ACTIVE_STATUS_CD", "ACTIVE_STATUS_DESC"),
        ("DATA_STATUS_CD", "DATA_STATUS_DESC"),
        ("REC_SRC_VOCAB_CD", "REC_SRC_VOCAB_DESC"),
        ("ONSET_PRECISION_CD", "ONSET_PRECISION_DESC"),
        ("CONTRIBUTOR_SYSTEM_CD", "CONTRIBUTOR_SYSTEM_DESC"),
    ]:
        df = decode(df, c, o)

    sub_norm = F.upper(F.trim(F.coalesce(
        F.col("SUBSTANCE_SOURCE_STRING"),
        F.col("SUBSTANCE_FTDESC"),
    )))
    current_window = (
        Window.partitionBy("ALLERGY_ID")
        .orderBy(
            F.col("BEG_EFFECTIVE_DT_TM").desc_nulls_last(),
            F.col("UPDT_DT_TM").desc_nulls_last(),
            F.col("ALLERGY_INSTANCE_ID").desc_nulls_last(),
        )
    )
    df = (df
          .withColumn(
              "ABSENCE_ASSERTION_IND",
              F.coalesce(sub_norm.isin(ABSENCE_STRINGS), F.lit(False)),
          )
          .withColumn(
              "SUBSTANCE_SNOMED_CODE",
              F.when(
                  F.col("SUBSTANCE_CONCEPT_CKI").startswith("SNOMED!"),
                  F.expr("substring_index(SUBSTANCE_CONCEPT_CKI, '!', -1)"),
              ),
          )
          .withColumn("CURRENT_IND", F.row_number().over(current_window) == 1))
    df = dq_columns(
        df,
        [
            "ONSET_DT_TM",
            "CREATED_DT_TM",
            "REVIEWED_DT_TM",
            "CANCEL_DT_TM",
            "REACTION_STATUS_DT_TM",
        ],
    )

    keep = [
        "ALLERGY_INSTANCE_ID", "ALLERGY_ID", "PERSON_ID", "ENCNTR_ID", "ORGANIZATION_ID", "Trust",
        "SUBSTANCE_NOM_ID", "SUB_CONCEPT_CKI", "SUBSTANCE_SOURCE_STRING", "SUBSTANCE_SHORT_STRING",
        "SUBSTANCE_SOURCE_IDENTIFIER", "SUBSTANCE_SOURCE_VOCABULARY_CD", "SUBSTANCE_CONCEPT_CKI",
        "SUBSTANCE_SNOMED_CODE", "SUBSTANCE_FTDESC", "SUBSTANCE_TYPE_CD", "SUBSTANCE_TYPE_DESC",
        "REACTION_CLASS_CD", "REACTION_CLASS_DESC", "REACTION_STATUS_CD", "REACTION_STATUS_DESC",
        "REACTION_STATUS_DT_TM", "REACTION_STATUS_DT_TM_FUTURE_IND",
        "REACTION_STATUS_DT_TM_SENTINEL_IND", "REACTION_STATUS_DT_TM_CLEAN",
        "SEVERITY_CD", "SEVERITY_DESC",
        "SOURCE_OF_INFO_CD", "SOURCE_OF_INFO_DESC", "SOURCE_OF_INFO_FT",
        "REC_SRC_VOCAB_CD", "REC_SRC_VOCAB_DESC", "REC_SRC_IDENTIFER", "REC_SRC_STRING",
        "ONSET_DT_TM", "ONSET_DT_TM_FUTURE_IND", "ONSET_DT_TM_SENTINEL_IND", "ONSET_DT_TM_CLEAN",
        "ONSET_PRECISION_CD", "ONSET_PRECISION_DESC", "ONSET_PRECISION_FLAG",
        "CREATED_DT_TM", "CREATED_DT_TM_FUTURE_IND", "CREATED_DT_TM_SENTINEL_IND", "CREATED_DT_TM_CLEAN",
        "CREATED_PRSNL_ID", "CANCEL_REASON_CD", "CANCEL_REASON_DESC",
        "CANCEL_DT_TM", "CANCEL_DT_TM_FUTURE_IND", "CANCEL_DT_TM_SENTINEL_IND", "CANCEL_DT_TM_CLEAN",
        "CANCEL_PRSNL_ID",
        "REVIEWED_DT_TM", "REVIEWED_DT_TM_FUTURE_IND", "REVIEWED_DT_TM_SENTINEL_IND", "REVIEWED_DT_TM_CLEAN",
        "REVIEWED_PRSNL_ID", "ORIG_PRSNL_ID", "VERIFIED_STATUS_FLAG",
        "ACTIVE_IND", "ACTIVE_STATUS_CD", "ACTIVE_STATUS_DESC", "ACTIVE_STATUS_DT_TM",
        "BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM",
        "DATA_STATUS_CD", "DATA_STATUS_DESC", "DATA_STATUS_DT_TM",
        "CONTRIBUTOR_SYSTEM_CD", "CONTRIBUTOR_SYSTEM_DESC", "CMB_FLAG",
        "ABSENCE_ASSERTION_IND", "CURRENT_IND", "ADC_UPDT",
    ]
    return (df.select(*keep)
            .withColumn("ALLERGY_INSTANCE_ID", F.col("ALLERGY_INSTANCE_ID").cast("bigint"))
            .withColumn("ALLERGY_ID", F.col("ALLERGY_ID").cast("bigint"))
            .withColumn("PERSON_ID", F.col("PERSON_ID").cast("bigint"))
            .withColumn("ENCNTR_ID", F.col("ENCNTR_ID").cast("bigint"))
            .withColumn("ORGANIZATION_ID", F.col("ORGANIZATION_ID").cast("bigint"))
            .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp()))

# COMMAND ----------

SOURCES = {
    "map_allergy:mill_allergy": "4_prod.raw.mill_allergy",
    "map_allergy:mill_nomenclature": "3_lookup.mill.mill_nomenclature",
    "map_allergy:mill_code_value": "3_lookup.mill.mill_code_value",
}
CURRENT_SOURCE_VERSIONS = _source_versions(list(SOURCES.values()))
if not FULL and _target_state_current(TARGET, CURRENT_SOURCE_VERSIONS):
    assert spark.catalog.tableExists(TARGET)
    assert spark.table(TARGET).where("ALLERGY_INSTANCE_ID IS NULL").limit(1).count() == 0
    dbutils.notebook.exit(json.dumps({
        "result": "NO_OP",
        "target": TARGET,
        "target_schema": TARGET_SCHEMA,
        "source_versions": CURRENT_SOURCE_VERSIONS,
    }, sort_keys=True))

src_max = {
    key: spark.sql(f"SELECT MAX(ADC_UPDT) m FROM {table_name}").collect()[0]["m"]
    for key, table_name in SOURCES.items()
}
lookups_advanced = any(
    src_max[key] is not None and src_max[key] > get_watermark(CONTROL, key)
    for key in ("map_allergy:mill_nomenclature", "map_allergy:mill_code_value")
)

if FULL or lookups_advanced or not spark.catalog.tableExists(TARGET):
    (build_frame(None)
     .write.format("delta")
     .option("delta.enableChangeDataFeed", "true")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(TARGET))
    print(f"FULL build -> {TARGET} (forced={lookups_advanced})")
else:
    since = get_watermark(CONTROL, "map_allergy:mill_allergy")
    inc = build_frame(since)
    inc.createOrReplaceTempView("_allergy_inc")
    spark.sql(f"""
        MERGE INTO {TARGET} t
        USING _allergy_inc s
        ON t.ALLERGY_INSTANCE_ID = s.ALLERGY_INSTANCE_ID
        WHEN MATCHED AND (t.ADC_UPDT < s.ADC_UPDT OR t.CURRENT_IND <> s.CURRENT_IND)
             THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"INCREMENTAL since {since}")

for key in SOURCES:
    set_watermark(CONTROL, key, src_max[key])

_record_source_versions(TARGET, CURRENT_SOURCE_VERSIONS)

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {TARGET}_current AS
    SELECT * FROM {TARGET} WHERE CURRENT_IND
""")
spark.sql(f"""
    COMMENT ON TABLE {TARGET} IS
    'Bronze allergy foundation. Grain: one source ALLERGY_INSTANCE_ID per row; every revision is retained and CURRENT_IND marks the latest instance per ALLERGY_ID. The map_allergy_current view exposes current rows. No Trust, active, or encounter filtering is applied. Substance is enriched from mill_nomenclature and SUB_CONCEPT_CKI is retained. ABSENCE_ASSERTION_IND marks No Known Allergies-family negative assertions, never positive clinical facts. Named exclusions: UPDT housekeeping, timezone columns, combine detail except CMB_FLAG, TXN_ID_TEXT, INST_ID, and row-status actor IDs. Reaction, comment, and review children arrive additively in S7/B7.'
""")
print("view + comment done")

# COMMAND ----------

# PROMOTION RUNBOOK (human-gated)
# 1. Before any prod write, integrate this code into
#    /Workspace/Shared/ADC-DB/Prod/Pipelines/Bronze/completeness_pipeline
#    (or an existing domain pipeline) and register retained-column contracts through
#    _bronze_common / bronze_assert_primary_contract.
# 2. Set target_schema=4_prod.bronze and use the production control table returned by
#    the bronze_control_schema() convention. The config guard forbids a dev control table.
# 3. Run one full build from the integrated prod notebook, then rerun every A1 gate against prod.
# 4. Register the step in bronze_pipeline after its dependencies and set task retries deliberately.
# 5. Verify the next weekly run against the measured S1 steady-state runtime.

dbutils.notebook.exit(json.dumps({"result": "BUILT", "target": TARGET, "target_schema": TARGET_SCHEMA}, sort_keys=True))


