# Databricks notebook source
# maternity_msds_pipeline - S1/A6. Four deterministic-replacement maternity sidecars.
# Family decision (2026-08-11): plural msds* is used by RDE precedent. Both plural and singular
# 201/106 families are live; all 1,049,024 distinct care-contact IDs overlap.
# Row identity: exact published-business duplicates collapse with SOURCE_DUPLICATE_COUNT;
# ROW_HASH is xxhash64 over the ordered published source-business columns. Nothing MERGEs on it.
# Person linkage retains unmatched rows and records an explicit PERSON_LINK_STATUS.
# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 10297778375253)
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
    "4_prod.bronze.map_mat_pregnancy",
    "4_prod.raw.msds201carecontactpreg",
    "4_prod.raw.msds106diagnosispreg",
    "3_lookup.dwh.lkp_mill_dir_snomed",
    "4_prod.raw.msds301labdel",
    "4_prod.raw.msds401babydemo",
    "4_prod.raw.mill_person_alias",
]
TARGET_TABLES = [
    f"{SCHEMA}.map_maternity_care_contact",
    f"{SCHEMA}.map_maternity_diagnosis",
    f"{SCHEMA}.map_maternity_labour_delivery",
    f"{SCHEMA}.map_maternity_baby_delivery",
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

from pyspark.sql import functions as F, Window

AUDIT_COLUMNS = ["SOURCE_SYSTEM", "IS_VALID", "RECORD_UPDATED_DT", "ADC_UPDT"]

def collapse_exact(src, business_columns, extra_aggs=None):
    aggs = [F.count(F.lit(1)).alias("SOURCE_DUPLICATE_COUNT")]
    aggs.extend(F.max(F.col(c)).alias(c) for c in AUDIT_COLUMNS)
    if extra_aggs:
        aggs.extend(extra_aggs)
    collapsed = src.groupBy(*business_columns).agg(*aggs)
    return collapsed.withColumn(
        "ROW_HASH",
        F.xxhash64(*[F.col(c) for c in business_columns]),
    )

preg = (spark.table("4_prod.bronze.map_mat_pregnancy")
        .select(
            F.col("Pregnancy_ID").cast("bigint").alias("_PREGNANCY_ID"),
            F.col("Person_ID").cast("bigint").alias("_PREG_PERSON_ID"),
        ))

def add_pregnancy_link(df):
    return (df
            .withColumn("PREGNANCY_ID_PARSED", F.expr("try_cast(PREGNANCYID AS BIGINT)"))
            .join(
                preg,
                F.col("PREGNANCY_ID_PARSED") == F.col("_PREGNANCY_ID"),
                "left",
            )
            .withColumn(
                "PERSON_LINK_STATUS",
                F.when(F.col("PREGNANCY_ID_PARSED").isNull(), F.lit("UNPARSEABLE_ID"))
                 .when(F.col("_PREG_PERSON_ID").isNull(), F.lit("UNMATCHED_PREGNANCY"))
                 .otherwise(F.lit("LINKED")),
            )
            .withColumn("PERSON_ID", F.col("_PREG_PERSON_ID"))
            .drop("_PREGNANCY_ID", "_PREG_PERSON_ID"))

def publish(df, table_name):
    (df.withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
       .write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{SCHEMA}.{table_name}"))

# COMMAND ----------

contact_business = [
    "CARECONID", "PREGNANCYID", "CCONTACTDATETIME", "ORGIDCOMM", "ADMINCATCODE",
    "CONTACTDURATION", "CONSULTTYPE", "CCSUBJECT", "MEDIUM", "LOCCODE",
    "ORGSITEIDOFTREAT", "GPTHERAPYIND", "ATTENDCODE", "CANCELDATE", "CANCELREASON",
    "REPLAPPTOFFDATE", "REPLAPPTDATE",
]
contact = collapse_exact(
    spark.table("4_prod.raw.msds201carecontactpreg"),
    contact_business,
)
contact = add_pregnancy_link(contact)
contact = dq_columns(
    contact,
    ["CCONTACTDATETIME", "CANCELDATE", "REPLAPPTOFFDATE", "REPLAPPTDATE"],
)
publish(contact, "map_maternity_care_contact")

# COMMAND ----------

diagnosis_business = [
    "PREGNANCYID", "DIAGSCHEME", "DIAG", "DIAGDATE",
    "COMPLICATINGDIAGIND", "LOCALFETALID", "FETALORDER",
]
diagnosis = collapse_exact(
    spark.table("4_prod.raw.msds106diagnosispreg"),
    diagnosis_business,
)
diagnosis = add_pregnancy_link(diagnosis)

snomed_window = (
    Window.partitionBy("SNOMED_CD")
    .orderBy(
        F.col("UPDT_DT_TM").desc_nulls_last(),
        F.col("NOMENCLATURE_ID").asc_nulls_last(),
    )
)
snomed = (spark.table("3_lookup.dwh.lkp_mill_dir_snomed")
          .withColumn("_RN", F.row_number().over(snomed_window))
          .filter(F.col("_RN") == 1)
          .select(
              F.col("SNOMED_CD").cast("string").alias("_SNOMED_CD"),
              F.col("SOURCE_STRING").alias("DIAG_SNOMED_DESC"),
          ))
diagnosis = (diagnosis.join(
    snomed,
    F.col("DIAG") == F.col("_SNOMED_CD"),
    "left",
).drop("_SNOMED_CD"))
diagnosis = dq_columns(diagnosis, ["DIAGDATE"])
publish(diagnosis, "map_maternity_diagnosis")

# COMMAND ----------

labour_business = [
    "LABOURDELIVERYID", "PREGNANCYID", "ORGSITEIDINTRA", "SETTINGINTRACARE",
    "REASONCHANGEDELSETTINGLAB", "LABOURONSETMETHOD", "LABOURONSETDATETIME",
    "CAESAREANDATETIME", "STARTDATETIMEMOTHERDELIVERYHPS", "DECISIONTODELIVERDATETIME",
    "ADMMETHCODEMOTHDELHSP", "DISCHARGEDATETIMEMOTHERHSP",
    "DISCHMETHCODEMOTHPOSTDELHSP", "DISCHDESTCODEMOTHPOSTDELHSP",
    "ORGIDPOSTNATALPATHLEADPROVIDER", "ROMDATETIME", "ROMMETHOD", "ROMREASON",
    "LABOURONSETSECONDSTAGEDATETIME", "LABOURTHIRDSTAGEENDDATETIME",
    "EPISIOTOMYREASON", "PLACENTADELIVERYMETHOD", "LABOURONSETPRESENTATION",
]
labour = collapse_exact(
    spark.table("4_prod.raw.msds301labdel"),
    labour_business,
)
labour = add_pregnancy_link(labour)
labour = dq_columns(
    labour,
    [
        "LABOURONSETDATETIME",
        "CAESAREANDATETIME",
        "DECISIONTODELIVERDATETIME",
        "ROMDATETIME",
        "DISCHARGEDATETIMEMOTHERHSP",
        "STARTDATETIMEMOTHERDELIVERYHPS",
        "LABOURONSETSECONDSTAGEDATETIME",
        "LABOURTHIRDSTAGEENDDATETIME",
    ],
)
publish(labour, "map_maternity_labour_delivery")

# COMMAND ----------

baby_business = [
    "LABOURDELIVERYID", "LOCALFETALID", "BIRTHORDERMATERNITYSUS",
    "PERSONBIRTHDATETIMEBABY", "BABYBIRTHDATETIME", "PREGOUTCOME",
    "PERSONPHENSEX", "ETHNICCATEGORYBABY", "PERSONDEATHDATETIMEBABY",
    "FETUSPRESENTATION", "GESTATIONLENGTHBIRTH", "DELIVERYMETHODCODE",
    "WATERDELIVERYIND", "ORGSITEIDACTUALDELIVERY", "SETTINGPLACEBIRTH",
    "CAREPROFLIDDEL", "BABYFIRSTFEEDDATETIME", "BABYFIRSTFEEDINDCODE",
    "BABYFIRSTFEEDBREASTMILKSTATUS", "BABYBREASTMILKSTATUSDISCHARGE",
    "SKINTOSKINCONTACT1HOURIND", "DISCHARGEDATETIMEBABYHSP", "BIRTHWEIGHT",
    "APGARSCORE5", "PLACETYPEACTUALDELIVERY", "PLACETYPEACTUALMIDWIFERY",
]
src401 = spark.table("4_prod.raw.msds401babydemo")
nhs_norm = F.regexp_replace(F.trim(F.col("NHSNUMBERBABY")), " ", "")
baby = collapse_exact(
    src401,
    baby_business,
    [
        F.countDistinct(F.when(nhs_norm != "", nhs_norm)).alias("_NHS_VALUE_COUNT"),
        F.min(F.when(nhs_norm != "", nhs_norm)).alias("_NHS_VALUE"),
    ],
)
baby = baby.withColumn(
    "_NHS_LINK_VALUE",
    F.when(F.col("_NHS_VALUE_COUNT") == 1, F.col("_NHS_VALUE")),
)

labour_link = (spark.table("4_prod.raw.msds301labdel")
               .select(
                   "LABOURDELIVERYID",
                   "PREGNANCYID",
                   F.lit(True).alias("_LABOUR_FOUND"),
               ))
baby = (baby.join(labour_link, "LABOURDELIVERYID", "left")
        .withColumn("PREGNANCY_ID_PARSED", F.expr("try_cast(PREGNANCYID AS BIGINT)"))
        .join(
            preg,
            F.col("PREGNANCY_ID_PARSED") == F.col("_PREGNANCY_ID"),
            "left",
        )
        .withColumn(
            "PERSON_LINK_STATUS",
            F.when(F.col("_LABOUR_FOUND").isNull(), F.lit("UNMATCHED_LABOUR"))
             .when(F.col("PREGNANCY_ID_PARSED").isNull(), F.lit("UNPARSEABLE_ID"))
             .when(F.col("_PREG_PERSON_ID").isNull(), F.lit("UNMATCHED_PREGNANCY"))
             .otherwise(F.lit("LINKED")),
        )
        .withColumn("PERSON_ID", F.col("_PREG_PERSON_ID"))
        .drop("_LABOUR_FOUND", "_PREGNANCY_ID", "_PREG_PERSON_ID"))

alias = (spark.table("4_prod.raw.mill_person_alias")
         .filter(
             (F.col("PERSON_ALIAS_TYPE_CD") == 18) &
             (F.col("ACTIVE_IND") == 1)
         )
         .withColumn("_ALIAS_NORM", F.regexp_replace(F.trim(F.col("ALIAS")), " ", ""))
         .filter(F.col("_ALIAS_NORM") != "")
         .groupBy("_ALIAS_NORM")
         .agg(
             F.min(F.col("PERSON_ID")).alias("_BABY_PID"),
             F.countDistinct(F.col("PERSON_ID")).alias("_BABY_PID_COUNT"),
         ))
baby = (baby.join(
            alias,
            F.col("_NHS_LINK_VALUE") == F.col("_ALIAS_NORM"),
            "left",
        )
        .withColumn(
            "BABY_PERSON_ID",
            F.when(F.col("_BABY_PID_COUNT") == 1, F.col("_BABY_PID").cast("bigint")),
        )
        .drop(
            "_NHS_VALUE_COUNT", "_NHS_VALUE", "_NHS_LINK_VALUE",
            "_ALIAS_NORM", "_BABY_PID", "_BABY_PID_COUNT",
        ))
baby = dq_columns(
    baby,
    [
        "PERSONBIRTHDATETIMEBABY",
        "BABYBIRTHDATETIME",
        "PERSONDEATHDATETIMEBABY",
        "BABYFIRSTFEEDDATETIME",
        "DISCHARGEDATETIMEBABYHSP",
    ],
)
publish(baby, "map_maternity_baby_delivery")

# COMMAND ----------

spark.sql(f"""
    COMMENT ON TABLE {SCHEMA}.map_maternity_care_contact IS
    'MSDS maternity care contacts from the plural msds201 family. Grain: one row per unique 17-column published business record; SOURCE_DUPLICATE_COUNT accounts for exact business duplicates and ROW_HASH is its deterministic row identifier. Both singular and plural families were live on 2026-08-11 with complete distinct CARECONID overlap; plural is chosen by RDE precedent. All rows are retained. PERSON_LINK_STATUS is LINKED, UNMATCHED_PREGNANCY, or UNPARSEABLE_ID. CTRL_ID is excluded as ingestion control metadata. Refresh is deterministic full replacement.'
""")
spark.sql(f"""
    COMMENT ON TABLE {SCHEMA}.map_maternity_diagnosis IS
    'MSDS maternity diagnoses from the plural msds106 family. Grain: one row per unique published seven-column business record with duplicate accounting and ROW_HASH. All rows are retained and linked to pregnancy/person when possible. DIAG_SNOMED_DESC uses the latest lookup timestamp with NOMENCLATURE_ID ascending as an explicit deterministic tie-breaker because the live lookup contains same-timestamp ties. Refresh is deterministic full replacement.'
""")
spark.sql(f"""
    COMMENT ON TABLE {SCHEMA}.map_maternity_labour_delivery IS
    'MSDS mother-side labour and delivery sidecar from msds301. Grain: one row per unique published 23-column business record with duplicate accounting and ROW_HASH. This table closes mother-side rde_msds_delivery fields without altering live map_mat tables. PERSON_LINK_STATUS preserves unmatched pregnancy rows. Refresh is deterministic full replacement.'
""")
spark.sql(f"""
    COMMENT ON TABLE {SCHEMA}.map_maternity_baby_delivery IS
    'MSDS baby-side delivery sidecar from msds401 chained through LABOURDELIVERYID to msds301 and then pregnancy/person. Grain: one row per unique published baby business record with duplicate accounting and ROW_HASH. PERSON_LINK_STATUS includes UNMATCHED_LABOUR, UNPARSEABLE_ID, UNMATCHED_PREGNANCY, and LINKED. NHSNUMBERBABY, NHSNUMBERSTATUSBABY, LPIDBABY, and ORGIDLOCALPATIENTIDBABY are build-only identifier exclusions and are never published; a uniquely resolved active type-18 NHS alias may populate BABY_PERSON_ID. This table supplies birth-setting/site and first-feed closure fields. Refresh is deterministic full replacement.'
""")
print("four maternity MSDS products replaced")
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
#    (or an existing domain pipeline) and register all four retained-column contracts through
#    _bronze_common / bronze_assert_primary_contract.
# 2. Set target_schema=4_prod.bronze in the integrated prod notebook.
# 3. Run the deterministic build once there, then rerun every A6 gate against prod.
# 4. Register the step in bronze_pipeline after map_mat_pregnancy and set task retries deliberately.
# 5. Verify the next weekly run against the measured S1 steady-state runtime.

dbutils.notebook.exit(json.dumps({"result": "BUILT", "target": TARGET_SCHEMA, "target_schema": TARGET_SCHEMA}, sort_keys=True))


