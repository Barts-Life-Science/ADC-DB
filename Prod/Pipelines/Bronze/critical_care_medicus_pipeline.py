# Databricks notebook source
# critical_care_medicus_pipeline - S2/A4. Design decisions: plan 2026-08-12 v2 Task 3.
# Union-first across seven byte-identical pat/daily schemas. Provenance is SOURCE_UNIT/SOURCE_SITE.
# Admission key is (SOURCE_UNIT, SOURCE_PAT_ID). Daily score key is
# (SOURCE_UNIT, SOURCE_DAILY_ID, SCORE_TYPE), with NONE_RECORDED marker rows.
# DAY_LATEST_IND is selected before score explode. NULL-date NHITU CCMDS stubs are retained.
# NHS linkage uses unique active alias type 18; identifiers and dgn_ult_code are excluded.
# Refresh is source-version due-check plus deterministic rebuild with tombstone carry-forward.
# Date-quality flags are derived by type; UPDATED_AT/updated_at and ADC_UPDT are admin stamps.

# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 681039215772972)
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
CONTROL = f"{CONTROL_SCHEMA}.s2_source_versions"
FULL = (
    _widget_text("full_rebuild", "false").lower() in ("1", "true", "yes")
    or _widget_text("force_full_refresh", "false").lower() == "true"
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

# ==== S2 BLOCK v1 (SYNC-WITH _completeness_common) ====
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DateType

def table_version(tbl):
    """Current Delta commit version (metadata-only read)."""
    return spark.sql(f"DESCRIBE HISTORY {tbl} LIMIT 1").collect()[0]["version"]

def due_check(control_table, pipeline, sources):
    """Master plan S2.3 rule-4 due-check. Returns (due, current_versions): due=False iff
    EVERY source table's Delta version matches the last recorded successful run.
    Per-source rows (rule 5) - never a combined high-watermark."""
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {control_table}
        (pipeline STRING, source STRING, version BIGINT, updated_at TIMESTAMP)""")
    cur = {t: table_version(t) for t in sources}
    seen = {r["source"]: r["version"] for r in spark.sql(
        f"SELECT source, version FROM {control_table} WHERE pipeline = '{pipeline}'").collect()}
    return any(seen.get(t) != v for t, v in cur.items()), cur

def record_versions(control_table, pipeline, versions):
    """Call ONLY after a successful publish - a crashed run must re-run in full."""
    for t, v in versions.items():
        spark.sql(f"""MERGE INTO {control_table} c
            USING (SELECT '{pipeline}' pipeline, '{t}' source, CAST({v} AS BIGINT) version) s
            ON c.pipeline = s.pipeline AND c.source = s.source
            WHEN MATCHED THEN UPDATE SET c.version = s.version, c.updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (pipeline, source, version, updated_at)
                 VALUES (s.pipeline, s.source, s.version, current_timestamp())""")

def dq_all_clinical(df, admin_stamps):
    """S2.2 date-quality standard, v2 rule: flag EVERY retained temporal column except the
    product's NAMED admin/system stamps (the declared contract) and derived *_CLEAN columns.
    Returns (df_with_flags, flagged_column_list) - log the list in the session log."""
    cols = [f.name for f in df.schema.fields
            if isinstance(f.dataType, (TimestampType, DateType))
            and f.name not in admin_stamps and not f.name.endswith("_CLEAN")]
    return dq_columns(df, cols), cols

def replace_with_tombstones(df, target, key_cols):
    """Deterministic replace with NO silent hard deletes (S2.2 lifecycle): rows present in
    the prior published version but absent from the fresh build are re-appended with
    SOURCE_PRESENT_IND=false, retaining their previous column values and stamps.
    A key that reappears at source is resurrected as present (its tombstone drops out)."""
    fresh = df.withColumn("SOURCE_PRESENT_IND", F.lit(True))
    v_prev = table_version(target) if spark.catalog.tableExists(target) else None
    (fresh.write.format("delta").mode("overwrite")
          .option("overwriteSchema", "true").saveAsTable(target))
    if v_prev is not None:
        prior = spark.read.option("versionAsOf", v_prev).table(target)
        gone = (prior.join(spark.table(target).select(*key_cols).distinct(),
                           key_cols, "left_anti")
                     .withColumn("SOURCE_PRESENT_IND", F.lit(False)))
        gone.write.format("delta").mode("append").saveAsTable(target)

def table_fingerprint(tbl, exclude=("PIPELINE_UPDT_DT_TM",)):
    """Canonical whole-row fingerprint: order-independent sum of xxhash64 over the JSON of
    every column except volatile stamps. Equal fingerprint == identical published content."""
    cols = [c for c in spark.table(tbl).columns if c not in exclude]
    return (spark.table(tbl)
            .select(F.sum(F.xxhash64(F.to_json(F.struct(*[F.col(c) for c in cols])))
                          .cast("decimal(38,0)")).alias("fp"))
            .collect()[0]["fp"])
# ==== END S2 BLOCK v1 ====

# COMMAND ----------

from pyspark.sql import functions as F, Window

UNITS = ["mtitu", "nhitu", "orbt", "whhdu", "whitu", "wohdu", "wxitu"]
SOURCES = [f"4_prod.medicus.medicus_{u}_{s}" for u in UNITS for s in ("pat", "daily")] \
        + ["4_prod.raw.mill_person_alias"]
PIPE = "medicus"
due, versions = due_check(CONTROL, PIPE, SOURCES)
if not (due or FULL):
    print("NO_OP: no source version advanced; targets untouched")
    dbutils.notebook.exit(json.dumps({"result": "NO_OP", "target": TARGET_SCHEMA, "target_schema": TARGET_SCHEMA}, sort_keys=True))

def read_union(suffix):
    out = None
    for u in UNITS:
        d = spark.table(f"4_prod.medicus.medicus_{u}_{suffix}")
        out = d if out is None else out.unionByName(d)
    return out

alias18 = (spark.table("4_prod.raw.mill_person_alias")
    .filter("PERSON_ALIAS_TYPE_CD = 18 AND ACTIVE_IND = 1")
    .withColumn("ALIAS_N", F.trim(F.col("ALIAS")))
    .groupBy("ALIAS_N").agg(F.min("PERSON_ID").alias("_PID"),
                            F.countDistinct("PERSON_ID").alias("_NP")))

pat = (read_union("pat")
    .withColumn("NHS_N", F.regexp_replace(F.col("nhs_number"), " ", ""))
    .withColumn("_valid", F.col("NHS_N").rlike("^[0-9]{10}$"))
    .join(alias18, F.col("NHS_N") == F.col("ALIAS_N"), "left")
    .withColumn("PERSON_ID", F.when(F.col("_valid") & (F.col("_NP") == 1), F.col("_PID")).cast("bigint"))
    .withColumn("PERSON_LINK_STATUS",
        F.when(~F.coalesce(F.col("_valid"), F.lit(False)), "NO_VALID_NHS")
         .when(F.col("PERSON_ID").isNotNull(), "LINKED")
         .when(F.col("_NP") > 1, "AMBIGUOUS_NHS")
         .otherwise("UNMATCHED_NHS"))
    .drop("ALIAS_N", "_PID", "_NP", "NHS_N", "_valid", "nhs_number", "hospital_number", "dgn_ult_code"))

pat, pat_dq_cols = dq_all_clinical(pat, admin_stamps={"updated_at", "ADC_UPDT"})
admission = (pat
    .withColumnRenamed("id", "SOURCE_PAT_ID").withColumnRenamed("unit", "UNIT_RAW")
    .withColumnRenamed("source_unit", "SOURCE_UNIT").withColumnRenamed("source_site", "SOURCE_SITE")
    .withColumn("ADMISSION_KEY", F.concat_ws(":", F.col("SOURCE_UNIT"), F.col("SOURCE_PAT_ID")))
    .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp()))
replace_with_tombstones(admission, f"{SCHEMA}.map_critical_care_admission",
                        ["SOURCE_UNIT", "SOURCE_PAT_ID"])

SCORES = [("apache_ii", "APACHE_II"), ("apache_iii", "APACHE_III"), ("saps_ii", "SAPS_II"),
          ("sofa", "SOFA"), ("lods", "LODS"), ("cpis", "CPIS"), ("icnarc_score", "ICNARC_SCORE"),
          ("icnarc_probability", "ICNARC_PROBABILITY"), ("ccmdstotalorgsupp", "CCMDS_TOTAL_ORG_SUPPORT")]
entry_exprs = ", ".join(
    f"named_struct('SCORE_TYPE', '{lbl}', 'SCORE_VALUE', CAST(try_cast({c} AS DOUBLE) AS DOUBLE), "
    f"'SCORE_VALUE_RAW', CAST({c} AS STRING))" for c, lbl in SCORES)
daily = read_union("daily")
w_day = (Window.partitionBy("source_unit", "pat_id", "date_daily")
         .orderBy(F.col("updated_at").desc_nulls_last(), F.col("id").desc()))
daily = (daily
    .withColumn("DAY_LATEST_IND", F.row_number().over(w_day) == 1)
    .withColumn("ROW_CLASS", F.when(F.col("date_daily").isNull(), "CCMDS_STUB").otherwise("DAILY"))
    .withColumn("_entries", F.expr(
        f"filter(array({entry_exprs}), "
        "s -> s.SCORE_VALUE IS NOT NULL OR (s.SCORE_VALUE_RAW IS NOT NULL AND s.SCORE_VALUE_RAW <> ''))"))
    .withColumn("_entries", F.expr(
        "CASE WHEN size(_entries) > 0 THEN _entries ELSE array(named_struct("
        "'SCORE_TYPE', 'NONE_RECORDED', 'SCORE_VALUE', CAST(NULL AS DOUBLE), "
        "'SCORE_VALUE_RAW', CAST(NULL AS STRING))) END")))
long = (daily.select(
        F.col("source_unit").alias("SOURCE_UNIT"), F.col("source_site").alias("SOURCE_SITE"),
        F.col("id").alias("SOURCE_DAILY_ID"), F.col("pat_id").alias("SOURCE_PAT_ID"),
        F.concat_ws(":", F.col("source_unit"), F.col("pat_id")).alias("ADMISSION_KEY"),
        F.col("date_daily").alias("DATE_DAILY"), F.col("date_score_calc").alias("DATE_SCORE_CALC"),
        F.col("updated_at").alias("UPDATED_AT"), "DAY_LATEST_IND", "ROW_CLASS", "ADC_UPDT",
        F.explode("_entries").alias("s"))
    .select("*", "s.SCORE_TYPE", "s.SCORE_VALUE", "s.SCORE_VALUE_RAW").drop("s"))
adm_person = admission.select("ADMISSION_KEY", "PERSON_ID", "PERSON_LINK_STATUS")
long = long.join(adm_person, "ADMISSION_KEY", "left")
long, daily_dq_cols = dq_all_clinical(long, admin_stamps={"UPDATED_AT", "ADC_UPDT"})
long = long.withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
replace_with_tombstones(long, f"{SCHEMA}.map_critical_care_daily_score",
                        ["SOURCE_UNIT", "SOURCE_DAILY_ID", "SCORE_TYPE"])

spark.sql(f"""COMMENT ON TABLE {SCHEMA}.map_critical_care_admission IS
'Grain: one row per SOURCE_UNIT and SOURCE_PAT_ID. ADMISSION_KEY is the composite source identity. NHS and hospital identifiers are excluded after unique active alias-18 linkage. PERSON_LINK_STATUS records linkage outcome. SOURCE_PRESENT_IND preserves source hard deletes as tombstones. Unit identity comes from SOURCE_UNIT and SOURCE_SITE; UNIT_RAW is ungoverned source text. MEDICUS_WOHDU is stale since 2025-12-23; historical depth to 2002 exists only for NHITU and ORBT. Refresh is per-source Delta-version due-check followed by deterministic rebuild. If Journey later registers this table, coordinate before rebuild-on-change refresh and likely convert to keyed MERGE.'""")
spark.sql(f"""COMMENT ON TABLE {SCHEMA}.map_critical_care_daily_score IS
'Grain: one row per SOURCE_UNIT, SOURCE_DAILY_ID, SCORE_TYPE. Long form contains nine score types; NONE_RECORDED is an explicit marker so every source daily row is represented. DAY_LATEST_IND is computed on source daily rows by UPDATED_AT descending nulls last then ID descending. NULL DATE_DAILY rows remain as CCMDS_STUB. PERSON_ID is inherited from admission. SOURCE_PRESENT_IND preserves source hard deletes as tombstones. UPDATED_AT and ADC_UPDT are declared admin stamps; other temporal fields carry quality flags. Refresh is per-source Delta-version due-check followed by deterministic rebuild. If Journey later registers this table, coordinate before rebuild-on-change refresh and likely convert to keyed MERGE.'""")
record_versions(CONTROL, PIPE, versions)
print(f"BUILD done; dq flagged: pat={pat_dq_cols}, daily={daily_dq_cols}")

# COMMAND ----------

# PROMOTION RUNBOOK (human-gated)
# 1. Copy into /Workspace/Shared/ADC-DB/Prod/Pipelines/Bronze/ and integrate retained-column
#    contracts into _bronze_common before the first production write. Fold into
#    completeness_pipeline if S1 promotion created it; otherwise the promoter chooses.
# 2. Set target_schema=4_prod.bronze and set control_table to the production control
#    location returned by the bronze_control_schema() convention. The config guard rejects
#    a production target paired with the dev control table.
# 3. Run one full build from the integrated production notebook, then rerun all A4 gates
#    against production.
# 4. Register after dependencies in bronze_pipeline orchestration. retries=0 is safe:
#    due-check plus deterministic rebuild makes a retry a no-op or a clean rebuild.
# 5. Compare the next weekly runtime with the S2 execution-log baseline.
# 6. Interface caveat: these products rebuild on source change. If Journey registers one
#    in silver_source_registry.py, coordinate under interface rule 1 before the next rebuild
#    and likely convert the target to keyed MERGE.

dbutils.notebook.exit(json.dumps({"result": "BUILT", "target": TARGET_SCHEMA, "target_schema": TARGET_SCHEMA}, sort_keys=True))


