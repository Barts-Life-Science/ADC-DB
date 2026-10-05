# Databricks notebook source
# Bronze encounter/GP practitioner relationships (OGR); scaffold copied from episode_pipeline; helper blocks are synced from _completeness_common.


def _text_widget(name, default):
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)


# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 603045651153504)
for _name, _default in [
    ("target_schema", "8_dev.bronze"),
    ("allow_production_write", "false"),
    ("force_rebuild", "false"),
    ("force_full_refresh", "false"),
    ("gates_only", "false"),
    ("interrupt_after_episode", "false"),
    ("expect_first_build_all_present", "false"),
    ("run_full_gates", "true"),
]:
    _text_widget(_name, _default)

TARGET_SCHEMA = dbutils.widgets.get("target_schema").strip()
ALLOW_PROD_WRITE = dbutils.widgets.get("allow_production_write").lower() == "true"
assert TARGET_SCHEMA.startswith("8_dev.") or ALLOW_PROD_WRITE, (
    f"Refusing to write {TARGET_SCHEMA} without allow_production_write=true")
CONTROL_SCHEMA = "6_mgmt.bronze" if TARGET_SCHEMA == "4_prod.bronze" else TARGET_SCHEMA
FORCE_REBUILD = (
    dbutils.widgets.get("force_rebuild").lower() == "true"
    or dbutils.widgets.get("force_full_refresh").lower() == "true"
)
GATES_ONLY = dbutils.widgets.get("gates_only").lower() == "true"
INTERRUPT_AFTER_EPISODE = dbutils.widgets.get("interrupt_after_episode").lower() == "true"
EXPECT_FIRST_BUILD_ALL_PRESENT = (
    dbutils.widgets.get("expect_first_build_all_present").lower() == "true"
)
RUN_FULL_GATES = dbutils.widgets.get("run_full_gates").lower() == "true"

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

# ==== S6b BLOCK v1 (SYNC-WITH _completeness_common) ====
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
S6B_SOURCE_VERSIONS = f"{CONTROL_SCHEMA}.s6b_source_versions"

def lookup_counterpart_tags(col_name):
    """Modal (ig_risk, ig_severity) for this column name across 4_prod.bronze — copied, never guessed.
    Returns None when no counterpart exists (caller must then decide explicitly)."""
    col_lit = col_name.replace("'", "''")
    rows = (spark.sql(f"""
        SELECT MAX(CASE WHEN tag_name='ig_risk' THEN tag_value END) r,
               MAX(CASE WHEN tag_name='ig_severity' THEN tag_value END) s, COUNT(*) n
        FROM `4_prod`.information_schema.column_tags
        WHERE schema_name='bronze' AND upper(column_name)=upper('{col_lit}')
        GROUP BY table_name""")
        .groupBy("r", "s").count()
        .orderBy(F.desc("count"), F.asc("r"), F.asc("s"))
        .collect())
    return (rows[0]["r"], rows[0]["s"]) if rows else None

def ig_tag_table(table, tag_map, default=('0', '0')):
    """tag_map is REQUIRED for every identifier/free-text column (direct identifiers = ('4','2')).
    Other columns: counterpart lookup, else default — and every defaulted column is PRINTED for
    the promoter to eyeball (never silently 0/0 an identifier)."""
    cols = [r.col_name for r in spark.sql(f"DESCRIBE {table}").collect()
            if r.col_name and not r.col_name.startswith('#')]
    for c in cols:
        if c in tag_map:
            risk, sev = tag_map[c]
        else:
            found = lookup_counterpart_tags(c)
            risk, sev = found if found else default
            if not found:
                print(f"IG-TAG DEFAULTED {table}.{c} -> {default} — REVIEW")
        assert risk is not None and sev is not None, (
            f"Incomplete counterpart tags for {table}.{c}: ig_risk={risk}, ig_severity={sev}")
        col_ident = c.replace("`", "``")
        risk_lit = str(risk).replace("'", "''")
        sev_lit = str(sev).replace("'", "''")
        spark.sql(
            f"ALTER TABLE {table} ALTER COLUMN `{col_ident}` "
            f"SET TAGS ('ig_risk'='{risk_lit}','ig_severity'='{sev_lit}')")

def ig_tag_gate(table):
    """Fail when any table column is missing either required IG tag."""
    cat, sch, tbl = table.split('.')
    sch_lit = sch.replace("'", "''")
    tbl_lit = tbl.replace("'", "''")
    cat_ident = cat.replace("`", "``")
    row = spark.sql(f"""
        WITH cols AS (
          SELECT column_name
          FROM `{cat_ident}`.information_schema.columns
          WHERE table_schema='{sch_lit}' AND table_name='{tbl_lit}'
        ),
        risk_tagged AS (
          SELECT DISTINCT column_name
          FROM `{cat_ident}`.information_schema.column_tags
          WHERE schema_name='{sch_lit}' AND table_name='{tbl_lit}' AND tag_name='ig_risk'
        ),
        severity_tagged AS (
          SELECT DISTINCT column_name
          FROM `{cat_ident}`.information_schema.column_tags
          WHERE schema_name='{sch_lit}' AND table_name='{tbl_lit}' AND tag_name='ig_severity'
        )
        SELECT
          COALESCE(SUM(CASE WHEN r.column_name IS NULL THEN 1 ELSE 0 END), 0) AS missing_risk,
          COALESCE(SUM(CASE WHEN s.column_name IS NULL THEN 1 ELSE 0 END), 0) AS missing_severity,
          COALESCE(SUM(CASE WHEN r.column_name IS NULL OR s.column_name IS NULL THEN 1 ELSE 0 END), 0)
            AS missing_either
        FROM cols c
        LEFT JOIN risk_tagged r ON c.column_name = r.column_name
        LEFT JOIN severity_tagged s ON c.column_name = s.column_name
        """).collect()[0]
    missing_risk = int(row["missing_risk"])
    missing_severity = int(row["missing_severity"])
    missing_either = int(row["missing_either"])
    assert missing_either == 0, (
        f"{missing_either} columns on {table} missing ig_risk and/or ig_severity "
        f"({missing_risk} missing ig_risk; {missing_severity} missing ig_severity)")

def present_filtered_count(table):
    """Row count of the CURRENT source view of a tombstoned product —
    the only count comparable to a raw/source count once tombstones exist."""
    return spark.sql(
        f"SELECT COUNT(*) c FROM {table} WHERE SOURCE_PRESENT_IND"
    ).collect()[0]["c"]

# ==== END S6b BLOCK v1 ====

# COMMAND ----------

import json
from pyspark.sql import Window
import pyspark.sql.functions as F

# OGR_EPRL_V1: encounter and registered-GP practitioner relationships for silver care_participation.
# TLA_BRONZE_FIX_V1: role filters try_cast - raw PERSON_PRSNL_R_CD holds 5 corrupt 1e84..1e113 values
# from the 2024-08-06 initial load, which overflow a BIGINT cast under ANSI mode.
TARGET_SCHEMA = dbutils.widgets.get("target_schema").strip()
SRC_EPRL = "4_prod.raw.mill_encntr_prsnl_reltn"
SRC_PPRL = "4_prod.raw.mill_person_prsnl_reltn"
SRC_ENCOUNTER = "4_prod.raw.mill_encounter"
CODE_VALUE = "3_lookup.mill.mill_code_value"
CONTROL_TABLE = f"{CONTROL_SCHEMA}.s6b_source_versions"
PIPELINE = "encounter_practitioner_pipeline"
ADMIN_STAMPS = {"UPDT_DT_TM", "LAST_UTC_TS", "ADC_UPDT"}
# Code set 333: attending, admitting, consulting, referring, locum attending, locum admitting, MH attending.
ENCOUNTER_ROLE_CODES = (1119, 1116, 1121, 1126, 673962, 673961, 435702461)
GP_ROLE_CODE = 1115  # code set 331 PCP "Registered GP"
T_EPRL = f"{TARGET_SCHEMA}.map_encounter_practitioner"
T_GP = f"{TARGET_SCHEMA}.map_person_gp"
# ig (risk, severity); staff names follow map_medical_personnel (4, 3); ids and dates (1, 1).
EPRL_TAGS = {"ENCNTR_PRSNL_RELTN_ID": (0, 0), "ENCNTR_ID": (1, 1), "PERSON_ID": (1, 1),
             "PRSNL_PERSON_ID": (1, 1), "FT_PRSNL_NAME": (4, 3),
             "BEG_EFFECTIVE_DT_TM": (1, 1), "END_EFFECTIVE_DT_TM": (1, 1)}
GP_TAGS = {"PERSON_PRSNL_RELTN_ID": (0, 0), "PERSON_ID": (1, 1), "PRSNL_PERSON_ID": (1, 1),
           "FT_PRSNL_NAME": (4, 3), "BEG_EFFECTIVE_DT_TM": (1, 1), "END_EFFECTIVE_DT_TM": (1, 1)}


def _latest(df, key):
    """One row per key: latest LAST_UTC_TS, then UPDT_CNT, then ADC_UPDT (NULLS LAST). Returns (df, dup_count)."""
    order = Window.partitionBy(key).orderBy(
        F.col("LAST_UTC_TS").desc_nulls_last(), F.col("UPDT_CNT").desc_nulls_last(),
        F.col("ADC_UPDT").desc_nulls_last())
    ranked = df.withColumn("_rn", F.row_number().over(order))
    dups = ranked.where(F.col("_rn") > 1).count()
    return ranked.where(F.col("_rn") == 1).drop("_rn"), dups


def _decode(df, code_col, desc_col):
    # Bare FK decode: code_value rows are not filtered on validity (project_mill_code_lookup_validity_split).
    cv = spark.table(CODE_VALUE).select(F.col("CODE_VALUE").cast("bigint").alias("_cv"),
                                        F.col("DISPLAY").alias(desc_col))
    return df.join(cv, F.col(code_col) == F.col("_cv"), "left").drop("_cv")


def _publish(df, target, key_cols, comment, tags):
    replace_with_tombstones(df, target, key_cols)
    spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', "
              "'delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')")
    spark.sql(f"COMMENT ON TABLE {target} IS '{comment}'")
    ig_tag_table(target, {c: (str(r), str(s)) for c, (r, s) in tags.items()}, default=("0", "0"))
    ig_tag_gate(target)


def build_encounter_practitioner():
    raw, dups = _latest(spark.table(SRC_EPRL).where(
        F.expr("try_cast(ENCNTR_PRSNL_R_CD AS BIGINT)").isin(*ENCOUNTER_ROLE_CODES)), "ENCNTR_PRSNL_RELTN_ID")
    enc, _ = _latest(spark.table(SRC_ENCOUNTER).select("ENCNTR_ID", "PERSON_ID", "LAST_UTC_TS", "UPDT_CNT", "ADC_UPDT"),
                     "ENCNTR_ID")
    enc = enc.select(F.col("ENCNTR_ID").cast("bigint").alias("_e_id"), F.col("PERSON_ID").cast("bigint").alias("PERSON_ID"))
    df = (
        raw.select(
            F.col("ENCNTR_PRSNL_RELTN_ID").cast("bigint").alias("ENCNTR_PRSNL_RELTN_ID"),
            F.col("ENCNTR_ID").cast("bigint").alias("ENCNTR_ID"),
            F.col("PRSNL_PERSON_ID").cast("bigint").alias("PRSNL_PERSON_ID"),
            F.col("ENCNTR_PRSNL_R_CD").cast("bigint").alias("ENCNTR_PRSNL_R_CD"),
            F.col("ACTIVE_IND").cast("int").alias("ACTIVE_IND"),
            "BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM",
            F.col("PRIORITY_SEQ").cast("int").alias("PRIORITY_SEQ"),
            F.col("FREE_TEXT_CD").cast("bigint").alias("FREE_TEXT_CD"), "FT_PRSNL_NAME",
            F.col("CONTRIBUTOR_SYSTEM_CD").cast("bigint").alias("CONTRIBUTOR_SYSTEM_CD"),
            F.col("EXPIRATION_IND").cast("int").alias("EXPIRATION_IND"),
            "UPDT_DT_TM", "LAST_UTC_TS", "ADC_UPDT", "Trust",
        )
        .join(enc, F.col("ENCNTR_ID") == F.col("_e_id"), "left").drop("_e_id")
    )
    df = _decode(df, "ENCNTR_PRSNL_R_CD", "ENCNTR_PRSNL_R_DESC")
    df, _flagged = dq_all_clinical(df, ADMIN_STAMPS)
    df = (df.withColumn("ROW_HASH", F.xxhash64(*[F.col(c) for c in sorted(df.columns)]))
            .withColumn("SOURCE_DUPLICATE_COUNT", F.lit(dups).cast("bigint"))
            .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp()))
    _publish(df, T_EPRL, ["ENCNTR_PRSNL_RELTN_ID"],
             "Millennium encounter-clinician relationships (7 care roles, code set 333); feeds spine_care_participation.",
             EPRL_TAGS)
    return dups


def build_person_gp():
    raw, dups = _latest(spark.table(SRC_PPRL).where(F.expr("try_cast(PERSON_PRSNL_R_CD AS BIGINT)") == GP_ROLE_CODE),
                        "PERSON_PRSNL_RELTN_ID")
    df = raw.select(
        F.col("PERSON_PRSNL_RELTN_ID").cast("bigint").alias("PERSON_PRSNL_RELTN_ID"),
        F.col("PERSON_ID").cast("bigint").alias("PERSON_ID"),
        F.col("PRSNL_PERSON_ID").cast("bigint").alias("PRSNL_PERSON_ID"),
        F.col("PERSON_PRSNL_R_CD").cast("bigint").alias("PERSON_PRSNL_R_CD"),
        F.col("ACTIVE_IND").cast("int").alias("ACTIVE_IND"),
        "BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM",
        F.col("PRIORITY_SEQ").cast("int").alias("PRIORITY_SEQ"),
        "FT_PRSNL_NAME", "UPDT_DT_TM", "LAST_UTC_TS", "ADC_UPDT",
    )
    df = _decode(df, "PERSON_PRSNL_R_CD", "PERSON_PRSNL_R_DESC")
    df, _flagged = dq_all_clinical(df, ADMIN_STAMPS)
    df = (df.withColumn("ROW_HASH", F.xxhash64(*[F.col(c) for c in sorted(df.columns)]))
            .withColumn("SOURCE_DUPLICATE_COUNT", F.lit(dups).cast("bigint"))
            .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp()))
    _publish(df, T_GP, ["PERSON_PRSNL_RELTN_ID"],
             "Millennium registered-GP relationships (PERSON_PRSNL_RELTN code 1115); feeds spine_care_participation.",
             GP_TAGS)
    return dups


sources = [SRC_EPRL, SRC_PPRL, SRC_ENCOUNTER]
due, cur = due_check(CONTROL_TABLE, PIPELINE, sources)
missing = [t for t in (T_EPRL, T_GP) if not spark.catalog.tableExists(t)]
if FORCE_REBUILD or due or missing:
    dup_counts = {"eprl": build_encounter_practitioner(), "gp": build_person_gp()}
    record_versions(CONTROL_TABLE, PIPELINE, cur)
    mode = "FORCED_REBUILD" if FORCE_REBUILD else "BUILD"
else:
    dup_counts, mode = {}, "NO_OP"
dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "result": mode, "duplicates": dup_counts},
                                 sort_keys=True, default=str))

