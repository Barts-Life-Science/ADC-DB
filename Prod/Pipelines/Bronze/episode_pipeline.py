# Databricks notebook source
# Bronze S6b A13 episode plane. Self-contained; helper blocks are synced from _completeness_common.


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
    ("expect_first_build_all_present", "true"),
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
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

SRC_EPISODE = "4_prod.raw.mill_episode"
SRC_RELTN = "4_prod.raw.mill_episode_encntr_reltn"
SRC_ENCOUNTER = "4_prod.raw.mill_encounter"
CODE_VALUE = "3_lookup.mill.mill_code_value"
T_EPISODE = f"{TARGET_SCHEMA}.map_episode"
T_RELTN = f"{TARGET_SCHEMA}.map_episode_encounter"
CONTROL_TABLE = f"{CONTROL_SCHEMA}.s6b_source_versions"

EPISODE_ADMIN_STAMPS = {"UPDT_DT_TM", "LAST_UTC_TS", "ADC_UPDT"}
RELTN_ADMIN_STAMPS = {"UPDT_DT_TM", "LAST_UTC_TS", "ADC_UPDT"}

EPISODE_DECODES = [
    ("EPISODE_TYPE_CD", "EPISODE_TYPE_DESC"),
    ("EPISODE_STATUS_CD", "EPISODE_STATUS_DESC"),
    ("SERVICE_CATEGORY_CD", "SERVICE_CATEGORY_DESC"),
    ("REFER_FACILITY_CD", "REFER_FACILITY_DESC"),
    ("EPISODE_CLOSE_REASON_CD", "EPISODE_CLOSE_REASON_DESC"),
    ("CONTRIBUTOR_SYSTEM_CD", "CONTRIBUTOR_SYSTEM_DESC"),
    ("ACTIVE_STATUS_CD", "ACTIVE_STATUS_DESC"),
    ("DATA_STATUS_CD", "DATA_STATUS_DESC"),
]
RELTN_DECODES = [
    ("CONTRIBUTOR_SYSTEM_CD", "CONTRIBUTOR_SYSTEM_DESC"),
    ("ACTIVE_STATUS_CD", "ACTIVE_STATUS_DESC"),
]


def qname(name):
    quote = chr(96)
    return ".".join(quote + part.replace(quote, quote + quote) + quote for part in name.split("."))


def _safe_comment(value):
    return str(value).replace("'", "''")


def _cast_integral_identifiers(df):
    out = df
    for field in df.schema.fields:
        if isinstance(field.dataType, T.DoubleType) and (
            field.name.endswith("_ID") or field.name.endswith("_CD")
        ):
            out = out.withColumn(
                field.name,
                F.expr(f"try_cast({qname(field.name)} AS BIGINT)"),
            )
    return out


def _add_decodes(df, specs):
    out = df
    base = spark.table(CODE_VALUE).select(
        F.expr("try_cast(CODE_VALUE AS BIGINT)").alias("_CODE_VALUE"),
        F.coalesce(F.col("DESCRIPTION"), F.col("DISPLAY")).alias("_CODE_DESC"),
    )
    for idx, (code_col, desc_col) in enumerate(specs):
        code_key = f"__CODE_VALUE_{idx}"
        desc_key = f"__CODE_DESC_{idx}"
        lookup = (
            base.withColumnRenamed("_CODE_VALUE", code_key)
                .withColumnRenamed("_CODE_DESC", desc_key)
        )
        out = (
            out.join(
                F.broadcast(lookup),
                F.col(code_col) == F.col(code_key),
                "left",
            )
            .withColumn(desc_col, F.col(desc_key))
            .drop(code_key, desc_key)
        )
    return out


def _business_hash(df):
    excluded = {
        "SOURCE_PRESENT_IND",
        "PIPELINE_UPDT_DT_TM",
    }
    cols = sorted(
        c for c in df.columns
        if c not in excluded
        and not c.endswith("_FUTURE_IND")
        and not c.endswith("_SENTINEL_IND")
        and not c.endswith("_CLEAN")
        and c != "ROW_HASH"
    )
    return df.withColumn(
        "ROW_HASH",
        F.sha2(
            F.concat_ws(
                "§",
                *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in cols],
            ),
            256,
        ),
    )


def _enable_delta_contract(table):
    spark.sql(
        f"ALTER TABLE {qname(table)} SET TBLPROPERTIES ("
        "'delta.enableChangeDataFeed'='true',"
        "'delta.enableRowTracking'='true',"
        "'delta.enableDeletionVectors'='true',"
        "'delta.appendOnly'='false')"
    )


def _tag_product(table):
    explicit = {}
    for col_name in spark.table(table).columns:
        if col_name in {"DISPLAY"}:
            explicit[col_name] = ("2", "2")
        elif col_name in {"TXN_ID_TEXT"}:
            explicit[col_name] = ("3", "2")
        elif col_name.endswith("_ID") or col_name.endswith("_CD"):
            explicit[col_name] = lookup_counterpart_tags(col_name) or ("2", "2")
    ig_tag_table(table, explicit)


def _publish(df, target, key_cols, comment):
    replace_with_tombstones(df, target, key_cols)
    _enable_delta_contract(target)
    spark.sql(f"COMMENT ON TABLE {qname(target)} IS '{_safe_comment(comment)}'")
    _tag_product(target)


def build_episode():
    raw = spark.table(SRC_EPISODE)
    key_stats = raw.agg(
        F.count("*").alias("n"),
        F.countDistinct("EPISODE_ID").alias("d"),
        F.sum(F.col("EPISODE_ID").isNull().cast("long")).alias("nulls"),
    ).collect()[0]
    assert key_stats["n"] == key_stats["d"] and int(key_stats["nulls"] or 0) == 0, (
        f"Raw episode key defect: {key_stats.asDict()}"
    )
    staged = _cast_integral_identifiers(raw)
    staged = _add_decodes(staged, EPISODE_DECODES)
    staged, flagged = dq_all_clinical(staged, EPISODE_ADMIN_STAMPS)
    staged = (
        _business_hash(staged)
        .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
    )
    _publish(
        staged,
        T_EPISODE,
        ["EPISODE_ID"],
        "One row per EPISODE_ID from 4_prod.raw.mill_episode; direct ENCNTR_ID is retained alongside the N:M episode-encounter relationship product.",
    )
    print("[A13] episode date-quality columns=" + json.dumps(flagged))
    return flagged


def build_episode_encounter():
    raw = spark.table(SRC_RELTN)
    hashed = raw.withColumn(
        "ROW_SRC_HASH",
        F.sha2(
            F.concat_ws(
                "§",
                *[
                    F.coalesce(F.col(c).cast("string"), F.lit("∅"))
                    for c in raw.columns
                ],
            ),
            256,
        ),
    )
    divergent = (
        hashed.groupBy("EPISODE_ENCNTR_RELTN_ID")
        .agg(
            F.count("*").alias("n"),
            F.countDistinct("ROW_SRC_HASH").alias("p"),
        )
        .where("n > 1 AND p > 1")
        .count()
    )
    assert divergent == 0, (
        f"{divergent} duplicated EPISODE_ENCNTR_RELTN_IDs carry DIVERGENT payloads — "
        "the exact-duplicate collapse is no longer safe; characterize before publishing"
    )
    w = Window.partitionBy("EPISODE_ENCNTR_RELTN_ID")
    deduped = (
        hashed
        .withColumn("SOURCE_DUPLICATE_COUNT", F.count("*").over(w))
        .withColumn(
            "rn",
            F.row_number().over(w.orderBy(F.col("ROW_SRC_HASH").asc())),
        )
        .filter("rn = 1")
        .drop("rn", "ROW_SRC_HASH")
    )
    staged = _cast_integral_identifiers(deduped)
    episode_keys = (
        spark.table(SRC_EPISODE)
        .select(F.expr("try_cast(EPISODE_ID AS BIGINT)").alias("EPISODE_ID"))
        .where("EPISODE_ID IS NOT NULL")
        .distinct()
        .withColumn("_EPISODE_FOUND", F.lit(True))
    )
    staged = (
        staged.join(episode_keys, ["EPISODE_ID"], "left")
        .withColumn(
            "EPISODE_LINK_STATUS",
            F.when(F.col("_EPISODE_FOUND"), F.lit("MATCHED_EPISODE"))
             .otherwise(F.lit("SOURCE_EPISODE_ABSENT")),
        )
        .drop("_EPISODE_FOUND")
    )
    staged = _add_decodes(staged, RELTN_DECODES)
    staged, flagged = dq_all_clinical(staged, RELTN_ADMIN_STAMPS)
    staged = (
        _business_hash(staged)
        .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
    )
    _publish(
        staged,
        T_RELTN,
        ["EPISODE_ENCNTR_RELTN_ID"],
        "One row per EPISODE_ENCNTR_RELTN_ID after invariant-guarded collapse of byte-identical source duplicates; multiple rows per EPISODE_ID/ENCNTR_ID pair are valid lifecycle and re-linkage data. EPISODE_LINK_STATUS retains and identifies raw relationship rows whose episode is absent from raw.mill_episode.",
    )
    print("[A13] reltn date-quality columns=" + json.dumps(flagged))
    return flagged, divergent


def _key_gate(table, key):
    row = spark.table(table).agg(
        F.count("*").alias("n"),
        F.countDistinct(key).alias("d"),
        F.sum(F.col(key).isNull().cast("long")).alias("nulls"),
    ).collect()[0]
    assert row["n"] == row["d"] and int(row["nulls"] or 0) == 0, (
        f"Key gate failed for {table}.{key}: {row.asDict()}"
    )


def _full_payload_divergence():
    raw = spark.table(SRC_RELTN)
    hashed = raw.withColumn(
        "_h",
        F.sha2(
            F.concat_ws(
                "§",
                *[
                    F.coalesce(F.col(c).cast("string"), F.lit("∅"))
                    for c in raw.columns
                ],
            ),
            256,
        ),
    )
    return (
        hashed.groupBy("EPISODE_ENCNTR_RELTN_ID")
        .agg(F.count("*").alias("n"), F.countDistinct("_h").alias("p"))
        .where("n > 1 AND p > 1")
        .count()
    )


def run_gates():
    assert spark.catalog.tableExists(T_EPISODE), f"GATE EXPECTED TABLE MISSING: {T_EPISODE}"
    assert spark.catalog.tableExists(T_RELTN), f"GATE EXPECTED TABLE MISSING: {T_RELTN}"

    episode_raw = spark.table(SRC_EPISODE).count()
    reltn_raw_row = spark.table(SRC_RELTN).agg(
        F.count("*").alias("n"),
        F.countDistinct("EPISODE_ENCNTR_RELTN_ID").alias("d"),
    ).collect()[0]
    episode_present = present_filtered_count(T_EPISODE)
    reltn_present = present_filtered_count(T_RELTN)
    reltn_weight = spark.table(T_RELTN).where("SOURCE_PRESENT_IND").agg(
        F.sum("SOURCE_DUPLICATE_COUNT").alias("n")
    ).collect()[0]["n"]
    assert episode_present == episode_raw
    assert reltn_present == reltn_raw_row["d"]
    assert int(reltn_weight) == int(reltn_raw_row["n"])

    _key_gate(T_EPISODE, "EPISODE_ID")
    _key_gate(T_RELTN, "EPISODE_ENCNTR_RELTN_ID")

    raw_episode_keys = (
        spark.table(SRC_EPISODE)
        .select(F.expr("try_cast(EPISODE_ID AS BIGINT)").alias("EPISODE_ID"))
        .where("EPISODE_ID IS NOT NULL").distinct()
    )
    raw_reltn_episode_keys = (
        spark.table(SRC_RELTN)
        .select(F.expr("try_cast(EPISODE_ID AS BIGINT)").alias("EPISODE_ID"))
        .where("EPISODE_ID IS NOT NULL").distinct()
    )
    raw_missing_episode = raw_reltn_episode_keys.join(
        raw_episode_keys, ["EPISODE_ID"], "left_anti"
    ).count()
    missing_episode = (
        spark.table(T_RELTN).where("SOURCE_PRESENT_IND")
        .select("EPISODE_ID").distinct()
        .join(
            spark.table(T_EPISODE).where("SOURCE_PRESENT_IND").select("EPISODE_ID"),
            ["EPISODE_ID"],
            "left_anti",
        )
        .count()
    )
    reltn_status = spark.table(T_RELTN).where("SOURCE_PRESENT_IND").select(
        "EPISODE_ID", "EPISODE_LINK_STATUS"
    )
    published_episode_keys = spark.table(T_EPISODE).where(
        "SOURCE_PRESENT_IND"
    ).select("EPISODE_ID").distinct()
    matched_missing = (
        reltn_status.where("EPISODE_LINK_STATUS = 'MATCHED_EPISODE'")
        .join(published_episode_keys, ["EPISODE_ID"], "left_anti")
        .count()
    )
    absent_found = (
        reltn_status.where("EPISODE_LINK_STATUS = 'SOURCE_EPISODE_ABSENT'")
        .join(published_episode_keys, ["EPISODE_ID"], "left_semi")
        .count()
    )
    status_mismatch = matched_missing + absent_found
    assert missing_episode == raw_missing_episode, (
        f"Bronze introduced containment drift: raw={raw_missing_episode}, bronze={missing_episode}"
    )
    assert status_mismatch == 0, status_mismatch

    reltn_encounters = (
        spark.table(T_RELTN).where("SOURCE_PRESENT_IND AND ENCNTR_ID IS NOT NULL")
        .select("ENCNTR_ID").distinct()
    )
    encounter_total = reltn_encounters.count()
    encounter_resolved = (
        reltn_encounters.join(
            spark.table(SRC_ENCOUNTER).select(
                F.expr("try_cast(ENCNTR_ID AS BIGINT)").alias("ENCNTR_ID")
            ).distinct(),
            ["ENCNTR_ID"],
            "left_semi",
        ).count()
    )

    for col_name in [
        "EPISODE_START_DT_TM", "EPISODE_STOP_DT_TM", "EPISODE_BREACH_DT_TM",
        "CREATE_DT_TM", "ACTIVE_STATUS_DT_TM", "BEG_EFFECTIVE_DT_TM",
        "END_EFFECTIVE_DT_TM", "DATA_STATUS_DT_TM",
    ]:
        for suffix in ("_FUTURE_IND", "_SENTINEL_IND", "_CLEAN"):
            assert col_name + suffix in spark.table(T_EPISODE).columns
    future_starts = spark.table(T_EPISODE).where(
        "SOURCE_PRESENT_IND AND EPISODE_START_DT_TM_FUTURE_IND"
    ).count()
    assert future_starts > 0

    ig_tag_gate(T_EPISODE)
    ig_tag_gate(T_RELTN)

    direct = (
        spark.table(T_EPISODE)
        .where("SOURCE_PRESENT_IND AND ENCNTR_ID IS NOT NULL")
        .select("EPISODE_ID", "ENCNTR_ID")
    )
    active_pairs = (
        spark.table(T_RELTN)
        .where("SOURCE_PRESENT_IND AND ACTIVE_IND = 1 AND ENCNTR_ID IS NOT NULL")
        .select("EPISODE_ID", "ENCNTR_ID").distinct()
    )
    direct_n = direct.count()
    agreement_n = direct.join(active_pairs, ["EPISODE_ID", "ENCNTR_ID"], "left_semi").count()

    for table in (T_EPISODE, T_RELTN):
        field = next(f for f in spark.table(table).schema.fields if f.name == "SOURCE_PRESENT_IND")
        assert isinstance(field.dataType, T.BooleanType)
    false_rows = {
        T_EPISODE: spark.table(T_EPISODE).where("NOT SOURCE_PRESENT_IND").count(),
        T_RELTN: spark.table(T_RELTN).where("NOT SOURCE_PRESENT_IND").count(),
    }
    if EXPECT_FIRST_BUILD_ALL_PRESENT:
        assert sum(false_rows.values()) == 0, false_rows

    divergent = _full_payload_divergence()
    assert divergent == 0

    metrics = {
        "episode_raw": episode_raw,
        "episode_present": episode_present,
        "reltn_raw": int(reltn_raw_row["n"]),
        "reltn_distinct_id": int(reltn_raw_row["d"]),
        "reltn_present": reltn_present,
        "reltn_duplicate_weight": int(reltn_weight),
        "raw_missing_episode_ids": raw_missing_episode,
        "missing_episode_ids": missing_episode,
        "episode_link_status_mismatch": status_mismatch,
        "encounter_distinct": encounter_total,
        "encounter_resolved": encounter_resolved,
        "encounter_resolution_pct": (
            100.0 * encounter_resolved / encounter_total if encounter_total else None
        ),
        "future_episode_starts": future_starts,
        "direct_episode_encounter_rows": direct_n,
        "direct_active_reltn_agreements": agreement_n,
        "direct_active_reltn_agreement_pct": (
            100.0 * agreement_n / direct_n if direct_n else None
        ),
        "false_rows": false_rows,
        "divergent_duplicate_ids": divergent,
        "episode_fingerprint": str(table_fingerprint(T_EPISODE)),
        "reltn_fingerprint": str(table_fingerprint(T_RELTN)),
        "episode_version": int(table_version(T_EPISODE)),
        "reltn_version": int(table_version(T_RELTN)),
    }
    print("[A13_GATES] " + json.dumps(metrics, sort_keys=True, default=str))
    return metrics


def zero_row_lifecycle_fixture():
    target = f"{TARGET_SCHEMA}.s6b_episode_zero_row_fixture"
    spark.sql(f"DROP TABLE IF EXISTS {qname(target)}")
    fixture = spark.createDataFrame([(1, "A"), (2, "B")], "id BIGINT, payload STRING")
    replace_with_tombstones(fixture, target, ["id"])
    empty = fixture.where("1=0")
    replace_with_tombstones(empty, target, ["id"])
    assert present_filtered_count(target) == 0
    assert spark.table(target).where("NOT SOURCE_PRESENT_IND").count() == 2
    replace_with_tombstones(fixture, target, ["id"])
    assert present_filtered_count(target) == 2
    assert spark.table(target).where("NOT SOURCE_PRESENT_IND").count() == 0
    spark.sql(f"DROP TABLE IF EXISTS {qname(target)}")
    print("[A13] zero-row lifecycle replay PASS")


if GATES_ONLY:
    run_gates()
    dbutils.notebook.exit(json.dumps({"mode": "GATES_ONLY", "status": "PASS"}))

sources = [SRC_EPISODE, SRC_RELTN]
due, current_versions = due_check(CONTROL_TABLE, "episode_pipeline", sources)
due = due or not spark.catalog.tableExists(T_EPISODE) or not spark.catalog.tableExists(T_RELTN)

if FORCE_REBUILD or due:
    episode_flagged = build_episode()
    if INTERRUPT_AFTER_EPISODE:
        raise RuntimeError(
            "INTENTIONAL_RETRY_FIXTURE: interrupted after map_episode publish and before "
            "map_episode_encounter/state commit"
        )
    reltn_flagged, divergent = build_episode_encounter()
    record_versions(CONTROL_TABLE, "episode_pipeline", current_versions)
    mode = "FORCED_REBUILD" if FORCE_REBUILD else "BUILD"
else:
    episode_flagged = [
        "ACTIVE_STATUS_DT_TM", "BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM",
        "DATA_STATUS_DT_TM", "CREATE_DT_TM", "EPISODE_BREACH_DT_TM",
        "EPISODE_START_DT_TM", "EPISODE_STOP_DT_TM",
    ]
    reltn_flagged = [
        "ACTIVE_STATUS_DT_TM", "BEG_EFFECTIVE_DT_TM",
        "END_EFFECTIVE_DT_TM", "CREATE_DT_TM",
    ]
    record_versions(CONTROL_TABLE, "episode_pipeline", current_versions)
    mode = "NO_OP"

if RUN_FULL_GATES:
    metrics = run_gates()
    zero_row_lifecycle_fixture()
else:
    assert mode == "NO_OP", "run_full_gates=false is allowed only for the unchanged NO_OP proof"
    metrics = {
        "episode_version": int(table_version(T_EPISODE)),
        "reltn_version": int(table_version(T_RELTN)),
        "state_versions": current_versions,
        "full_gates": "SKIPPED_FOR_CHEAP_NO_OP",
    }
    print("[A13_NO_OP] " + json.dumps(metrics, sort_keys=True, default=str))

PROMOTION_RUNBOOK = """
HUMAN-GATED ONLY — do not execute from S6b dev:
1. Copy this notebook to /Workspace/Shared/ADC-DB/Prod/Pipelines/Bronze/episode_pipeline.
2. Retarget to 4_prod.bronze only in an approved promotion window.
3. Build both new A13 tables, enable CDF/rowTracking/deletion vectors, apply IG tags, and re-run G1-G8.
4. Add one Bronze_Pipeline step after raw Millennium ingestion; record integrated runtime.
5. Notify Journey that map_episode and map_episode_encounter are live with the grains documented here.
"""
print(PROMOTION_RUNBOOK)
dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "result": mode,
    "mode": mode,
    "target": TARGET_SCHEMA,
    "target_schema": TARGET_SCHEMA,
    "episode_flagged_columns": episode_flagged,
    "reltn_flagged_columns": reltn_flagged,
    "metrics": metrics,
}, sort_keys=True, default=str))


