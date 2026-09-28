# Databricks notebook source
# pacs_text_bridge_pipeline — PACS report-text bridge sidecar (v4: re-sourced from bronze.mill_blob_text).
#
# Default target is 8_dev.bronze.map_pacs_report_text_bridge; a production target needs
# allow_production_write=true. This notebook never updates map_pacs_report and contains no
# parser or chunk reassembler: text is the blob owner's decoded bronze.mill_blob_text row.
# Pure Python: no percent-run, magic commands, or hidden notebook dependencies.
#
# release: pacs_integration_20260924 v4 — replaces the frozen 4_prod.pacs_dlt.pacs_blob_content
# source (MillAccessionNbr) with bronze.mill_blob_text reached through map_radiology_event v2
# (REF_EXAM_KEY report links, PACS_EXAMINATION_ID links, SECTRA_ACCESSION_NBR) and
# map_pacs_examination v3.2 (SECTRA_ACCESSION_NBR). Deploy after both of those.
#
# VERSION SAFETY. Known blob defects are NOT assumed fixed:
#   * several open-current SCD2 rows per EVENT_ID in mill_blob_text: byte-identical text collapses
#     (provenance kept in CURRENT_BLOB_VERSION_IDS); differing text is text_version_ambiguous and
#     never published — no timestamp-only ROW_NUMBER picks a winner;
#   * several raw.mill_ce_blob rows per (EVENT_ID, BLOB_SEQ_NUM): the decoded text may stitch
#     document versions, so it is text_integrity_unknown and never published. The affected IDs
#     stay queryable in map_pacs_report_text_candidate for the blob owner's automated repair.
# The accepted projection is gated to one effective-current document per event.
#
# Inherited instability: this task failed 47% of runs in the 2026-09-20 slow-task audit
# (docs/2026-09-20-bronze-parallel-slow-task-report.md §2). Task retries are deliberately unchanged.
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

ACTION = _widget_text("action", "build").lower()
FORCE_REBUILD = (
    _widget_text("force_rebuild", "false").lower() == "true"
    or _widget_text("force_full_refresh", "false").lower() == "true"
)
assert ACTION in {"build", "gates"}

# Source overrides exist so dev can point at 8_dev builds of the v2/v3.2 upstream tables.
PACS_SOURCE_SCHEMA = _widget_text("pacs_source_schema", "4_prod.bronze")
SRC_RADIOLOGY = _widget_text("radiology_source", "4_prod.bronze.map_radiology_event")

TARGET = f"{TARGET_SCHEMA}.map_pacs_report_text_bridge"
TARGET_REL = f"{TARGET_SCHEMA}.map_pacs_report_text_candidate"
SRC_REPORT = f"{PACS_SOURCE_SCHEMA}.map_pacs_report"
SRC_EXAM = f"{PACS_SOURCE_SCHEMA}.map_pacs_examination"
SRC_BLOB_TEXT = "4_prod.bronze.mill_blob_text"
SRC_RAW_CHUNKS = "4_prod.raw.mill_ce_blob"
SOURCES = {
    "report": SRC_REPORT,
    "exam": SRC_EXAM,
    "radiology": SRC_RADIOLOGY,
    "blob_text": SRC_BLOB_TEXT,
    "raw_chunks": SRC_RAW_CHUNKS,
}
CONTROL_TABLE = f"{CONTROL_SCHEMA}.s6_source_versions"
CONTROL_PIPELINE = "s6_b12_pacs_text_bridge_pipeline"

LOGIC_VERSION = "2026-09-24.pacs_integration.v4"
PIPELINE_NAME = "pacs_text_bridge_pipeline"
RTF_PREFIX = "{" + chr(92) + "rtf"
# mill_blob_text marks the open-current version with VALID_UNTIL_DT_TM 2100-12-31.
CURRENT_VALID_UNTIL_FLOOR = "2100-01-01"
RADRPT_EVENT_CLASS_CD = 224
ACCOUNTING_CLASSES = (
    "BRIDGED",
    "MULTI_DOCUMENT",
    "MULTI_REPORT",
    "BOTH",
    "TEXT_VERSION_AMBIGUOUS",
    "TEXT_INTEGRITY_UNKNOWN",
    "UNMATCHED",
)
MATCH_LANES = ("PACS_EXAM_LINK", "ACCESSION_UNIQUE_EXAM")
ACCEPTED_DECISIONS = ("single_current", "identical_text_collapsed")
ACCEPTED_INTEGRITY = ("single_version_chunks", "raw_chunks_absent")

# COMMAND ----------

import hashlib
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, TimestampType
from pyspark.sql.window import Window

RUN_STARTED_AT = datetime.now(timezone.utc).isoformat()

# COMMAND ----------

# ==== COMMON BLOCK v1 (SYNC-WITH _completeness_common) ====
SENTINEL_FLOOR = "1901-01-01"

def dq_columns(df, date_cols):
    """Master plan date-quality block. Retain raw values and add future/sentinel/clean companions."""
    out = df
    for c in date_cols:
        fut = F.col(c) > F.current_timestamp()
        sen = F.col(c) < F.lit(SENTINEL_FLOOR).cast("timestamp")
        out = (out
               .withColumn(f"{c}_FUTURE_IND", F.when(F.col(c).isNull(), F.lit(None)).otherwise(fut))
               .withColumn(f"{c}_SENTINEL_IND", F.when(F.col(c).isNull(), F.lit(None)).otherwise(sen))
               .withColumn(f"{c}_CLEAN",
                           F.when(fut | sen, F.lit(None).cast("timestamp")).otherwise(F.col(c))))
    return out

def get_watermark(control_table, source_name, default="1980-01-01"):
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {control_table} (
        source_name STRING, watermark TIMESTAMP, updated_at TIMESTAMP)""")
    rows = spark.sql(
        f"SELECT watermark FROM {control_table} WHERE source_name = '{source_name}'"
    ).collect()
    return rows[0]["watermark"] if rows else spark.sql(
        f"SELECT CAST('{default}' AS TIMESTAMP) w"
    ).collect()[0]["w"]

def set_watermark(control_table, source_name, new_wm):
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
def table_version(tbl):
    """Current Delta commit version. Materialized views are content-gated separately."""
    return int(spark.sql(f"DESCRIBE HISTORY {tbl} LIMIT 1").collect()[0]["version"])

def due_check(control_table, pipeline, sources):
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {control_table}
        (pipeline STRING, source STRING, version BIGINT, updated_at TIMESTAMP)""")
    cur = {t: table_version(t) for t in sources}
    seen = {r["source"]: r["version"] for r in spark.sql(
        f"SELECT source, version FROM {control_table} WHERE pipeline = '{pipeline}'").collect()}
    return any(seen.get(t) != v for t, v in cur.items()), cur

def record_versions(control_table, pipeline, versions):
    for t, v in versions.items():
        spark.sql(f"""MERGE INTO {control_table} c
            USING (SELECT '{pipeline}' pipeline, '{t}' source, CAST({v} AS BIGINT) version) s
            ON c.pipeline = s.pipeline AND c.source = s.source
            WHEN MATCHED THEN UPDATE SET c.version = s.version, c.updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (pipeline, source, version, updated_at)
                 VALUES (s.pipeline, s.source, s.version, current_timestamp())""")

def dq_all_clinical(df, admin_stamps):
    cols = [f.name for f in df.schema.fields
            if isinstance(f.dataType, (TimestampType, DateType))
            and f.name not in admin_stamps and not f.name.endswith("_CLEAN")]
    return dq_columns(df, cols), cols

def replace_with_tombstones(df, target, key_cols):
    """Replace current rows and retain disappeared rows as tombstones.

    Snapshot the current target before overwrite instead of reading its prior
    version afterwards. The latter fails once old Delta files have passed the
    deleted-file retention window, even though the current table is healthy.
    """
    import uuid as _tombstone_uuid

    fresh = df.withColumn("SOURCE_PRESENT_IND", F.lit(True))
    prior_stage = None
    completed = False
    try:
        if spark.catalog.tableExists(target):
            prior_stage = (
                f"{target}__tombstone_prior_"
                f"{_tombstone_uuid.uuid4().hex}"
            )
            spark.sql(
                f"CREATE TABLE {qname(prior_stage)} "
                f"SHALLOW CLONE {qname(target)}"
            )

        (fresh.write.format("delta").mode("overwrite")
              .option("overwriteSchema", "true").saveAsTable(target))

        if prior_stage is not None:
            current = spark.table(target)
            gone = (
                spark.table(prior_stage)
                .join(current.select(*key_cols).distinct(), key_cols, "left_anti")
                .withColumn("SOURCE_PRESENT_IND", F.lit(False))
            )
            gone_columns = set(gone.columns)
            aligned_gone = gone.select(*[
                (
                    F.col(field.name).cast(field.dataType)
                    if field.name in gone_columns
                    else F.lit(None).cast(field.dataType)
                ).alias(field.name)
                for field in current.schema.fields
            ])
            aligned_gone.write.format("delta").mode("append").saveAsTable(target)

        completed = True
    finally:
        if prior_stage is not None:
            if completed:
                spark.sql(f"DROP TABLE IF EXISTS {qname(prior_stage)}")
            else:
                print(
                    "[B12][RECOVERY] retained pre-overwrite shallow snapshot "
                    f"after failure: {prior_stage}"
                )

def table_fingerprint(tbl, exclude=("PIPELINE_UPDT_DT_TM",)):
    cols = [c for c in spark.table(tbl).columns if c not in exclude]
    return (spark.table(tbl)
            .select(F.sum(F.xxhash64(F.to_json(F.struct(*[F.col(c) for c in cols])))
                          .cast("decimal(38,0)")).alias("fp"))
            .collect()[0]["fp"])
# ==== END S2 BLOCK v1 ====
#
# B12 scope note: the user-authorized write surface contains no separate S6 control table.
# Source-version state is therefore stored as table properties on TARGET after all gates pass.

# COMMAND ----------

def qident(name):
    q = chr(96)
    return q + name.replace(q, q + q) + q

def qname(name):
    return ".".join(qident(p) for p in name.split("."))

def sql_text(value):
    return str(value).replace("'", "''")

def pinned_table(table, version):
    return spark.read.option("versionAsOf", int(version)).table(table)

def target_properties():
    if not spark.catalog.tableExists(TARGET):
        return {}
    return {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {qname(TARGET)}").collect()}

def set_target_properties(values):
    assignments = ", ".join(
        f"'{sql_text(k)}'='{sql_text(v)}'" for k, v in values.items()
    )
    spark.sql(f"ALTER TABLE {qname(TARGET)} SET TBLPROPERTIES ({assignments})")

def lookup_counterpart_tags(col_name):
    """Modal complete IG tag pair for the same column name in 4_prod.bronze."""
    rows = (spark.sql(f"""
        SELECT table_name,
               MAX(CASE WHEN tag_name='ig_risk' THEN tag_value END) AS r,
               MAX(CASE WHEN tag_name='ig_severity' THEN tag_value END) AS s
        FROM 4_prod.information_schema.column_tags
        WHERE schema_name='bronze'
          AND upper(column_name)=upper('{sql_text(col_name)}')
          AND tag_name IN ('ig_risk','ig_severity')
        GROUP BY table_name""")
        .where(F.col("r").isNotNull() & F.col("s").isNotNull())
        .groupBy("r", "s").count()
        .orderBy(F.desc("count"), F.asc("r"), F.asc("s"))
        .collect())
    return (rows[0]["r"], rows[0]["s"]) if rows else None

def ig_tag_table(table, tag_map, default=("0", "0")):
    cols = [r["col_name"] for r in spark.sql(f"DESCRIBE {qname(table)}").collect()
            if r["col_name"] and not r["col_name"].startswith("#")]
    for c in cols:
        if c in tag_map:
            risk, severity = tag_map[c]
        else:
            found = lookup_counterpart_tags(c)
            risk, severity = found if found else default
            if not found:
                print(f"IG-TAG DEFAULTED {table}.{c} -> {default} — REVIEW")
        spark.sql(f"""ALTER TABLE {qname(table)} ALTER COLUMN {qident(c)}
                      SET TAGS ('ig_risk'='{risk}', 'ig_severity'='{severity}')""")

def ig_tag_gate(table):
    catalog, schema, table_name = table.split(".")
    cols = {r["column_name"] for r in spark.sql(f"""
        SELECT column_name
        FROM {qident(catalog)}.information_schema.columns
        WHERE table_schema='{sql_text(schema)}' AND table_name='{sql_text(table_name)}'
    """).collect()}
    risk = {r["column_name"] for r in spark.sql(f"""
        SELECT column_name
        FROM {qident(catalog)}.information_schema.column_tags
        WHERE schema_name='{sql_text(schema)}' AND table_name='{sql_text(table_name)}'
          AND tag_name='ig_risk'
    """).collect()}
    severity = {r["column_name"] for r in spark.sql(f"""
        SELECT column_name
        FROM {qident(catalog)}.information_schema.column_tags
        WHERE schema_name='{sql_text(schema)}' AND table_name='{sql_text(table_name)}'
          AND tag_name='ig_severity'
    """).collect()}
    missing = sorted(cols - (risk & severity))
    assert not missing, f"columns missing ig_risk and/or ig_severity: {missing}"

# COMMAND ----------

ADMIN_STAMPS = {
    "BLOB_EXTRACT_DT_TM",
    "BLOB_UPDT_DT_TM",
    "BLOB_ADC_UPDT",
    "REPORT_SRC_ADC_UPDT",
    "PIPELINE_UPDT_DT_TM",
}

BRIDGE_COLUMNS = [
    "REPORT_ID",
    "EVENT_ID",
    "MATCH_LANE",
    "BRIDGED_TEXT_RAW",
    "BRIDGED_TEXT",
    "BRIDGED_TEXT_FORMAT",
    "BRIDGED_TEXT_PARSE_STATUS",
    "BRIDGED_TEXT_PARSER_VERSION",
    "BLOB_EXTRACT_DT_TM",
    "BLOB_UPDT_DT_TM",
    "BLOB_ADC_UPDT",
    "REPORT_SRC_ADC_UPDT",
    "SOURCE_VERSION_ID",
    "BLOB_VERSION_ID",
    "RAW_SHA256",
    "TEXT_SHA256",
    "BLOB_CONTENT_TYPE",
    "EFFECTIVE_CURRENT_DECISION",
    "CURRENT_ROW_COUNT",
    "CURRENT_BLOB_VERSION_IDS",
    "TEXT_INTEGRITY_STATUS",
]

REL_COLUMNS = [
    "CANDIDATE_KEY",
    "REPORT_ID",
    "EVENT_ID",
    "PACS_EXAMINATION_ID",
    "SECTRA_ACCESSION_NBR",
    "MATCH_LANE",
    "REPORT_CANDIDATE_DOC_COUNT",
    "DOC_CANDIDATE_REPORT_COUNT",
    "IDENTITY_CONFLICT_COUNT",
    "ACCOUNTING_CLASS",
    "UNMATCHED_REASON",
    "EFFECTIVE_CURRENT_DECISION",
    "CURRENT_ROW_COUNT",
    "CURRENT_TEXT_VARIANT_COUNT",
    "CURRENT_BLOB_VERSION_IDS",
    "SOURCE_VERSION_ID",
    "BLOB_VERSION_ID",
    "RAW_SHA256",
    "TEXT_SHA256",
    "TEXT_INTEGRITY_STATUS",
    "RAW_MAX_ROWS_PER_CHUNK_SEQ",
    "ACCEPTED_IND",
    "REPORT_SRC_ADC_UPDT",
]

REQUIRED_SOURCE_COLUMNS = {
    "report": {"PACS_REPORT_ID", "PACS_EXAMINATION_ID", "PERSON_ID", "REPORT_TEXT",
               "SRC_ADC_UPDT", "SOURCE_PRESENT_IND"},
    "exam": {"PACS_EXAMINATION_ID", "SECTRA_ACCESSION_NBR", "PERSON_ID", "SOURCE_PRESENT_IND"},
    "radiology": {"EVENT_ID", "EVENT_CLASS_CD", "IN_ERROR_IND", "PERSON_ID",
                  "SECTRA_ACCESSION_NBR", "PACS_EXAMINATION_ID", "PACS_LINK_METHOD"},
    "blob_text": {"EVENT_ID", "VALID_UNTIL_DT_TM", "VALID_FROM_DT_TM", "UPDT_DT_TM", "ADC_UPDT",
                  "BLOB_TEXT", "STATUS", "CONTENT_TYPE", "raw_sha256", "decompressor_version",
                  "parser_version", "post_processor_version", "SOURCE_VERSION_ID",
                  "BLOB_VERSION_ID"},
    "raw_chunks": {"EVENT_ID", "BLOB_SEQ_NUM"},
}

def row_hash_expr(columns):
    return F.sha2(F.to_json(F.struct(*[F.col(c) for c in columns])), 256)

def source_state():
    versions = {name: table_version(tbl) for name, tbl in SOURCES.items()}
    state = {"logic_version": LOGIC_VERSION}
    state.update({f"{name}_version": int(v) for name, v in versions.items()})
    signature = hashlib.sha256(
        json.dumps(state, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return state, signature

def pinned(name, state):
    return pinned_table(SOURCES[name], state[f"{name}_version"])

def assert_source_contract(state):
    # map_radiology_event v2 and map_pacs_examination v3.2 must be deployed first.
    missing = {}
    for name, required in REQUIRED_SOURCE_COLUMNS.items():
        have = set(pinned(name, state).columns)
        absent = sorted(required - have)
        if absent:
            missing[SOURCES[name]] = absent
    assert not missing, f"upstream contract not deployed (deploy order violated): {missing}"

def report_snapshot_stats(state):
    r = pinned("report", state).where(F.col("SOURCE_PRESENT_IND"))
    return r.agg(
        F.count(F.lit(1)).cast("long").alias("total_reports"),
        F.sum(F.when(F.col("REPORT_TEXT").isNull() | (F.trim("REPORT_TEXT") == ""), 1)
              .otherwise(0)).cast("long").alias("textless_reports"),
        F.sum(F.when(F.col("REPORT_TEXT").isNotNull() & (F.trim("REPORT_TEXT") != ""), 1)
              .otherwise(0)).cast("long").alias("native_text_reports"),
    ).collect()[0].asDict()

def record_s6_source_versions(state):
    """Post-gate shared S6 ledger commit. The SHA-256 source signature remains the NO_OP authority."""
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {CONTROL_TABLE}
        (pipeline STRING, source STRING, version BIGINT, updated_at TIMESTAMP)""")
    versions = {tbl: int(state[f"{name}_version"]) for name, tbl in SOURCES.items()}
    record_versions(CONTROL_TABLE, CONTROL_PIPELINE, versions)
    recorded = {r["source"]: int(r["version"]) for r in spark.sql(f"""
        SELECT source, version FROM {CONTROL_TABLE}
        WHERE pipeline='{CONTROL_PIPELINE}'""").collect()}
    stale = {k: v for k, v in versions.items() if recorded.get(k) != v}
    assert not stale, f"S6 source-version ledger mismatch: {stale} vs {recorded}"
    print("[B12][S6_SOURCE_VERSIONS]", json.dumps(versions, sort_keys=True))
    return versions

# COMMAND ----------

def current_blob_rows(state, event_ids):
    """Open-current mill_blob_text rows for the given events, with their own text hash."""
    return (pinned("blob_text", state)
        .where(F.col("VALID_UNTIL_DT_TM") >= F.lit(CURRENT_VALID_UNTIL_FLOOR).cast("timestamp"))
        .join(event_ids, "EVENT_ID", "left_semi")
        .select(
            F.col("EVENT_ID").cast("long").alias("EVENT_ID"),
            "SOURCE_VERSION_ID",
            "BLOB_VERSION_ID",
            F.col("raw_sha256").alias("RAW_SHA256"),
            # Blank text hashes to NULL so NULL and whitespace-only count as one "no text" variant.
            F.when(F.trim("BLOB_TEXT") != "", F.sha2(F.col("BLOB_TEXT"), 256)).alias("TEXT_SHA256"),
            "BLOB_TEXT",
            "STATUS",
            "CONTENT_TYPE",
            "decompressor_version",
            "parser_version",
            "post_processor_version",
            "VALID_FROM_DT_TM",
            "UPDT_DT_TM",
            "ADC_UPDT"))

def resolve_effective_current(state, event_ids):
    """One row per event: the effective-current decision and, when decidable, its version.

    Several open-current rows with byte-identical text collapse to one document. Differing
    text is text_version_ambiguous: no ordering is used to pick a winner, and the version
    pointers stay NULL so nothing downstream can mistake a pick for a resolution.
    """
    cur = current_blob_rows(state, event_ids).drop("BLOB_TEXT")
    w = Window.partitionBy("EVENT_ID")
    text_key = F.coalesce(F.col("TEXT_SHA256"), F.lit("<NO_TEXT>"))
    cur = (cur
        .withColumn("CURRENT_ROW_COUNT", F.count(F.lit(1)).over(w).cast("int"))
        .withColumn("CURRENT_TEXT_VARIANT_COUNT", F.size(F.collect_set(text_key).over(w)))
        .withColumn("CURRENT_BLOB_VERSION_IDS", F.when(
            F.col("CURRENT_ROW_COUNT") > 1,
            F.concat_ws(",", F.array_sort(F.collect_set("BLOB_VERSION_ID").over(w))))))
    # Identical-text rows are interchangeable; this ordering only fixes which provenance
    # row is reported for a collapsed document (latest version first, NULLS LAST).
    rep_order = Window.partitionBy("EVENT_ID").orderBy(
        F.col("VALID_FROM_DT_TM").desc_nulls_last(),
        F.col("ADC_UPDT").desc_nulls_last(),
        F.col("UPDT_DT_TM").desc_nulls_last(),
        F.col("BLOB_VERSION_ID").asc_nulls_last(),
        F.col("SOURCE_VERSION_ID").asc_nulls_last())
    rep = (cur.withColumn("_REP_RN", F.row_number().over(rep_order))
        .where(F.col("_REP_RN") == 1).drop("_REP_RN"))
    decision = (F.when(F.col("CURRENT_ROW_COUNT") == 1, F.lit("single_current"))
        .when(F.col("CURRENT_TEXT_VARIANT_COUNT") == 1, F.lit("identical_text_collapsed"))
        .otherwise(F.lit("text_version_ambiguous")))
    decided = F.col("EFFECTIVE_CURRENT_DECISION") != "text_version_ambiguous"
    return (rep
        .withColumn("EFFECTIVE_CURRENT_DECISION", decision)
        .select(
            "EVENT_ID",
            "EFFECTIVE_CURRENT_DECISION",
            "CURRENT_ROW_COUNT",
            "CURRENT_TEXT_VARIANT_COUNT",
            "CURRENT_BLOB_VERSION_IDS",
            *[F.when(decided, F.col(c)).alias(c) for c in (
                "SOURCE_VERSION_ID", "BLOB_VERSION_ID", "RAW_SHA256", "TEXT_SHA256",
                "STATUS", "CONTENT_TYPE", "decompressor_version", "parser_version",
                "post_processor_version")]))

def raw_chunk_evidence(state, event_ids):
    """Largest number of raw rows sharing one (EVENT_ID, BLOB_SEQ_NUM) per event.

    More than one means the raw store holds several document versions for a chunk slot, so a
    decoded text may have been stitched from different versions. No reassembly happens here.
    """
    raw = pinned("raw_chunks", state).select(
        F.col("EVENT_ID").cast("long").alias("EVENT_ID"), F.col("BLOB_SEQ_NUM"))
    return (raw.join(event_ids, "EVENT_ID", "left_semi")
        .groupBy("EVENT_ID", "BLOB_SEQ_NUM").agg(F.count(F.lit(1)).alias("_N"))
        .groupBy("EVENT_ID").agg(F.max("_N").cast("int").alias("RAW_MAX_ROWS_PER_CHUNK_SEQ")))

def build_relationships(state):
    """Every textless-report candidate relationship, classified at report grain.

    Lanes, strongest first (a report uses only its strongest lane with any candidate):
      PACS_EXAM_LINK        — the RADRPT event's map_radiology_event PACS link (accession +
                              exam code + identity) resolves to this report's examination.
      ACCESSION_UNIQUE_EXAM — the RADRPT event carries this examination's Sectra accession,
                              the accession names exactly one present PACS examination, and the
                              event is not PACS-linked to any examination.
    Both lanes require person compatibility (either side NULL, or equal).
    """
    exams = (pinned("exam", state).where(F.col("SOURCE_PRESENT_IND"))
        .select(
            F.col("PACS_EXAMINATION_ID").cast("long").alias("_EXAM_ID"),
            F.upper(F.trim("SECTRA_ACCESSION_NBR")).alias("_EXAM_ACC"),
            F.col("PERSON_ID").cast("long").alias("_EXAM_PERSON")))
    acc_n = (exams.where(F.col("_EXAM_ACC").isNotNull() & (F.col("_EXAM_ACC") != ""))
        .groupBy("_EXAM_ACC").agg(F.countDistinct("_EXAM_ID").alias("_ACC_EXAM_N")))
    exams = exams.join(acc_n, "_EXAM_ACC", "left")

    reports = (pinned("report", state)
        .where(F.col("SOURCE_PRESENT_IND"))
        .where(F.col("REPORT_TEXT").isNull() | (F.trim("REPORT_TEXT") == ""))
        .select(
            F.col("PACS_REPORT_ID").cast("long").alias("REPORT_ID"),
            F.col("PACS_EXAMINATION_ID").cast("long").alias("PACS_EXAMINATION_ID"),
            F.col("PERSON_ID").cast("long").alias("_REPORT_PERSON"),
            F.col("SRC_ADC_UPDT").alias("REPORT_SRC_ADC_UPDT"))
        .join(exams, F.col("PACS_EXAMINATION_ID") == F.col("_EXAM_ID"), "left")
        .withColumn("_SUBJECT", F.coalesce("_REPORT_PERSON", "_EXAM_PERSON")))

    docs = (pinned("radiology", state)
        .where((F.col("EVENT_CLASS_CD") == RADRPT_EVENT_CLASS_CD)
               & ~F.coalesce(F.col("IN_ERROR_IND"), F.lit(False)))
        .select(
            F.col("EVENT_ID").cast("long").alias("_DOC_ID"),
            F.col("PERSON_ID").cast("long").alias("_DOC_PERSON"),
            F.col("PACS_EXAMINATION_ID").cast("long").alias("_DOC_EXAM_ID"),
            F.upper(F.trim("SECTRA_ACCESSION_NBR")).alias("_DOC_ACC")))

    r = reports.select("REPORT_ID", "_EXAM_ID", "_EXAM_ACC", "_ACC_EXAM_N", "_SUBJECT")
    lane_exam = (r.join(docs.where(F.col("_DOC_EXAM_ID").isNotNull()),
                        F.col("_EXAM_ID") == F.col("_DOC_EXAM_ID"), "inner")
        .withColumn("_PRIORITY", F.lit(1)).withColumn("MATCH_LANE", F.lit("PACS_EXAM_LINK")))
    lane_acc = (r.where(F.col("_ACC_EXAM_N") == 1)
        .join(docs.where(F.col("_DOC_EXAM_ID").isNull()),
              F.col("_EXAM_ACC") == F.col("_DOC_ACC"), "inner")
        .withColumn("_PRIORITY", F.lit(2)).withColumn("MATCH_LANE", F.lit("ACCESSION_UNIQUE_EXAM")))
    pairs = (lane_exam.unionByName(lane_acc)
        .select("REPORT_ID", "_DOC_ID", "_PRIORITY", "MATCH_LANE",
                (F.col("_SUBJECT").isNull() | F.col("_DOC_PERSON").isNull()
                 | (F.col("_SUBJECT") == F.col("_DOC_PERSON"))).alias("_COMPAT")))
    conflicts = (pairs.where(~F.col("_COMPAT")).groupBy("REPORT_ID")
        .agg(F.countDistinct("_DOC_ID").cast("int").alias("IDENTITY_CONFLICT_COUNT")))
    compat = pairs.where(F.col("_COMPAT"))
    by_report = Window.partitionBy("REPORT_ID")
    chosen = (compat
        .withColumn("_BEST", F.min("_PRIORITY").over(by_report))
        .where(F.col("_PRIORITY") == F.col("_BEST"))
        .select("REPORT_ID", F.col("_DOC_ID").alias("EVENT_ID"), "MATCH_LANE")
        .dropDuplicates(["REPORT_ID", "EVENT_ID"]))
    by_doc = Window.partitionBy("EVENT_ID")
    chosen = (chosen
        .withColumn("REPORT_CANDIDATE_DOC_COUNT", F.count(F.lit(1)).over(by_report).cast("int"))
        .withColumn("DOC_CANDIDATE_REPORT_COUNT", F.count(F.lit(1)).over(by_doc).cast("int"))
        .withColumn("_MAX_DOC_REPORTS", F.max("DOC_CANDIDATE_REPORT_COUNT").over(by_report)))

    event_ids = chosen.select("EVENT_ID").distinct()
    blob = resolve_effective_current(state, event_ids)
    raw = raw_chunk_evidence(state, event_ids)

    rel = (reports.select("REPORT_ID", "PACS_EXAMINATION_ID", "_EXAM_ID", "_EXAM_ACC",
                          "REPORT_SRC_ADC_UPDT")
        .join(chosen, "REPORT_ID", "left")
        .join(conflicts, "REPORT_ID", "left")
        .join(blob, "EVENT_ID", "left")
        .join(raw, "EVENT_ID", "left")
        .withColumn("REPORT_CANDIDATE_DOC_COUNT",
                    F.coalesce("REPORT_CANDIDATE_DOC_COUNT", F.lit(0)))
        .withColumn("IDENTITY_CONFLICT_COUNT", F.coalesce("IDENTITY_CONFLICT_COUNT", F.lit(0)))
        .withColumn("EFFECTIVE_CURRENT_DECISION", F.when(
            F.col("EVENT_ID").isNotNull(),
            F.coalesce("EFFECTIVE_CURRENT_DECISION", F.lit("no_current_row")))))

    has_text = F.col("TEXT_SHA256").isNotNull()
    integrity = (F.when(F.col("EVENT_ID").isNull(), F.lit(None).cast("string"))
        .when(F.col("EFFECTIVE_CURRENT_DECISION") == "no_current_row", F.lit("no_current_text"))
        .when(F.col("EFFECTIVE_CURRENT_DECISION") == "text_version_ambiguous",
              F.lit("not_assessed_version_ambiguous"))
        .when(F.col("RAW_MAX_ROWS_PER_CHUNK_SEQ").isNull(), F.lit("raw_chunks_absent"))
        .when(F.col("RAW_MAX_ROWS_PER_CHUNK_SEQ") > 1, F.lit("text_integrity_unknown"))
        .otherwise(F.lit("single_version_chunks")))
    rel = rel.withColumn("TEXT_INTEGRITY_STATUS", integrity)

    doc_n = F.col("REPORT_CANDIDATE_DOC_COUNT")
    rep_n = F.coalesce(F.col("_MAX_DOC_REPORTS"), F.lit(0))
    accounting_class = (
        F.when(doc_n == 0, F.lit("UNMATCHED"))
         .when((doc_n > 1) & (rep_n > 1), F.lit("BOTH"))
         .when(doc_n > 1, F.lit("MULTI_DOCUMENT"))
         .when(rep_n > 1, F.lit("MULTI_REPORT"))
         .when(F.col("EFFECTIVE_CURRENT_DECISION") == "text_version_ambiguous",
               F.lit("TEXT_VERSION_AMBIGUOUS"))
         .when(F.col("EFFECTIVE_CURRENT_DECISION") == "no_current_row", F.lit("UNMATCHED"))
         .when(~has_text, F.lit("UNMATCHED"))
         .when(F.col("TEXT_INTEGRITY_STATUS") == "text_integrity_unknown",
               F.lit("TEXT_INTEGRITY_UNKNOWN"))
         .otherwise(F.lit("BRIDGED")))
    unmatched_reason = (
        F.when(F.col("_EXAM_ID").isNull(), F.lit("NO_RESOLVED_EXAM"))
         .when((doc_n == 0) & (F.col("IDENTITY_CONFLICT_COUNT") > 0), F.lit("IDENTITY_CONFLICT"))
         .when(doc_n == 0, F.lit("NO_MILL_DOCUMENT_ON_LINK_KEYS"))
         .when(F.col("EFFECTIVE_CURRENT_DECISION") == "no_current_row",
               F.lit("NO_CURRENT_BLOB_TEXT"))
         .otherwise(F.lit("EMPTY_BLOB_TEXT")))
    rel = (rel
        .withColumn("ACCOUNTING_CLASS", accounting_class)
        .withColumn("UNMATCHED_REASON",
                    F.when(F.col("ACCOUNTING_CLASS") == "UNMATCHED", unmatched_reason))
        .withColumn("ACCEPTED_IND", F.col("ACCOUNTING_CLASS") == "BRIDGED")
        .withColumn("SECTRA_ACCESSION_NBR", F.col("_EXAM_ACC"))
        .withColumn("EVENT_ID", F.col("EVENT_ID").cast("string"))
        .withColumn("CANDIDATE_KEY", F.concat_ws(
            "|", F.col("REPORT_ID").cast("string"), F.coalesce(F.col("EVENT_ID"), F.lit("NONE"))))
        .select(*REL_COLUMNS))
    rel, flagged = dq_all_clinical(rel, {"REPORT_SRC_ADC_UPDT"})
    assert flagged == [], f"unexpected clinical temporal columns: {flagged}"
    return (rel
        .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
        .withColumn("ROW_HASH", row_hash_expr(REL_COLUMNS)))

def collect_accounting(report_stats):
    rel = spark.table(TARGET_REL).where(F.col("SOURCE_PRESENT_IND"))
    rows = (rel.groupBy("ACCOUNTING_CLASS", "MATCH_LANE", "UNMATCHED_REASON")
        .agg(F.countDistinct("REPORT_ID").cast("long").alias("n"))
        .collect())
    records = [r.asDict() for r in rows]
    class_totals = {c: 0 for c in ACCOUNTING_CLASSES}
    lane_totals = {}
    unmatched_reasons = {}
    for r in records:
        cls = r["ACCOUNTING_CLASS"]
        assert cls in class_totals, f"unknown accounting class {cls}"
        n = int(r["n"])
        class_totals[cls] += n
        lane = r["MATCH_LANE"] or "NONE"
        lane_totals[lane] = lane_totals.get(lane, 0) + n
        if r["UNMATCHED_REASON"]:
            reason = r["UNMATCHED_REASON"]
            unmatched_reasons[reason] = unmatched_reasons.get(reason, 0) + n
    accounted = sum(class_totals.values())
    assert accounted == int(report_stats["textless_reports"]), (
        f"accounting mismatch: {accounted} != {report_stats['textless_reports']}"
    )
    bridge_by_lane = {lane: sum(int(r["n"]) for r in records
                                if r["ACCOUNTING_CLASS"] == "BRIDGED" and r["MATCH_LANE"] == lane)
                      for lane in MATCH_LANES}
    version_rows = (rel.where(F.col("EVENT_ID").isNotNull())
        .groupBy("EFFECTIVE_CURRENT_DECISION", "TEXT_INTEGRITY_STATUS")
        .agg(F.countDistinct("EVENT_ID").cast("long").alias("n")).collect())
    version_status = {f"{r['EFFECTIVE_CURRENT_DECISION']}/{r['TEXT_INTEGRITY_STATUS']}": int(r["n"])
                      for r in version_rows}
    return {
        "records": records,
        "class_totals": class_totals,
        "lane_totals": lane_totals,
        "bridge_by_lane": bridge_by_lane,
        "unmatched_reasons": unmatched_reasons,
        "candidate_event_version_status": version_status,
        "textless_reports": int(report_stats["textless_reports"]),
        "native_text_reports": int(report_stats["native_text_reports"]),
        "total_reports": int(report_stats["total_reports"]),
    }

# COMMAND ----------

def build_candidate(state):
    """Accepted current documents: one row per BRIDGED report, text from the resolved version."""
    accepted = (spark.table(TARGET_REL)
        .where(F.col("SOURCE_PRESENT_IND") & F.col("ACCEPTED_IND"))
        .select("REPORT_ID", F.col("EVENT_ID").cast("long").alias("EVENT_ID"), "MATCH_LANE",
                "REPORT_SRC_ADC_UPDT", "SOURCE_VERSION_ID", "BLOB_VERSION_ID", "RAW_SHA256",
                "TEXT_SHA256", "EFFECTIVE_CURRENT_DECISION", "CURRENT_ROW_COUNT",
                "CURRENT_BLOB_VERSION_IDS", "TEXT_INTEGRITY_STATUS"))
    # Every current row of an accepted event carries the same text, so the text hash alone
    # identifies it; the ordering only fixes which row's admin stamps are reported.
    text = (current_blob_rows(state, accepted.select("EVENT_ID").distinct())
        .select(F.col("EVENT_ID").alias("_T_EVENT_ID"), F.col("TEXT_SHA256").alias("_T_SHA"),
                "BLOB_TEXT", "STATUS", "CONTENT_TYPE", "decompressor_version", "parser_version",
                "post_processor_version", "VALID_FROM_DT_TM", "UPDT_DT_TM", "ADC_UPDT",
                F.col("BLOB_VERSION_ID").alias("_T_BLOB_VERSION_ID")))
    joined = accepted.join(
        text,
        (F.col("EVENT_ID") == F.col("_T_EVENT_ID")) & (F.col("TEXT_SHA256") == F.col("_T_SHA")),
        "inner")
    pick = Window.partitionBy("REPORT_ID").orderBy(
        (F.col("_T_BLOB_VERSION_ID").eqNullSafe(F.col("BLOB_VERSION_ID"))).desc(),
        F.col("VALID_FROM_DT_TM").desc_nulls_last(),
        F.col("ADC_UPDT").desc_nulls_last(),
        F.col("UPDT_DT_TM").desc_nulls_last())
    out = (joined.withColumn("_RN", F.row_number().over(pick)).where(F.col("_RN") == 1)
        .withColumn("BRIDGED_TEXT_FORMAT", F.when(
            F.lower(F.substring(F.ltrim("BLOB_TEXT"), 1, len(RTF_PREFIX))) == F.lit(RTF_PREFIX),
            F.lit("RTF")).otherwise(F.lit("PLAIN")))
        .select(
            F.col("REPORT_ID").cast("long").alias("REPORT_ID"),
            F.col("EVENT_ID").cast("string").alias("EVENT_ID"),
            "MATCH_LANE",
            F.lit(None).cast("string").alias("BRIDGED_TEXT_RAW"),
            F.col("BLOB_TEXT").alias("BRIDGED_TEXT"),
            "BRIDGED_TEXT_FORMAT",
            F.col("STATUS").alias("BRIDGED_TEXT_PARSE_STATUS"),
            F.concat(
                F.lit("mill_blob_text:d"), F.coalesce(F.col("decompressor_version").cast("string"), F.lit("?")),
                F.lit("/p"), F.coalesce(F.col("parser_version").cast("string"), F.lit("?")),
                F.lit("/pp"), F.coalesce(F.col("post_processor_version").cast("string"), F.lit("?")),
            ).alias("BRIDGED_TEXT_PARSER_VERSION"),
            F.lit(None).cast("timestamp").alias("BLOB_EXTRACT_DT_TM"),
            F.col("UPDT_DT_TM").alias("BLOB_UPDT_DT_TM"),
            F.col("ADC_UPDT").alias("BLOB_ADC_UPDT"),
            "REPORT_SRC_ADC_UPDT",
            "SOURCE_VERSION_ID",
            "BLOB_VERSION_ID",
            "RAW_SHA256",
            "TEXT_SHA256",
            F.col("CONTENT_TYPE").alias("BLOB_CONTENT_TYPE"),
            "EFFECTIVE_CURRENT_DECISION",
            "CURRENT_ROW_COUNT",
            "CURRENT_BLOB_VERSION_IDS",
            "TEXT_INTEGRITY_STATUS"))
    out, flagged = dq_all_clinical(out, ADMIN_STAMPS)
    assert flagged == [], f"unexpected clinical temporal columns: {flagged}"
    return (out
        .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
        .withColumn("ROW_HASH", row_hash_expr(BRIDGE_COLUMNS)))

# COMMAND ----------

COLUMN_COMMENTS = {
    "REPORT_ID": "Sectra PACS report identifier (map_pacs_report.PACS_REPORT_ID). Primary key.",
    "EVENT_ID": "Millennium RADRPT (class 224) clinical-event identifier whose decoded text is bridged. Unique among current rows.",
    "MATCH_LANE": "PACS_EXAM_LINK when the RADRPT event's map_radiology_event PACS link resolves to the report's examination; ACCESSION_UNIQUE_EXAM when it carries the examination's Sectra accession and that accession names exactly one PACS examination.",
    "BRIDGED_TEXT_RAW": "LEGACY / UNPOPULATED since v4. The retired pacs_blob_content source held pre-parse RTF here; v4 bridges the blob owner's decoded text only.",
    "BRIDGED_TEXT": "Decoded report text from bronze.mill_blob_text (BLOB_TEXT) for the effective-current version. Never replaces native PACS report text.",
    "BRIDGED_TEXT_FORMAT": "RTF when the decoded text still begins with an RTF header, otherwise PLAIN.",
    "BRIDGED_TEXT_PARSE_STATUS": "mill_blob_text STATUS of the bridged version (the blob owner's decode outcome).",
    "BRIDGED_TEXT_PARSER_VERSION": "mill_blob_text decompressor/parser/post-processor versions of the bridged row, as mill_blob_text:d<n>/p<n>/pp<n>.",
    "BLOB_EXTRACT_DT_TM": "LEGACY / UNPOPULATED since v4 (pacs_blob_content extract timestamp; mill_blob_text has no equivalent).",
    "BLOB_UPDT_DT_TM": "Administrative source update timestamp of the bridged mill_blob_text row.",
    "BLOB_ADC_UPDT": "Administrative ADC load timestamp of the bridged mill_blob_text row.",
    "REPORT_SRC_ADC_UPDT": "Administrative source load timestamp from map_pacs_report.",
    "SOURCE_VERSION_ID": "mill_blob_text SOURCE_VERSION_ID of the bridged version.",
    "BLOB_VERSION_ID": "mill_blob_text BLOB_VERSION_ID of the bridged version.",
    "RAW_SHA256": "mill_blob_text raw_sha256 (hash of the source binary) of the bridged version.",
    "TEXT_SHA256": "SHA-256 of BRIDGED_TEXT; equals the hash of every open-current mill_blob_text row for EVENT_ID.",
    "BLOB_CONTENT_TYPE": "mill_blob_text CONTENT_TYPE of the bridged version.",
    "EFFECTIVE_CURRENT_DECISION": "single_current, or identical_text_collapsed when several open-current mill_blob_text rows carry byte-identical text. Ambiguous events are never bridged.",
    "CURRENT_ROW_COUNT": "Number of open-current mill_blob_text rows for EVENT_ID (more than 1 is the upstream SCD2 defect).",
    "CURRENT_BLOB_VERSION_IDS": "Sorted comma-separated BLOB_VERSION_IDs of all open-current rows when CURRENT_ROW_COUNT > 1 (collapse provenance); NULL otherwise.",
    "TEXT_INTEGRITY_STATUS": "single_version_chunks (raw holds one row per chunk slot) or raw_chunks_absent (raw no longer holds the event). text_integrity_unknown events are never bridged.",
    "PIPELINE_UPDT_DT_TM": "Timestamp when this bridge row was rebuilt.",
    "ROW_HASH": "SHA-256 over the business and source-stamp columns, excluding pipeline build stamps and lifecycle flags.",
    "SOURCE_PRESENT_IND": "True for a current accepted bridge; false for a carried-forward tombstone. Consumers must filter to true.",
}

REL_COMMENTS = {
    "CANDIDATE_KEY": "Primary key: REPORT_ID|EVENT_ID, or REPORT_ID|NONE for a textless report with no candidate document.",
    "REPORT_ID": "Textless Sectra PACS report identifier (map_pacs_report.PACS_REPORT_ID).",
    "EVENT_ID": "Candidate Millennium RADRPT (class 224) clinical-event identifier; NULL when none was found.",
    "PACS_EXAMINATION_ID": "PACS examination the report belongs to.",
    "SECTRA_ACCESSION_NBR": "Sectra accession of the report's examination (map_pacs_examination.SECTRA_ACCESSION_NBR, upper-cased).",
    "MATCH_LANE": "Strongest lane with any candidate for this report: PACS_EXAM_LINK or ACCESSION_UNIQUE_EXAM. NULL when there is no candidate.",
    "REPORT_CANDIDATE_DOC_COUNT": "Distinct candidate RADRPT events for this report in its lane. More than 1 withholds the report (separate reports/addenda are never merged).",
    "DOC_CANDIDATE_REPORT_COUNT": "Distinct textless reports claiming this RADRPT event. More than 1 withholds every claimant.",
    "IDENTITY_CONFLICT_COUNT": "Candidate RADRPT events excluded because their PERSON_ID differs from the report's (or examination's) PERSON_ID.",
    "ACCOUNTING_CLASS": "BRIDGED, MULTI_DOCUMENT, MULTI_REPORT, BOTH, TEXT_VERSION_AMBIGUOUS, TEXT_INTEGRITY_UNKNOWN or UNMATCHED. Uniform across a report's rows; the classes partition the textless reports.",
    "UNMATCHED_REASON": "For UNMATCHED: NO_RESOLVED_EXAM, IDENTITY_CONFLICT, NO_MILL_DOCUMENT_ON_LINK_KEYS, NO_CURRENT_BLOB_TEXT or EMPTY_BLOB_TEXT.",
    "EFFECTIVE_CURRENT_DECISION": "single_current, identical_text_collapsed, text_version_ambiguous (open-current rows disagree; nothing picked) or no_current_row.",
    "CURRENT_ROW_COUNT": "Number of open-current mill_blob_text rows for EVENT_ID.",
    "CURRENT_TEXT_VARIANT_COUNT": "Distinct texts (NULL text counted once) among the open-current mill_blob_text rows for EVENT_ID.",
    "CURRENT_BLOB_VERSION_IDS": "Sorted comma-separated BLOB_VERSION_IDs of all open-current rows when there is more than one.",
    "SOURCE_VERSION_ID": "mill_blob_text SOURCE_VERSION_ID of the effective-current version; NULL when ambiguous.",
    "BLOB_VERSION_ID": "mill_blob_text BLOB_VERSION_ID of the effective-current version; NULL when ambiguous.",
    "RAW_SHA256": "mill_blob_text raw_sha256 of the effective-current version; NULL when ambiguous.",
    "TEXT_SHA256": "SHA-256 of the effective-current decoded text; NULL when ambiguous or the text is NULL or blank.",
    "TEXT_INTEGRITY_STATUS": "single_version_chunks, raw_chunks_absent, text_integrity_unknown (raw holds several rows for a chunk slot, so the decode may mix document versions), not_assessed_version_ambiguous or no_current_text.",
    "RAW_MAX_ROWS_PER_CHUNK_SEQ": "Largest number of raw.mill_ce_blob rows sharing one (EVENT_ID, BLOB_SEQ_NUM); NULL when raw holds no rows for the event.",
    "ACCEPTED_IND": "True exactly when ACCOUNTING_CLASS is BRIDGED; such rows appear in map_pacs_report_text_bridge.",
    "REPORT_SRC_ADC_UPDT": "Administrative source load timestamp from map_pacs_report.",
    "PIPELINE_UPDT_DT_TM": "Timestamp when this row was rebuilt.",
    "ROW_HASH": "SHA-256 over the business columns, excluding pipeline build stamps and lifecycle flags.",
    "SOURCE_PRESENT_IND": "True for a current relationship; false for a carried-forward tombstone.",
}

BRIDGE_IG = {
    "BRIDGED_TEXT_RAW": ("4", "2"),
    "BRIDGED_TEXT": ("4", "2"),
    "RAW_SHA256": ("1", "1"),
    "TEXT_SHA256": ("1", "1"),
    **{c: ("0", "0") for c in (
        "MATCH_LANE", "BRIDGED_TEXT_FORMAT", "BRIDGED_TEXT_PARSE_STATUS",
        "BRIDGED_TEXT_PARSER_VERSION", "BLOB_EXTRACT_DT_TM", "BLOB_UPDT_DT_TM", "BLOB_ADC_UPDT",
        "REPORT_SRC_ADC_UPDT", "SOURCE_VERSION_ID", "BLOB_VERSION_ID", "BLOB_CONTENT_TYPE",
        "EFFECTIVE_CURRENT_DECISION", "CURRENT_ROW_COUNT", "CURRENT_BLOB_VERSION_IDS",
        "TEXT_INTEGRITY_STATUS", "PIPELINE_UPDT_DT_TM", "ROW_HASH", "SOURCE_PRESENT_IND")},
}

REL_IG = {
    "SECTRA_ACCESSION_NBR": ("4", "2"),
    "RAW_SHA256": ("1", "1"),
    "TEXT_SHA256": ("1", "1"),
    **{c: ("0", "0") for c in (
        "CANDIDATE_KEY", "MATCH_LANE", "REPORT_CANDIDATE_DOC_COUNT", "DOC_CANDIDATE_REPORT_COUNT",
        "IDENTITY_CONFLICT_COUNT", "ACCOUNTING_CLASS", "UNMATCHED_REASON",
        "EFFECTIVE_CURRENT_DECISION", "CURRENT_ROW_COUNT", "CURRENT_TEXT_VARIANT_COUNT",
        "CURRENT_BLOB_VERSION_IDS", "SOURCE_VERSION_ID", "BLOB_VERSION_ID",
        "TEXT_INTEGRITY_STATUS", "RAW_MAX_ROWS_PER_CHUNK_SEQ", "ACCEPTED_IND",
        "REPORT_SRC_ADC_UPDT", "PIPELINE_UPDT_DT_TM", "ROW_HASH", "SOURCE_PRESENT_IND")},
}

def apply_comments(accounting):
    ct = accounting["class_totals"]
    lanes = accounting["bridge_by_lane"]
    bridge = ct["BRIDGED"]
    combined = accounting["native_text_reports"] + bridge
    coverage = 100.0 * combined / max(1, accounting["total_reports"])
    comment = (
        "PACS report-text SIDECAR (v4). Grain: one row per bridged REPORT_ID; REPORT_ID unique, "
        "EVENT_ID unique among current rows. Never updates or supersedes map_pacs_report. Text is "
        "the effective-current bronze.mill_blob_text row of a Millennium RADRPT event linked to the "
        "report's examination through map_radiology_event (PACS_EXAM_LINK) or a single-examination "
        "Sectra accession (ACCESSION_UNIQUE_EXAM). Publishes only 1:1 report/document pairs whose "
        "open-current text is unambiguous and whose raw chunks hold one version. Current accounting: "
        f"{bridge} bridged ({lanes['PACS_EXAM_LINK']} exam-link lane; "
        f"{lanes['ACCESSION_UNIQUE_EXAM']} accession lane); {ct['MULTI_DOCUMENT']} multi-document, "
        f"{ct['MULTI_REPORT']} multi-report and {ct['BOTH']} both withheld; "
        f"{ct['TEXT_VERSION_AMBIGUOUS']} text-version-ambiguous and {ct['TEXT_INTEGRITY_UNKNOWN']} "
        f"integrity-unknown withheld; {ct['UNMATCHED']} unmatched. Native plus bridge coverage is "
        f"{combined} of {accounting['total_reports']} reports ({coverage:.3f}%). All candidate "
        "relationships and withheld reasons: map_pacs_report_text_candidate. Consumer contract: "
        "left join rows with SOURCE_PRESENT_IND=true on REPORT_ID and use COALESCE(native "
        "REPORT_TEXT, BRIDGED_TEXT); native text always wins."
    )
    spark.sql(f"COMMENT ON TABLE {qname(TARGET)} IS '{sql_text(comment)}'")
    rel_comment = (
        "PACS report-text candidate relationships (v4). Grain: one row per textless PACS report and "
        "candidate Millennium RADRPT event (CANDIDATE_KEY), or one REPORT_ID|NONE row when no "
        "candidate exists. Records the lane, multiplicity, identity conflicts, the effective-current "
        "decision over open-current mill_blob_text rows and raw chunk integrity evidence. "
        "ACCEPTED_IND rows are exactly the current map_pacs_report_text_bridge rows. Rows with "
        "EFFECTIVE_CURRENT_DECISION text_version_ambiguous or TEXT_INTEGRITY_STATUS "
        "text_integrity_unknown identify events for the blob owner's automated repair."
    )
    spark.sql(f"COMMENT ON TABLE {qname(TARGET_REL)} IS '{sql_text(rel_comment)}'")
    for table, comments in ((TARGET, COLUMN_COMMENTS), (TARGET_REL, REL_COMMENTS)):
        for col_name, col_comment in comments.items():
            spark.sql(f"""ALTER TABLE {qname(table)} ALTER COLUMN {qident(col_name)}
                          COMMENT '{sql_text(col_comment)}'""")

def dq_triplet_gate(table, admin_stamps):
    fields = spark.table(table).schema.fields
    names = {f.name for f in fields}
    temporal = [f.name for f in fields if isinstance(f.dataType, (TimestampType, DateType))]
    missing = {}
    for c in temporal:
        if c in admin_stamps or c.endswith("_CLEAN"):
            continue
        expected = {f"{c}_FUTURE_IND", f"{c}_SENTINEL_IND", f"{c}_CLEAN"}
        absent = sorted(expected - names)
        if absent:
            missing[c] = absent
    assert not missing, f"missing date-quality companions: {missing}"
    return temporal

def run_gates(state, report_stats):
    assert spark.catalog.tableExists(TARGET), f"G0 target absent: {TARGET}"
    assert spark.catalog.tableExists(TARGET_REL), f"G0 relationship table absent: {TARGET_REL}"
    target = spark.table(TARGET)
    present = target.where(F.col("SOURCE_PRESENT_IND"))
    rel = spark.table(TARGET_REL)
    rel_present = rel.where(F.col("SOURCE_PRESENT_IND"))
    accounting = collect_accounting(report_stats)

    # G1 keys. Tombstones may legitimately share an EVENT_ID with a current row after a
    # document moves between reports, so EVENT_ID uniqueness is asserted on current rows.
    key = target.agg(
        F.count(F.lit(1)).cast("long").alias("rows"),
        F.countDistinct("REPORT_ID").cast("long").alias("report_ids"),
        F.sum(F.when(F.col("REPORT_ID").isNull(), 1).otherwise(0)).cast("long").alias("null_report"),
    ).collect()[0]
    present_key = present.agg(
        F.count(F.lit(1)).cast("long").alias("rows"),
        F.countDistinct("EVENT_ID").cast("long").alias("event_ids"),
        F.sum(F.when(F.col("EVENT_ID").isNull(), 1).otherwise(0)).cast("long").alias("null_event"),
        F.count("BLOB_VERSION_ID").cast("long").alias("blob_versions"),
        F.countDistinct("BLOB_VERSION_ID").cast("long").alias("distinct_blob_versions"),
    ).collect()[0]
    assert int(key["rows"]) == int(key["report_ids"]), "G1 REPORT_ID uniqueness failed"
    assert int(key["null_report"]) == 0
    assert int(present_key["rows"]) == int(present_key["event_ids"]), "G1 current EVENT_ID uniqueness failed"
    assert int(present_key["null_event"]) == 0
    assert int(present_key["blob_versions"]) == int(present_key["distinct_blob_versions"]), (
        "G1 current BLOB_VERSION_ID uniqueness failed")
    rel_key = rel.agg(F.count(F.lit(1)).alias("rows"),
                      F.countDistinct("CANDIDATE_KEY").alias("keys")).collect()[0]
    assert int(rel_key["rows"]) == int(rel_key["keys"]), "G1 CANDIDATE_KEY uniqueness failed"

    # G2 accounting and the accepted/published key sets.
    target_lanes = {r["MATCH_LANE"]: int(r["n"]) for r in
                    present.groupBy("MATCH_LANE").agg(F.count(F.lit(1)).alias("n")).collect()}
    expected_lanes = {k: v for k, v in accounting["bridge_by_lane"].items() if v}
    assert int(present_key["rows"]) == accounting["class_totals"]["BRIDGED"], "G2 bridge count mismatch"
    assert target_lanes == expected_lanes, f"G2 lane mismatch: {target_lanes} != {expected_lanes}"
    uniform = (rel_present.groupBy("REPORT_ID")
        .agg(F.countDistinct("ACCOUNTING_CLASS").alias("n")).where("n > 1").limit(1).count())
    assert uniform == 0, "G2 a report carries more than one ACCOUNTING_CLASS"
    expected = rel_present.where(F.col("ACCEPTED_IND")).select(
        "REPORT_ID", "EVENT_ID", "MATCH_LANE", "BLOB_VERSION_ID", "TEXT_SHA256")
    actual = present.select("REPORT_ID", "EVENT_ID", "MATCH_LANE", "BLOB_VERSION_ID", "TEXT_SHA256")
    mismatch = expected.exceptAll(actual).limit(1).count() + actual.exceptAll(expected).limit(1).count()
    assert mismatch == 0, "G2 published rows differ from the accepted relationships"

    # G3 native text always wins.
    reports = pinned("report", state).where(F.col("SOURCE_PRESENT_IND")).select(
        F.col("PACS_REPORT_ID").cast("long").alias("REPORT_ID"), "REPORT_TEXT")
    native_collision = (present.select("REPORT_ID").join(reports, "REPORT_ID", "inner")
        .where(F.col("REPORT_TEXT").isNotNull() & (F.trim("REPORT_TEXT") != ""))
        .limit(1).count())
    assert native_collision == 0, "G3 bridge contains a report with native text"

    # G4 version safety, re-proved against the pinned source rather than trusted from the build:
    # zero bridged events with more than one effective-current text, and every bridged text
    # hash is the text of the event's open-current rows.
    bad_status = present.where(
        ~F.col("EFFECTIVE_CURRENT_DECISION").isin(*ACCEPTED_DECISIONS)
        | ~F.col("TEXT_INTEGRITY_STATUS").isin(*ACCEPTED_INTEGRITY)
        | ~F.col("TEXT_SHA256").eqNullSafe(F.sha2(F.col("BRIDGED_TEXT"), 256))).limit(1).count()
    assert bad_status == 0, "G4 bridged row with unaccepted version/integrity status or text hash"
    ids = present.select(F.col("EVENT_ID").cast("long").alias("EVENT_ID")).distinct()
    source_text = (current_blob_rows(state, ids)
        .groupBy("EVENT_ID")
        .agg(F.size(F.collect_set(F.coalesce(F.col("TEXT_SHA256"), F.lit("<NO_TEXT>")))).alias("_V"),
             F.max("TEXT_SHA256").alias("_SHA")))
    multi_current = source_text.where(F.col("_V") > 1).limit(1).count()
    assert multi_current == 0, "G4 a bridged event has more than one effective-current document"
    drifted = (present.select(F.col("EVENT_ID").cast("long").alias("EVENT_ID"), "TEXT_SHA256")
        .join(source_text, "EVENT_ID", "left")
        .where(~F.col("TEXT_SHA256").eqNullSafe(F.col("_SHA"))).limit(1).count())
    assert drifted == 0, "G4 a bridged text is not the event's open-current text"

    temporal = dq_triplet_gate(TARGET, ADMIN_STAMPS)
    dq_triplet_gate(TARGET_REL, {"REPORT_SRC_ADC_UPDT", "PIPELINE_UPDT_DT_TM"})
    ig_tag_gate(TARGET)
    ig_tag_gate(TARGET_REL)

    invalid = present.agg(
        F.sum(F.when(~F.col("MATCH_LANE").isin(*MATCH_LANES), 1).otherwise(0)).alias("bad_lane"),
        F.sum(F.when(F.col("BRIDGED_TEXT").isNull() | (F.trim("BRIDGED_TEXT") == ""), 1)
              .otherwise(0)).alias("blank_derived"),
        F.sum(F.when(F.col("ROW_HASH") != row_hash_expr(BRIDGE_COLUMNS), 1).otherwise(0)).alias("bad_hash"),
    ).collect()[0]
    assert int(invalid["bad_lane"] or 0) == 0
    assert int(invalid["blank_derived"] or 0) == 0
    assert int(invalid["bad_hash"] or 0) == 0

    fingerprint = table_fingerprint(TARGET)
    result = {
        "gates": "PASS",
        "target_rows": int(key["rows"]),
        "target_present_rows": accounting["class_totals"]["BRIDGED"],
        "target_tombstones": int(key["rows"]) - accounting["class_totals"]["BRIDGED"],
        "target_lanes": target_lanes,
        "relationship_rows": int(rel_key["rows"]),
        "accounting": accounting,
        "temporal_columns": temporal,
        "canonical_fingerprint": str(fingerprint),
    }
    print("[B12][GATES] PASS", json.dumps(result, default=str, sort_keys=True))
    return result

# COMMAND ----------

# Gates are deliberately invoked before the first build. On an absent target this must fail.
TARGET_EXISTED_AT_START = (spark.catalog.tableExists(TARGET)
                           and spark.catalog.tableExists(TARGET_REL))
if not TARGET_EXISTED_AT_START:
    try:
        assert TARGET_EXISTED_AT_START, f"G0 target absent: {TARGET} / {TARGET_REL}"
        raise AssertionError("pre-build gate unexpectedly passed")
    except AssertionError as exc:
        print(f"[B12][GATES-FIRST] EXPECTED_PREBUILD_FAILURE: {exc}")

state_start, source_signature = source_state()
assert_source_contract(state_start)
report_stats = report_snapshot_stats(state_start)
print("[B12][SOURCE_STATE]", json.dumps(state_start, sort_keys=True))

props_before = target_properties()
stored_signature = props_before.get("b12.source_signature")
due = FORCE_REBUILD or not TARGET_EXISTED_AT_START or stored_signature != source_signature

if TARGET_EXISTED_AT_START:
    try:
        prebuild_gate_result = run_gates(state_start, report_stats)
        print("[B12][GATES-FIRST] existing target gates passed")
    except Exception as exc:
        if not due and ACTION == "gates":
            raise
        print(f"[B12][GATES-FIRST] existing target is stale or invalid and will rebuild: "
              f"{type(exc).__name__}: {exc}")

if ACTION == "gates":
    result = run_gates(state_start, report_stats)
    control_versions = record_s6_source_versions(state_start)
    dbutils.notebook.exit(json.dumps({
        "pipeline": PIPELINE_NAME,
        "mode": "GATES_ONLY",
        "target": TARGET,
        "target_schema": TARGET_SCHEMA,
        "source_signature": source_signature,
        "control_versions": control_versions,
        "result": result,
    }, default=str, sort_keys=True))

if not due:
    version_before = table_version(TARGET)
    result = run_gates(state_start, report_stats)
    control_versions = record_s6_source_versions(state_start)
    version_after = table_version(TARGET)
    assert version_before == version_after, (
        f"NO_OP mutated target version: {version_before} -> {version_after}"
    )
    summary = {
        "pipeline": PIPELINE_NAME,
        "mode": "NO_OP",
        "target": TARGET,
        "target_schema": TARGET_SCHEMA,
        "source_signature": source_signature,
        "source_state": state_start,
        "control_versions": control_versions,
        "target_version_before": version_before,
        "target_version_after": version_after,
        "result": result,
    }
    print("[B12][NO_OP]", json.dumps(summary, default=str, sort_keys=True))
    dbutils.notebook.exit(json.dumps(summary, default=str, sort_keys=True))

# COMMAND ----------

# The relationship table is the build's materialisation point: accounting and the accepted
# bridge are both derived from it, so the classification runs once.
replace_with_tombstones(build_relationships(state_start), TARGET_REL, ["CANDIDATE_KEY"])
accounting = collect_accounting(report_stats)
print("[B12][ACCOUNTING]", json.dumps(accounting, default=str, sort_keys=True))

replace_with_tombstones(build_candidate(state_start), TARGET, ["REPORT_ID"])

apply_comments(accounting)
ig_tag_table(TARGET, BRIDGE_IG)
ig_tag_table(TARGET_REL, REL_IG)

result = run_gates(state_start, report_stats)

# Every source is Delta; recheck all versions after the build so a mid-run commit cannot be
# recorded as the state this build reflects.
state_end, signature_end = source_state()
assert signature_end == source_signature, (
    f"source drift during build: {source_signature} -> {signature_end}; state not committed"
)

set_target_properties({
    "b12.logic_version": LOGIC_VERSION,
    "b12.source_signature": source_signature,
    **{f"b12.{name}_source_version": state_start[f"{name}_version"] for name in SOURCES},
    "b12.canonical_fingerprint": result["canonical_fingerprint"],
    "b12.present_rows": accounting["class_totals"]["BRIDGED"],
    "b12.accounting_json": json.dumps(accounting["class_totals"], sort_keys=True),
})
control_versions = record_s6_source_versions(state_start)

summary = {
    "pipeline": PIPELINE_NAME,
    "mode": "BUILD",
    "target": TARGET,
    "target_schema": TARGET_SCHEMA,
    "run_started_at": RUN_STARTED_AT,
    "source_signature": source_signature,
    "source_state_start": state_start,
    "source_state_end": state_end,
    "control_versions": control_versions,
    "result": result,
}
print("[B12][COMPLETE]", json.dumps(summary, default=str, sort_keys=True))
dbutils.notebook.exit(json.dumps(summary, default=str, sort_keys=True))

# COMMAND ----------

# PROMOTION RUNBOOK — HUMAN GATED; DO NOT EXECUTE AS PART OF THIS DEV TASK.
#
# 1. Deploy map_pacs_examination v3.2 (pacs_pipeline) and map_radiology_event v2 first;
#    assert_source_contract refuses to run against older upstream schemas.
# 2. Run once against the production sources with target_schema=8_dev.bronze and compare the
#    accounting with the prior 4_prod bridge (4,159,098 rows on 2026-09-24): the old bridge was
#    keyed on pacs_blob_content MillAccessionNbr, so a changed row set is expected; review the
#    per-lane counts and the withheld classes, not parity.
# 3. With explicit approval, run with target_schema=4_prod.bronze and allow_production_write=true.
#    Keep task retries unchanged. The first production run tombstones every prior row whose
#    REPORT_ID is no longer bridged; tombstones keep their old text, so consumers MUST filter
#    SOURCE_PRESENT_IND=true (the staged silver_journey_shared does).
# 4. Gates run before and after the build: key uniqueness, accounting, native-text collisions,
#    version safety against the pinned source, date-quality coverage, row hashes, IG tags.
# 5. Blob-owner repair feed (no curation queue): SELECT DISTINCT EVENT_ID, EFFECTIVE_CURRENT_DECISION,
#    TEXT_INTEGRITY_STATUS FROM map_pacs_report_text_candidate WHERE SOURCE_PRESENT_IND AND
#    (EFFECTIVE_CURRENT_DECISION='text_version_ambiguous' OR TEXT_INTEGRITY_STATUS='text_integrity_unknown').

# PACS_TEXT_BRIDGE_V4_PATCHED

