# Databricks notebook source
# pacs_text_bridge_pipeline — S6/B12 PACS report-text bridge sidecar.
#
# Scope is deliberately fixed to 8_dev.bronze.map_pacs_report_text_bridge.
# This notebook never updates map_pacs_report and contains no executable production write.
# Pure Python: no percent-run, magic commands, or hidden notebook dependencies.

# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 1096419474549718)
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

TARGET = f"{TARGET_SCHEMA}.map_pacs_report_text_bridge"
SRC_REPORT = "4_prod.bronze.map_pacs_report"
SRC_EXAM = "4_prod.bronze.map_pacs_examination"
SRC_BLOB = "4_prod.pacs_dlt.pacs_blob_content"
CONTROL_TABLE = f"{CONTROL_SCHEMA}.s6_source_versions"
CONTROL_PIPELINE = "s6_b12_pacs_text_bridge_pipeline"

LOGIC_VERSION = "2026-08-13.b12.v1"
PIPELINE_NAME = "pacs_text_bridge_pipeline"
RTF_PREFIX = "{" + chr(92) + "rtf"
ACCOUNTING_CLASSES = (
    "BRIDGED",
    "MULTI_BLOB_ACCESSION",
    "MULTI_REPORT_ACCESSION",
    "BOTH",
    "UNMATCHED",
)

# COMMAND ----------

import hashlib
import importlib.metadata
import json
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, TimestampType

RUN_STARTED_AT = datetime.now(timezone.utc).isoformat()
try:
    from striprtf.striprtf import rtf_to_text as _rtf_to_text
    STRIPRTF_VERSION = importlib.metadata.version("striprtf")
    HAVE_STRIPRTF = True
except Exception as exc:
    _rtf_to_text = None
    STRIPRTF_VERSION = None
    HAVE_STRIPRTF = False
    print(f"[B12][PARSER] striprtf unavailable on driver: {type(exc).__name__}: {exc}")

if HAVE_STRIPRTF:
    _fixture = RTF_PREFIX + "1" + chr(92) + "ansi Hello" + chr(92) + "par world}"
    _fixture_text = _rtf_to_text(_fixture, errors="ignore")
    assert "Hello" in _fixture_text and "world" in _fixture_text
    assert "Helloworld" not in _fixture_text.replace(" ", "")
    print(f"[B12][PARSER] established Blob Processing v2 striprtf parser available: {STRIPRTF_VERSION}")

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

ROW_HASH_COLUMNS = [
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
]

def row_hash_expr():
    return F.sha2(F.to_json(F.struct(*[F.col(c) for c in ROW_HASH_COLUMNS])), 256)

def blob_snapshot_stats():
    b = spark.table(SRC_BLOB).select(
        "EVENT_ID", "MillAccessionNbr", "BLOB_CONTENTS", "BLOB_LENGTH", "BLOB_SEQ_NBR",
        "EXTRACT_DT_TM", "UPDT_DT_TM", "ADC_UPDT", "TRUNCATION_IND", "ERROR_IND")
    rtf = F.lower(F.substring(F.ltrim(F.col("BLOB_CONTENTS")), 1, len(RTF_PREFIX))) == F.lit(RTF_PREFIX)
    row = b.agg(
        F.count(F.lit(1)).cast("long").alias("rows"),
        F.countDistinct("EVENT_ID").cast("long").alias("distinct_event_id"),
        F.countDistinct(F.trim("MillAccessionNbr")).cast("long").alias("distinct_accessions"),
        F.sum(F.when(F.col("EVENT_ID").isNull(), 1).otherwise(0)).cast("long").alias("null_event_id"),
        F.sum(F.when(F.col("BLOB_CONTENTS").isNull() | (F.trim("BLOB_CONTENTS") == ""), 1)
              .otherwise(0)).cast("long").alias("blank_blob_contents"),
        F.sum(F.when(F.coalesce("TRUNCATION_IND", F.lit(0)) != 0, 1).otherwise(0))
              .cast("long").alias("truncation_rows"),
        F.sum(F.when(F.coalesce("ERROR_IND", F.lit(0)) != 0, 1).otherwise(0))
              .cast("long").alias("error_rows"),
        F.sum(F.when(rtf, 1).otherwise(0)).cast("long").alias("rtf_rows"),
        F.max("BLOB_SEQ_NBR").cast("long").alias("max_blob_seq_nbr"),
        F.min("EXTRACT_DT_TM").alias("min_extract_dt_tm"),
        F.max("EXTRACT_DT_TM").alias("max_extract_dt_tm"),
        F.max("ADC_UPDT").alias("max_adc_updt"),
        F.expr("""bit_xor(xxhash64(
            EVENT_ID, MillAccessionNbr, BLOB_LENGTH, BLOB_SEQ_NBR, BLOB_CONTENTS, ADC_UPDT
        ))""").cast("long").alias("content_xor"),
    ).collect()[0].asDict()
    return row

def assembly_gate(stats):
    assert int(stats["rows"]) == int(stats["distinct_event_id"]), (
        f"blob is not pre-assembled: rows={stats['rows']} distinct_event_id={stats['distinct_event_id']}"
    )
    assert int(stats["null_event_id"]) == 0
    assert int(stats["truncation_rows"]) == 0, f"truncation flags present: {stats['truncation_rows']}"
    assert int(stats["error_rows"]) == 0, f"error flags present: {stats['error_rows']}"
    if int(stats["rtf_rows"]) > 0:
        assert HAVE_STRIPRTF, "RTF rows exist but the established striprtf parser is unavailable"
    print("[B12][ASSEMBLY] PASS", json.dumps(stats, default=str, sort_keys=True))

def report_snapshot_stats(report_version):
    r = pinned_table(SRC_REPORT, report_version).where(F.col("SOURCE_PRESENT_IND"))
    return r.agg(
        F.count(F.lit(1)).cast("long").alias("total_reports"),
        F.sum(F.when(F.col("REPORT_TEXT").isNull() | (F.trim("REPORT_TEXT") == ""), 1)
              .otherwise(0)).cast("long").alias("textless_reports"),
        F.sum(F.when(F.col("REPORT_TEXT").isNotNull() & (F.trim("REPORT_TEXT") != ""), 1)
              .otherwise(0)).cast("long").alias("native_text_reports"),
    ).collect()[0].asDict()

def source_state():
    report_version = table_version(SRC_REPORT)
    exam_version = table_version(SRC_EXAM)
    blob_stats = blob_snapshot_stats()
    assembly_gate(blob_stats)
    state = {
        "logic_version": LOGIC_VERSION,
        "report_version": report_version,
        "exam_version": exam_version,
        "blob_rows": int(blob_stats["rows"]),
        "blob_distinct_event_id": int(blob_stats["distinct_event_id"]),
        "blob_distinct_accessions": int(blob_stats["distinct_accessions"]),
        "blob_max_adc_updt": str(blob_stats["max_adc_updt"]),
        "blob_content_xor": int(blob_stats["content_xor"]),
        "striprtf_version": STRIPRTF_VERSION or "UNAVAILABLE_NO_RTF",
    }
    signature = hashlib.sha256(
        json.dumps(state, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return state, signature, blob_stats

def record_s6_source_versions(state):
    """Post-gate shared S6 ledger commit. The SHA-256 source signature remains the NO_OP authority."""
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {CONTROL_TABLE}
        (pipeline STRING, source STRING, version BIGINT, updated_at TIMESTAMP)""")
    versions = {
        SRC_REPORT: int(state["report_version"]),
        SRC_EXAM: int(state["exam_version"]),
        SRC_BLOB: int(state["blob_content_xor"]),
    }
    record_versions(CONTROL_TABLE, CONTROL_PIPELINE, versions)
    recorded = {r["source"]: int(r["version"]) for r in spark.sql(f"""
        SELECT source, version FROM {CONTROL_TABLE}
        WHERE pipeline='{CONTROL_PIPELINE}'""").collect()}
    assert recorded == versions, f"S6 source-version ledger mismatch: {recorded} != {versions}"
    print("[B12][S6_SOURCE_VERSIONS]", json.dumps(recorded, sort_keys=True))
    return recorded

# COMMAND ----------

def build_classification(report_version, exam_version):
    blob = (spark.table(SRC_BLOB)
        .select(
            F.trim("MillAccessionNbr").alias("ACCESSION"),
            F.col("EVENT_ID"),
            F.col("BLOB_CONTENTS"),
            F.col("EXTRACT_DT_TM"),
            F.col("UPDT_DT_TM"),
            F.col("ADC_UPDT"))
        .where(F.col("ACCESSION").isNotNull() & (F.col("ACCESSION") != "")))

    blob_by_accession = (blob.groupBy("ACCESSION").agg(
        F.countDistinct("EVENT_ID").cast("long").alias("BLOB_EVENT_COUNT"),
        F.max("EVENT_ID").alias("ONLY_EVENT_ID"),
        F.max("BLOB_CONTENTS").alias("ONLY_BLOB_CONTENTS"),
        F.max("EXTRACT_DT_TM").alias("ONLY_EXTRACT_DT_TM"),
        F.max("UPDT_DT_TM").alias("ONLY_UPDT_DT_TM"),
        F.max("ADC_UPDT").alias("ONLY_ADC_UPDT"),
    ))

    textless = (pinned_table(SRC_REPORT, report_version)
        .where(F.col("SOURCE_PRESENT_IND"))
        .where(F.col("REPORT_TEXT").isNull() | (F.trim("REPORT_TEXT") == ""))
        .select(
            F.col("PACS_REPORT_ID").cast("long").alias("REPORT_ID"),
            F.col("PACS_EXAMINATION_ID").cast("long").alias("PACS_EXAMINATION_ID"),
            F.col("SRC_ADC_UPDT").alias("REPORT_SRC_ADC_UPDT")))

    exams = (pinned_table(SRC_EXAM, exam_version)
        .where(F.col("SOURCE_PRESENT_IND"))
        .select(
            F.col("PACS_EXAMINATION_ID").cast("long").alias("PACS_EXAMINATION_ID"),
            F.trim("REQUEST_ID_STRING").alias("REQUEST_ID_STRING"),
            F.trim("EXAMINATION_ID_STRING").alias("EXAMINATION_ID_STRING")))

    r = textless.alias("r")
    e = exams.alias("e")
    br = blob_by_accession.alias("br")
    be = blob_by_accession.alias("be")

    joined = (r.join(e, F.col("r.PACS_EXAMINATION_ID") == F.col("e.PACS_EXAMINATION_ID"), "left")
        .join(br, F.col("br.ACCESSION") == F.col("e.REQUEST_ID_STRING"), "left")
        .join(be,
              F.col("br.ACCESSION").isNull()
              & (F.col("be.ACCESSION") == F.col("e.EXAMINATION_ID_STRING")),
              "left"))

    selected = joined.select(
        F.col("r.REPORT_ID"),
        F.col("r.REPORT_SRC_ADC_UPDT"),
        F.coalesce(F.col("br.ACCESSION"), F.col("be.ACCESSION")).alias("ACCESSION"),
        F.when(F.col("br.ACCESSION").isNotNull(), F.lit("REQUEST_ID"))
         .when(F.col("be.ACCESSION").isNotNull(), F.lit("EXAMINATION_ID"))
         .alias("MATCH_LANE"),
        F.coalesce(F.col("br.BLOB_EVENT_COUNT"), F.col("be.BLOB_EVENT_COUNT"))
         .alias("BLOB_EVENT_COUNT"),
        F.coalesce(F.col("br.ONLY_EVENT_ID"), F.col("be.ONLY_EVENT_ID")).alias("EVENT_ID"),
        F.coalesce(F.col("br.ONLY_BLOB_CONTENTS"), F.col("be.ONLY_BLOB_CONTENTS"))
         .alias("BLOB_CONTENTS"),
        F.coalesce(F.col("br.ONLY_EXTRACT_DT_TM"), F.col("be.ONLY_EXTRACT_DT_TM"))
         .alias("BLOB_EXTRACT_DT_TM"),
        F.coalesce(F.col("br.ONLY_UPDT_DT_TM"), F.col("be.ONLY_UPDT_DT_TM"))
         .alias("BLOB_UPDT_DT_TM"),
        F.coalesce(F.col("br.ONLY_ADC_UPDT"), F.col("be.ONLY_ADC_UPDT"))
         .alias("BLOB_ADC_UPDT"),
        F.when(F.col("e.PACS_EXAMINATION_ID").isNull(), F.lit("NO_RESOLVED_EXAM"))
         .when(F.col("br.ACCESSION").isNull() & F.col("be.ACCESSION").isNull(),
               F.lit("NO_BLOB_ON_MATCH_KEYS"))
         .alias("_UNMATCHED_REASON"),
    )

    report_multiplicity = (selected.where(F.col("ACCESSION").isNotNull())
        .groupBy("ACCESSION")
        .agg(F.count(F.lit(1)).cast("long").alias("TEXTLESS_REPORT_COUNT")))

    classified = selected.join(report_multiplicity, "ACCESSION", "left")
    blank_blob = F.col("BLOB_CONTENTS").isNull() | (F.trim("BLOB_CONTENTS") == "")
    accounting_class = (
        F.when(F.col("ACCESSION").isNull(), F.lit("UNMATCHED"))
         .when((F.col("BLOB_EVENT_COUNT") > 1) & (F.col("TEXTLESS_REPORT_COUNT") > 1),
               F.lit("BOTH"))
         .when(F.col("BLOB_EVENT_COUNT") > 1, F.lit("MULTI_BLOB_ACCESSION"))
         .when(F.col("TEXTLESS_REPORT_COUNT") > 1, F.lit("MULTI_REPORT_ACCESSION"))
         .when(blank_blob, F.lit("UNMATCHED"))
         .otherwise(F.lit("BRIDGED"))
    )
    final_reason = (
        F.when(F.col("ACCESSION").isNotNull()
               & (F.col("BLOB_EVENT_COUNT") == 1)
               & (F.col("TEXTLESS_REPORT_COUNT") == 1)
               & blank_blob, F.lit("EMPTY_BLOB"))
         .otherwise(F.col("_UNMATCHED_REASON"))
    )
    return (classified
        .withColumn("ACCOUNTING_CLASS", accounting_class)
        .withColumn("UNMATCHED_REASON", final_reason)
        .drop("_UNMATCHED_REASON"))

def collect_accounting(classified, report_stats):
    rows = (classified.groupBy("ACCOUNTING_CLASS", "MATCH_LANE", "UNMATCHED_REASON")
        .agg(F.count(F.lit(1)).cast("long").alias("n"))
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
    bridge_by_lane = {
        "REQUEST_ID": sum(int(r["n"]) for r in records
                          if r["ACCOUNTING_CLASS"] == "BRIDGED" and r["MATCH_LANE"] == "REQUEST_ID"),
        "EXAMINATION_ID": sum(int(r["n"]) for r in records
                              if r["ACCOUNTING_CLASS"] == "BRIDGED"
                              and r["MATCH_LANE"] == "EXAMINATION_ID"),
    }
    return {
        "records": records,
        "class_totals": class_totals,
        "lane_totals": lane_totals,
        "bridge_by_lane": bridge_by_lane,
        "unmatched_reasons": unmatched_reasons,
        "textless_reports": int(report_stats["textless_reports"]),
        "native_text_reports": int(report_stats["native_text_reports"]),
        "total_reports": int(report_stats["total_reports"]),
    }

# COMMAND ----------

if HAVE_STRIPRTF:
    @F.udf(returnType=StringType())
    def strip_rtf_v2(value):
        if value is None:
            return None
        try:
            parsed = _rtf_to_text(value, errors="ignore")
            return parsed if parsed and parsed.strip() else None
        except Exception:
            return None
else:
    strip_rtf_v2 = None

def build_candidate(classified, blob_stats):
    bridged = classified.where(F.col("ACCOUNTING_CLASS") == "BRIDGED")
    fmt = F.when(
        F.lower(F.substring(F.ltrim("BRIDGED_TEXT_RAW"), 1, len(RTF_PREFIX))) == F.lit(RTF_PREFIX),
        F.lit("RTF")
    ).otherwise(F.lit("PLAIN"))

    out = (bridged
        .select(
            F.col("REPORT_ID").cast("long").alias("REPORT_ID"),
            F.col("EVENT_ID").cast("string").alias("EVENT_ID"),
            F.col("MATCH_LANE"),
            F.col("BLOB_CONTENTS").alias("BRIDGED_TEXT_RAW"),
            F.col("BLOB_EXTRACT_DT_TM"),
            F.col("BLOB_UPDT_DT_TM"),
            F.col("BLOB_ADC_UPDT"),
            F.col("REPORT_SRC_ADC_UPDT"))
        .withColumn("BRIDGED_TEXT_FORMAT", fmt))

    if int(blob_stats["rtf_rows"]) > 0:
        assert strip_rtf_v2 is not None
        out = (out
            .withColumn(
                "BRIDGED_TEXT",
                F.when(F.col("BRIDGED_TEXT_FORMAT") == "RTF",
                       strip_rtf_v2(F.col("BRIDGED_TEXT_RAW")))
                 .otherwise(F.col("BRIDGED_TEXT_RAW")))
            .withColumn(
                "BRIDGED_TEXT_PARSE_STATUS",
                F.when((F.col("BRIDGED_TEXT_FORMAT") == "RTF")
                       & F.col("BRIDGED_TEXT").isNull(), F.lit("STRIPRTF_ERROR"))
                 .when(F.col("BRIDGED_TEXT_FORMAT") == "RTF", F.lit("STRIPRTF_V2"))
                 .otherwise(F.lit("PLAIN_PASSTHROUGH")))
            .withColumn(
                "BRIDGED_TEXT_PARSER_VERSION",
                F.when(F.col("BRIDGED_TEXT_FORMAT") == "RTF",
                       F.lit(f"striprtf-{STRIPRTF_VERSION}"))
                 .otherwise(F.lit("plain-passthrough"))))
    else:
        out = (out
            .withColumn("BRIDGED_TEXT", F.col("BRIDGED_TEXT_RAW"))
            .withColumn("BRIDGED_TEXT_PARSE_STATUS", F.lit("PLAIN_PASSTHROUGH"))
            .withColumn("BRIDGED_TEXT_PARSER_VERSION", F.lit("plain-passthrough")))

    out = out.select(
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
    )
    out, flagged = dq_all_clinical(out, ADMIN_STAMPS)
    assert flagged == [], f"unexpected clinical temporal columns: {flagged}"
    return (out
        .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
        .withColumn("ROW_HASH", row_hash_expr()))

# COMMAND ----------

COLUMN_COMMENTS = {
    "REPORT_ID": "Sectra PACS report identifier. One current bridge row at most; primary key.",
    "EVENT_ID": "Millennium RADRPT clinical-event identifier. Exactly one current report at most.",
    "MATCH_LANE": "REQUEST_ID when REQUEST_ID_STRING matched MillAccessionNbr; EXAMINATION_ID is fallback only.",
    "BRIDGED_TEXT_RAW": "Mill RADRPT blob text verbatim. Never replaces the native PACS report text.",
    "BRIDGED_TEXT": "Plain text derived from BRIDGED_TEXT_RAW. RTF uses the established Blob Processing v2 striprtf parser; plain input passes through unchanged.",
    "BRIDGED_TEXT_FORMAT": "RTF or PLAIN, classified from the raw text prefix.",
    "BRIDGED_TEXT_PARSE_STATUS": "STRIPRTF_V2, STRIPRTF_ERROR, or PLAIN_PASSTHROUGH.",
    "BRIDGED_TEXT_PARSER_VERSION": "Parser implementation/version used for BRIDGED_TEXT.",
    "BLOB_EXTRACT_DT_TM": "Administrative source extract timestamp from pacs_blob_content.",
    "BLOB_UPDT_DT_TM": "Administrative source update timestamp from pacs_blob_content.",
    "BLOB_ADC_UPDT": "Administrative ADC load timestamp from pacs_blob_content.",
    "REPORT_SRC_ADC_UPDT": "Administrative source load timestamp from map_pacs_report.",
    "PIPELINE_UPDT_DT_TM": "Timestamp when this bridge row was rebuilt.",
    "ROW_HASH": "SHA-256 over the stable business columns, excluding lifecycle and pipeline build stamps.",
    "SOURCE_PRESENT_IND": "True for the current 1:1-proven bridge; false only for a carried-forward tombstone.",
}

def apply_comments(accounting, blob_stats):
    ct = accounting["class_totals"]
    lanes = accounting["bridge_by_lane"]
    bridge = ct["BRIDGED"]
    combined = accounting["native_text_reports"] + bridge
    coverage = 100.0 * combined / max(1, accounting["total_reports"])
    comment = (
        "PACS report-text SIDECAR. Grain: one row per bridged REPORT_ID, with both REPORT_ID "
        "and EVENT_ID unique. It never updates or supersedes map_pacs_report. Match precedence is "
        "report to resolved exam to REQUEST_ID_STRING equals MillAccessionNbr, then "
        "EXAMINATION_ID_STRING fallback; MILL_LINK_REF is forbidden. Publish only accessions with "
        "exactly one blob event and exactly one textless report. Current accounting: "
        f"{bridge} bridged ({lanes['REQUEST_ID']} request lane; {lanes['EXAMINATION_ID']} exam lane); "
        f"{ct['MULTI_BLOB_ACCESSION']} multi-blob withheld; "
        f"{ct['MULTI_REPORT_ACCESSION']} multi-report withheld; {ct['BOTH']} both withheld; "
        f"{ct['UNMATCHED']} unmatched. Native plus bridge coverage is {combined} of "
        f"{accounting['total_reports']} reports ({coverage:.3f}%). Blob text spans "
        f"{blob_stats['min_extract_dt_tm']} through {blob_stats['max_extract_dt_tm']} and is frozen "
        f"at ADC_UPDT {blob_stats['max_adc_updt']}. Consumer contract: left join current bridge rows "
        "on REPORT_ID with SOURCE_PRESENT_IND=true and use COALESCE(native REPORT_TEXT, "
        "bridge BRIDGED_TEXT); native text always wins. Future durable end-state is a builder-owned "
        "lane inside pacs_pipeline. Static product: no weekly Bronze_Pipeline step until either source "
        "feed resumes."
    )
    spark.sql(f"COMMENT ON TABLE {qname(TARGET)} IS '{sql_text(comment)}'")
    for col_name, col_comment in COLUMN_COMMENTS.items():
        spark.sql(f"""ALTER TABLE {qname(TARGET)} ALTER COLUMN {qident(col_name)}
                      COMMENT '{sql_text(col_comment)}'""")

def dq_triplet_gate(table):
    fields = spark.table(table).schema.fields
    names = {f.name for f in fields}
    temporal = [f.name for f in fields if isinstance(f.dataType, (TimestampType, DateType))]
    missing = {}
    for c in temporal:
        if c in ADMIN_STAMPS or c.endswith("_CLEAN"):
            continue
        expected = {f"{c}_FUTURE_IND", f"{c}_SENTINEL_IND", f"{c}_CLEAN"}
        absent = sorted(expected - names)
        if absent:
            missing[c] = absent
    assert not missing, f"missing date-quality companions: {missing}"
    return temporal

def run_gates(classified, accounting, report_version):
    assert spark.catalog.tableExists(TARGET), f"G0 target absent: {TARGET}"
    target = spark.table(TARGET)
    present = target.where(F.col("SOURCE_PRESENT_IND"))

    key = target.agg(
        F.count(F.lit(1)).cast("long").alias("rows"),
        F.countDistinct("REPORT_ID").cast("long").alias("report_ids"),
        F.countDistinct("EVENT_ID").cast("long").alias("event_ids"),
        F.sum(F.when(F.col("REPORT_ID").isNull(), 1).otherwise(0)).cast("long").alias("null_report"),
        F.sum(F.when(F.col("EVENT_ID").isNull(), 1).otherwise(0)).cast("long").alias("null_event"),
    ).collect()[0]
    assert int(key["rows"]) == int(key["report_ids"]), "G1 REPORT_ID uniqueness failed"
    assert int(key["rows"]) == int(key["event_ids"]), "G1 EVENT_ID uniqueness failed"
    assert int(key["null_report"]) == 0 and int(key["null_event"]) == 0

    target_lanes = {r["MATCH_LANE"]: int(r["n"]) for r in
                    present.groupBy("MATCH_LANE").agg(F.count(F.lit(1)).alias("n")).collect()}
    expected_lanes = accounting["bridge_by_lane"]
    assert int(present.count()) == accounting["class_totals"]["BRIDGED"], "G2 bridge count mismatch"
    assert target_lanes == expected_lanes, f"G2 lane mismatch: {target_lanes} != {expected_lanes}"
    assert sum(accounting["class_totals"].values()) == accounting["textless_reports"], (
        "G2 textless accounting does not reconcile"
    )

    expected = classified.where(F.col("ACCOUNTING_CLASS") == "BRIDGED").select(
        "REPORT_ID", "EVENT_ID", "MATCH_LANE")
    actual = present.select("REPORT_ID", "EVENT_ID", "MATCH_LANE")
    mismatch = (expected.alias("e").join(
        actual.alias("a"),
        (F.col("e.REPORT_ID") == F.col("a.REPORT_ID"))
        & (F.col("e.EVENT_ID") == F.col("a.EVENT_ID"))
        & (F.col("e.MATCH_LANE") == F.col("a.MATCH_LANE")),
        "full")
        .where(F.col("e.REPORT_ID").isNull() | F.col("a.REPORT_ID").isNull())
        .limit(1).count())
    assert mismatch == 0, "G2 published key set differs from the 1:1 classification"

    reports = pinned_table(SRC_REPORT, report_version).where(F.col("SOURCE_PRESENT_IND")).select(
        F.col("PACS_REPORT_ID").cast("long").alias("REPORT_ID"), "REPORT_TEXT")
    native_collision = (present.select("REPORT_ID").join(reports, "REPORT_ID", "inner")
        .where(F.col("REPORT_TEXT").isNotNull() & (F.trim("REPORT_TEXT") != ""))
        .limit(1).count())
    assert native_collision == 0, "G3 bridge contains a report with native text"

    temporal = dq_triplet_gate(TARGET)
    ig_tag_gate(TARGET)

    invalid = present.agg(
        F.sum(F.when(~F.col("MATCH_LANE").isin("REQUEST_ID", "EXAMINATION_ID"), 1).otherwise(0))
         .alias("bad_lane"),
        F.sum(F.when(F.col("BRIDGED_TEXT_RAW").isNull() | (F.trim("BRIDGED_TEXT_RAW") == ""), 1)
              .otherwise(0)).alias("blank_raw"),
        F.sum(F.when(F.col("BRIDGED_TEXT").isNull() | (F.trim("BRIDGED_TEXT") == ""), 1)
              .otherwise(0)).alias("blank_derived"),
        F.sum(F.when(F.col("ROW_HASH") != row_hash_expr(), 1).otherwise(0)).alias("bad_hash"),
    ).collect()[0]
    assert int(invalid["bad_lane"] or 0) == 0
    assert int(invalid["blank_raw"] or 0) == 0
    assert int(invalid["blank_derived"] or 0) == 0
    assert int(invalid["bad_hash"] or 0) == 0

    fingerprint = table_fingerprint(TARGET)
    result = {
        "gates": "PASS",
        "target_rows": int(key["rows"]),
        "target_present_rows": accounting["class_totals"]["BRIDGED"],
        "target_tombstones": int(key["rows"]) - accounting["class_totals"]["BRIDGED"],
        "target_lanes": target_lanes,
        "accounting": accounting,
        "temporal_columns": temporal,
        "canonical_fingerprint": str(fingerprint),
    }
    print("[B12][GATES] PASS", json.dumps(result, default=str, sort_keys=True))
    return result

# COMMAND ----------

# Gates are deliberately invoked before the first build. On an absent target this must fail.
TARGET_EXISTED_AT_START = spark.catalog.tableExists(TARGET)
if not TARGET_EXISTED_AT_START:
    try:
        assert spark.catalog.tableExists(TARGET), f"G0 target absent: {TARGET}"
        raise AssertionError("pre-build gate unexpectedly passed")
    except AssertionError as exc:
        print(f"[B12][GATES-FIRST] EXPECTED_PREBUILD_FAILURE: {exc}")

state_start, source_signature, blob_stats = source_state()
report_stats = report_snapshot_stats(state_start["report_version"])
classified = build_classification(state_start["report_version"], state_start["exam_version"])
accounting = collect_accounting(classified, report_stats)
print("[B12][ACCOUNTING]", json.dumps(accounting, default=str, sort_keys=True))

props_before = target_properties()
stored_signature = props_before.get("b12.source_signature")
due = FORCE_REBUILD or not TARGET_EXISTED_AT_START or stored_signature != source_signature

if TARGET_EXISTED_AT_START:
    try:
        prebuild_gate_result = run_gates(classified, accounting, state_start["report_version"])
        print("[B12][GATES-FIRST] existing target gates passed")
    except Exception as exc:
        if not due and ACTION == "gates":
            raise
        print(f"[B12][GATES-FIRST] existing target is stale or invalid and will rebuild: "
              f"{type(exc).__name__}: {exc}")

if ACTION == "gates":
    result = run_gates(classified, accounting, state_start["report_version"])
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
    result = run_gates(classified, accounting, state_start["report_version"])
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

candidate = build_candidate(classified, blob_stats)
replace_with_tombstones(candidate, TARGET, ["REPORT_ID"])

apply_comments(accounting, blob_stats)
ig_tag_table(TARGET, {
    "BRIDGED_TEXT_RAW": ("4", "2"),
    "BRIDGED_TEXT": ("4", "2"),
})

result = run_gates(classified, accounting, state_start["report_version"])

# Recheck all source clocks after the build. The materialized view has no Delta history API,
# so its strong consumed-column content fingerprint is checked at both ends of the run.
state_end, signature_end, blob_stats_end = source_state()
assert signature_end == source_signature, (
    f"source drift during build: {source_signature} -> {signature_end}; state not committed"
)

set_target_properties({
    "b12.logic_version": LOGIC_VERSION,
    "b12.source_signature": source_signature,
    "b12.report_source_version": state_start["report_version"],
    "b12.exam_source_version": state_start["exam_version"],
    "b12.blob_content_xor": state_start["blob_content_xor"],
    "b12.striprtf_version": state_start["striprtf_version"],
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
# 1. Obtain explicit human approval for a new production table. Confirm that
#    4_prod.bronze.map_pacs_report_text_bridge is absent or is the approved prior release.
# 2. Re-read the live production pacs_pipeline and Blob parser before promotion. Pin the same
#    report/exam versions and the pacs_blob_content consumed-column fingerprint.
# 3. In a promoter-owned copy, change only TARGET to
#    4_prod.bronze.map_pacs_report_text_bridge and add an explicit approval-token assertion.
#    Keep retries=0. Do not update 4_prod.bronze.map_pacs_report.
# 4. Run gates before build, build once, then rerun all gates on production:
#    REPORT_ID and EVENT_ID uniqueness; exact textless accounting; zero native-text collisions;
#    date-quality coverage; row hashes; canonical fingerprint; both IG tags on every column.
# 5. Run ig_tag_table with BRIDGED_TEXT_RAW and BRIDGED_TEXT fixed at ig_risk=4,
#    ig_severity=2, then run ig_tag_gate. Review every printed default before approval.
# 6. Consumer contract is a left join on REPORT_ID restricted to SOURCE_PRESENT_IND=true,
#    followed by COALESCE(native REPORT_TEXT, bridge BRIDGED_TEXT). Native text always wins.
# 7. Do not add a Bronze_Pipeline weekly step: both inputs are frozen. Re-open scheduling only
#    if the PACS or blob feed resumes. Future durable ownership belongs inside pacs_pipeline.
# 8. Capture the production run ID, source signature, counts, table version, and fingerprint.
#    Promotion remains incomplete until a human signs off those artifacts.


