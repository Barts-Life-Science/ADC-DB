# Databricks notebook source
# MAGIC %md
# MAGIC # Blob OCR slow lane — PaddleOCR GPU
# MAGIC
# MAGIC Production-safe GPU OCR for image-only PDFs materialized by Blob 4_Files.
# MAGIC The CPU Blob path remains responsible for decompression and embedded-text
# MAGIC extraction. This task only processes PDFs whose current bronze row is still
# MAGIC `OCR queued` (or a legacy `PDF extraction failed`).
# MAGIC
# MAGIC Modes:
# MAGIC
# MAGIC - `PLAN`: read-only candidate and queue summary.
# MAGIC - `BOOTSTRAP`: create control/output tables and stage PaddleOCR models.
# MAGIC - `CANARY`: OCR selected documents and write only the canary output table.
# MAGIC - `RECONCILE`: finish durable history/queue state for OCR text already written to bronze.
# MAGIC - `APPLY`: claim queue rows, OCR them, update bronze/history, and advance leases.
# MAGIC
# MAGIC Dependencies install from the offline wheelhouse named by `WHEELHOUSE_REQUIREMENTS`
# MAGIC (hash-pinned, `--no-index`: no network). OCR runs in `paddle_runtime` worker
# MAGIC processes (`OCR_WORKERS` on one GPU) using the verified models at `MODEL_ROOT`.
# MAGIC Never invokes Tesseract.

# COMMAND ----------

dbutils.widgets.text("WHEELHOUSE_REQUIREMENTS", "/Volumes/6_mgmt/default/scripts/paddle_wheelhouse/v1/requirements.txt")
get_ipython().run_line_magic("pip", "install --quiet -r " + dbutils.widgets.get("WHEELHOUSE_REQUIREMENTS").strip())

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "./PaddleRuntime"

# COMMAND ----------

# PADDLE_RUNTIME_POOL_V1: OCR runs in paddle_runtime worker processes; see ./PaddleRuntime.
import gc
import hashlib
import importlib.metadata as importlib_metadata
import json
import os
import re
import time
import uuid
from datetime import datetime, timezone

from delta.tables import DeltaTable
import paddle_runtime
from paddle_runtime import pdf
from paddle_runtime.models import verified_models
from paddle_runtime.pool import WorkerPool, pool_config
from pyspark.sql import Row, functions as F, types as T


NOTEBOOK_STARTED_MONOTONIC = time.monotonic()


dbutils.widgets.dropdown(
    "MODE", "PLAN", ["PLAN", "BOOTSTRAP", "CANARY", "RECONCILE", "APPLY"]
)
dbutils.widgets.text("RUN_ID", "")
dbutils.widgets.text("STATE_SCHEMA", "4_prod.tmp")
dbutils.widgets.text("BRONZE_TABLE", "4_prod.bronze.mill_blob_text")
dbutils.widgets.text("HISTORY_TABLE", "4_prod.logs.mill_blob_extractor_history")
dbutils.widgets.text("MANIFEST_TABLE", "4_prod.6_mgmt.file_manifest")
dbutils.widgets.text("MAX_DOCUMENTS", "250")
dbutils.widgets.text("MAX_ATTEMPTS", "3")
dbutils.widgets.text("LEASE_MINUTES", "120")
dbutils.widgets.text("OCR_MAX_PAGES", "10")
dbutils.widgets.text("OCR_DPI", "170")
dbutils.widgets.text("OCR_PAGE_BATCH_SIZE", "4")
dbutils.widgets.text("OCR_TOTAL_TIMEOUT_SEC", "240")
dbutils.widgets.text("OCR_TASK_BUDGET_SEC", "5400")
dbutils.widgets.text("OCR_FINALIZE_RESERVE_SEC", "1200")
dbutils.widgets.text("OCR_MIN_CONFIDENCE", "0.35")
dbutils.widgets.dropdown("PREPROCESSING", "none", ["none", "clahe"])
dbutils.widgets.text("OCR_LANGUAGE", "en")
dbutils.widgets.dropdown("OCR_MODEL_VERSION", "PP-OCRv6", ["PP-OCRv6", "PP-OCRv5"])
dbutils.widgets.text("OCR_DEVICE", "gpu:0")
dbutils.widgets.text("MODEL_ROOT", "/Volumes/6_mgmt/default/scripts/paddleocr_models/ppocrv6_en_v1")
dbutils.widgets.text("MODEL_FINGERPRINT", "82c7d78477f7ae17d1875bcf19eb978f2cb2103cd04be618577743b7ae4cd767")
dbutils.widgets.text("OCR_WORKERS", "4")
dbutils.widgets.text("CANARY_VOLUME_PATH", "")


MODE = dbutils.widgets.get("MODE").strip().upper()
RUN_ID = dbutils.widgets.get("RUN_ID").strip()
STATE_SCHEMA = dbutils.widgets.get("STATE_SCHEMA").strip()
BRONZE = dbutils.widgets.get("BRONZE_TABLE").strip()
HISTORY = dbutils.widgets.get("HISTORY_TABLE").strip()
MANIFEST = dbutils.widgets.get("MANIFEST_TABLE").strip()
MAX_DOCUMENTS = int(dbutils.widgets.get("MAX_DOCUMENTS") or "250")
MAX_ATTEMPTS = int(dbutils.widgets.get("MAX_ATTEMPTS") or "3")
LEASE_MINUTES = int(dbutils.widgets.get("LEASE_MINUTES") or "120")
OCR_MAX_PAGES = int(dbutils.widgets.get("OCR_MAX_PAGES") or "10")
OCR_DPI = int(dbutils.widgets.get("OCR_DPI") or "170")
OCR_PAGE_BATCH_SIZE = int(dbutils.widgets.get("OCR_PAGE_BATCH_SIZE") or "4")
OCR_TOTAL_TIMEOUT_SEC = int(dbutils.widgets.get("OCR_TOTAL_TIMEOUT_SEC") or "240")
OCR_TASK_BUDGET_SEC = int(dbutils.widgets.get("OCR_TASK_BUDGET_SEC") or "5400")
OCR_FINALIZE_RESERVE_SEC = int(dbutils.widgets.get("OCR_FINALIZE_RESERVE_SEC") or "1200")
OCR_MIN_CONFIDENCE = float(dbutils.widgets.get("OCR_MIN_CONFIDENCE") or "0.35")
PREPROCESSING = dbutils.widgets.get("PREPROCESSING").strip().lower()
OCR_LANGUAGE = dbutils.widgets.get("OCR_LANGUAGE").strip()
OCR_MODEL_VERSION = dbutils.widgets.get("OCR_MODEL_VERSION").strip()
OCR_DEVICE = dbutils.widgets.get("OCR_DEVICE").strip()
MODEL_ROOT = dbutils.widgets.get("MODEL_ROOT").strip().rstrip("/")
MODEL_FINGERPRINT = dbutils.widgets.get("MODEL_FINGERPRINT").strip()
OCR_WORKERS = int(dbutils.widgets.get("OCR_WORKERS") or "4")
CANARY_VOLUME_PATH = dbutils.widgets.get("CANARY_VOLUME_PATH").strip()

FILE_QUEUE = f"{STATE_SCHEMA}.file_queue"
OCR_QUEUE = f"{STATE_SCHEMA}.blob_ocr_queue"
OCR_OUTPUT = f"{STATE_SCHEMA}.blob_ocr_output"
OCR_PAGE_OUTPUT = f"{STATE_SCHEMA}.blob_ocr_page_output"
OCR_CANARY_OUTPUT = f"{STATE_SCHEMA}.blob_ocr_canary_output"
OCR_HEARTBEAT = f"{STATE_SCHEMA}.blob_ocr_heartbeat"
ENGINE_KIND = "paddleocr_v3"

OCR_PARSER_VERSION = 5
OCR_POST_PROCESSOR_VERSION = 5
MAX_TEXT_CHARS = 10_000_000
SUPPORTED_SOURCE_STATUSES = ("OCR queued", "PDF extraction failed")
QUEUE_KEY_SCHEMA = T.StructType([
    T.StructField("EVENT_ID", T.LongType(), False),
    T.StructField("ADC_UPDT", T.TimestampType(), True),
    T.StructField("raw_sha256", T.StringType(), False),
])

if MODE not in {"PLAN", "BOOTSTRAP", "CANARY", "RECONCILE", "APPLY"}:
    raise ValueError(f"Unsupported MODE={MODE!r}")
if not 1 <= MAX_DOCUMENTS <= 5000:
    raise ValueError("MAX_DOCUMENTS must be between 1 and 5000")
if not 1 <= MAX_ATTEMPTS <= 20:
    raise ValueError("MAX_ATTEMPTS must be between 1 and 20")
if not 1 <= OCR_MAX_PAGES <= 100:
    raise ValueError("OCR_MAX_PAGES must be between 1 and 100")
if not 72 <= OCR_DPI <= 300:
    raise ValueError("OCR_DPI must be between 72 and 300")
if not 1 <= OCR_PAGE_BATCH_SIZE <= 32:
    raise ValueError("OCR_PAGE_BATCH_SIZE must be between 1 and 32")
if not 600 <= OCR_TASK_BUDGET_SEC <= 86400:
    raise ValueError("OCR_TASK_BUDGET_SEC must be between 600 and 86400")
if not 60 <= OCR_FINALIZE_RESERVE_SEC < OCR_TASK_BUDGET_SEC:
    raise ValueError(
        "OCR_FINALIZE_RESERVE_SEC must be at least 60 and less than OCR_TASK_BUDGET_SEC"
    )
if not 0.0 <= OCR_MIN_CONFIDENCE <= 1.0:
    raise ValueError("OCR_MIN_CONFIDENCE must be between 0 and 1")
if PREPROCESSING not in {"none", "clahe"}:
    raise ValueError("PREPROCESSING must be none or clahe")
if not 1 <= OCR_WORKERS <= 8:
    raise ValueError("OCR_WORKERS must be between 1 and 8")
if not re.fullmatch(r"[0-9a-f]{64}", MODEL_FINGERPRINT):
    raise ValueError("MODEL_FINGERPRINT must be the 64-hex staged model fingerprint")
if (OCR_MODEL_VERSION, OCR_LANGUAGE) != ("PP-OCRv6", "en"):
    raise ValueError("MODEL_ROOT holds PP-OCRv6 en models; OCR_MODEL_VERSION/OCR_LANGUAGE must match")


# COMMAND ----------

_TABLE_RE = re.compile(r"^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+){1,2}$")


def validate_table_name(value):
    if not _TABLE_RE.fullmatch(value or ""):
        raise ValueError(f"Unsafe table identifier: {value!r}")
    return value


def quote_table(value):
    validate_table_name(value)
    return ".".join(f"`{part}`" for part in value.split("."))


def table_exists(value):
    try:
        spark.table(value).schema
        return True
    except Exception as exc:
        message = str(exc).lower()
        if any(
            marker in message
            for marker in (
                "table_or_view_not_found",
                "table or view not found",
                "cannot be found",
                "no such table",
            )
        ):
            return False
        raise


def ensure_columns(table_name, columns):
    existing = {field.name.lower() for field in spark.table(table_name).schema.fields}
    missing = [(name, data_type) for name, data_type in columns if name.lower() not in existing]
    if missing:
        rendered = ", ".join(f"`{name}` {data_type}" for name, data_type in missing)
        spark.sql(f"ALTER TABLE {quote_table(table_name)} ADD COLUMNS ({rendered})")


for _table in (STATE_SCHEMA, BRONZE, HISTORY, MANIFEST, FILE_QUEUE, OCR_QUEUE, OCR_OUTPUT,
               OCR_PAGE_OUTPUT, OCR_CANARY_OUTPUT, OCR_HEARTBEAT):
    validate_table_name(_table)


def bootstrap_tables():
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {quote_table(OCR_QUEUE)} (
      run_id STRING,
      EVENT_ID BIGINT,
      ADC_UPDT TIMESTAMP,
      raw_sha256 STRING,
      volume_path STRING,
      status STRING,
      attempt_count INT,
      lease_owner STRING,
      lease_expires_ts TIMESTAMP,
      last_error STRING,
      created_ts TIMESTAMP,
      updated_ts TIMESTAMP,
      completed_ts TIMESTAMP
    ) USING DELTA
    """)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {quote_table(OCR_OUTPUT)} (
      run_id STRING,
      EVENT_ID BIGINT,
      ADC_UPDT TIMESTAMP,
      raw_sha256 STRING,
      volume_path STRING,
      text STRING,
      text_length BIGINT,
      status STRING,
      parse_status STRING,
      pages_processed INT,
      page_count INT,
      attempt_count INT,
      engine STRING,
      model_version STRING,
      paddleocr_version STRING,
      paddle_version STRING,
      device STRING,
      line_count INT,
      mean_confidence DOUBLE,
      min_confidence DOUBLE,
      truncated BOOLEAN,
      metrics STRING,
      output_ts TIMESTAMP
    ) USING DELTA
    """)
    ensure_columns(OCR_OUTPUT, [
        ("engine", "STRING"),
        ("model_version", "STRING"),
        ("paddleocr_version", "STRING"),
        ("paddle_version", "STRING"),
        ("device", "STRING"),
        ("line_count", "INT"),
        ("mean_confidence", "DOUBLE"),
        ("min_confidence", "DOUBLE"),
        ("truncated", "BOOLEAN"),
    ])
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {quote_table(OCR_PAGE_OUTPUT)} (
      run_id STRING,
      EVENT_ID BIGINT,
      ADC_UPDT TIMESTAMP,
      raw_sha256 STRING,
      page_index INT,
      text STRING,
      line_count INT,
      mean_confidence DOUBLE,
      min_confidence DOUBLE,
      lines_json STRING,
      status STRING,
      elapsed_seconds DOUBLE,
      engine STRING,
      model_version STRING,
      output_ts TIMESTAMP
    ) USING DELTA
    """)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {quote_table(OCR_CANARY_OUTPUT)} (
      canary_id STRING,
      run_id STRING,
      EVENT_ID BIGINT,
      ADC_UPDT TIMESTAMP,
      raw_sha256 STRING,
      volume_path STRING,
      text STRING,
      text_length BIGINT,
      status STRING,
      parse_status STRING,
      pages_processed INT,
      page_count INT,
      attempt_count INT,
      engine STRING,
      model_version STRING,
      paddleocr_version STRING,
      paddle_version STRING,
      device STRING,
      line_count INT,
      mean_confidence DOUBLE,
      min_confidence DOUBLE,
      truncated BOOLEAN,
      metrics STRING,
      output_ts TIMESTAMP
    ) USING DELTA
    """)


# COMMAND ----------

def package_version(name):
    try:
        return importlib_metadata.version(name)
    except Exception:
        return None


def gpu_report():
    """Versions and device, read without creating a CUDA context in the driver."""
    import subprocess

    report = {
        "paddleocr": package_version("paddleocr"),
        "paddlepaddle_gpu": package_version("paddlepaddle-gpu"),
        "PyMuPDF": package_version("PyMuPDF"),
        "paddle_runtime": paddle_runtime.__version__,
        "model_root": MODEL_ROOT,
        "model_fingerprint": MODEL_FINGERPRINT,
        "device_requested": OCR_DEVICE,
        "workers": OCR_WORKERS,
    }
    try:
        listed = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader"],
            capture_output=True, text=True, timeout=60, check=True,
        ).stdout.strip().splitlines()
    except Exception as exc:
        listed = []
        report["gpu_query_error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
    report["gpu_count"] = len(listed)
    report["gpu_name"] = listed[0] if listed else None
    return report


def record_heartbeat(payload, status="success"):
    """One row per finished APPLY/BOOTSTRAP run; OCR_Staleness_Gate reads the latest."""
    if MODE not in {"APPLY", "BOOTSTRAP"}:
        return None
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {quote_table(OCR_HEARTBEAT)} (
      heartbeat_ts TIMESTAMP,
      status STRING,
      mode STRING,
      processed INT,
      payload STRING
    ) USING DELTA
    """)
    (
        spark.createDataFrame(
            [(status, MODE, int(payload.get("processed") or 0), json.dumps(payload, default=str))],
            "status string, mode string, processed int, payload string",
        )
        .select(F.current_timestamp().alias("heartbeat_ts"), "status", "mode", "processed", "payload")
        .write.mode("append").insertInto(OCR_HEARTBEAT)
    )
    return None


def start_pool():
    """OCR_WORKERS processes on one GPU; each verifies the models and self-tests before work."""
    return WorkerPool.start(
        pool_config(
            "paddle_runtime.engine:PageOCR",
            {
                "model_root": MODEL_ROOT,
                "expected_fingerprint": MODEL_FINGERPRINT,
                "device": OCR_DEVICE,
                "rec_score_thresh": OCR_MIN_CONFIDENCE,
                "rec_batch_size": max(1, OCR_PAGE_BATCH_SIZE * 2),
            },
            handlers="paddle_runtime.pdf:handlers",
        ),
        OCR_WORKERS,
    )


# COMMAND ----------

def candidate_frame():
    bronze_candidates = (
        spark.table(BRONZE)
        .filter(F.col("CONTENT_TYPE") == "application/pdf")
        .filter(F.col("STATUS").isin(*SUPPORTED_SOURCE_STATUSES))
        .filter(F.col("raw_sha256").isNotNull())
        .filter(F.col("BLOB_TEXT").isNull() | (F.length(F.trim(F.col("BLOB_TEXT"))) == 0))
        .select("EVENT_ID", "ADC_UPDT", "raw_sha256")
    )

    queue_candidates = (
        spark.table(FILE_QUEUE).alias("q")
        .filter(
            (F.col("q.status") == "extracted")
            & (F.col("q.content_type") == "application/pdf")
            & F.col("q.volume_path").isNotNull()
        )
        .join(
            bronze_candidates.alias("b"),
            (F.col("q.EVENT_ID") == F.col("b.EVENT_ID"))
            & F.col("q.ADC_UPDT").eqNullSafe(F.col("b.ADC_UPDT"))
            & (F.col("q.raw_sha256") == F.col("b.raw_sha256")),
            "inner",
        )
        .select(
            F.coalesce(F.col("q.run_id"), F.lit("legacy")).alias("run_id"),
            F.col("q.EVENT_ID").cast("long").alias("EVENT_ID"),
            F.col("q.ADC_UPDT").alias("ADC_UPDT"),
            F.col("q.raw_sha256").alias("raw_sha256"),
            F.col("q.volume_path").alias("volume_path"),
        )
    )

    manifest_candidates = (
        spark.table(MANIFEST).alias("m")
        .filter(
            (F.col("m.STATUS") == "Extracted")
            & (F.col("m.content_type") == "application/pdf")
            & F.col("m.volume_path").isNotNull()
        )
        .join(
            bronze_candidates.alias("b"),
            (F.col("m.EVENT_ID") == F.col("b.EVENT_ID"))
            & F.col("m.ADC_UPDT").eqNullSafe(F.col("b.ADC_UPDT"))
            & (F.col("m.raw_sha256") == F.col("b.raw_sha256")),
            "inner",
        )
        .select(
            F.coalesce(F.col("m.run_id"), F.lit("legacy")).alias("run_id"),
            F.col("m.EVENT_ID").cast("long").alias("EVENT_ID"),
            F.col("m.ADC_UPDT").alias("ADC_UPDT"),
            F.col("m.raw_sha256").alias("raw_sha256"),
            F.col("m.volume_path").alias("volume_path"),
        )
    )

    return (
        queue_candidates.unionByName(manifest_candidates)
        .dropDuplicates(["EVENT_ID", "ADC_UPDT", "raw_sha256"])
        .withColumn("status", F.lit("pending"))
        .withColumn("attempt_count", F.lit(0).cast("int"))
        .withColumn("lease_owner", F.lit(None).cast("string"))
        .withColumn("lease_expires_ts", F.lit(None).cast("timestamp"))
        .withColumn("last_error", F.lit(None).cast("string"))
        .withColumn("created_ts", F.current_timestamp())
        .withColumn("updated_ts", F.current_timestamp())
        .withColumn("completed_ts", F.lit(None).cast("timestamp"))
    )


def manual_canary_frame(path):
    if not path:
        return None
    if not os.path.exists(path):
        raise FileNotFoundError(f"CANARY_VOLUME_PATH does not exist: {path}")
    with open(path, "rb") as handle:
        digest = hashlib.sha256(handle.read()).hexdigest()
    event_id = -int(digest[:12], 16)
    schema = T.StructType([
        T.StructField("run_id", T.StringType(), False),
        T.StructField("EVENT_ID", T.LongType(), False),
        T.StructField("ADC_UPDT", T.TimestampType(), True),
        T.StructField("raw_sha256", T.StringType(), False),
        T.StructField("volume_path", T.StringType(), False),
        T.StructField("attempt_count", T.IntegerType(), False),
    ])
    return spark.createDataFrame(
        [("manual_canary", event_id, None, digest, path, 0)],
        schema=schema,
    )


required_sources = (BRONZE, HISTORY, MANIFEST, FILE_QUEUE)
missing_sources = [table_name for table_name in required_sources if not table_exists(table_name)]
if missing_sources:
    raise RuntimeError(f"Missing required source tables: {missing_sources}")

if MODE == "BOOTSTRAP":
    bootstrap_tables()
elif MODE in {"CANARY", "RECONCILE", "APPLY"}:
    missing_control = [
        table_name for table_name in (OCR_QUEUE, OCR_OUTPUT, OCR_PAGE_OUTPUT, OCR_CANARY_OUTPUT)
        if not table_exists(table_name)
    ]
    if missing_control:
        raise RuntimeError(
            f"Missing OCR control tables: {missing_control}. Run MODE=BOOTSTRAP once."
        )


def history_source_from_results(result_frame):
    return result_frame.select(
        "run_id", "EVENT_ID", "ADC_UPDT", "raw_sha256",
        F.lit(3).cast("int").alias("decompressor_version"),
        F.lit(OCR_PARSER_VERSION).cast("int").alias("parser_version"),
        F.lit(OCR_POST_PROCESSOR_VERSION).cast("int").alias("post_processor_version"),
        "status",
        F.col("truncated").alias("truncation_flag"),
        F.when(F.col("truncated"), F.lit("ocr_page_or_time_budget"))
         .otherwise(F.lit(None).cast("string")).alias("truncation_reason"),
        F.lit(None).cast("string").alias("ftfy_explain"),
        F.lit("file_volume_paddleocr_gpu").alias("decompression_strategy"),
        F.lit("ocr_paddle").alias("source"),
        F.current_date().alias("extracted_date"),
    )


def append_missing_history(result_frame):
    history_source = history_source_from_results(result_frame)
    target_schema = spark.table(HISTORY).schema
    history_aligned = history_source.select(*[
        F.col(field.name).cast(field.dataType).alias(field.name)
        if field.name in history_source.columns
        else F.lit(None).cast(field.dataType).alias(field.name)
        for field in target_schema.fields
    ])
    existing = (
        spark.table(HISTORY)
        .filter(F.col("source") == "ocr_paddle")
        .select("run_id", "EVENT_ID", "ADC_UPDT", "source")
        .alias("h")
    )
    source = history_aligned.alias("s")
    missing = source.join(
        existing,
        (F.col("s.run_id") == F.col("h.run_id"))
        & (F.col("s.EVENT_ID") == F.col("h.EVENT_ID"))
        & F.col("s.ADC_UPDT").eqNullSafe(F.col("h.ADC_UPDT"))
        & (F.col("s.source") == F.col("h.source")),
        "left_anti",
    ).select("s.*")
    missing.write.mode("append").insertInto(HISTORY)


def recover_committed_ocr_rows():
    if MODE not in {"RECONCILE", "APPLY"}:
        return 0

    queue = spark.table(OCR_QUEUE).alias("q")
    output = spark.table(OCR_OUTPUT).alias("o")
    bronze = spark.table(BRONZE).alias("b")
    key_match_qo = (
        (F.col("q.EVENT_ID") == F.col("o.EVENT_ID"))
        & F.col("q.ADC_UPDT").eqNullSafe(F.col("o.ADC_UPDT"))
        & (F.col("q.raw_sha256") == F.col("o.raw_sha256"))
    )
    key_match_ob = (
        (F.col("o.EVENT_ID") == F.col("b.EVENT_ID"))
        & F.col("o.ADC_UPDT").eqNullSafe(F.col("b.ADC_UPDT"))
        & (F.col("o.raw_sha256") == F.col("b.raw_sha256"))
    )
    recovery_query = (
        queue
        .filter(F.col("q.status").isin("processing", "pending", "retryable", "superseded"))
        .join(output, key_match_qo, "inner")
        .filter(
            F.col("o.parse_status").isin("ocr_complete", "ocr_partial")
            & F.col("o.text").isNotNull()
        )
        .join(bronze, key_match_ob, "inner")
        .filter(
            F.col("b.STATUS").isin("Decoded", "OCR partial")
            & ((F.col("q.status") != "superseded") | (F.col("b.BLOB_TEXT") == F.col("o.text")))
            & F.col("b.BLOB_TEXT").isNotNull()
            & (F.length(F.trim(F.col("b.BLOB_TEXT"))) > 0)
        )
        .select(
            *[F.col(f"o.{name}").alias(name) for name in spark.table(OCR_OUTPUT).columns],
            (F.col("b.BLOB_TEXT") == F.col("o.text")).alias("_ocr_text_current"),
        )
        .dropDuplicates(["EVENT_ID", "ADC_UPDT", "raw_sha256"])
    )
    recovery_rows = recovery_query.collect()
    if not recovery_rows:
        return 0

    recovered = spark.createDataFrame(recovery_rows, schema=recovery_query.schema)
    append_missing_history(recovered.filter(F.col("_ocr_text_current")).drop("_ocr_text_current"))

    queue_recovery = (
        recovered
        .select("EVENT_ID", "ADC_UPDT", "raw_sha256", "parse_status", "_ocr_text_current")
        .withColumn(
            "next_status",
            F.when(~F.col("_ocr_text_current"), F.lit("superseded"))
             .when(F.col("parse_status") == "ocr_complete", F.lit("complete"))
             .otherwise(F.lit("partial")),
        )
    )
    (
        DeltaTable.forName(spark, OCR_QUEUE)
        .alias("t")
        .merge(
            queue_recovery.alias("s"),
            "t.EVENT_ID=s.EVENT_ID AND t.ADC_UPDT <=> s.ADC_UPDT "
            "AND t.raw_sha256=s.raw_sha256",
        )
        .whenMatchedUpdate(
            set={
                "status": "s.next_status",
                "lease_owner": "NULL",
                "lease_expires_ts": "NULL",
                "last_error": "NULL",
                "updated_ts": "current_timestamp()",
                "completed_ts": "current_timestamp()",
            }
        )
        .execute()
    )
    return len(recovery_rows)


def release_deferred_claims(rows, owner, reason="deferred before OCR: task budget reserved for durable finalization"):
    if MODE != "APPLY" or not rows:
        return 0
    deferred_claims = spark.createDataFrame(
        [
            (int(row.EVENT_ID), row.ADC_UPDT, row.raw_sha256)
            for row in rows
        ],
        schema=QUEUE_KEY_SCHEMA,
    )
    deferred_claims.createOrReplaceTempView("_optimum_paddle_ocr_deferred_claims")
    spark.sql(f"""
    MERGE INTO {quote_table(OCR_QUEUE)} t
    USING _optimum_paddle_ocr_deferred_claims s
    ON t.EVENT_ID=s.EVENT_ID
     AND t.ADC_UPDT <=> s.ADC_UPDT
     AND t.raw_sha256=s.raw_sha256
    WHEN MATCHED AND t.lease_owner='{owner}' THEN UPDATE SET
      t.status=CASE WHEN coalesce(t.attempt_count,1) <= 1 THEN 'pending' ELSE 'retryable' END,
      t.attempt_count=greatest(coalesce(t.attempt_count,1)-1,0),
      t.lease_owner=NULL,
      t.lease_expires_ts=NULL,
      t.last_error='{reason}',
      t.updated_ts=current_timestamp(),
      t.completed_ts=NULL
    """)
    return len(rows)


recovered_count = recover_committed_ocr_rows()

if MODE == "RECONCILE":
    queue_summary = [row.asDict() for row in (
        spark.table(OCR_QUEUE)
        .groupBy("status")
        .count()
        .orderBy(F.desc("count"))
        .collect()
    )]
    payload = {
        "status": "reconciled",
        "recovered_committed_rows": recovered_count,
        "queue": queue_summary,
    }
    print(json.dumps(payload, indent=2, default=str))
    dbutils.jobs.taskValues.set(key="OCR_RESULT", value=json.dumps(payload, default=str))
    dbutils.notebook.exit(json.dumps(payload, default=str))

candidates = candidate_frame().persist()
candidate_count = int(candidates.count())
queue_summary = []
if table_exists(OCR_QUEUE):
    queue_summary = [row.asDict() for row in (
        spark.table(OCR_QUEUE)
        .groupBy("status")
        .count()
        .orderBy(F.desc("count"))
        .collect()
    )]

print(json.dumps({
    "mode": MODE,
    "run_id": RUN_ID or None,
    "recovered_committed_rows": recovered_count,
    "new_candidates": candidate_count,
    "queue": queue_summary,
}, indent=2, default=str))

if MODE == "PLAN":
    display(candidates.select("run_id", "EVENT_ID", "ADC_UPDT", "raw_sha256", "volume_path").limit(100))
    candidates.unpersist()
    dbutils.notebook.exit(json.dumps({
        "status": "plan",
        "new_candidates": candidate_count,
        "queue": queue_summary,
    }, default=str))


# COMMAND ----------

if MODE == "BOOTSTRAP":
    report = gpu_report()
    if report["gpu_count"] < 1:
        raise RuntimeError("BOOTSTRAP must run on GPU compute: " + json.dumps(report, default=str))
    verified = verified_models(MODEL_ROOT, MODEL_FINGERPRINT)
    pool_started = time.monotonic()
    pool = start_pool()
    try:
        workers_ready = [worker.runtime_info for worker in pool.workers]
    finally:
        pool.close()
    report.update({
        "engine_kind": ENGINE_KIND,
        "model_version": OCR_MODEL_VERSION,
        "language": OCR_LANGUAGE,
        "models": {"fingerprint": verified["fingerprint"], "spec": verified["spec"]},
        "worker_startup_seconds": round(time.monotonic() - pool_started, 3),
        "workers_ready": workers_ready,
        "bootstrapped_at_utc": datetime.now(timezone.utc).isoformat(),
    })
    candidates.unpersist()
    record_heartbeat(report, status="bootstrap")
    print(json.dumps(report, indent=2, default=str))
    dbutils.notebook.exit(json.dumps({"status": "bootstrapped", **report}, default=str))


# COMMAND ----------

if MODE == "APPLY" and candidate_count:
    (
        DeltaTable.forName(spark, OCR_QUEUE)
        .alias("t")
        .merge(
            candidates.alias("s"),
            "t.EVENT_ID=s.EVENT_ID AND t.ADC_UPDT <=> s.ADC_UPDT "
            "AND t.raw_sha256=s.raw_sha256",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

if MODE == "CANARY" and CANARY_VOLUME_PATH:
    selected = manual_canary_frame(CANARY_VOLUME_PATH)
else:
    source = spark.table(OCR_QUEUE) if MODE == "APPLY" else candidates
    eligible = source.filter(
        F.col("status").isin("pending", "retryable")
        | (
            (F.col("status") == "processing")
            & (
                F.col("lease_expires_ts").isNull()
                | (F.col("lease_expires_ts") < F.current_timestamp())
            )
        )
    )
    if "attempt_count" in eligible.columns:
        eligible = eligible.filter(F.coalesce(F.col("attempt_count"), F.lit(0)) < MAX_ATTEMPTS)
    selected = (
        eligible
        .withColumn(
            "_run_priority",
            F.when(F.lit(bool(RUN_ID)) & (F.col("run_id") == F.lit(RUN_ID)), F.lit(0))
             .otherwise(F.lit(1)),
        )
        .orderBy("_run_priority", "created_ts", "EVENT_ID")
        .limit(MAX_DOCUMENTS)
        .select("run_id", "EVENT_ID", "ADC_UPDT", "raw_sha256", "volume_path", "attempt_count")
    )

selected_count = int(selected.count())
if selected_count == 0:
    candidates.unpersist()
    result = {"status": "success", "mode": MODE, "selected": 0, "new_candidates": candidate_count}
    record_heartbeat(result)
    dbutils.jobs.taskValues.set(key="OCR_RESULT", value=json.dumps(result))
    dbutils.notebook.exit(json.dumps(result))

lease_owner = uuid.uuid4().hex
if MODE == "APPLY":
    selected.select("EVENT_ID", "ADC_UPDT", "raw_sha256").createOrReplaceTempView(
        "_optimum_paddle_ocr_claims"
    )
    spark.sql(f"""
    MERGE INTO {quote_table(OCR_QUEUE)} t
    USING _optimum_paddle_ocr_claims s
    ON t.EVENT_ID=s.EVENT_ID
     AND t.ADC_UPDT <=> s.ADC_UPDT
     AND t.raw_sha256=s.raw_sha256
    WHEN MATCHED AND (
      t.status IN ('pending','retryable')
      OR t.lease_expires_ts IS NULL
      OR t.lease_expires_ts < current_timestamp()
    ) THEN UPDATE SET
      t.status='processing',
      t.attempt_count=coalesce(t.attempt_count,0)+1,
      t.lease_owner='{lease_owner}',
      t.lease_expires_ts=current_timestamp() + INTERVAL {LEASE_MINUTES} MINUTES,
      t.last_error=NULL,
      t.updated_ts=current_timestamp()
    """)
    selected = (
        spark.table(OCR_QUEUE)
        .filter(F.col("lease_owner") == lease_owner)
        .select("run_id", "EVENT_ID", "ADC_UPDT", "raw_sha256", "volume_path", "attempt_count")
    )
    selected_count = int(selected.count())

candidates.unpersist()
selected_rows = selected.collect()
selected_count = len(selected_rows)
if selected_count == 0:
    result = {
        "status": "success",
        "mode": MODE,
        "selected": 0,
        "new_candidates": candidate_count,
        "recovered_committed_rows": recovered_count,
    }
    record_heartbeat(result)
    dbutils.jobs.taskValues.set(key="OCR_RESULT", value=json.dumps(result))
    dbutils.notebook.exit(json.dumps(result))

processing_cutoff_seconds = OCR_TASK_BUDGET_SEC - OCR_FINALIZE_RESERVE_SEC
if (
    MODE == "APPLY"
    and time.monotonic() - NOTEBOOK_STARTED_MONOTONIC >= processing_cutoff_seconds
):
    deferred_count = release_deferred_claims(selected_rows, lease_owner)
    payload = {
        "status": "success",
        "mode": MODE,
        "selected": selected_count,
        "processed": 0,
        "deferred": deferred_count,
        "budget_exhausted": True,
        "recovered_committed_rows": recovered_count,
        "states": {},
    }
    record_heartbeat(payload)
    print(json.dumps(payload, indent=2, default=str))
    dbutils.jobs.taskValues.set(key="OCR_RESULT", value=json.dumps(payload, default=str))
    dbutils.notebook.exit(json.dumps(payload, default=str))


# COMMAND ----------

report = gpu_report()
if report["gpu_count"] < 1:
    release_deferred_claims(selected_rows, lease_owner, "deferred before OCR: no GPU visible")
    raise RuntimeError("PaddleOCR task is not running on GPU compute: " + json.dumps(report, default=str))

engine_kind = ENGINE_KIND
engine_started = time.monotonic()
try:
    pool = start_pool()
except Exception:
    # A startup failure (OOM, changed models) must not burn an attempt on every claimed row.
    release_deferred_claims(selected_rows, lease_owner, "deferred before OCR: worker pool failed to start")
    raise
engine_init_seconds = round(time.monotonic() - engine_started, 3)
report["engine_kind"] = engine_kind
report["engine_init_seconds"] = engine_init_seconds
report["paddle_module_version"] = pool.workers[0].runtime_info.get("paddle")
report["cuda"] = pool.workers[0].runtime_info.get("cuda")
print(json.dumps(report, indent=2, default=str))


result_schema = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("EVENT_ID", T.LongType()),
    T.StructField("ADC_UPDT", T.TimestampType()),
    T.StructField("raw_sha256", T.StringType()),
    T.StructField("volume_path", T.StringType()),
    T.StructField("text", T.StringType()),
    T.StructField("text_length", T.LongType()),
    T.StructField("status", T.StringType()),
    T.StructField("parse_status", T.StringType()),
    T.StructField("pages_processed", T.IntegerType()),
    T.StructField("page_count", T.IntegerType()),
    T.StructField("attempt_count", T.IntegerType()),
    T.StructField("engine", T.StringType()),
    T.StructField("model_version", T.StringType()),
    T.StructField("paddleocr_version", T.StringType()),
    T.StructField("paddle_version", T.StringType()),
    T.StructField("device", T.StringType()),
    T.StructField("line_count", T.IntegerType()),
    T.StructField("mean_confidence", T.DoubleType()),
    T.StructField("min_confidence", T.DoubleType()),
    T.StructField("truncated", T.BooleanType()),
    T.StructField("metrics", T.StringType()),
])

page_schema = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("EVENT_ID", T.LongType()),
    T.StructField("ADC_UPDT", T.TimestampType()),
    T.StructField("raw_sha256", T.StringType()),
    T.StructField("page_index", T.IntegerType()),
    T.StructField("text", T.StringType()),
    T.StructField("line_count", T.IntegerType()),
    T.StructField("mean_confidence", T.DoubleType()),
    T.StructField("min_confidence", T.DoubleType()),
    T.StructField("lines_json", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("elapsed_seconds", T.DoubleType()),
    T.StructField("engine", T.StringType()),
    T.StructField("model_version", T.StringType()),
])

def document_task(row):
    return {
        "kind": "pdf",
        "path": row.volume_path,
        "max_pages": OCR_MAX_PAGES,
        "dpi": OCR_DPI,
        "page_batch_size": OCR_PAGE_BATCH_SIZE,
        "total_timeout_sec": OCR_TOTAL_TIMEOUT_SEC,
        "preprocessing": PREPROCESSING,
    }


def run_documents(pool, rows, make_task, seconds, stop_when):
    """Drain rows through the pool: (completed [(row, outcome)], failed (row, exc) | None, deferred [row]).

    A failed task stops dispatch; in-flight tasks still complete. Rows never dispatched are deferred.
    Rows are matched to results by task identity, never by path.
    """
    tasks = [make_task(row) for row in rows]
    row_for = {id(task): row for task, row in zip(tasks, rows)}
    completed, failed, finished = [], None, set()
    try:
        for task, outcome in pool.run_all(tasks, lambda task: seconds, stop_when=stop_when):
            completed.append((row_for[id(task)], outcome))
            finished.add(id(task))
    except Exception as exc:
        failed_task = getattr(pool, "failed_task", None)
        if failed_task is None or id(failed_task) not in row_for:
            raise
        failed = (row_for[id(failed_task)], exc)
        finished.add(id(failed_task))
    deferred = [row for task, row in zip(tasks, rows) if id(task) not in finished]
    return completed, failed, deferred


def past_processing_cutoff():
    return MODE == "APPLY" and time.monotonic() - NOTEBOOK_STARTED_MONOTONIC >= processing_cutoff_seconds


def document_rows(row, outcome):
    summary = pdf.summarise_document(outcome, OCR_MAX_PAGES, MAX_TEXT_CHARS)
    text_value = summary["text"]
    metrics = {
        "elapsed_seconds": summary["elapsed_seconds"],
        "engine_init_seconds": engine_init_seconds,
        "engine": engine_kind,
        "model_version": OCR_MODEL_VERSION,
        "model_fingerprint": MODEL_FINGERPRINT,
        "language": OCR_LANGUAGE,
        "device": OCR_DEVICE,
        "workers": OCR_WORKERS,
        "dpi": OCR_DPI,
        "page_batch_size": OCR_PAGE_BATCH_SIZE,
        "min_confidence": OCR_MIN_CONFIDENCE,
        "preprocessing": PREPROCESSING,
        "pages_processed": summary["pages_processed"],
        "page_count": summary["page_count"],
        "line_count": summary["line_count"],
        "soft_timeout": summary["soft_timeout"],
        "truncated": summary["truncated"],
        "errors": summary["errors"][:50],
    }
    keys = {
        "run_id": row.run_id,
        "EVENT_ID": int(row.EVENT_ID),
        "ADC_UPDT": row.ADC_UPDT,
        "raw_sha256": row.raw_sha256,
    }
    result = {
        **keys,
        "volume_path": row.volume_path,
        "text": text_value,
        "text_length": len(text_value) if text_value else None,
        "status": summary["status"],
        "parse_status": summary["parse_status"],
        "pages_processed": summary["pages_processed"],
        "page_count": summary["page_count"],
        "attempt_count": int(row.attempt_count or 0),
        "engine": engine_kind,
        "model_version": OCR_MODEL_VERSION,
        "paddleocr_version": report.get("paddleocr"),
        "paddle_version": report.get("paddle_module_version") or report.get("paddlepaddle_gpu"),
        "device": OCR_DEVICE,
        "line_count": summary["line_count"],
        "mean_confidence": summary["mean_confidence"],
        "min_confidence": summary["min_confidence"],
        "truncated": summary["truncated"],
        "metrics": json.dumps(metrics, sort_keys=True, ensure_ascii=False),
    }
    pages = [
        {**keys, **page, "engine": engine_kind, "model_version": OCR_MODEL_VERSION}
        for page in summary["page_rows"]
    ]
    return result, pages


document_results = []
page_results = []


def add_document(row, outcome):
    document_result, document_pages = document_rows(row, outcome)
    document_results.append(document_result)
    page_results.extend(document_pages)
    print(json.dumps({
        "EVENT_ID": document_result["EVENT_ID"],
        "parse_status": document_result["parse_status"],
        "pages": document_result["pages_processed"],
        "lines": document_result["line_count"],
        "text_length": document_result["text_length"],
    }, default=str))


document_seconds = OCR_TOTAL_TIMEOUT_SEC + 300
remaining = list(selected_rows)
pool_restarts = 0
while remaining:
    completed, failed, remaining = run_documents(
        pool, remaining, document_task, document_seconds, past_processing_cutoff
    )
    for completed_row, outcome in completed:
        add_document(completed_row, outcome)
    if failed is None:
        break
    failed_row, failure = failed
    add_document(failed_row, pdf.failed_outcome(f"{type(failure).__name__}: {str(failure)[:500]}", document_seconds))
    pool.close()
    pool = None
    if not remaining or pool_restarts >= 2 or past_processing_cutoff():
        break
    pool_restarts += 1
    try:
        pool = start_pool()
    except Exception as exc:
        report["pool_restart_error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
        break
if pool is not None:
    pool.close()
report["pool_restarts"] = pool_restarts
deferred_rows = remaining
gc.collect()

if MODE == "APPLY" and deferred_rows:
    release_deferred_claims(deferred_rows, lease_owner)

results = spark.createDataFrame(document_results, schema=result_schema)
pages = spark.createDataFrame(page_results, schema=page_schema) if page_results else None
processed_count = len(document_results)
deferred_count = len(deferred_rows)

if MODE == "APPLY" and processed_count == 0:
    payload = {
        "status": "success",
        "mode": MODE,
        "selected": selected_count,
        "processed": 0,
        "deferred": deferred_count,
        "budget_exhausted": bool(deferred_count),
        "recovered_committed_rows": recovered_count,
        "states": {},
        "engine": report,
    }
    record_heartbeat(payload)
    print(json.dumps(payload, indent=2, default=str))
    dbutils.jobs.taskValues.set(key="OCR_RESULT", value=json.dumps(payload, default=str))
    dbutils.notebook.exit(json.dumps(payload, default=str))


# COMMAND ----------

if MODE == "CANARY":
    canary_id = uuid.uuid4().hex
    canary_frame = results.withColumn("canary_id", F.lit(canary_id)).withColumn(
        "output_ts", F.current_timestamp()
    )
    target_fields = spark.table(OCR_CANARY_OUTPUT).schema.fields
    canary_aligned = canary_frame.select(*[
        F.col(field.name).cast(field.dataType).alias(field.name)
        if field.name in canary_frame.columns
        else F.lit(None).cast(field.dataType).alias(field.name)
        for field in target_fields
    ])
    canary_aligned.write.mode("append").insertInto(OCR_CANARY_OUTPUT)
    summary = {
        row["parse_status"]: int(row["count"])
        for row in results.groupBy("parse_status").count().collect()
    }
    payload = {
        "status": "canary_complete",
        "canary_id": canary_id,
        "selected": selected_count,
        "processed": processed_count,
        "states": summary,
        "target": OCR_CANARY_OUTPUT,
    }
    dbutils.jobs.taskValues.set(key="OCR_RESULT", value=json.dumps(payload))
    display(canary_aligned.select(
        "EVENT_ID", "volume_path", "parse_status", "pages_processed", "line_count",
        "mean_confidence", "text_length", "text",
    ))
    dbutils.notebook.exit(json.dumps(payload, default=str))


# COMMAND ----------

output_frame = results.withColumn("output_ts", F.current_timestamp())
(
    DeltaTable.forName(spark, OCR_OUTPUT)
    .alias("t")
    .merge(
        output_frame.alias("s"),
        "t.EVENT_ID=s.EVENT_ID AND t.ADC_UPDT <=> s.ADC_UPDT "
        "AND t.raw_sha256=s.raw_sha256",
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

if pages is not None:
    page_frame = pages.withColumn("output_ts", F.current_timestamp())
    (
        DeltaTable.forName(spark, OCR_PAGE_OUTPUT)
        .alias("t")
        .merge(
            page_frame.alias("s"),
            "t.EVENT_ID=s.EVENT_ID AND t.ADC_UPDT <=> s.ADC_UPDT "
            "AND t.raw_sha256=s.raw_sha256 AND t.page_index=s.page_index",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

successful = results.filter(
    F.col("parse_status").isin("ocr_complete", "ocr_partial") & F.col("text").isNotNull()
)
current_target_key_query = (
    spark.table(BRONZE)
    .filter(F.col("STATUS").isin(*SUPPORTED_SOURCE_STATUSES))
    .filter(F.col("BLOB_TEXT").isNull() | (F.length(F.trim(F.col("BLOB_TEXT"))) == 0))
    .select("BLOB_VERSION_ID", "EVENT_ID", "ADC_UPDT", "raw_sha256")
    .join(
        successful.select("EVENT_ID", "ADC_UPDT", "raw_sha256"),
        ["EVENT_ID", "ADC_UPDT", "raw_sha256"],
        "inner",
    )
    .distinct()
)
current_target_key_rows = current_target_key_query.collect()
current_target_keys = spark.createDataFrame(current_target_key_rows, current_target_key_query.schema)
applicable = (
    successful.alias("o")
    .join(
        current_target_keys.alias("k"),
        (F.col("o.EVENT_ID") == F.col("k.EVENT_ID"))
        & F.col("o.ADC_UPDT").eqNullSafe(F.col("k.ADC_UPDT"))
        & (F.col("o.raw_sha256") == F.col("k.raw_sha256")),
        "inner",
    )
    .select("o.*", F.col("k.BLOB_VERSION_ID").alias("BLOB_VERSION_ID"))
)

if current_target_key_rows:
    (
        DeltaTable.forName(spark, BRONZE)
        .alias("t")
        .merge(
            applicable.alias("s"),
            "t.BLOB_VERSION_ID=s.BLOB_VERSION_ID",
        )
        .whenMatchedUpdate(
            condition=(
                "t.STATUS IN ('OCR queued','PDF extraction failed') "
                "AND (t.BLOB_TEXT IS NULL OR trim(t.BLOB_TEXT) = '')"
            ),
            set={
                "BLOB_TEXT": "s.text",
                "TEXT_LENGTH": "s.text_length",
                "STATUS": "CASE WHEN s.parse_status = 'ocr_complete' THEN 'Decoded' ELSE 'OCR partial' END",
                "ENCODING": "'utf-8'",
                "anon_text": "NULL",
                "parser_version": f"greatest(coalesce(t.parser_version, 0), {OCR_PARSER_VERSION})",
                "post_processor_version": (
                    f"greatest(coalesce(t.post_processor_version, 0), {OCR_POST_PROCESSOR_VERSION})"
                ),
            },
        )
        .execute()
    )
    append_missing_history(applicable)


# COMMAND ----------

matched_keys = current_target_keys.withColumn("_target_current", F.lit(True))
queue_updates = (
    results.join(
        matched_keys,
        ["EVENT_ID", "ADC_UPDT", "raw_sha256"],
        "left",
    )
    .withColumn(
        "next_status",
        F.when(
            F.col("parse_status") == "ocr_complete",
            F.when(F.col("_target_current"), F.lit("complete")).otherwise(F.lit("superseded")),
        )
        .when(
            F.col("parse_status") == "ocr_partial",
            F.when(F.col("_target_current"), F.lit("partial")).otherwise(F.lit("superseded")),
        )
        .when(F.col("parse_status") == "ocr_empty", F.lit("empty"))
        .when(F.col("attempt_count") >= MAX_ATTEMPTS, F.lit("failed_terminal"))
        .otherwise(F.lit("retryable")),
    )
    .withColumn(
        "last_error",
        F.when(
            F.col("next_status").isin("complete", "partial", "empty", "superseded"),
            F.lit(None).cast("string"),
        ).otherwise(F.concat_ws(": ", "status", "metrics")),
    )
)
(
    DeltaTable.forName(spark, OCR_QUEUE)
    .alias("t")
    .merge(
        queue_updates.alias("s"),
        "t.EVENT_ID=s.EVENT_ID AND t.ADC_UPDT <=> s.ADC_UPDT "
        "AND t.raw_sha256=s.raw_sha256",
    )
    .whenMatchedUpdate(
        set={
            "status": "s.next_status",
            "lease_owner": "NULL",
            "lease_expires_ts": "NULL",
            "last_error": "s.last_error",
            "updated_ts": "current_timestamp()",
            "completed_ts": (
                "CASE WHEN s.next_status IN "
                "('complete','partial','empty','superseded','failed_terminal') "
                "THEN current_timestamp() ELSE NULL END"
            ),
        }
    )
    .execute()
)

summary = {
    row["next_status"]: int(row["count"])
    for row in queue_updates.groupBy("next_status").count().collect()
}
payload = {
    "status": "success",
    "mode": MODE,
    "selected": selected_count,
    "processed": processed_count,
    "deferred": deferred_count,
    "budget_exhausted": bool(deferred_count),
    "recovered_committed_rows": recovered_count,
    "states": summary,
    "engine": report,
}
record_heartbeat(payload)
print(json.dumps(payload, indent=2, default=str))
dbutils.jobs.taskValues.set(key="OCR_RESULT", value=json.dumps(payload, default=str))
dbutils.notebook.exit(json.dumps(payload, default=str))
