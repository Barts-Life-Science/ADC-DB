# Databricks notebook source
# Version-grain repair staged 2026-08-25; processing keyed by SOURCE_VERSION_ID.
# Blob 2 v4: resumable, idempotent processing with shared outputs, attempt
# leases, automatic transient retry, deterministic failure isolation and quarantine.
#
# Efficiency revision (2026-07-31), logic-preserving except where noted:
# - the orchestrator preserves the child's retry classification instead of
#   overwriting it with the generic WorkflowException verdict
# - state-table MERGEs/UPDATEs retry through Delta optimistic-concurrency conflicts
# - infrastructure failures (worker_error/udf_error/timeout) are split from data
#   failures: the real exception text is persisted, a storm guard fails the shard
#   attempt instead of mass-quarantining, escalation lands in 'quarantined_infra'
#   (terminal for coverage, excluded from the circuit breaker), and a preflight
#   canary aborts the run early when the worker environment is broken
# - a continuous slot-filling scheduler replaces the wave barrier; the wall-clock
#   budget is enforced per dispatch instead of per wave
# - keys with an 'observed' quarantine row are deprioritized so fresh work gets
#   the per-attempt row budget first
#
# Dependencies are attached to the job cluster. Runtime installation is prohibited
# because every dbutils.notebook.run shard would otherwise restart its interpreter.

# COMMAND ----------

# MAGIC %run ./blob_shared_lib

# COMMAND ----------

# MAGIC %run ./blob_pipeline_lib

# COMMAND ----------

_V4_BLOB_PROCESS_POOL_CLASS = BlobProcessPool
_V4_PROCESS_TIMEOUT_SECONDS = int(PROCESS_TIMEOUT_SEC)

# COMMAND ----------

import hashlib
import json
import math
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait as futures_wait

from pyspark import StorageLevel
from pyspark.sql import Row, functions as F, types as T


dbutils.widgets.text("RUN_ID", "")
dbutils.widgets.text("SHARDS", "16")
dbutils.widgets.text("SHARD_ID", "")
dbutils.widgets.text("ATTEMPT_ID", "")
dbutils.widgets.text("ATTEMPT_NO", "")
dbutils.widgets.text("MAX_ROWS_PER_SHARD", "10000")
dbutils.widgets.text("MAX_CONCURRENT_SHARDS", "4")
dbutils.widgets.text("SHARD_TIMEOUT_SEC", "21600")
dbutils.widgets.text("MAX_SHARD_RETRIES", "2")
dbutils.widgets.text("RETRY_BASE_SEC", "30")
dbutils.widgets.text("MAX_NONRETRYABLE_KEY_ATTEMPTS", "3")
dbutils.widgets.text("MAX_QUARANTINE_COUNT", "1000")
dbutils.widgets.text("MAX_QUARANTINE_RATE", "0.002")
dbutils.widgets.text("PREFLIGHT_CANARY_KEYS", "8")
dbutils.widgets.text("INFRA_STORM_RATE", "0.5")
dbutils.widgets.text("INFRA_STORM_MIN_ROWS", "100")
dbutils.widgets.text("STATE_SCHEMA", "4_prod.tmp")
dbutils.widgets.text("METADATA_TABLE", "4_prod.tmp.pipeline_runs")
dbutils.widgets.text("HISTORY_TABLE", "4_prod.logs.mill_blob_extractor_history")  # deprecated, unused
dbutils.widgets.text("STAGING_DB", "4_prod.tmp")  # deprecated, unused
dbutils.widgets.text("PROCESSING_CONFIG_TABLE", "4_prod.tmp.pipeline_runs")  # deprecated, unused
dbutils.widgets.text("MAX_ORCHESTRATOR_SEC", "14400")
dbutils.widgets.text("MAX_WAVES", "8")
dbutils.widgets.dropdown("FAIL_ON_PARTIAL", "true", ["true", "false"])

# COMMAND ----------



RUN_ID = dbutils.widgets.get("RUN_ID").strip()
SHARDS = int(dbutils.widgets.get("SHARDS") or "16")
SHARD_ID_RAW = dbutils.widgets.get("SHARD_ID").strip()
ATTEMPT_ID = dbutils.widgets.get("ATTEMPT_ID").strip()
ATTEMPT_NO_RAW = dbutils.widgets.get("ATTEMPT_NO").strip()
MAX_ROWS_PER_SHARD = int(dbutils.widgets.get("MAX_ROWS_PER_SHARD") or "10000")
MAX_CONCURRENT_SHARDS = int(dbutils.widgets.get("MAX_CONCURRENT_SHARDS") or "4")
SHARD_TIMEOUT_SEC = int(dbutils.widgets.get("SHARD_TIMEOUT_SEC") or "21600")
MAX_SHARD_RETRIES = int(dbutils.widgets.get("MAX_SHARD_RETRIES") or "2")
RETRY_BASE_SEC = int(dbutils.widgets.get("RETRY_BASE_SEC") or "30")
MAX_NONRETRYABLE_KEY_ATTEMPTS = int(
    dbutils.widgets.get("MAX_NONRETRYABLE_KEY_ATTEMPTS") or "3"
)
MAX_QUARANTINE_COUNT = int(dbutils.widgets.get("MAX_QUARANTINE_COUNT") or "1000")
MAX_QUARANTINE_RATE = float(dbutils.widgets.get("MAX_QUARANTINE_RATE") or "0.002")
PREFLIGHT_CANARY_KEYS = int(dbutils.widgets.get("PREFLIGHT_CANARY_KEYS") or "8")
INFRA_STORM_RATE = float(dbutils.widgets.get("INFRA_STORM_RATE") or "0.5")
INFRA_STORM_MIN_ROWS = int(dbutils.widgets.get("INFRA_STORM_MIN_ROWS") or "100")
STATE_SCHEMA = dbutils.widgets.get("STATE_SCHEMA").strip()
METADATA_TABLE = dbutils.widgets.get("METADATA_TABLE").strip()
MAX_ORCHESTRATOR_SEC = int(dbutils.widgets.get("MAX_ORCHESTRATOR_SEC") or "14400")
MAX_WAVES = int(dbutils.widgets.get("MAX_WAVES") or "8")
FAIL_ON_PARTIAL = dbutils.widgets.get("FAIL_ON_PARTIAL").lower() == "true"

IS_ORCHESTRATOR = SHARD_ID_RAW == ""
SHARD_ID = None if IS_ORCHESTRATOR else int(SHARD_ID_RAW)
ATTEMPT_NO = int(ATTEMPT_NO_RAW) if ATTEMPT_NO_RAW else None

if not RUN_ID:
    raise ValueError("RUN_ID is required")
safe_value(RUN_ID, "run id")
validate_table_name(STATE_SCHEMA)
validate_table_name(METADATA_TABLE)
if not 1 <= SHARDS <= 128:
    raise ValueError("SHARDS must be between 1 and 128")
if SHARD_ID is not None and not 0 <= SHARD_ID < SHARDS:
    raise ValueError("SHARD_ID is outside the configured range")
if MAX_ROWS_PER_SHARD < 1:
    raise ValueError("MAX_ROWS_PER_SHARD must be positive")
if not 1 <= MAX_CONCURRENT_SHARDS <= 16:
    raise ValueError("MAX_CONCURRENT_SHARDS must be between 1 and 16")
if PREFLIGHT_CANARY_KEYS < 0:
    raise ValueError("PREFLIGHT_CANARY_KEYS must be zero or positive")
if not 0.0 < INFRA_STORM_RATE <= 1.0:
    raise ValueError("INFRA_STORM_RATE must be in (0, 1]")
if INFRA_STORM_MIN_ROWS < 1:
    raise ValueError("INFRA_STORM_MIN_ROWS must be positive")

RUN_EVENTS = f"{STATE_SCHEMA}.run_events"
RUN_CHUNKS = f"{STATE_SCHEMA}.run_chunks"
ATTEMPTS = f"{STATE_SCHEMA}.shard_attempts"
BATCH_OUTPUT = f"{STATE_SCHEMA}.batch_output"
HISTORY_OUTPUT = f"{STATE_SCHEMA}.history_output"
QUARANTINE = f"{STATE_SCHEMA}.quarantine"
PIPELINE_EVENTS = f"{STATE_SCHEMA}.pipeline_events"
for required in (
    METADATA_TABLE, RUN_EVENTS, RUN_CHUNKS, ATTEMPTS,
    BATCH_OUTPUT, HISTORY_OUTPUT, QUARANTINE, PIPELINE_EVENTS,
):
    if not table_exists(required):
        raise RuntimeError(f"Missing v4 table {required}")

# Infrastructure failures are environment-shaped (broken worker subprocess,
# stuck executor) rather than properties of the blob bytes; they escalate to
# 'quarantined_infra' (terminal for coverage, excluded from the circuit breaker)
# instead of 'quarantined'. blob_length_mismatch stays a data failure.
_DATA_FATAL_PARSE_STATUSES = ("decompression_failed",)
_INFRA_PARSE_STATUSES = ("worker_error", "udf_error", "timeout")
_FATAL_PARSE_STATUSES = _DATA_FATAL_PARSE_STATUSES + _INFRA_PARSE_STATUSES


def run_row():
    rows = spark.table(METADATA_TABLE).filter(F.col("run_id") == RUN_ID).limit(2).collect()
    if len(rows) != 1:
        raise RuntimeError(f"Expected one run row for {RUN_ID}; found {len(rows)}")
    return rows[0]


def completed_keys():
    return completed_output_keys(BATCH_OUTPUT, HISTORY_OUTPUT, RUN_ID)


def terminal_quarantine_keys():
    return quarantined_keys(QUARANTINE, RUN_ID)


def pending_keys():
    """Return every unresolved valid source-version key for the run."""
    work = (
        spark.table(RUN_EVENTS)
        .filter((F.col("run_id") == RUN_ID) & (F.col("chunk_integrity_status") == "valid"))
        .select("EVENT_ID", "SOURCE_VERSION_ID", "ADC_UPDT", "source_commit_version", "shard_id")
    )
    accounted = completed_keys().unionByName(terminal_quarantine_keys()).select(
        "EVENT_ID", "SOURCE_VERSION_ID"
    ).distinct()
    return null_safe_join(work, accounted, BLOB_PROCESSING_KEYS, "left_anti")


def pending_keys_for_shard(shard_id):
    return pending_keys().filter(F.col("shard_id") == int(shard_id))


def pending_counts_by_shard():
    """One Spark action replaces the former sixteen serial count actions."""
    return {
        int(row["shard_id"]): int(row["pending_count"])
        for row in (
            pending_keys()
            .groupBy("shard_id")
            .agg(F.count(F.lit(1)).alias("pending_count"))
            .collect()
        )
    }


def next_attempt_no(shard_id):
    row = (
        spark.table(ATTEMPTS)
        .filter((F.col("run_id") == RUN_ID) & (F.col("shard_id") == int(shard_id)))
        .agg(F.max("attempt_no").alias("m"))
        .collect()[0]
    )
    return int(row["m"] or 0) + 1


def create_attempt(shard_id, attempt_id, attempt_no, status="submitted"):
    schema = spark.table(ATTEMPTS).schema
    row = Row(
        run_id=RUN_ID,
        shard_id=int(shard_id),
        attempt_id=attempt_id,
        attempt_no=int(attempt_no),
        status=status,
        lease_owner=attempt_id,
        lease_expires_ts=utc_now_naive() + timedelta(seconds=SHARD_TIMEOUT_SEC),
        selected_events=None,
        completed_events=None,
        quarantined_events=None,
        error_class=None,
        error_message=None,
        failure_fingerprint=None,
        databricks_trace_id=None,
        retryable=None,
        started_ts=utc_now_naive(),
        heartbeat_ts=utc_now_naive(),
        ended_ts=None,
    )
    spark.createDataFrame([row], schema=schema).write.mode("append").insertInto(ATTEMPTS)


def update_attempt(attempt_id, status, **values):
    assignments = [
        f"status = {sql_string(status)}",
        "heartbeat_ts = current_timestamp()",
    ]
    if status in {"success", "failed", "no_work"}:
        assignments.append("ended_ts = current_timestamp()")
        assignments.append("lease_expires_ts = NULL")
    for key, value in values.items():
        if isinstance(value, bool):
            rendered = "true" if value else "false"
        elif isinstance(value, (int, float)):
            rendered = str(value)
        else:
            rendered = sql_string(value)
        assignments.append(f"{key} = {rendered}")
    run_sql_with_retry(
        f"UPDATE {quote_table(ATTEMPTS)} SET {', '.join(assignments)} "
        f"WHERE run_id = {sql_string(RUN_ID)} AND attempt_id = {sql_string(attempt_id)}",
        label=f"update attempt {attempt_id}",
    )


def record_nonretryable_rows(failures, failure_count):
    """Bulk-upsert deterministic failures at immutable source-version grain."""
    if failure_count == 0:
        return {"quarantined": 0, "quarantined_infra": 0, "observed": 0}

    metrics_error = F.get_json_object(F.col("metrics"), "$.error")
    keyed = (
        failures
        .withColumn("error_class", F.coalesce(F.col("parse_status"), F.lit("processing_failure")))
        .withColumn(
            "_failure_kind",
            F.when(F.col("parse_status").isin(*_INFRA_PARSE_STATUSES), F.lit("infra"))
            .otherwise(F.lit("data")),
        )
        .withColumn(
            "error_message",
            F.substring(F.coalesce(F.when(metrics_error != "", metrics_error), F.col("STATUS"), F.lit("processing failure")), 1, 4000),
        )
        .withColumn("failure_fingerprint", failure_fingerprint_col(F.col("error_class"), F.col("error_message")))
    )
    existing = (
        spark.table(QUARANTINE)
        .filter(F.col("run_id") == RUN_ID)
        .select(
            "EVENT_ID", "SOURCE_VERSION_ID", "failure_fingerprint",
            F.col("nonretryable_attempts").alias("_previous_attempts"),
            F.col("first_failure_ts").alias("_first_failure_ts"),
        )
    )
    staged = (
        keyed.join(existing, ["EVENT_ID", "SOURCE_VERSION_ID", "failure_fingerprint"], "left")
        .withColumn("nonretryable_attempts", F.coalesce(F.col("_previous_attempts"), F.lit(0)) + F.lit(1))
        .withColumn(
            "status",
            F.when(
                F.col("nonretryable_attempts") >= MAX_NONRETRYABLE_KEY_ATTEMPTS,
                F.when(F.col("_failure_kind") == "infra", F.lit("quarantined_infra")).otherwise(F.lit("quarantined")),
            ).otherwise(F.lit("observed")),
        )
        .select(
            F.lit(RUN_ID).alias("run_id"),
            F.lit(int(SHARD_ID)).cast("int").alias("shard_id"),
            F.col("EVENT_ID").cast("long"),
            "SOURCE_VERSION_ID", "ADC_UPDT",
            F.col("source_commit_version").cast("long"),
            F.when(F.col("_failure_kind") == "infra", F.lit("infrastructure_failure"))
            .otherwise(F.lit("deterministic_processing_failure")).alias("reason_code"),
            "error_class", "error_message", "failure_fingerprint",
            F.col("nonretryable_attempts").cast("int"),
            F.concat_ws(":", F.lit(RUN_ID), F.col("EVENT_ID").cast("string"), F.col("SOURCE_VERSION_ID")).alias("raw_reference"),
            "status",
            F.coalesce(F.col("_first_failure_ts"), F.current_timestamp()).alias("first_failure_ts"),
            F.current_timestamp().alias("last_failure_ts"),
            F.lit(None).cast("string").alias("resolution"),
            F.lit(None).cast("string").alias("reviewed_by"),
            F.lit(None).cast("timestamp").alias("reviewed_ts"),
        )
    )
    counts = staged.agg(
        F.sum(F.when(F.col("status") == "quarantined", 1).otherwise(0)).alias("quarantined"),
        F.sum(F.when(F.col("status") == "quarantined_infra", 1).otherwise(0)).alias("quarantined_infra"),
        F.sum(F.when(F.col("status") == "observed", 1).otherwise(0)).alias("observed"),
    ).first()
    merge_all(
        staged,
        QUARANTINE,
        "t.run_id = s.run_id AND t.EVENT_ID = s.EVENT_ID "
        "AND t.SOURCE_VERSION_ID = s.SOURCE_VERSION_ID "
        "AND t.failure_fingerprint = s.failure_fingerprint",
    )
    escalated_count = int(counts["quarantined"] or 0) + int(counts["quarantined_infra"] or 0)
    if escalated_count:
        log_pipeline_events_bulk(
            PIPELINE_EVENTS,
            staged.filter(F.col("status").isin("quarantined", "quarantined_infra")).select(
                F.lit(meta["pipeline_id"]).alias("pipeline_id"),
                "run_id", "shard_id", "EVENT_ID", "ADC_UPDT",
                F.when(F.col("status") == "quarantined_infra", F.lit("WARN")).otherwise(F.lit("ERROR")).alias("severity"),
                F.when(F.col("status") == "quarantined_infra", F.lit("key_quarantined_infra")).otherwise(F.lit("key_quarantined")).alias("event_type"),
                F.col("error_message").alias("message"),
                F.to_json(F.struct("SOURCE_VERSION_ID", "reason_code", "error_class", "failure_fingerprint", "nonretryable_attempts")).alias("details"),
            ),
        )
    return {
        "quarantined": int(counts["quarantined"] or 0),
        "quarantined_infra": int(counts["quarantined_infra"] or 0),
        "observed": int(counts["observed"] or 0),
    }


# COMMAND ----------

# The UDF cells sit above the orchestrator block so the preflight canary can run
# the real executor -> loky -> subprocess path before any shard is dispatched.

_udf_schema = T.StructType([
    T.StructField("EVENT_ID", T.LongType()),
    T.StructField("VALID_UNTIL_DT_TM", T.TimestampType()),
    T.StructField("VALID_FROM_DT_TM", T.TimestampType()),
    T.StructField("UPDT_DT_TM", T.TimestampType()),
    T.StructField("UPDT_ID", T.LongType()),
    T.StructField("UPDT_TASK", T.LongType()),
    T.StructField("UPDT_CNT", T.LongType()),
    T.StructField("UPDT_APPLCTX", T.LongType()),
    T.StructField("LAST_UTC_TS", T.TimestampType()),
    T.StructField("ADC_UPDT", T.TimestampType()),
    T.StructField("BLOB_BINARY", T.BinaryType()),
    T.StructField("CONTENT_TYPE", T.StringType()),
    T.StructField("ENCODING", T.StringType()),
    T.StructField("BLOB_TEXT", T.StringType()),
    T.StructField("BINARY_SIZE", T.LongType()),
    T.StructField("TEXT_LENGTH", T.LongType()),
    T.StructField("STATUS", T.StringType()),
    T.StructField("anon_text", T.StringType()),
    T.StructField("ENCNTR_ID", T.LongType()),
    T.StructField("Trust", T.StringType()),
    T.StructField("raw_sha256", T.StringType()),
    T.StructField("decompressor_version", T.IntegerType()),
    T.StructField("parser_version", T.IntegerType()),
    T.StructField("post_processor_version", T.IntegerType()),
    T.StructField("history_status", T.StringType()),
    T.StructField("truncation_flag", T.BooleanType()),
    T.StructField("truncation_reason", T.StringType()),
    T.StructField("ftfy_explain", T.StringType()),
    T.StructField("decompression_strategy", T.StringType()),
    T.StructField("parse_status", T.StringType()),
    T.StructField("metrics", T.StringType()),
])


def _safe_int(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        try:
            return int(float(str(value)))
        except Exception:
            return None


def _raw_content_sha256_local(chunks):
    digest = hashlib.sha256()
    for chunk in sorted(
        chunks or [],
        key=lambda item: (
            item.get("BLOB_SEQ_NUM") is None,
            item.get("BLOB_SEQ_NUM") or 0,
        ),
    ):
        contents = chunk.get("BLOB_CONTENTS")
        if contents:
            digest.update(bytes(contents))
    return digest.hexdigest()


def _combine_blob_chunks_local(chunks):
    marker = b"ocf_blob\x00"
    output = []
    for chunk in sorted(
        chunks or [],
        key=lambda item: (
            item.get("BLOB_SEQ_NUM") is None,
            item.get("BLOB_SEQ_NUM") or 0,
        ),
    ):
        contents = chunk.get("BLOB_CONTENTS")
        if not contents:
            continue
        payload = bytes(contents)
        if payload.endswith(marker):
            payload = payload[:-len(marker)]
        output.append(payload)
    return b"".join(output)


_BLOB_PROCESS_POOL_CLASS = _V4_BLOB_PROCESS_POOL_CLASS
_PROCESS_TIMEOUT_SECONDS = _V4_PROCESS_TIMEOUT_SECONDS
_POOL_STATE = {"pool": None}

_CHUNK_STRUCT = F.struct(
    "BLOB_SEQ_NUM", "VALID_UNTIL_DT_TM", "VALID_FROM_DT_TM", "UPDT_DT_TM",
    "UPDT_ID", "UPDT_TASK", "UPDT_CNT", "UPDT_APPLCTX", "LAST_UTC_TS",
    "ADC_UPDT", "COMPRESSION_CD", "BLOB_CONTENTS", "BLOB_LENGTH",
    "ENCNTR_ID", "Trust",
)


def _worker_pool():
    if _POOL_STATE["pool"] is None:
        _POOL_STATE["pool"] = _BLOB_PROCESS_POOL_CLASS(timeout=_PROCESS_TIMEOUT_SECONDS)
    return _POOL_STATE["pool"]


def _chunk_value(chunk, name):
    try:
        return chunk[name]
    except Exception:
        return getattr(chunk, name, None)


@F.udf(returnType=_udf_schema)
def process_blob_udf(event_id, adc_updt, chunks_data):
    raw_sha = None
    try:
        sorted_chunks = sorted(
            chunks_data or [],
            key=lambda chunk: (
                _chunk_value(chunk, "BLOB_SEQ_NUM") is None,
                _chunk_value(chunk, "BLOB_SEQ_NUM") or 0,
            ),
        )
        first = sorted_chunks[0] if sorted_chunks else None
        original_chunks = [
            {
                "BLOB_SEQ_NUM": _chunk_value(chunk, "BLOB_SEQ_NUM"),
                "BLOB_CONTENTS": _chunk_value(chunk, "BLOB_CONTENTS"),
            }
            for chunk in sorted_chunks
        ]
        raw_sha = _raw_content_sha256_local(original_chunks)
        combined = _combine_blob_chunks_local(original_chunks)
        compression_cd = _chunk_value(first, "COMPRESSION_CD") if first else None
        blob_length = _chunk_value(first, "BLOB_LENGTH") if first else None

        if len(combined) > 16 * 1024 * 1024:
            result = {
                "text": None, "content_type": None, "encoding": None,
                "binary_size": len(combined), "text_length": None,
                "status": f"Compressed too large: {len(combined)} bytes",
                "parse_status": "compressed_too_large",
                "truncation_flag": False, "truncation_reason": None,
                "ftfy_explain": None, "decompression_strategy": None,
                "metrics": {
                    "compressed_bytes": len(combined),
                    "chunk_count": len(sorted_chunks),
                },
            }
        else:
            result = _worker_pool().process(
                combined, compression_cd, len(sorted_chunks), blob_length
            )

        status = result.get("status") or "Processing worker error"
        return Row(
            EVENT_ID=_safe_int(event_id),
            VALID_UNTIL_DT_TM=_chunk_value(first, "VALID_UNTIL_DT_TM") if first else None,
            VALID_FROM_DT_TM=_chunk_value(first, "VALID_FROM_DT_TM") if first else None,
            UPDT_DT_TM=_chunk_value(first, "UPDT_DT_TM") if first else None,
            UPDT_ID=_safe_int(_chunk_value(first, "UPDT_ID")) if first else None,
            UPDT_TASK=_safe_int(_chunk_value(first, "UPDT_TASK")) if first else None,
            UPDT_CNT=_safe_int(_chunk_value(first, "UPDT_CNT")) if first else None,
            UPDT_APPLCTX=_safe_int(_chunk_value(first, "UPDT_APPLCTX")) if first else None,
            LAST_UTC_TS=_chunk_value(first, "LAST_UTC_TS") if first else None,
            ADC_UPDT=adc_updt,
            BLOB_BINARY=None,
            CONTENT_TYPE=result.get("content_type"),
            ENCODING=result.get("encoding"),
            BLOB_TEXT=result.get("text"),
            BINARY_SIZE=_safe_int(result.get("binary_size")),
            TEXT_LENGTH=_safe_int(result.get("text_length")),
            STATUS=status,
            anon_text=None,
            ENCNTR_ID=_safe_int(_chunk_value(first, "ENCNTR_ID")) if first else None,
            Trust=_chunk_value(first, "Trust") if first else None,
            raw_sha256=raw_sha,
            decompressor_version=DECOMPRESSOR_VERSION,
            parser_version=PARSER_VERSION,
            post_processor_version=POST_PROCESSOR_VERSION,
            history_status=status,
            truncation_flag=bool(result.get("truncation_flag", False)),
            truncation_reason=result.get("truncation_reason"),
            ftfy_explain=result.get("ftfy_explain"),
            decompression_strategy=result.get("decompression_strategy"),
            parse_status=result.get("parse_status"),
            metrics=json.dumps(result.get("metrics") or {}, default=str, sort_keys=True),
        )
    except Exception as exc:
        status = f"Processing error: {type(exc).__name__}"
        return Row(
            EVENT_ID=_safe_int(event_id), VALID_UNTIL_DT_TM=None,
            VALID_FROM_DT_TM=None, UPDT_DT_TM=None, UPDT_ID=None, UPDT_TASK=None,
            UPDT_CNT=None, UPDT_APPLCTX=None, LAST_UTC_TS=None, ADC_UPDT=adc_updt,
            BLOB_BINARY=None, CONTENT_TYPE=None, ENCODING=None, BLOB_TEXT=None,
            BINARY_SIZE=None, TEXT_LENGTH=None, STATUS=status, anon_text=None,
            ENCNTR_ID=None, Trust=None, raw_sha256=raw_sha,
            decompressor_version=DECOMPRESSOR_VERSION,
            parser_version=PARSER_VERSION,
            post_processor_version=POST_PROCESSOR_VERSION,
            history_status=status, truncation_flag=False, truncation_reason=None,
            ftfy_explain=None, decompression_strategy=None, parse_status="udf_error",
            metrics=json.dumps({
                "error": f"{type(exc).__name__}: {str(exc)[:500]}"
            }),
        )


# COMMAND ----------

if IS_ORCHESTRATOR:
    meta = run_row()
    if int(meta["shard_count"]) != SHARDS:
        raise RuntimeError(
            f"Run {RUN_ID} was created with SHARDS={meta['shard_count']}; supplied {SHARDS}"
        )
    if meta["status"] in {
        "processing_complete", "merging", "merge_failed",
        "merge_complete", "files_processing", "files_partial", "complete",
    }:
        dbutils.notebook.exit(json.dumps({
            "status": "success",
            "run_id": RUN_ID,
            "already_processed": True,
            "run_status": meta["status"],
        }, sort_keys=True))
    if meta["status"] not in {
        "worklist_ready", "processing", "processing_partial", "processing_failed",
    }:
        raise RuntimeError(
            f"Run {RUN_ID} is not processable from status {meta['status']!r}"
        )

    update_run_status(METADATA_TABLE, RUN_ID, "processing", error_message=None)
    notebook_path = get_current_notebook_path()
    orchestrator_started = time.monotonic()

    # RDE_REPAIR_20260901_PREFLIGHT_INDENT
    # Keep the function inside IS_ORCHESTRATOR; its invocation below must be a sibling,
    # not a recursive call at the end of preflight_canary.
    def preflight_canary(sample_size):
        """Exercise real decoding at immutable source-version grain before dispatch."""
        if sample_size <= 0:
            return
        candidates = (
            spark.table(RUN_EVENTS)
            .filter((F.col("run_id") == RUN_ID) & (F.col("chunk_integrity_status") == "valid"))
            .orderBy(F.col("compressed_size").asc_nulls_last(), "EVENT_ID", "SOURCE_VERSION_ID")
            .limit(sample_size)
            .select("EVENT_ID", "SOURCE_VERSION_ID", "ADC_UPDT")
        )
        grouped = (
            null_safe_join(
                spark.table(RUN_CHUNKS).filter(F.col("run_id") == RUN_ID),
                F.broadcast(candidates.select(*BLOB_PROCESSING_KEYS)),
                BLOB_PROCESSING_KEYS,
                "inner",
            )
            .groupBy(*BLOB_PROCESSING_KEYS)
            .agg(F.max("ADC_UPDT").alias("ADC_UPDT"), F.collect_list(_CHUNK_STRUCT).alias("chunks_data"))
        )
        rows = grouped.select(
            process_blob_udf("EVENT_ID", "ADC_UPDT", "chunks_data").alias("r")
        ).select("r.parse_status", "r.metrics").collect()
        if not rows:
            return
        if all(row["parse_status"] in _INFRA_PARSE_STATUSES for row in rows):
            errors = []
            for row in rows[:3]:
                try:
                    errors.append(json.loads(row["metrics"] or "{}").get("error") or "unknown")
                except Exception:
                    errors.append("unknown")
            log_pipeline_event(
                PIPELINE_EVENTS, "ERROR", "processing_preflight_failed", errors[0],
                pipeline_id=meta["pipeline_id"], run_id=RUN_ID,
                details={"canary_keys": len(rows), "sample_errors": errors, "dependency_report": dependency_report()},
            )
            update_run_status(METADATA_TABLE, RUN_ID, "processing_failed", error_message=("PREFLIGHT_INFRA_FAILURE: " + errors[0])[:8000])
            raise RuntimeError("PREFLIGHT_INFRA_FAILURE: " + safe_json(errors))
        print(f"preflight canary passed on {len(rows)} keys")

    preflight_canary(PREFLIGHT_CANARY_KEYS)

    def attempt_row(attempt_id):
        rows = (
            spark.table(ATTEMPTS)
            .filter((F.col("run_id") == RUN_ID) & (F.col("attempt_id") == attempt_id))
            .limit(1)
            .collect()
        )
        return rows[0] if rows else None

    def resolve_child_failure(attempt_id, exc):
        """Prefer the child's own classification of the real exception over the
        generic WorkflowException surfaced by dbutils.notebook.run. Overwriting
        the child's attempt row here previously destroyed retryable=true verdicts
        and disabled shard retries entirely."""
        row = attempt_row(attempt_id)
        if row is not None and row["status"] == "failed" and row["retryable"] is not None:
            return {
                "error_class": row["error_class"],
                "error_message": row["error_message"],
                "failure_fingerprint": row["failure_fingerprint"],
                "databricks_trace_id": row["databricks_trace_id"],
                "retryable": bool(row["retryable"]),
                "classified_by": "child",
            }
        info = classify_exception(exc)
        info["classified_by"] = "orchestrator"
        if (
            row is None
            or row["status"] in ("submitted", "running")
            or (row["status"] == "failed" and row["retryable"] is None)
        ):
            # The child never recorded an outcome (hard timeout kill, driver loss,
            # or a partial failure write): the orchestrator's classification is
            # the only one available.
            update_attempt(
                attempt_id,
                "failed",
                error_class=info["error_class"],
                error_message=info["error_message"],
                failure_fingerprint=info["failure_fingerprint"],
                databricks_trace_id=info["databricks_trace_id"],
                retryable=info["retryable"],
            )
        else:
            # The child ended terminally (success/no_work) yet the orchestrator
            # still saw an exception (payload transport anomaly). Leave the
            # child's terminal row alone and retry the wrapper.
            info["retryable"] = True
        return info

    def run_child(shard_id):
        last_failure = None
        for retry_index in range(MAX_SHARD_RETRIES + 1):
            attempt_no = next_attempt_no(shard_id)
            attempt_id = uuid.uuid4().hex
            create_attempt(shard_id, attempt_id, attempt_no)
            try:
                raw = dbutils.notebook.run(
                    notebook_path,
                    SHARD_TIMEOUT_SEC,
                    {
                        "RUN_ID": RUN_ID,
                        "SHARDS": str(SHARDS),
                        "SHARD_ID": str(shard_id),
                        "ATTEMPT_ID": attempt_id,
                        "ATTEMPT_NO": str(attempt_no),
                        "MAX_ROWS_PER_SHARD": str(MAX_ROWS_PER_SHARD),
                        "MAX_CONCURRENT_SHARDS": str(MAX_CONCURRENT_SHARDS),
                        "SHARD_TIMEOUT_SEC": str(SHARD_TIMEOUT_SEC),
                        "MAX_SHARD_RETRIES": str(MAX_SHARD_RETRIES),
                        "RETRY_BASE_SEC": str(RETRY_BASE_SEC),
                        "MAX_NONRETRYABLE_KEY_ATTEMPTS": str(MAX_NONRETRYABLE_KEY_ATTEMPTS),
                        "MAX_QUARANTINE_COUNT": str(MAX_QUARANTINE_COUNT),
                        "MAX_QUARANTINE_RATE": str(MAX_QUARANTINE_RATE),
                        "PREFLIGHT_CANARY_KEYS": str(PREFLIGHT_CANARY_KEYS),
                        "INFRA_STORM_RATE": str(INFRA_STORM_RATE),
                        "INFRA_STORM_MIN_ROWS": str(INFRA_STORM_MIN_ROWS),
                        "STATE_SCHEMA": STATE_SCHEMA,
                        "METADATA_TABLE": METADATA_TABLE,
                        "MAX_ORCHESTRATOR_SEC": str(MAX_ORCHESTRATOR_SEC),
                        "MAX_WAVES": str(MAX_WAVES),
                        "FAIL_ON_PARTIAL": "false",
                    },
                )
                payload = json.loads(raw)
                if payload.get("status") != "success":
                    raise RuntimeError(raw)
                return payload
            except Exception as exc:
                info = resolve_child_failure(attempt_id, exc)
                last_failure = {"status": "failed", "shard_id": shard_id, **info}
                if not info["retryable"] or retry_index >= MAX_SHARD_RETRIES:
                    return last_failure
                delay = retry_delay_seconds(retry_index + 1, RETRY_BASE_SEC)
                print(f"shard {shard_id}: transient failure; retrying in {delay}s")
                time.sleep(delay)
        return last_failure

    # Continuous slot-filling scheduler: keep up to MAX_CONCURRENT_SHARDS children
    # in flight, re-checking the wall-clock budget before every dispatch instead
    # of once per wave. Each shard may be dispatched at most MAX_WAVES times
    # (the same total-attempt ceiling the wave loop enforced). Pending counts are
    # maintained from the child payloads, so the expensive pending anti-join runs
    # once up front; the authoritative coverage_summary below still gates the run.
    pending = pending_counts_by_shard()
    print(f"{sum(pending.values()):,} pending keys across {len(pending)} shards")
    passes = {}
    in_flight = {}
    results = []
    stop_reason = None
    executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_SHARDS)
    try:
        while True:
            while stop_reason is None and len(in_flight) < MAX_CONCURRENT_SHARDS:
                if time.monotonic() - orchestrator_started >= MAX_ORCHESTRATOR_SEC:
                    stop_reason = "budget_exhausted"
                    break
                busy = set(in_flight.values())
                candidates = [
                    shard_id for shard_id in sorted(pending)
                    if pending.get(shard_id, 0) > 0
                    and passes.get(shard_id, 0) < MAX_WAVES
                    and shard_id not in busy
                ]
                if not candidates:
                    break
                shard_id = candidates[0]
                passes[shard_id] = passes.get(shard_id, 0) + 1
                print(
                    f"dispatch shard {shard_id} "
                    f"(pass {passes[shard_id]}/{MAX_WAVES}, "
                    f"{pending.get(shard_id, 0):,} pending keys)"
                )
                in_flight[executor.submit(run_child, shard_id)] = shard_id
            if not in_flight:
                break
            done, _ = futures_wait(set(in_flight), return_when=FIRST_COMPLETED)
            for future in done:
                shard_id = in_flight.pop(future)
                result = future.result()
                results.append(result)
                print(
                    f"shard {result['shard_id']}: {result['status']} "
                    f"selected={result.get('selected_events', 'n/a')} "
                    f"completed={result.get('completed_events', 'n/a')} "
                    f"quarantined={result.get('quarantined_events', 'n/a')}"
                )
                if result.get("status") != "success":
                    stop_reason = stop_reason or "shard_failed"
                else:
                    remaining = result.get("remaining_in_shard")
                    if remaining is None:
                        pending[shard_id] = pending_counts_by_shard().get(shard_id, 0)
                    else:
                        pending[shard_id] = int(remaining)
    finally:
        executor.shutdown(wait=True)
    if stop_reason:
        print(f"dispatch stopped: {stop_reason}")

    summary = coverage_summary(
        RUN_EVENTS, BATCH_OUTPUT, HISTORY_OUTPUT, QUARANTINE, RUN_ID
    )
    if summary["overlap"]:
        raise RuntimeError(f"Completed/quarantined key overlap: {summary}")

    failures = [r for r in results if r.get("status") != "success"]
    if summary["unresolved"] == 0:
        update_run_status(
            METADATA_TABLE, RUN_ID, "processing_complete",
            completed_events=summary["completed"],
            quarantined_events=summary["quarantined"],
            processing_completed_ts=utc_now_naive(),
            error_message=None,
        )
        has_more = False
        status = "success"
    else:
        next_status = "processing_failed" if failures else "processing_partial"
        update_run_status(
            METADATA_TABLE, RUN_ID, next_status,
            completed_events=summary["completed"],
            quarantined_events=summary["quarantined"],
            error_message=safe_json(failures)[:8000] if failures else None,
        )
        has_more = True
        status = "partial"

    result = {
        "status": status,
        "run_id": RUN_ID,
        "coverage": summary,
        "failed_shards": failures,
        "has_more": has_more,
        "waves": max(passes.values()) if passes else 0,
        "dispatches": sum(passes.values()),
        "stop_reason": stop_reason,
        "elapsed_seconds": round(time.monotonic() - orchestrator_started, 1),
    }
    dbutils.jobs.taskValues.set(key="RUN_ID", value=RUN_ID)
    dbutils.jobs.taskValues.set(
        key="HAS_MORE_PROCESSING", value="true" if has_more else "false"
    )
    if has_more and FAIL_ON_PARTIAL:
        raise RuntimeError("BLOB_PROCESSING_INCOMPLETE: " + safe_json(result))
    dbutils.notebook.exit(json.dumps(result, default=str, sort_keys=True))


# COMMAND ----------

meta = run_row()
if int(meta["shard_count"]) != SHARDS:
    raise RuntimeError("SHARDS does not match the run configuration")

if not ATTEMPT_ID:
    ATTEMPT_ID = uuid.uuid4().hex
    ATTEMPT_NO = next_attempt_no(SHARD_ID)
    create_attempt(SHARD_ID, ATTEMPT_ID, ATTEMPT_NO, status="running")
else:
    update_attempt(ATTEMPT_ID, "running")



# COMMAND ----------

def process_key_subset(keys, key_count):
    if key_count == 0:
        return {"completed": 0, "quarantined": 0, "quarantined_infra": 0, "observed": 0}

    selected_chunks = null_safe_join(
        spark.table(RUN_CHUNKS).filter(F.col("run_id") == RUN_ID),
        F.broadcast(keys.select(*BLOB_PROCESSING_KEYS)),
        BLOB_PROCESSING_KEYS,
        "inner",
    )
    grouped = selected_chunks.groupBy(*BLOB_PROCESSING_KEYS).agg(
        F.max("ADC_UPDT").alias("ADC_UPDT"),
        F.collect_list(_CHUNK_STRUCT).alias("chunks_data"),
    )
    reconstructed = int(grouped.count())
    if reconstructed != key_count:
        raise RuntimeError(f"Selected {key_count} keys but reconstructed {reconstructed} source versions")

    desired_partitions = min(80, max(1, math.ceil(key_count / 200)))
    materialized_name = hashlib.sha256(
        f"{RUN_ID}|{SHARD_ID}|{ATTEMPT_ID}".encode("utf-8")
    ).hexdigest()[:20]
    materialized_table = f"{STATE_SCHEMA}._blob2_materialized_{materialized_name}"
    try:
        raw_frame = (
            grouped.repartition(desired_partitions, "SOURCE_VERSION_ID")
            .select(
                "SOURCE_VERSION_ID",
                process_blob_udf("EVENT_ID", "ADC_UPDT", "chunks_data").alias("r"),
            )
            .select("SOURCE_VERSION_ID", "r.*")
        )
        with_blob_version_id(raw_frame).write.mode("overwrite").saveAsTable(materialized_table)
        raw = spark.table(materialized_table)

        decoded_length_mismatch = F.get_json_object(F.col("metrics"), "$.blob_length_mismatch").cast("boolean") == F.lit(True)
        infra_condition = F.coalesce(F.col("parse_status").isin(*_INFRA_PARSE_STATUSES), F.lit(False))
        fatal_condition = F.coalesce(F.col("parse_status").isin(*_FATAL_PARSE_STATUSES), F.lit(False)) | F.coalesce(decoded_length_mismatch, F.lit(False))
        counts = raw.agg(
            F.count(F.lit(1)).alias("materialized"),
            F.sum(F.when(fatal_condition, 1).otherwise(0)).alias("fatal"),
            F.sum(F.when(infra_condition, 1).otherwise(0)).alias("infra"),
        ).first()
        materialized = int(counts["materialized"] or 0)
        failure_count = int(counts["fatal"] or 0)
        infra_count = int(counts["infra"] or 0)
        if materialized != key_count:
            raise RuntimeError(f"Materialized {materialized} outputs for {key_count} selected keys")

        failures = (
            raw.filter(fatal_condition)
            .join(keys.select(*BLOB_PROCESSING_KEYS, "source_commit_version"), list(BLOB_PROCESSING_KEYS), "left")
            .select("EVENT_ID", "SOURCE_VERSION_ID", "ADC_UPDT", "source_commit_version", "STATUS", "parse_status", "metrics")
        )
        successful = raw.filter(~fatal_condition)
        successful_count = materialized - failure_count

        target_columns = [
            "EVENT_ID", "SOURCE_VERSION_ID", "BLOB_VERSION_ID",
            "VALID_UNTIL_DT_TM", "VALID_FROM_DT_TM", "UPDT_DT_TM",
            "UPDT_ID", "UPDT_TASK", "UPDT_CNT", "UPDT_APPLCTX", "LAST_UTC_TS",
            "ADC_UPDT", "BLOB_BINARY", "CONTENT_TYPE", "ENCODING", "BLOB_TEXT",
            "BINARY_SIZE", "TEXT_LENGTH", "STATUS", "anon_text", "ENCNTR_ID",
            "Trust", "raw_sha256", "decompressor_version", "parser_version",
            "post_processor_version", "metrics",
        ]
        if successful_count:
            batch = (
                successful.select(*target_columns)
                .withColumn("run_id", F.lit(RUN_ID))
                .withColumn("shard_id", F.lit(int(SHARD_ID)).cast("int"))
                .withColumn("output_ts", F.current_timestamp())
            )
            history = (
                successful.select(
                    "EVENT_ID", "SOURCE_VERSION_ID", "BLOB_VERSION_ID", "ADC_UPDT",
                    "raw_sha256", "decompressor_version", "parser_version", "post_processor_version",
                    F.col("history_status").alias("status"),
                    "truncation_flag", "truncation_reason", "ftfy_explain",
                    "decompression_strategy", "parse_status", "metrics",
                )
                .withColumn("run_id", F.lit(RUN_ID))
                .withColumn("shard_id", F.lit(int(SHARD_ID)).cast("int"))
                .withColumn("source", F.lit("forward"))
                .withColumn("output_ts", F.current_timestamp())
            )
            merge_all(
                batch, BATCH_OUTPUT,
                "t.run_id = s.run_id AND t.EVENT_ID = s.EVENT_ID "
                "AND t.SOURCE_VERSION_ID = s.SOURCE_VERSION_ID",
            )
            merge_all(
                history, HISTORY_OUTPUT,
                "t.run_id = s.run_id AND t.EVENT_ID = s.EVENT_ID "
                "AND t.SOURCE_VERSION_ID = s.SOURCE_VERSION_ID AND t.source = s.source",
            )

        if infra_count >= INFRA_STORM_MIN_ROWS and infra_count / max(1, materialized) >= INFRA_STORM_RATE:
            samples = [
                row["error"] or "unknown infra failure"
                for row in failures.filter(F.col("parse_status").isin(*_INFRA_PARSE_STATUSES))
                .select(F.get_json_object(F.col("metrics"), "$.error").alias("error"))
                .limit(3).collect()
            ]
            log_pipeline_event(
                PIPELINE_EVENTS, "ERROR", "infra_failure_storm",
                samples[0] if samples else "infra failure storm",
                pipeline_id=meta["pipeline_id"], run_id=RUN_ID, shard_id=int(SHARD_ID),
                details={"infra_count": infra_count, "materialized": materialized, "sample_errors": samples},
            )
            raise RuntimeError(
                f"INFRA_FAILURE_STORM: {infra_count}/{materialized} rows failed; samples: {' | '.join(samples)}"
            )

        failure_outcome = record_nonretryable_rows(failures, failure_count)
        return {"completed": successful_count, **failure_outcome}
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {quote_table(materialized_table)}")


# Keys that already carry an observed quarantine row are sorted behind fresh work.
_observed_keys = (
    spark.table(QUARANTINE)
    .filter((F.col("run_id") == RUN_ID) & (F.col("status") == "observed"))
    .select(*BLOB_PROCESSING_KEYS)
    .distinct()
    .withColumn("_observed", F.lit(1))
)
selected_frame = (
    null_safe_join(pending_keys_for_shard(SHARD_ID), _observed_keys, BLOB_PROCESSING_KEYS, "left")
    .withColumn("_has_observed", F.coalesce(F.col("_observed"), F.lit(0)))
    .orderBy(F.col("_has_observed").asc(), "source_commit_version", "EVENT_ID", "SOURCE_VERSION_ID")
    .limit(MAX_ROWS_PER_SHARD)
    .drop("_observed", "_has_observed")
)
selected_name = hashlib.sha256(
    f"selected|{RUN_ID}|{SHARD_ID}|{ATTEMPT_ID}".encode("utf-8")
).hexdigest()[:20]
selected_table = f"{STATE_SCHEMA}._blob2_selected_{selected_name}"
selected_frame.write.mode("overwrite").saveAsTable(selected_table)
selected = spark.table(selected_table)
selected_count = int(selected.count())

if selected_count == 0:
    spark.sql(f"DROP TABLE IF EXISTS {quote_table(selected_table)}")
    update_attempt(ATTEMPT_ID, "no_work", selected_events=0, completed_events=0, quarantined_events=0, retryable=False)
    dbutils.notebook.exit(json.dumps({
        "status": "success", "shard_id": SHARD_ID, "selected_events": 0,
        "completed_events": 0, "quarantined_events": 0,
        "quarantined_infra_events": 0, "observed_events": 0,
        "remaining_in_shard": 0,
    }, sort_keys=True))

try:
    outcome = process_key_subset(selected, selected_count)
    update_attempt(
        ATTEMPT_ID, "success", selected_events=selected_count,
        completed_events=outcome["completed"],
        quarantined_events=outcome["quarantined"] + outcome["quarantined_infra"],
        retryable=False,
    )
    total_quarantined = int(
        spark.table(QUARANTINE)
        .filter((F.col("run_id") == RUN_ID) & (F.col("status") == "quarantined"))
        .select(*BLOB_PROCESSING_KEYS).distinct().count()
    )
    total_events = int(meta["total_events"] or 0)
    if total_events <= 0:
        total_events = int(spark.table(RUN_EVENTS).filter(F.col("run_id") == RUN_ID).count())
    quarantine_rate = total_quarantined / max(1, total_events)
    if total_quarantined >= MAX_QUARANTINE_COUNT or quarantine_rate >= MAX_QUARANTINE_RATE:
        raise RuntimeError(
            f"QUARANTINE_CIRCUIT_BREAKER: {total_quarantined}/{total_events} ({quarantine_rate:.4%}) keys quarantined"
        )
    remaining = int(pending_keys_for_shard(SHARD_ID).count())
    result_payload = {
        "status": "success", "shard_id": SHARD_ID,
        "selected_events": selected_count, "completed_events": outcome["completed"],
        "quarantined_events": outcome["quarantined"],
        "quarantined_infra_events": outcome["quarantined_infra"],
        "observed_events": outcome["observed"], "remaining_in_shard": remaining,
    }
except Exception as exc:
    info = classify_exception(exc)
    update_attempt(
        ATTEMPT_ID, "failed", selected_events=selected_count,
        error_class=info["error_class"], error_message=info["error_message"],
        failure_fingerprint=info["failure_fingerprint"],
        databricks_trace_id=info["databricks_trace_id"], retryable=info["retryable"],
    )
    raise
finally:
    spark.sql(f"DROP TABLE IF EXISTS {quote_table(selected_table)}")

dbutils.notebook.exit(json.dumps(result_payload, default=str, sort_keys=True))

