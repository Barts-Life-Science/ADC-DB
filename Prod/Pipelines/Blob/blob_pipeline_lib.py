# Databricks notebook source
# Version-grain repair staged 2026-08-25; SOURCE_VERSION_ID/BLOB_VERSION_ID contract.
# Shared control-plane helpers for the Blob v4 notebooks.
# This notebook deliberately contains no production defaults or side effects.

from datetime import datetime, timezone, timedelta
import hashlib
import json
import random
import re
import time
import uuid

from delta.tables import DeltaTable
from pyspark.sql import Row, SparkSession, functions as F, types as T

# Functions loaded with %run do not reliably inherit Databricks' implicit
# notebook globals on all runtimes. Bind them explicitly once.
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
try:
    dbutils
except NameError:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)


# COMMAND ----------

_TABLE_RE = re.compile(r"^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+){1,2}$")
_SAFE_VALUE_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")

RETRYABLE_ERROR_MARKERS = (
    "internal_error",
    "unexpected error occurred",
    "executor lost",
    "executorlostfailure",
    "fetchfailed",
    "timeout",
    "timed out",
    "temporarily unavailable",
    "concurrent",
    "conflict",
    "ioexception",
    "connection reset",
    "service unavailable",
    "throttl",
    "infra_failure_storm",
)

NONRETRYABLE_ERROR_MARKERS = (
    "schema",
    "missing required columns",
    "unsafe table identifier",
    "incompatible processing configuration",
    "source_chunk_integrity",
)

# Delta optimistic-concurrency failures that are safe to retry in place when the
# write itself is idempotent (keyed MERGEs, guarded UPDATEs).
DELTA_CONFLICT_MARKERS = (
    "concurrentappendexception",
    "concurrentdeletereadexception",
    "concurrentdeletedeleteexception",
    "concurrenttransactionexception",
    "concurrentwriteexception",
    "metadatachangedexception",
    "protocolchangedexception",
    "delta_concurrent",
    "concurrent update",
    "commit conflict",
)

SAFE_NO_RELEVANT_OPERATIONS = {
    "OPTIMIZE",
    "VACUUM START",
    "VACUUM END",
    "ANALYZE",
    "DELETE",
    "SET TBLPROPERTIES",
    "UNSET TBLPROPERTIES",
}

RUN_TRANSITIONS = {
    "materializing": {"worklist_ready", "source_integrity_blocked", "cdf_gap_blocked", "failed"},
    "worklist_ready": {"processing", "processing_complete", "failed"},
    "processing": {"processing_partial", "processing_complete", "processing_failed"},
    "processing_partial": {"processing", "processing_complete", "processing_failed"},
    "processing_failed": {"processing", "processing_complete"},
    "processing_complete": {"merging"},
    "merging": {"merge_complete", "merge_failed"},
    "merge_failed": {"merging"},
    "merge_complete": {"files_processing", "complete"},
    "files_processing": {"files_partial", "complete"},
    "files_partial": {"files_processing", "complete"},
}


def validate_table_name(value: str) -> str:
    if not value or not _TABLE_RE.fullmatch(value):
        raise ValueError(f"Unsafe table identifier: {value!r}")
    return value


def quote_table(value: str) -> str:
    validate_table_name(value)
    return ".".join(f"`{part}`" for part in value.split("."))


def sql_string(value) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def safe_value(value: str, label: str = "value") -> str:
    if not value or not _SAFE_VALUE_RE.fullmatch(value):
        raise ValueError(f"Unsafe {label}: {value!r}")
    return value


def table_exists(table_name: str) -> bool:
    validate_table_name(table_name)
    try:
        _ = spark.table(table_name).schema
        return True
    except Exception as exc:
        message = str(exc).lower()
        missing_markers = (
            "table_or_view_not_found",
            "table or view not found",
            "cannot be found",
            "no such table",
        )
        if any(marker in message for marker in missing_markers):
            return False
        raise


def utc_now_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def safe_json(value) -> str:
    return json.dumps(value, default=str, sort_keys=True, separators=(",", ":"))


def failure_fingerprint(error_class: str, message: str) -> str:
    normalized = re.sub(r"\b\d+\b", "#", (message or "").lower())
    normalized = re.sub(r"\s+", " ", normalized).strip()[:2000]
    return hashlib.sha256(f"{error_class}|{normalized}".encode("utf-8")).hexdigest()


def failure_fingerprint_col(error_class_col, message_col):
    """Column-level twin of failure_fingerprint(); the two must stay in sync so
    scalar and bulk quarantine paths accumulate attempts on the same fingerprint."""
    normalized = F.lower(F.coalesce(message_col, F.lit("")))
    normalized = F.regexp_replace(normalized, r"\b\d+\b", "#")
    normalized = F.substring(F.trim(F.regexp_replace(normalized, r"\s+", " ")), 1, 2000)
    return F.sha2(
        F.concat_ws("|", F.coalesce(error_class_col, F.lit("")), normalized), 256
    )


def extract_databricks_trace_id(message: str):
    match = re.search(r"trace id:\s*([^\s]+)", message or "", re.IGNORECASE)
    return match.group(1).rstrip(".") if match else None


def classify_exception(exc) -> dict:
    error_class = type(exc).__name__
    message = str(exc)
    lowered = f"{error_class}: {message}".lower()
    retryable = any(marker in lowered for marker in RETRYABLE_ERROR_MARKERS)
    if any(marker in lowered for marker in NONRETRYABLE_ERROR_MARKERS):
        retryable = False
    return {
        "error_class": error_class,
        "error_message": message[:8000],
        "retryable": retryable,
        "failure_fingerprint": failure_fingerprint(error_class, message),
        "databricks_trace_id": extract_databricks_trace_id(message),
    }


def retry_delay_seconds(attempt_no: int, base_seconds: int = 30, cap_seconds: int = 600) -> int:
    exponent = max(0, attempt_no - 1)
    return min(cap_seconds, base_seconds * (2 ** exponent)) + random.randint(0, 15)


def is_delta_conflict_exception(exc) -> bool:
    lowered = f"{type(exc).__name__}: {exc}".lower()
    return any(marker in lowered for marker in DELTA_CONFLICT_MARKERS)


def with_delta_retry(operation, label: str = "delta write", max_retries: int = 5,
                     base_seconds: int = 10, cap_seconds: int = 300):
    """Run an idempotent Delta write, retrying in place on optimistic-concurrency
    conflicts. Non-conflict errors and exhausted retries re-raise the original
    exception (its message still matches the retryable markers, so shard-level
    retry classification is preserved)."""
    for attempt in range(max_retries + 1):
        try:
            return operation()
        except Exception as exc:
            if not is_delta_conflict_exception(exc) or attempt >= max_retries:
                raise
            delay = min(cap_seconds, base_seconds * (2 ** attempt)) + random.randint(0, 15)
            print(
                f"{label}: delta conflict "
                f"(attempt {attempt + 1}/{max_retries + 1}); retrying in {delay}s"
            )
            time.sleep(delay)


def run_sql_with_retry(sql_text: str, label: str = "sql update", max_retries: int = 5):
    return with_delta_retry(lambda: spark.sql(sql_text), label=label, max_retries=max_retries)


def align_to_table(frame, table_name):
    source_columns = set(frame.columns)
    expressions = []
    for field in spark.table(table_name).schema:
        if field.name in source_columns:
            expressions.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            expressions.append(F.lit(None).cast(field.dataType).alias(field.name))
    return frame.select(*expressions)


def merge_all(frame, table_name, condition, label=None):
    aligned = align_to_table(frame, table_name)

    def _execute():
        (
            DeltaTable.forName(spark, table_name)
            .alias("t")
            .merge(aligned.alias("s"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    with_delta_retry(_execute, label=label or f"merge into {table_name}")


def get_current_notebook_path() -> str:
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()


def assert_transition(current_status: str, next_status: str):
    if current_status == next_status:
        return
    allowed = RUN_TRANSITIONS.get(current_status, set())
    if next_status not in allowed:
        raise RuntimeError(f"Illegal run transition {current_status!r} -> {next_status!r}")


def log_pipeline_event(
    event_table: str,
    severity: str,
    event_type: str,
    message: str,
    pipeline_id=None,
    run_id=None,
    shard_id=None,
    event_id=None,
    adc_updt=None,
    details=None,
):
    if not table_exists(event_table):
        return
    schema = spark.table(event_table).schema
    payload = {
        "event_uuid": uuid.uuid4().hex,
        "event_ts": utc_now_naive(),
        "pipeline_id": pipeline_id,
        "run_id": run_id,
        "shard_id": shard_id,
        "EVENT_ID": event_id,
        "ADC_UPDT": adc_updt,
        "severity": severity,
        "event_type": event_type,
        "message": (message or "")[:4000],
        "details": safe_json(details)[:16000] if details is not None else None,
    }
    spark.createDataFrame([Row(**payload)], schema=schema).write.mode("append").insertInto(event_table)


def log_pipeline_events_bulk(event_table: str, frame):
    """Append many pipeline events in one write instead of a row-per-insert loop.

    The frame supplies pipeline_id/run_id/shard_id/EVENT_ID/ADC_UPDT/severity/
    event_type/message/details; event_uuid and event_ts are stamped here and any
    remaining table columns are null-padded by align_to_table.
    """
    if not table_exists(event_table):
        return
    enriched = (
        frame
        .withColumn("event_uuid", F.expr("uuid()"))
        .withColumn("event_ts", F.current_timestamp())
    )
    align_to_table(enriched, event_table).write.mode("append").insertInto(event_table)


_RUN_STATUS_UNSET = object()


def update_run_status(
    runs_table: str,
    run_id: str,
    next_status: str,
    error_message=_RUN_STATUS_UNSET,
    **values,
):
    rows = spark.table(runs_table).filter(F.col("run_id") == run_id).limit(2).collect()
    if len(rows) != 1:
        raise RuntimeError(f"Expected one run row for {run_id}; found {len(rows)}")
    assert_transition(rows[0]["status"], next_status)
    assignments = [f"status = {sql_string(next_status)}", "updated_ts = current_timestamp()"]
    if error_message is not _RUN_STATUS_UNSET:
        rendered_error = None if error_message is None else str(error_message)[:8000]
        assignments.append(f"error_message = {sql_string(rendered_error)}")
    for key, value in values.items():
        if isinstance(value, bool):
            rendered = "true" if value else "false"
        elif isinstance(value, (int, float)):
            rendered = str(value)
        else:
            rendered = sql_string(value)
        assignments.append(f"`{key}` = {rendered}")
    run_sql_with_retry(
        f"UPDATE {quote_table(runs_table)} SET {', '.join(assignments)} "
        f"WHERE run_id = {sql_string(run_id)}",
        label=f"update run status {run_id} -> {next_status}",
    )


def acquire_run_lease(runs_table: str, run_id: str, owner: str, lease_minutes: int = 30) -> bool:
    safe_value(owner, "lease owner")
    spark.sql(f"""
      UPDATE {quote_table(runs_table)}
      SET lease_owner = {sql_string(owner)},
          lease_expires_ts = current_timestamp() + INTERVAL {int(lease_minutes)} MINUTES,
          updated_ts = current_timestamp()
      WHERE run_id = {sql_string(run_id)}
        AND (
          lease_owner IS NULL
          OR lease_owner = {sql_string(owner)}
          OR lease_expires_ts IS NULL
          OR lease_expires_ts < current_timestamp()
        )
    """)
    rows = (
        spark.table(runs_table)
        .filter((F.col("run_id") == run_id) & (F.col("lease_owner") == owner))
        .limit(1)
        .count()
    )
    return rows == 1


def release_run_lease(runs_table: str, run_id: str, owner: str):
    spark.sql(f"""
      UPDATE {quote_table(runs_table)}
      SET lease_owner = NULL, lease_expires_ts = NULL, updated_ts = current_timestamp()
      WHERE run_id = {sql_string(run_id)} AND lease_owner = {sql_string(owner)}
    """)


def history_relevance(operation: str, operation_metrics) -> tuple:
    operation = (operation or "").upper().strip()
    metrics = operation_metrics or {}
    if isinstance(metrics, str):
        try:
            metrics = json.loads(metrics)
        except Exception:
            metrics = {}

    def metric_int(*names):
        for name in names:
            value = metrics.get(name)
            if value not in (None, ""):
                try:
                    return int(value)
                except Exception:
                    pass
        return None

    if operation in SAFE_NO_RELEVANT_OPERATIONS:
        return ("irrelevant_change_types_only", operation.lower().replace(" ", "_"))

    if operation == "MERGE":
        inserted = metric_int("numTargetRowsInserted")
        updated = metric_int("numTargetRowsUpdated", "numTargetRowsMatchedUpdated")
        if (inserted or 0) > 0 or (updated or 0) > 0:
            return ("potential_relevant_changes", "merge_insert_or_update")
        deleted = metric_int("numTargetRowsDeleted", "numTargetRowsMatchedDeleted")
        if inserted == 0 and updated == 0 and deleted is not None:
            return ("irrelevant_change_types_only", "delete_only_merge")
        return ("unknown", "merge_metrics_inconclusive")

    if operation == "UPDATE":
        updated = metric_int("numUpdatedRows", "numTargetRowsUpdated")
        if updated == 0:
            return ("irrelevant_change_types_only", "zero_row_update")
        return ("potential_relevant_changes", "update")

    if operation in {"WRITE", "STREAMING UPDATE", "COPY INTO", "CREATE OR REPLACE TABLE AS SELECT"}:
        output = metric_int("numOutputRows", "numTargetRowsInserted")
        if output == 0:
            return ("irrelevant_change_types_only", "zero_output_write")
        return ("potential_relevant_changes", operation.lower().replace(" ", "_"))

    return ("unknown", operation.lower().replace(" ", "_") or "unknown_operation")


def with_inbox_key(frame):
    key_parts = [
        F.coalesce(F.col("pipeline_id"), F.lit("")),
        F.coalesce(F.col("source_table"), F.lit("")),
        F.coalesce(F.col("commit_version").cast("string"), F.lit("")),
        F.coalesce(F.col("EVENT_ID").cast("string"), F.lit("")),
        F.coalesce(F.col("ADC_UPDT").cast("string"), F.lit("")),
        F.coalesce(F.col("BLOB_SEQ_NUM").cast("string"), F.lit("")),
        F.coalesce(F.col("change_type"), F.lit("")),
    ]
    return frame.withColumn("inbox_key", F.sha2(F.concat_ws("||", *key_parts), 256))


def null_safe_join(left, right, keys, how="inner"):
    """Join on equality keys while treating NULL as a real key value.

    The result keeps all left columns and only non-key, non-duplicate columns from
    the right frame. Supported call sites use inner/left/left-semi/left-anti joins.
    """
    keys = list(keys)
    if not keys:
        raise ValueError("null_safe_join requires at least one key")

    left_alias = "_ns_left"
    right_alias = "_ns_right"
    condition = None
    for key in keys:
        clause = F.col(f"{left_alias}.`{key}`").eqNullSafe(
            F.col(f"{right_alias}.`{key}`")
        )
        condition = clause if condition is None else condition & clause

    joined = left.alias(left_alias).join(right.alias(right_alias), condition, how)
    normalized_how = how.lower().replace("_", "").replace(" ", "")
    left_columns = [
        F.col(f"{left_alias}.`{column}`").alias(column)
        for column in left.columns
    ]
    if normalized_how in {"leftanti", "leftsemi"}:
        return joined.select(*left_columns)

    right_columns = [
        F.col(f"{right_alias}.`{column}`").alias(column)
        for column in right.columns
        if column not in keys and column not in left.columns
    ]
    return joined.select(*left_columns, *right_columns)


def chunk_integrity(frame):
    # BLOB_CONTENTS is the compressed/wrapped representation while BLOB_LENGTH
    # describes decoded bytes. Comparing their raw lengths would reject nearly
    # every compressed production blob. Exact decoded length is enforced in Blob 2.
    grouped = (
        frame.groupBy("EVENT_ID", "ADC_UPDT")
        .agg(
            F.count("*").cast("long").alias("chunk_rows"),
            F.countDistinct("BLOB_SEQ_NUM").cast("long").alias("distinct_chunk_numbers"),
            F.min("BLOB_SEQ_NUM").cast("long").alias("min_chunk_number"),
            F.max("BLOB_SEQ_NUM").cast("long").alias("max_chunk_number"),
            F.sum(F.length("BLOB_CONTENTS")).cast("long").alias("assembled_bytes"),
            F.sum(F.when(F.col("BLOB_CONTENTS").isNull(), 1).otherwise(0))
             .cast("long").alias("null_chunk_contents"),
            F.min("BLOB_LENGTH").cast("long").alias("min_blob_length"),
            F.max("BLOB_LENGTH").cast("long").alias("max_blob_length"),
            F.min("COMPRESSION_CD").cast("long").alias("min_compression_cd"),
            F.max("COMPRESSION_CD").cast("long").alias("max_compression_cd"),
        )
        .withColumn(
            "sequence_span",
            F.col("max_chunk_number") - F.col("min_chunk_number") + F.lit(1),
        )
        .withColumn(
            "chunk_integrity_status",
            F.when(F.col("min_chunk_number").isNull(), F.lit("invalid"))
            .when(F.col("chunk_rows") != F.col("distinct_chunk_numbers"), F.lit("invalid"))
            .when(F.col("distinct_chunk_numbers") != F.col("sequence_span"), F.lit("invalid"))
            .when(F.col("null_chunk_contents") != 0, F.lit("invalid"))
            .when(F.col("min_blob_length").isNull(), F.lit("invalid"))
            .when(F.col("min_blob_length") != F.col("max_blob_length"), F.lit("invalid"))
            .when(F.col("min_compression_cd").isNull(), F.lit("invalid"))
            .when(F.col("min_compression_cd") != F.col("max_compression_cd"), F.lit("invalid"))
            .otherwise(F.lit("valid")),
        )
        .withColumn(
            "chunk_integrity_reason",
            F.when(F.col("min_chunk_number").isNull(), F.lit("missing_chunk_number"))
            .when(F.col("chunk_rows") != F.col("distinct_chunk_numbers"), F.lit("duplicate_chunk_number"))
            .when(F.col("distinct_chunk_numbers") != F.col("sequence_span"), F.lit("non_contiguous_chunk_numbers"))
            .when(F.col("null_chunk_contents") != 0, F.lit("missing_chunk_contents"))
            .when(F.col("min_blob_length").isNull(), F.lit("missing_blob_length"))
            .when(F.col("min_blob_length") != F.col("max_blob_length"), F.lit("inconsistent_blob_length"))
            .when(F.col("min_compression_cd").isNull(), F.lit("missing_compression_code"))
            .when(F.col("min_compression_cd") != F.col("max_compression_cd"), F.lit("inconsistent_compression_code"))
            .otherwise(F.lit(None).cast("string")),
        )
    )
    return grouped


def completed_output_keys(batch_table: str, history_table: str, run_id: str):
    batch = (
        spark.table(batch_table)
        .filter(F.col("run_id") == run_id)
        .select("EVENT_ID", "ADC_UPDT")
        .distinct()
    )
    history = (
        spark.table(history_table)
        .filter(F.col("run_id") == run_id)
        .select("EVENT_ID", "ADC_UPDT")
        .distinct()
    )
    return null_safe_join(
        batch, history, ["EVENT_ID", "ADC_UPDT"], "inner"
    ).distinct()


def quarantined_keys(quarantine_table: str, run_id: str):
    # quarantined_infra is terminal for run accounting (the run can complete) but
    # is excluded from the quarantine circuit breaker, which counts only
    # status == 'quarantined'.
    return (
        spark.table(quarantine_table)
        .filter(
            (F.col("run_id") == run_id)
            & F.col("status").isin("quarantined", "waived", "quarantined_infra")
        )
        .select("EVENT_ID", "ADC_UPDT")
        .distinct()
    )


def coverage_summary(
    run_events_table: str,
    batch_table: str,
    history_table: str,
    quarantine_table: str,
    run_id: str,
):
    work = (
        spark.table(run_events_table)
        .filter(F.col("run_id") == run_id)
        .select("EVENT_ID", "ADC_UPDT")
        .distinct()
    )
    completed = completed_output_keys(batch_table, history_table, run_id)
    quarantined = quarantined_keys(quarantine_table, run_id)
    overlap = null_safe_join(
        completed, quarantined, ["EVENT_ID", "ADC_UPDT"], "inner"
    ).count()
    accounted = completed.unionByName(quarantined).distinct()
    unresolved = null_safe_join(
        work, accounted, ["EVENT_ID", "ADC_UPDT"], "left_anti"
    )
    return {
        "work": int(work.count()),
        "completed": int(completed.count()),
        "quarantined": int(quarantined.count()),
        "overlap": int(overlap),
        "unresolved": int(unresolved.count()),
    }


print("blob_pipeline_lib_v4 loaded (efficiency update)")
# Version-grain repair 2026-08-25. These definitions deliberately override the
# v4 EVENT_ID+ADC_UPDT helpers above while retaining the rest of the library.
spark.conf.set("spark.sql.session.timeZone", "UTC")
BLOB_SOURCE_VERSION_COLUMNS = (
    "EVENT_ID", "VALID_FROM_DT_TM", "UPDT_DT_TM", "UPDT_CNT",
)
BLOB_PROCESSING_KEYS = ("EVENT_ID", "SOURCE_VERSION_ID")


# RDE_REPAIR_20260830_CANONICAL_SOURCE_VERSION_ID
# Canonicalise numeric source keys before hashing: raw stores these as DOUBLE,
# while cdf_inbox stores them as BIGINT. Direct string casts do not agree.
def blob_source_version_id_expr():
    return F.sha2(
        F.concat_ws(
            "\x1e",
            F.lit("mill_ce_blob_source_v1"),
            F.coalesce(F.col("EVENT_ID").cast("long").cast("string"), F.lit("<NULL>")),
            F.coalesce(F.col("VALID_FROM_DT_TM").cast("string"), F.lit("<NULL>")),
            F.coalesce(F.col("UPDT_DT_TM").cast("string"), F.lit("<NULL>")),
            F.coalesce(F.col("UPDT_CNT").cast("long").cast("string"), F.lit("<NULL>")),
        ),
        256,
    )


def with_blob_source_version_id(frame):
    missing = sorted(set(BLOB_SOURCE_VERSION_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f"Cannot derive SOURCE_VERSION_ID; missing columns: {missing}")
    return frame.withColumn("SOURCE_VERSION_ID", blob_source_version_id_expr())


def blob_text_sha256_expr():
    return F.sha2(
        F.to_json(F.struct(
            F.col("BLOB_TEXT").isNull().alias("is_null"),
            F.col("BLOB_TEXT").alias("value"),
        )),
        256,
    )


def blob_version_id_expr():
    content_identity = F.when(
        F.col("raw_sha256").isNotNull() & (F.trim(F.col("raw_sha256")) != ""),
        F.concat(F.lit("raw:"), F.col("raw_sha256")),
    ).otherwise(F.concat(F.lit("text:"), blob_text_sha256_expr()))
    return F.sha2(
        F.concat_ws("\x1e", F.lit("mill_blob_text_v1"), F.col("SOURCE_VERSION_ID"), content_identity),
        256,
    )


def with_blob_version_id(frame):
    source = frame if "SOURCE_VERSION_ID" in frame.columns else with_blob_source_version_id(frame)
    return source.withColumn("BLOB_VERSION_ID", blob_version_id_expr())


def chunk_integrity(frame):
    grouped = (
        frame.groupBy(*BLOB_PROCESSING_KEYS)
        .agg(
            F.max("ADC_UPDT").alias("ADC_UPDT"),
            F.count("*").cast("long").alias("chunk_rows"),
            F.countDistinct("BLOB_SEQ_NUM").cast("long").alias("distinct_chunk_numbers"),
            F.min("BLOB_SEQ_NUM").cast("long").alias("min_chunk_number"),
            F.max("BLOB_SEQ_NUM").cast("long").alias("max_chunk_number"),
            F.sum(F.length("BLOB_CONTENTS")).cast("long").alias("assembled_bytes"),
            F.sum(F.when(F.col("BLOB_CONTENTS").isNull(), 1).otherwise(0)).cast("long").alias("null_chunk_contents"),
            F.min("BLOB_LENGTH").cast("long").alias("min_blob_length"),
            F.max("BLOB_LENGTH").cast("long").alias("max_blob_length"),
            F.min("COMPRESSION_CD").cast("long").alias("min_compression_cd"),
            F.max("COMPRESSION_CD").cast("long").alias("max_compression_cd"),
        )
        .withColumn("sequence_span", F.col("max_chunk_number") - F.col("min_chunk_number") + F.lit(1))
        .withColumn(
            "chunk_integrity_status",
            F.when(F.col("min_chunk_number").isNull(), F.lit("invalid"))
            .when(F.col("chunk_rows") != F.col("distinct_chunk_numbers"), F.lit("invalid"))
            .when(F.col("distinct_chunk_numbers") != F.col("sequence_span"), F.lit("invalid"))
            .when(F.col("null_chunk_contents") != 0, F.lit("invalid"))
            .when(F.col("min_blob_length").isNull(), F.lit("invalid"))
            .when(F.col("min_blob_length") != F.col("max_blob_length"), F.lit("invalid"))
            .when(F.col("min_compression_cd").isNull(), F.lit("invalid"))
            .when(F.col("min_compression_cd") != F.col("max_compression_cd"), F.lit("invalid"))
            .otherwise(F.lit("valid")),
        )
        .withColumn(
            "chunk_integrity_reason",
            F.when(F.col("min_chunk_number").isNull(), F.lit("missing_chunk_number"))
            .when(F.col("chunk_rows") != F.col("distinct_chunk_numbers"), F.lit("duplicate_chunk_number"))
            .when(F.col("distinct_chunk_numbers") != F.col("sequence_span"), F.lit("non_contiguous_chunk_numbers"))
            .when(F.col("null_chunk_contents") != 0, F.lit("missing_chunk_contents"))
            .when(F.col("min_blob_length").isNull(), F.lit("missing_blob_length"))
            .when(F.col("min_blob_length") != F.col("max_blob_length"), F.lit("inconsistent_blob_length"))
            .when(F.col("min_compression_cd").isNull(), F.lit("missing_compression_code"))
            .when(F.col("min_compression_cd") != F.col("max_compression_cd"), F.lit("inconsistent_compression_code"))
            .otherwise(F.lit(None).cast("string")),
        )
    )
    return grouped


def completed_output_keys(batch_table: str, history_table: str, run_id: str):
    batch = spark.table(batch_table).filter(F.col("run_id") == run_id).select(*BLOB_PROCESSING_KEYS).distinct()
    history = spark.table(history_table).filter(F.col("run_id") == run_id).select(*BLOB_PROCESSING_KEYS).distinct()
    return null_safe_join(batch, history, BLOB_PROCESSING_KEYS, "inner").distinct()


def quarantined_keys(quarantine_table: str, run_id: str):
    return (
        spark.table(quarantine_table)
        .filter((F.col("run_id") == run_id) & F.col("status").isin("quarantined", "waived", "quarantined_infra"))
        .select(*BLOB_PROCESSING_KEYS)
        .distinct()
    )


def coverage_summary(run_events_table, batch_table, history_table, quarantine_table, run_id):
    work = spark.table(run_events_table).filter(F.col("run_id") == run_id).select(*BLOB_PROCESSING_KEYS).distinct()
    completed = completed_output_keys(batch_table, history_table, run_id)
    quarantined = quarantined_keys(quarantine_table, run_id)
    overlap = null_safe_join(completed, quarantined, BLOB_PROCESSING_KEYS, "inner").count()
    accounted = completed.unionByName(quarantined).distinct()
    unresolved = null_safe_join(work, accounted, BLOB_PROCESSING_KEYS, "left_anti")
    return {
        "work": int(work.count()),
        "completed": int(completed.count()),
        "quarantined": int(quarantined.count()),
        "overlap": int(overlap),
        "unresolved": int(unresolved.count()),
    }

print("blob_pipeline_lib_v5 version-grain overrides loaded")

