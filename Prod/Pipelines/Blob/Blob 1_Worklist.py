# Databricks notebook source
# MAGIC %run ./blob_pipeline_lib

# COMMAND ----------

# Version-grain repair staged 2026-08-25; no EVENT_ID+ADC_UPDT assembly.

# COMMAND ----------

from pyspark.sql.window import Window

dbutils.widgets.text("RUN_ID", "")
dbutils.widgets.text("PIPELINE_ID", "mill_blob_text_barts_v4")
dbutils.widgets.text("SOURCE_TABLE", "4_prod.raw.mill_ce_blob")
dbutils.widgets.text("CHUNK_SOURCE_TABLE", "4_prod.raw.mill_ce_blob")
dbutils.widgets.text("TARGET_TABLE", "4_prod.bronze.mill_blob_text")
dbutils.widgets.text("STAGING_DB", "4_prod.tmp")  # deprecated, unused
dbutils.widgets.text("STATE_SCHEMA", "4_prod.tmp")
dbutils.widgets.text("METADATA_TABLE", "4_prod.tmp.pipeline_runs")
dbutils.widgets.text("CHECKPOINT_TABLE", "4_prod.tmp.pipeline_checkpoint")
dbutils.widgets.text("TRUST_FILTER", "Barts")
dbutils.widgets.text("MAX_EVENTS", "250000")
dbutils.widgets.text("MAX_INGEST_VERSIONS", "500")
dbutils.widgets.text("SHARDS", "16")
dbutils.widgets.text("INITIAL_START_VERSION", "")
dbutils.widgets.text("MAX_QUARANTINE_COUNT", "1000")
dbutils.widgets.text("MAX_QUARANTINE_RATE", "0.002")
dbutils.widgets.dropdown("GAP_POLICY", "classify", ["classify", "fail"])
dbutils.widgets.dropdown("OPTIMIZE_WORKLIST", "false", ["false", "true"])

# COMMAND ----------



REQUESTED_RUN_ID = dbutils.widgets.get("RUN_ID").strip()
PIPELINE_ID = dbutils.widgets.get("PIPELINE_ID").strip()
SOURCE_TABLE = dbutils.widgets.get("SOURCE_TABLE").strip()
CHUNK_SOURCE_TABLE = dbutils.widgets.get("CHUNK_SOURCE_TABLE").strip()
TARGET_TABLE = dbutils.widgets.get("TARGET_TABLE").strip()
STATE_SCHEMA = dbutils.widgets.get("STATE_SCHEMA").strip()
METADATA_TABLE = dbutils.widgets.get("METADATA_TABLE").strip()
CHECKPOINT_TABLE = dbutils.widgets.get("CHECKPOINT_TABLE").strip()
TRUST_FILTER = dbutils.widgets.get("TRUST_FILTER").strip()
MAX_EVENTS = int(dbutils.widgets.get("MAX_EVENTS") or "0")
MAX_INGEST_VERSIONS = int(dbutils.widgets.get("MAX_INGEST_VERSIONS") or "500")
SHARDS = int(dbutils.widgets.get("SHARDS") or "16")
INITIAL_START_VERSION_RAW = dbutils.widgets.get("INITIAL_START_VERSION").strip()
MAX_QUARANTINE_COUNT = int(dbutils.widgets.get("MAX_QUARANTINE_COUNT") or "1000")
MAX_QUARANTINE_RATE = float(dbutils.widgets.get("MAX_QUARANTINE_RATE") or "0.002")
GAP_POLICY = dbutils.widgets.get("GAP_POLICY").strip().lower()
OPTIMIZE_WORKLIST = dbutils.widgets.get("OPTIMIZE_WORKLIST").lower() == "true"

for table_name in (
    SOURCE_TABLE, CHUNK_SOURCE_TABLE, TARGET_TABLE, STATE_SCHEMA,
    METADATA_TABLE, CHECKPOINT_TABLE,
):
    validate_table_name(table_name)
safe_value(PIPELINE_ID, "pipeline id")
if REQUESTED_RUN_ID:
    safe_value(REQUESTED_RUN_ID, "run id")
if not TRUST_FILTER:
    raise ValueError("TRUST_FILTER cannot be empty")
if not 1 <= SHARDS <= 128:
    raise ValueError("SHARDS must be between 1 and 128")
if MAX_INGEST_VERSIONS < 1:
    raise ValueError("MAX_INGEST_VERSIONS must be positive")

SOURCE_COMMITS = f"{STATE_SCHEMA}.source_commits"
CDF_INBOX = f"{STATE_SCHEMA}.cdf_inbox"
RUNS = f"{STATE_SCHEMA}.pipeline_runs"
RUN_EVENTS = f"{STATE_SCHEMA}.run_events"
RUN_CHUNKS = f"{STATE_SCHEMA}.run_chunks"
QUARANTINE = f"{STATE_SCHEMA}.quarantine"
PIPELINE_EVENTS = f"{STATE_SCHEMA}.pipeline_events"
for required in (
    SOURCE_COMMITS, CDF_INBOX, RUNS, RUN_EVENTS, RUN_CHUNKS,
    QUARANTINE, PIPELINE_EVENTS, CHECKPOINT_TABLE,
):
    if not table_exists(required):
        raise RuntimeError(f"Missing v4 state table {required}; run Blob v4 Bootstrap")


# COMMAND ----------

# align_to_table / merge_all now come from blob_pipeline_lib; the shared merge_all
# retries through Delta optimistic-concurrency conflicts.

def checkpoint_row():
    rows = (
        spark.table(CHECKPOINT_TABLE)
        .filter(
            (F.col("pipeline_id") == PIPELINE_ID)
            & (F.col("source_table") == SOURCE_TABLE)
            & (F.col("trust_filter") == TRUST_FILTER)
        )
        .limit(2)
        .collect()
    )
    if len(rows) != 1:
        raise RuntimeError(
            f"Expected one checkpoint for {PIPELINE_ID}/{SOURCE_TABLE}/{TRUST_FILTER}; "
            f"found {len(rows)}. Seed it with Blob v4 Bootstrap."
        )
    return rows[0]


def update_checkpoint(ingested_version=None, merged_version=None, ingest_run_id=None, merge_run_id=None):
    assignments = ["updated_ts = current_timestamp()"]
    if ingested_version is not None:
        assignments.append(
            "cdf_ingested_version = greatest("
            f"coalesce(cdf_ingested_version, -1), {int(ingested_version)})"
        )
    if merged_version is not None:
        assignments.append(
            "target_merged_version = greatest("
            f"coalesce(target_merged_version, -1), {int(merged_version)})"
        )
    if ingest_run_id is not None:
        assignments.append(f"last_ingest_run_id = {sql_string(ingest_run_id)}")
    if merge_run_id is not None:
        assignments.append(f"last_merge_run_id = {sql_string(merge_run_id)}")
    run_sql_with_retry(f"""
      UPDATE {quote_table(CHECKPOINT_TABLE)}
      SET {", ".join(assignments)}
      WHERE pipeline_id = {sql_string(PIPELINE_ID)}
        AND source_table = {sql_string(SOURCE_TABLE)}
        AND trust_filter = {sql_string(TRUST_FILTER)}
    """, label="update checkpoint")


def source_history():
    return spark.sql(f"DESCRIBE HISTORY {quote_table(SOURCE_TABLE)}").select(
        F.col("version").cast("long").alias("version"),
        F.col("timestamp").cast("timestamp").alias("timestamp"),
        F.col("operation").cast("string").alias("operation"),
        "operationMetrics",
    )


def read_cdf_range(start_version, end_version):
    frame = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", int(start_version))
        .option("endingVersion", int(end_version))
        .table(SOURCE_TABLE)
    )
    _ = frame.schema
    return frame


def read_cdf_resilient(start_version, end_version):
    try:
        return read_cdf_range(start_version, end_version)
    except Exception:
        if start_version >= end_version:
            raise
        midpoint = (start_version + end_version) // 2
        left = read_cdf_resilient(start_version, midpoint)
        right = read_cdf_resilient(midpoint + 1, end_version)
        return left.unionByName(right, allowMissingColumns=True)


def version_is_readable(version):
    try:
        _ = read_cdf_range(version, version).schema
        return True
    except Exception as exc:
        if "DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION" in str(exc):
            return False
        raise


def earliest_readable_version(start_version, end_version):
    if version_is_readable(start_version):
        return start_version
    low, high = start_version + 1, end_version
    answer = None
    while low <= high:
        midpoint = (low + high) // 2
        if version_is_readable(midpoint):
            answer = midpoint
            high = midpoint - 1
        else:
            low = midpoint + 1
    return answer


def commit_rows_from_history(history_rows, counts_by_version, status, gap_disposition=None):
    schema = spark.table(SOURCE_COMMITS).schema
    payloads = []
    for row in history_rows:
        metrics = row["operationMetrics"]
        metrics_dict = metrics.asDict(recursive=True) if hasattr(metrics, "asDict") else (metrics or {})
        relevance, reason = history_relevance(row["operation"], metrics_dict)
        count = counts_by_version.get(int(row["version"]))
        if count is not None:
            relevance = "relevant_rows_archived" if count > 0 else "irrelevant_change_types_only"
            reason = "cdf_relevant_row_count"
        payloads.append(Row(
            pipeline_id=PIPELINE_ID,
            source_table=SOURCE_TABLE,
            version=int(row["version"]),
            commit_timestamp=row["timestamp"],
            operation=row["operation"],
            relevant_change_rows=int(count or 0),
            relevance_class=relevance,
            gap_disposition=gap_disposition or reason,
            ingest_status=status,
            assigned_run_id=None,
            details=safe_json({"operationMetrics": metrics_dict, "classification_reason": reason}),
            ingested_ts=utc_now_naive() if status in {"ingested", "gap_skipped"} else None,
            merged_ts=None,
        ))
    if payloads:
        frame = spark.createDataFrame(payloads, schema=schema)
        merge_all(
            frame,
            SOURCE_COMMITS,
            "t.pipeline_id = s.pipeline_id AND t.source_table = s.source_table "
            "AND t.version = s.version",
        )


def classify_and_record_gap(history, gap_start, gap_end):
    expected_versions = set(range(int(gap_start), int(gap_end) + 1))
    rows = (
        history.filter(
            (F.col("version") >= int(gap_start)) & (F.col("version") <= int(gap_end))
        )
        .orderBy("version")
        .collect()
    )
    found_versions = {int(row["version"]) for row in rows}
    blockers = []
    for row in rows:
        metrics = row["operationMetrics"]
        metrics_dict = metrics.asDict(recursive=True) if hasattr(metrics, "asDict") else (metrics or {})
        relevance, reason = history_relevance(row["operation"], metrics_dict)
        if relevance != "irrelevant_change_types_only":
            blockers.append({
                "version": int(row["version"]),
                "operation": row["operation"],
                "relevance": relevance,
                "reason": reason,
            })
    if found_versions != expected_versions:
        blockers.append({
            "missing_history_versions": sorted(expected_versions - found_versions),
            "relevance": "unknown",
        })

    if GAP_POLICY == "fail" or blockers:
        commit_rows_from_history(rows, {}, "gap_blocked", "unspooled_relevant_or_unknown")
        details = {
            "gap_start": gap_start,
            "gap_end": gap_end,
            "blockers": blockers,
        }
        log_pipeline_event(
            PIPELINE_EVENTS, "ERROR", "cdf_gap_blocked",
            "CDF gap contains unspooled insert/update candidates or unknown versions",
            pipeline_id=PIPELINE_ID, details=details,
        )
        raise RuntimeError("CDF_GAP_BLOCKED: " + safe_json(details))

    commit_rows_from_history(rows, {}, "gap_skipped", "irrelevant_change_types_only")
    log_pipeline_event(
        PIPELINE_EVENTS, "WARN", "cdf_gap_auto_advanced",
        f"Auto-advanced CDF gap {gap_start}..{gap_end}; only delete/maintenance changes found",
        pipeline_id=PIPELINE_ID,
        details={"gap_start": gap_start, "gap_end": gap_end},
    )


# COMMAND ----------

history = source_history()
latest_row = history.agg(F.max("version").alias("latest")).collect()[0]
if latest_row["latest"] is None:
    raise RuntimeError(f"No Delta history found for {SOURCE_TABLE}")
latest_version = int(latest_row["latest"])

checkpoint = checkpoint_row()
if checkpoint["cdf_ingested_version"] is None:
    if not INITIAL_START_VERSION_RAW:
        raise RuntimeError(
            "Checkpoint is unseeded. Supply INITIAL_START_VERSION or seed "
            "SEED_CDF_INGESTED_VERSION with Blob v4 Bootstrap."
        )
    current_ingested = int(INITIAL_START_VERSION_RAW) - 1
else:
    current_ingested = int(checkpoint["cdf_ingested_version"])

ingest_start = current_ingested + 1
ingest_end = min(latest_version, ingest_start + MAX_INGEST_VERSIONS - 1)

if ingest_start <= ingest_end:
    readable_start = earliest_readable_version(ingest_start, ingest_end)
    if readable_start is None:
        classify_and_record_gap(history, ingest_start, ingest_end)
        update_checkpoint(ingested_version=ingest_end)
        current_ingested = ingest_end
    else:
        if readable_start > ingest_start:
            classify_and_record_gap(history, ingest_start, readable_start - 1)
            update_checkpoint(ingested_version=readable_start - 1)
            current_ingested = readable_start - 1

        cdf = read_cdf_resilient(readable_start, ingest_end)
        required = {
            "EVENT_ID", "BLOB_SEQ_NUM", "VALID_UNTIL_DT_TM", "VALID_FROM_DT_TM",
            "UPDT_DT_TM", "UPDT_ID", "UPDT_TASK", "UPDT_CNT", "UPDT_APPLCTX",
            "LAST_UTC_TS", "ADC_UPDT", "COMPRESSION_CD", "BLOB_CONTENTS",
            "BLOB_LENGTH", "ENCNTR_ID", "Trust", "_change_type",
            "_commit_version", "_commit_timestamp",
        }
        missing = sorted(required - set(cdf.columns))
        if missing:
            raise RuntimeError(f"CDF is missing required columns: {missing}")

        relevant = (
            cdf.filter(F.col("_change_type").isin("insert", "update_postimage"))
            .filter(F.col("Trust") == TRUST_FILTER)
            .select(
                F.lit(PIPELINE_ID).alias("pipeline_id"),
                F.lit(SOURCE_TABLE).alias("source_table"),
                F.lit(TRUST_FILTER).alias("trust_filter"),
                F.col("EVENT_ID").cast("long").alias("EVENT_ID"),
                F.col("BLOB_SEQ_NUM").cast("long").alias("BLOB_SEQ_NUM"),
                "VALID_UNTIL_DT_TM", "VALID_FROM_DT_TM", "UPDT_DT_TM",
                F.col("UPDT_ID").cast("long").alias("UPDT_ID"),
                F.col("UPDT_TASK").cast("long").alias("UPDT_TASK"),
                F.col("UPDT_CNT").cast("long").alias("UPDT_CNT"),
                F.col("UPDT_APPLCTX").cast("long").alias("UPDT_APPLCTX"),
                "LAST_UTC_TS", "ADC_UPDT",
                F.col("COMPRESSION_CD").cast("long").alias("COMPRESSION_CD"),
                "BLOB_CONTENTS",
                F.col("BLOB_LENGTH").cast("long").alias("BLOB_LENGTH"),
                F.col("ENCNTR_ID").cast("long").alias("ENCNTR_ID"),
                "Trust",
                F.col("_change_type").alias("change_type"),
                F.col("_commit_version").cast("long").alias("commit_version"),
                F.col("_commit_timestamp").alias("commit_timestamp"),
                F.current_timestamp().alias("ingested_ts"),
            )
        )
        relevant = with_inbox_key(relevant)
        merge_all(relevant, CDF_INBOX, "t.inbox_key = s.inbox_key")

        counts = {
            int(row["commit_version"]): int(row["count"])
            for row in relevant.groupBy("commit_version").count().collect()
        }
        history_rows = (
            history.filter(
                (F.col("version") >= readable_start) & (F.col("version") <= ingest_end)
            )
            .orderBy("version")
            .collect()
        )
        expected = set(range(readable_start, ingest_end + 1))
        found = {int(row["version"]) for row in history_rows}
        if found != expected:
            raise RuntimeError(
                "History is missing versions inside the readable ingest range: "
                + str(sorted(expected - found))
            )
        commit_rows_from_history(history_rows, counts, "ingested")
        update_checkpoint(ingested_version=ingest_end)
        current_ingested = ingest_end
        print(f"Spool complete: versions {readable_start}..{ingest_end}, relevant rows={sum(counts.values()):,}")
else:
    print(f"CDF inbox already current at source version {latest_version}")


# COMMAND ----------

ACTIVE_STATUSES = {
    "materializing", "worklist_ready", "processing", "processing_partial",
    "processing_failed", "processing_complete", "merging", "merge_failed",
    "merge_complete", "files_processing", "files_partial",
    "source_integrity_blocked",
}

def active_run():
    frame = spark.table(RUNS).filter(
        (F.col("pipeline_id") == PIPELINE_ID)
        & (F.col("source_table") == SOURCE_TABLE)
        & (F.col("trust_filter") == TRUST_FILTER)
        & F.col("status").isin(*sorted(ACTIVE_STATUSES))
    )
    if REQUESTED_RUN_ID:
        frame = frame.filter(F.col("run_id") == REQUESTED_RUN_ID)
    rows = frame.orderBy(F.col("created_ts").asc()).limit(2).collect()
    if len(rows) > 1:
        raise RuntimeError(f"Multiple active runs found for {PIPELINE_ID}: {[r['run_id'] for r in rows]}")
    return rows[0] if rows else None


def quarantine_integrity_failures(run_row, integrity):
    invalid = integrity.filter(F.col("chunk_integrity_status") != "valid")
    invalid_count = int(invalid.count())
    total_count = int(integrity.count())
    if invalid_count == 0:
        return 0

    payload = invalid.select(
        F.lit(run_row["run_id"]).alias("run_id"),
        F.pmod(F.xxhash64("SOURCE_VERSION_ID"), F.lit(int(run_row["shard_count"]))).cast("int").alias("shard_id"),
        F.col("EVENT_ID").cast("long").alias("EVENT_ID"),
        "SOURCE_VERSION_ID",
        "ADC_UPDT",
        F.lit(int(run_row["source_end_version"])).cast("long").alias("source_commit_version"),
        F.lit("source_chunk_integrity").alias("reason_code"),
        F.lit("ChunkIntegrityError").alias("error_class"),
        F.col("chunk_integrity_reason").alias("error_message"),
        F.sha2(F.concat_ws("||", F.lit("source_chunk_integrity"), F.col("chunk_integrity_reason")), 256).alias("failure_fingerprint"),
        F.lit(1).cast("int").alias("nonretryable_attempts"),
        F.concat_ws(":", F.lit(run_row["run_id"]), F.col("EVENT_ID").cast("string"), F.col("SOURCE_VERSION_ID")).alias("raw_reference"),
        F.lit("quarantined").alias("status"),
        F.current_timestamp().alias("first_failure_ts"),
        F.current_timestamp().alias("last_failure_ts"),
        F.lit(None).cast("string").alias("resolution"),
        F.lit(None).cast("string").alias("reviewed_by"),
        F.lit(None).cast("timestamp").alias("reviewed_ts"),
    )
    merge_all(
        payload,
        QUARANTINE,
        "t.run_id = s.run_id AND t.EVENT_ID = s.EVENT_ID "
        "AND t.SOURCE_VERSION_ID = s.SOURCE_VERSION_ID "
        "AND t.failure_fingerprint = s.failure_fingerprint",
    )
    rate = invalid_count / max(1, total_count)
    if invalid_count >= MAX_QUARANTINE_COUNT or rate >= MAX_QUARANTINE_RATE:
        update_run_status(
            RUNS, run_row["run_id"], "source_integrity_blocked",
            error_message=(
                f"Chunk integrity circuit breaker: {invalid_count}/{total_count} "
                f"({rate:.4%}) invalid"
            ),
            quarantined_events=invalid_count,
            total_events=total_count,
        )
        raise RuntimeError(
            f"SOURCE_CHUNK_INTEGRITY_BLOCKED: {invalid_count}/{total_count} invalid event versions"
        )
    return invalid_count


def materialize_run(run_row):
    run_id = run_row["run_id"]
    start_version = int(run_row["source_start_version"])
    end_version = int(run_row["source_end_version"])
    shard_count = int(run_row["shard_count"])

    inbox = with_blob_source_version_id(
        spark.table(CDF_INBOX).filter(
            (F.col("pipeline_id") == PIPELINE_ID)
            & (F.col("source_table") == SOURCE_TABLE)
            & (F.col("trust_filter") == TRUST_FILTER)
            & (F.col("commit_version") >= start_version)
            & (F.col("commit_version") <= end_version)
        )
    )
    missing_version_fields = inbox.filter(
        F.col("EVENT_ID").isNull()
        | F.col("VALID_FROM_DT_TM").isNull()
        | F.col("UPDT_DT_TM").isNull()
        | F.col("UPDT_CNT").isNull()
    ).limit(1).count()
    if missing_version_fields:
        raise RuntimeError("Blob source version fields contain NULL; refusing ambiguous assembly")

    event_keys = inbox.groupBy(*BLOB_PROCESSING_KEYS).agg(
        F.max("ADC_UPDT").alias("_version_adc_updt"),
        F.max("commit_version").cast("long").alias("source_commit_version"),
    )

    # Always re-read the full raw source for selected immutable versions. A CDF
    # commit can contain only a subset of a version's chunks, so assembling from
    # the inbox can produce an incomplete or Frankenstein payload.
    raw = spark.table(CHUNK_SOURCE_TABLE).filter(F.col("Trust") == TRUST_FILTER)
    missing_cols = sorted({
        "EVENT_ID", "ADC_UPDT", "BLOB_SEQ_NUM", "BLOB_CONTENTS", "BLOB_LENGTH",
        "VALID_UNTIL_DT_TM", "VALID_FROM_DT_TM", "UPDT_DT_TM", "UPDT_ID",
        "UPDT_TASK", "UPDT_CNT", "UPDT_APPLCTX", "LAST_UTC_TS",
        "COMPRESSION_CD", "ENCNTR_ID", "Trust",
    } - set(raw.columns))
    if missing_cols:
        raise RuntimeError(f"CHUNK_SOURCE_TABLE is missing columns: {missing_cols}")
    chunk_source = null_safe_join(
        with_blob_source_version_id(raw),
        F.broadcast(event_keys.select(*BLOB_PROCESSING_KEYS)),
        BLOB_PROCESSING_KEYS,
        "inner",
    )

    conflicting_versions = (
        chunk_source.groupBy(*BLOB_PROCESSING_KEYS, "BLOB_SEQ_NUM")
        .agg(F.countDistinct(F.sha2("BLOB_CONTENTS", 256)).alias("_content_variants"))
        .filter(F.col("_content_variants") > 1)
        .select(*BLOB_PROCESSING_KEYS)
        .distinct()
        .withColumn("_content_conflict", F.lit(True))
    )

    dedupe_window = Window.partitionBy(*BLOB_PROCESSING_KEYS, "BLOB_SEQ_NUM").orderBy(
        F.when(F.col("BLOB_CONTENTS").isNotNull(), F.lit(0)).otherwise(F.lit(1)).asc(),
        F.col("ADC_UPDT").desc_nulls_last(),
        F.col("VALID_UNTIL_DT_TM").desc_nulls_last(),
        F.sha2("BLOB_CONTENTS", 256).desc_nulls_last(),
    )
    deduped_chunks = (
        chunk_source.withColumn("_rank", F.row_number().over(dedupe_window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
    selected_chunks = (
        null_safe_join(deduped_chunks, event_keys, BLOB_PROCESSING_KEYS, "inner")
        .select(
            F.lit(run_id).alias("run_id"),
            F.lit(PIPELINE_ID).alias("pipeline_id"),
            F.col("EVENT_ID").cast("long").alias("EVENT_ID"),
            "SOURCE_VERSION_ID",
            F.col("_version_adc_updt").alias("ADC_UPDT"),
            F.col("source_commit_version").cast("long").alias("source_commit_version"),
            F.col("BLOB_SEQ_NUM").cast("long").alias("BLOB_SEQ_NUM"),
            "VALID_UNTIL_DT_TM", "VALID_FROM_DT_TM", "UPDT_DT_TM",
            F.col("UPDT_ID").cast("long").alias("UPDT_ID"),
            F.col("UPDT_TASK").cast("long").alias("UPDT_TASK"),
            F.col("UPDT_CNT").cast("long").alias("UPDT_CNT"),
            F.col("UPDT_APPLCTX").cast("long").alias("UPDT_APPLCTX"),
            "LAST_UTC_TS",
            F.col("COMPRESSION_CD").cast("long").alias("COMPRESSION_CD"),
            "BLOB_CONTENTS",
            F.col("BLOB_LENGTH").cast("long").alias("BLOB_LENGTH"),
            F.col("ENCNTR_ID").cast("long").alias("ENCNTR_ID"),
            "Trust",
            F.current_timestamp().alias("created_ts"),
        )
    )

    merge_all(
        selected_chunks,
        RUN_CHUNKS,
        "t.run_id = s.run_id AND t.EVENT_ID = s.EVENT_ID "
        "AND t.SOURCE_VERSION_ID = s.SOURCE_VERSION_ID "
        "AND t.BLOB_SEQ_NUM = s.BLOB_SEQ_NUM",
    )

    persisted_chunks = spark.table(RUN_CHUNKS).filter(F.col("run_id") == run_id)
    integrity = (
        chunk_integrity(persisted_chunks)
        .join(conflicting_versions, list(BLOB_PROCESSING_KEYS), "left")
        .withColumn(
            "chunk_integrity_status",
            F.when(F.col("_content_conflict") == True, F.lit("invalid"))
            .otherwise(F.col("chunk_integrity_status")),
        )
        .withColumn(
            "chunk_integrity_reason",
            F.when(F.col("_content_conflict") == True, F.lit("conflicting_chunk_content"))
            .otherwise(F.col("chunk_integrity_reason")),
        )
        .drop("_content_conflict")
    )
    events = (
        null_safe_join(event_keys, integrity, BLOB_PROCESSING_KEYS, "left")
        .select(
            F.lit(run_id).alias("run_id"),
            F.lit(PIPELINE_ID).alias("pipeline_id"),
            F.col("EVENT_ID").cast("long").alias("EVENT_ID"),
            "SOURCE_VERSION_ID",
            F.col("_version_adc_updt").alias("ADC_UPDT"),
            F.col("source_commit_version").cast("long").alias("source_commit_version"),
            F.pmod(F.xxhash64("SOURCE_VERSION_ID"), F.lit(shard_count)).cast("int").alias("shard_id"),
            F.col("assembled_bytes").cast("long").alias("compressed_size"),
            F.col("chunk_rows").cast("long").alias("chunk_count"),
            F.col("max_blob_length").cast("long").alias("expected_blob_length"),
            F.coalesce(F.col("chunk_integrity_status"), F.lit("invalid")).alias("chunk_integrity_status"),
            F.when(F.col("chunk_integrity_status") == "valid", F.lit(None).cast("string"))
            .otherwise(F.coalesce(F.col("chunk_integrity_reason"), F.lit("missing_chunks")))
            .alias("chunk_integrity_reason"),
            F.current_timestamp().alias("created_ts"),
        )
    )
    merge_all(
        events,
        RUN_EVENTS,
        "t.run_id = s.run_id AND t.EVENT_ID = s.EVENT_ID "
        "AND t.SOURCE_VERSION_ID = s.SOURCE_VERSION_ID",
    )

    # RDE_REPAIR_20260830_FAIL_CLOSED_MISSING_CHUNKS
    # Gate the complete event-key set, not only keys that happened to produce chunks.
    # A zero-row chunk join must therefore trip the source-integrity circuit breaker.
    invalid_count = quarantine_integrity_failures(run_row, events)

    total_events = int(events.count())
    update_run_status(
        RUNS, run_id, "worklist_ready",
        total_events=total_events,
        completed_events=0,
        quarantined_events=invalid_count,
        error_message=None,
    )
    log_pipeline_event(
        PIPELINE_EVENTS, "INFO", "worklist_ready",
        f"Materialized {total_events} source versions for source {start_version}..{end_version}",
        pipeline_id=PIPELINE_ID, run_id=run_id,
        details={"invalid_chunk_versions": invalid_count, "shards": shard_count},
    )
    return total_events


# COMMAND ----------

active = active_run()
if active is not None:
    if active["status"] == "materializing":
        materialize_run(active)
        active = spark.table(RUNS).filter(F.col("run_id") == active["run_id"]).collect()[0]
    print(f"Resuming active run {active['run_id']} status={active['status']}")
    dbutils.jobs.taskValues.set(key="RUN_ID", value=active["run_id"])
    dbutils.jobs.taskValues.set(key="HAS_WORK", value="true")
    dbutils.notebook.exit(f"RESUME:{active['run_id']}:{active['status']}")


# COMMAND ----------

checkpoint = checkpoint_row()
merged_version = int(checkpoint["target_merged_version"]) if checkpoint["target_merged_version"] is not None else -1
ingested_version = int(checkpoint["cdf_ingested_version"]) if checkpoint["cdf_ingested_version"] is not None else -1

commit_rows = (
    spark.table(SOURCE_COMMITS)
    .filter(
        (F.col("pipeline_id") == PIPELINE_ID)
        & (F.col("source_table") == SOURCE_TABLE)
        & (F.col("version") > merged_version)
        & (F.col("version") <= ingested_version)
        & F.col("ingest_status").isin("ingested", "gap_skipped")
    )
    .orderBy("version")
    .collect()
)

if not commit_rows:
    dbutils.jobs.taskValues.set(key="RUN_ID", value="")
    dbutils.jobs.taskValues.set(key="HAS_WORK", value="false")
    dbutils.notebook.exit("NO_WORK")

expected_versions = list(range(merged_version + 1, ingested_version + 1))
actual_versions = [int(row["version"]) for row in commit_rows]
if actual_versions != expected_versions[:len(actual_versions)]:
    raise RuntimeError(
        f"Non-contiguous source commit manifest after merged version {merged_version}: "
        f"first actual versions={actual_versions[:20]}"
    )

event_counts = {
    int(row["commit_version"]): int(row["event_count"])
    for row in (
        spark.table(CDF_INBOX)
        .filter(
            (F.col("pipeline_id") == PIPELINE_ID)
            & (F.col("source_table") == SOURCE_TABLE)
            & (F.col("commit_version") > merged_version)
            & (F.col("commit_version") <= ingested_version)
        )
        .groupBy("commit_version")
        .agg(F.countDistinct(F.struct("EVENT_ID", "VALID_FROM_DT_TM", "UPDT_DT_TM", "UPDT_CNT")).alias("event_count"))
        .collect()
    )
}

if sum(event_counts.values()) == 0:
    update_checkpoint(merged_version=ingested_version)
    run_sql_with_retry(f"""
      UPDATE {quote_table(SOURCE_COMMITS)}
      SET merged_ts = current_timestamp()
      WHERE pipeline_id = {sql_string(PIPELINE_ID)}
        AND source_table = {sql_string(SOURCE_TABLE)}
        AND version > {merged_version}
        AND version <= {ingested_version}
    """, label="fast-forward source commits")
    dbutils.jobs.taskValues.set(key="RUN_ID", value="")
    dbutils.jobs.taskValues.set(key="HAS_WORK", value="false")
    dbutils.notebook.exit(f"NO_RELEVANT_CHANGES:{merged_version + 1}..{ingested_version}")

selected = []
cumulative = 0
included_relevant = False
for row in commit_rows:
    version = int(row["version"])
    count = event_counts.get(version, 0)
    if selected and included_relevant and MAX_EVENTS > 0 and cumulative + count > MAX_EVENTS:
        break
    selected.append(version)
    cumulative += count
    included_relevant = included_relevant or count > 0
    if included_relevant and MAX_EVENTS > 0 and cumulative >= MAX_EVENTS:
        break

if not selected:
    raise RuntimeError("Unable to select a contiguous source commit range")

run_id = REQUESTED_RUN_ID or (
    datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:8]
)
safe_value(run_id, "run id")
start_version = selected[0]
end_version = selected[-1]
has_more_source = end_version < ingested_version

run_schema = spark.table(RUNS).schema
run_row = Row(
    run_id=run_id,
    pipeline_id=PIPELINE_ID,
    source_table=SOURCE_TABLE,
    chunk_source_table=CHUNK_SOURCE_TABLE,
    target_table=TARGET_TABLE,
    trust_filter=TRUST_FILTER,
    source_start_version=start_version,
    source_end_version=end_version,
    shard_count=SHARDS,
    status="materializing",
    total_events=0,
    completed_events=0,
    quarantined_events=0,
    has_more_source=has_more_source,
    error_message=None,
    lease_owner=None,
    lease_expires_ts=None,
    created_ts=utc_now_naive(),
    updated_ts=utc_now_naive(),
    processing_completed_ts=None,
    merged_ts=None,
    files_completed_ts=None,
)
spark.createDataFrame([run_row], schema=run_schema).write.mode("append").insertInto(RUNS)

run_sql_with_retry(f"""
  UPDATE {quote_table(SOURCE_COMMITS)}
  SET assigned_run_id = {sql_string(run_id)}
  WHERE pipeline_id = {sql_string(PIPELINE_ID)}
    AND source_table = {sql_string(SOURCE_TABLE)}
    AND version >= {start_version}
    AND version <= {end_version}
""", label="assign source commits to run")

created = spark.table(RUNS).filter(F.col("run_id") == run_id).collect()[0]
total_events = materialize_run(created)

if OPTIMIZE_WORKLIST and total_events >= 100000:
    for table_name in (RUN_EVENTS, RUN_CHUNKS):
        try:
            spark.sql(f"OPTIMIZE {quote_table(table_name)} WHERE run_id = {sql_string(run_id)}")
        except Exception as exc:
            print(f"non-fatal optimize warning for {table_name}: {exc}")

dbutils.jobs.taskValues.set(key="RUN_ID", value=run_id)
dbutils.jobs.taskValues.set(key="HAS_WORK", value="true")
print(
    f"Created run {run_id}: {total_events:,} event versions, "
    f"source versions {start_version}..{end_version}, has_more_source={has_more_source}"
)

