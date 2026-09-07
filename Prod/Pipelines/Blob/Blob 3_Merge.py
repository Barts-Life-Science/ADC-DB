# Databricks notebook source
# MAGIC %run ./blob_pipeline_lib

# COMMAND ----------

# Version-grain repair staged 2026-08-25; target MERGE keyed by BLOB_VERSION_ID.

# COMMAND ----------

import json

from delta.tables import DeltaTable
from pyspark.sql import Row, functions as F
from pyspark.sql.window import Window


dbutils.widgets.text("RUN_ID", "")
dbutils.widgets.text("STATE_SCHEMA", "4_prod.tmp")
dbutils.widgets.text("METADATA_TABLE", "4_prod.tmp.pipeline_runs")
dbutils.widgets.text("CHECKPOINT_TABLE", "4_prod.tmp.pipeline_checkpoint")
dbutils.widgets.text("HISTORY_TABLE", "4_prod.logs.mill_blob_extractor_history")
dbutils.widgets.text("METRICS_TABLE", "4_prod.logs.mill_blob_metrics")
# Default fixed from 4_prod.6mgmt (schema does not exist), which silently skipped
# the terminal-manifest anti-join and re-pended already-extracted files.
dbutils.widgets.text("MANIFEST_TABLE", "4_prod.6_mgmt.file_manifest")
dbutils.widgets.text("PROCESSING_CONFIG_TABLE", "4_prod.tmp.pipeline_runs")  # deprecated, unused
dbutils.widgets.dropdown("CLEANUP", "false", ["false", "true"])

# COMMAND ----------



RUN_ID = dbutils.widgets.get("RUN_ID").strip()
STATE_SCHEMA = dbutils.widgets.get("STATE_SCHEMA").strip()
METADATA_TABLE = dbutils.widgets.get("METADATA_TABLE").strip()
CHECKPOINT_TABLE = dbutils.widgets.get("CHECKPOINT_TABLE").strip()
HISTORY_TABLE = dbutils.widgets.get("HISTORY_TABLE").strip()
METRICS_TABLE = dbutils.widgets.get("METRICS_TABLE").strip()
MANIFEST_TABLE = dbutils.widgets.get("MANIFEST_TABLE").strip()
CLEANUP = dbutils.widgets.get("CLEANUP").lower() == "true"

if not RUN_ID:
    raise ValueError("RUN_ID is required")
safe_value(RUN_ID, "run id")
for value in (
    STATE_SCHEMA, METADATA_TABLE, CHECKPOINT_TABLE, HISTORY_TABLE, METRICS_TABLE,
    MANIFEST_TABLE,
):
    validate_table_name(value)

RUN_EVENTS = f"{STATE_SCHEMA}.run_events"
BATCH_OUTPUT = f"{STATE_SCHEMA}.batch_output"
HISTORY_OUTPUT = f"{STATE_SCHEMA}.history_output"
QUARANTINE = f"{STATE_SCHEMA}.quarantine"
SOURCE_COMMITS = f"{STATE_SCHEMA}.source_commits"
FILE_QUEUE = f"{STATE_SCHEMA}.file_queue"
PIPELINE_EVENTS = f"{STATE_SCHEMA}.pipeline_events"

for required in (
    METADATA_TABLE, CHECKPOINT_TABLE, RUN_EVENTS, BATCH_OUTPUT,
    HISTORY_OUTPUT, QUARANTINE, SOURCE_COMMITS, FILE_QUEUE,
):
    if not table_exists(required):
        raise RuntimeError(f"Missing required v4 table {required}")


# COMMAND ----------

def get_run():
    rows = spark.table(METADATA_TABLE).filter(F.col("run_id") == RUN_ID).limit(2).collect()
    if len(rows) != 1:
        raise RuntimeError(f"Expected one run row for {RUN_ID}; found {len(rows)}")
    return rows[0]


def assert_unique(frame, keys, label):
    if frame.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count():
        raise RuntimeError(f"{label} contains duplicate keys {keys}")


def merge_all_columns(source, target_table, condition, preserve_on_null=()):
    aligned = align_to_table(source, target_table)
    target_columns = [field.name for field in spark.table(target_table).schema]
    tick = chr(96)
    update_map = {}
    insert_map = {}
    for column in target_columns:
        quoted = f"{tick}{column}{tick}"
        insert_map[column] = f"s.{quoted}"
        if column in preserve_on_null:
            update_map[column] = f"coalesce(s.{quoted}, t.{quoted})"
        else:
            update_map[column] = f"s.{quoted}"

    def _execute():
        (
            DeltaTable.forName(spark, target_table)
            .alias("t")
            .merge(aligned.alias("s"), condition)
            .whenMatchedUpdate(set=update_map)
            .whenNotMatchedInsert(values=insert_map)
            .execute()
        )

    with_delta_retry(_execute, label=f"merge into {target_table}")


def finalize_checkpoint(meta):
    end_version = int(meta["source_end_version"])
    run_sql_with_retry(f"""
      UPDATE {quote_table(CHECKPOINT_TABLE)}
      SET target_merged_version = greatest(
            coalesce(target_merged_version, -1), {end_version}
          ),
          last_merge_run_id = {sql_string(RUN_ID)},
          updated_ts = current_timestamp()
      WHERE pipeline_id = {sql_string(meta["pipeline_id"])}
        AND source_table = {sql_string(meta["source_table"])}
        AND trust_filter = {sql_string(meta["trust_filter"])}
    """, label="finalize checkpoint")
    run_sql_with_retry(f"""
      UPDATE {quote_table(SOURCE_COMMITS)}
      SET merged_ts = coalesce(merged_ts, current_timestamp())
      WHERE pipeline_id = {sql_string(meta["pipeline_id"])}
        AND source_table = {sql_string(meta["source_table"])}
        AND version >= {int(meta["source_start_version"])}
        AND version <= {end_version}
    """, label="stamp source commits merged")


def result_exit(status, meta, summary=None, **extra):
    payload = {
        "status": status,
        "run_id": RUN_ID,
        "metadata_status": meta["status"],
        "coverage": summary,
        **extra,
    }
    encoded = json.dumps(payload, default=str, sort_keys=True)
    dbutils.jobs.taskValues.set(key="MERGE_RESULT", value=encoded)
    dbutils.notebook.exit(encoded)


# COMMAND ----------

meta = get_run()
if meta["status"] in {"merge_complete", "complete", "files_processing", "files_partial"}:
    finalize_checkpoint(meta)
    result_exit("already_complete", meta)

summary = coverage_summary(
    RUN_EVENTS, BATCH_OUTPUT, HISTORY_OUTPUT, QUARANTINE, RUN_ID
)
if summary["overlap"]:
    raise RuntimeError(f"Completed and quarantined keys overlap: {summary}")

if meta["status"] not in {"processing_complete", "merging", "merge_failed"}:
    result_exit("waiting_for_processing", meta, summary)

if summary["unresolved"] != 0:
    result_exit("waiting_for_processing", meta, summary)

if not table_exists(meta["target_table"]):
    raise RuntimeError(f"Target table does not exist: {meta['target_table']}")
if not table_exists(HISTORY_TABLE):
    raise RuntimeError(f"History target does not exist: {HISTORY_TABLE}")
if not table_exists(METRICS_TABLE):
    raise RuntimeError(f"Metrics target does not exist: {METRICS_TABLE}")

if meta["status"] != "merging":
    update_run_status(METADATA_TABLE, RUN_ID, "merging", error_message=None)


# COMMAND ----------

completed_keys = completed_output_keys(BATCH_OUTPUT, HISTORY_OUTPUT, RUN_ID)
batch = null_safe_join(
    spark.table(BATCH_OUTPUT).filter(F.col("run_id") == RUN_ID),
    completed_keys,
    BLOB_PROCESSING_KEYS,
    "inner",
)
history = null_safe_join(
    spark.table(HISTORY_OUTPUT).filter(F.col("run_id") == RUN_ID),
    completed_keys,
    BLOB_PROCESSING_KEYS,
    "inner",
)

batch = with_blob_version_id(batch)
assert_unique(batch, BLOB_PROCESSING_KEYS, "batch output source versions")
assert_unique(batch, ["BLOB_VERSION_ID"], "batch output blob versions")

batch_for_target = batch.drop("run_id", "shard_id", "metrics", "output_ts")
merge_all_columns(
    batch_for_target,
    meta["target_table"],
    "t.BLOB_VERSION_ID = s.BLOB_VERSION_ID",
    preserve_on_null=("anon_text",),
)

history_for_target = history.drop("shard_id", "output_ts")
if "extracted_date" not in history_for_target.columns:
    history_for_target = history_for_target.withColumn("extracted_date", F.current_date())
merge_all_columns(
    history_for_target,
    HISTORY_TABLE,
    "t.run_id = s.run_id AND t.EVENT_ID = s.EVENT_ID "
    "AND t.SOURCE_VERSION_ID = s.SOURCE_VERSION_ID AND t.source <=> s.source",
)

metrics_source = batch.select(
    "BLOB_VERSION_ID", "EVENT_ID", F.lit(RUN_ID).alias("JOB_ID"), "ADC_UPDT",
    "STATUS", "metrics", F.current_timestamp().alias("process_ts"),
)
assert_unique(metrics_source, ["JOB_ID", "BLOB_VERSION_ID"], "metrics output")
merge_all_columns(
    metrics_source,
    METRICS_TABLE,
    "t.JOB_ID = s.JOB_ID AND t.BLOB_VERSION_ID = s.BLOB_VERSION_ID",
)

completed_blob_ids = batch.select("BLOB_VERSION_ID").distinct()
target_keys = spark.table(meta["target_table"]).join(
    F.broadcast(completed_blob_ids), "BLOB_VERSION_ID", "inner"
).select("BLOB_VERSION_ID")
target_count = int(target_keys.distinct().count())
target_duplicates = int(target_keys.groupBy("BLOB_VERSION_ID").count().filter(F.col("count") > 1).count())
history_count = int(
    spark.table(HISTORY_TABLE).filter(F.col("run_id") == RUN_ID)
    .select(*BLOB_PROCESSING_KEYS).distinct().count()
)
metrics_count = int(
    spark.table(METRICS_TABLE).filter(F.col("JOB_ID") == RUN_ID)
    .select("BLOB_VERSION_ID").distinct().count()
)

validation = {
    "expected_completed": summary["completed"],
    "target_keys": target_count,
    "target_duplicates": target_duplicates,
    "history_keys": history_count,
    "metrics_keys": metrics_count,
    "quarantined": summary["quarantined"],
}
if (
    target_count != summary["completed"]
    or target_duplicates != 0
    or history_count != summary["completed"]
    or metrics_count != summary["completed"]
):
    run_sql_with_retry(f"""
      UPDATE {quote_table(METADATA_TABLE)}
      SET status = 'merge_failed',
          error_message = {sql_string(safe_json(validation))},
          updated_ts = current_timestamp()
      WHERE run_id = {sql_string(RUN_ID)}
    """, label="mark run merge_failed")
    raise RuntimeError("Post-merge validation failed: " + safe_json(validation))


# COMMAND ----------

file_candidates = (
    batch.filter(
        F.col("CONTENT_TYPE").isin(
            "application/pdf", "image/tiff", "image/jpeg", "image/png"
        )
        & F.col("raw_sha256").isNotNull()
    )
    .select(
        F.lit(RUN_ID).alias("run_id"),
        "shard_id",
        "EVENT_ID",
        "ADC_UPDT",
        "raw_sha256",
        F.col("CONTENT_TYPE").alias("content_type"),
        F.lit("pending").alias("status"),
        F.lit(0).cast("int").alias("attempt_count"),
        F.lit(None).cast("string").alias("lease_owner"),
        F.lit(None).cast("timestamp").alias("lease_expires_ts"),
        F.lit(None).cast("string").alias("volume_path"),
        F.lit(None).cast("string").alias("file_sha256"),
        F.col("BINARY_SIZE").cast("long").alias("byte_size"),
        F.lit(None).cast("string").alias("last_error"),
        F.current_timestamp().alias("created_ts"),
        F.current_timestamp().alias("updated_ts"),
        F.lit(None).cast("timestamp").alias("completed_ts"),
    )
)

file_window = Window.partitionBy("EVENT_ID", "raw_sha256").orderBy(
    F.col("ADC_UPDT").desc_nulls_last(),
    F.col("shard_id").asc_nulls_last(),
)
file_candidates = (
    file_candidates
    .withColumn("_file_rank", F.row_number().over(file_window))
    .filter(F.col("_file_rank") == 1)
    .drop("_file_rank")
)

if table_exists(MANIFEST_TABLE):
    terminal_manifest = (
        spark.table(MANIFEST_TABLE)
        .filter(
            F.col("STATUS").isin(
                "Extracted", "Skipped - oversize", "Skipped - not in-scope type"
            )
        )
        .select("EVENT_ID", "raw_sha256")
        .distinct()
    )
    file_candidates = null_safe_join(
        file_candidates,
        F.broadcast(terminal_manifest),
        ["EVENT_ID", "raw_sha256"],
        "left_anti",
    )

if file_candidates.limit(1).count():
    def _merge_file_queue():
        (
            DeltaTable.forName(spark, FILE_QUEUE)
            .alias("t")
            .merge(
                file_candidates.alias("s"),
                "t.EVENT_ID = s.EVENT_ID AND t.raw_sha256 = s.raw_sha256",
            )
            .whenMatchedUpdate(
                condition=(
                    "t.run_id <> s.run_id "
                    "AND t.status IN ('retryable', 'failed_terminal')"
                ),
                set={
                    "run_id": "s.run_id",
                    "shard_id": "s.shard_id",
                    "ADC_UPDT": "s.ADC_UPDT",
                    "content_type": "s.content_type",
                    "status": "'pending'",
                    "attempt_count": "0",
                    "lease_owner": "NULL",
                    "lease_expires_ts": "NULL",
                    "volume_path": "NULL",
                    "file_sha256": "NULL",
                    "byte_size": "s.byte_size",
                    "last_error": "NULL",
                    "updated_ts": "current_timestamp()",
                    "completed_ts": "NULL",
                },
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    with_delta_retry(_merge_file_queue, label=f"merge into {FILE_QUEUE}")

run_sql_with_retry(f"""
  UPDATE {quote_table(METADATA_TABLE)}
  SET status = 'merge_complete',
      completed_events = {summary["completed"]},
      quarantined_events = {summary["quarantined"]},
      merged_ts = current_timestamp(),
      updated_ts = current_timestamp(),
      error_message = NULL
  WHERE run_id = {sql_string(RUN_ID)}
""", label="mark run merge_complete")
meta = get_run()
finalize_checkpoint(meta)

log_pipeline_event(
    PIPELINE_EVENTS, "INFO", "merge_complete",
    f"Merged {summary['completed']} keys and accounted for {summary['quarantined']} quarantined keys",
    pipeline_id=meta["pipeline_id"], run_id=RUN_ID, details=validation,
)

result_exit("success", meta, summary, validation=validation, cleanup_performed=False)
