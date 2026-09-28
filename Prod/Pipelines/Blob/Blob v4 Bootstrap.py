# Databricks notebook source
# MAGIC %run ./blob_pipeline_lib

# COMMAND ----------

# Version-grain state migration staged 2026-08-25.

# COMMAND ----------

# MAGIC %md
# MAGIC   - State schema: 4_prod.tmp
# MAGIC   - Manifest: 8_dev.default.mill_blob_files
# MAGIC   - Volume: /Volumes/8_dev/default/dev_test_volume/files
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.widgets.text("STATE_SCHEMA", "8_dev.blob_v4")
dbutils.widgets.text("PIPELINE_ID", "mill_blob_text_barts_v4")
dbutils.widgets.text("SOURCE_TABLE", "4_prod.raw.mill_ce_blob")
dbutils.widgets.text("TRUST_FILTER", "Barts")
dbutils.widgets.text("SEED_CDF_INGESTED_VERSION", "")
dbutils.widgets.text("SEED_TARGET_MERGED_VERSION", "")
dbutils.widgets.dropdown("CREATE_SCHEMA", "true", ["true", "false"])

# COMMAND ----------

# MAGIC %md
# MAGIC   STATE_SCHEMA                    4_prod.blob_v4
# MAGIC   PIPELINE_ID                     mill_blob_text_barts_v4
# MAGIC   SOURCE_TABLE                    4_prod.raw.mill_ce_blob
# MAGIC   TRUST_FILTER                    Barts
# MAGIC   SEED_CDF_INGESTED_VERSION       3774
# MAGIC   SEED_TARGET_MERGED_VERSION      3774
# MAGIC   CREATE_SCHEMA                   true
# MAGIC

# COMMAND ----------



STATE_SCHEMA = dbutils.widgets.get("STATE_SCHEMA").strip()
PIPELINE_ID = dbutils.widgets.get("PIPELINE_ID").strip()
SOURCE_TABLE = dbutils.widgets.get("SOURCE_TABLE").strip()
TRUST_FILTER = dbutils.widgets.get("TRUST_FILTER").strip()
SEED_INGESTED = dbutils.widgets.get("SEED_CDF_INGESTED_VERSION").strip()
SEED_MERGED = dbutils.widgets.get("SEED_TARGET_MERGED_VERSION").strip()
CREATE_SCHEMA = dbutils.widgets.get("CREATE_SCHEMA").lower() == "true"

validate_table_name(STATE_SCHEMA)
validate_table_name(SOURCE_TABLE)
safe_value(PIPELINE_ID, "pipeline id")
if not TRUST_FILTER:
    raise ValueError("TRUST_FILTER cannot be empty")


catalog_name, schema_name = STATE_SCHEMA.split(".")
if CREATE_SCHEMA:
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {quote_table(STATE_SCHEMA)} "
        "COMMENT 'Blob v4 durable pipeline state'"
    )

def q(name):
    return quote_table(f"{STATE_SCHEMA}.{name}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {q("pipeline_checkpoint")} (
  pipeline_id STRING NOT NULL, source_table STRING NOT NULL, trust_filter STRING NOT NULL,
  cdf_ingested_version BIGINT, target_merged_version BIGINT,
  last_ingest_run_id STRING, last_merge_run_id STRING, updated_ts TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors'='true')
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {q("source_commits")} (
  pipeline_id STRING NOT NULL, source_table STRING NOT NULL, version BIGINT NOT NULL,
  commit_timestamp TIMESTAMP, operation STRING, relevant_change_rows BIGINT,
  relevance_class STRING, gap_disposition STRING, ingest_status STRING NOT NULL,
  assigned_run_id STRING, details STRING, ingested_ts TIMESTAMP, merged_ts TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors'='true')
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {q("cdf_inbox")} (
  inbox_key STRING NOT NULL, pipeline_id STRING NOT NULL, source_table STRING NOT NULL,
  trust_filter STRING NOT NULL, EVENT_ID BIGINT, BLOB_SEQ_NUM BIGINT,
  VALID_UNTIL_DT_TM TIMESTAMP, VALID_FROM_DT_TM TIMESTAMP, UPDT_DT_TM TIMESTAMP,
  UPDT_ID BIGINT, UPDT_TASK BIGINT, UPDT_CNT BIGINT, UPDT_APPLCTX BIGINT,
  LAST_UTC_TS TIMESTAMP, ADC_UPDT TIMESTAMP, COMPRESSION_CD BIGINT,
  BLOB_CONTENTS BINARY, BLOB_LENGTH BIGINT, ENCNTR_ID BIGINT, Trust STRING,
  change_type STRING, commit_version BIGINT NOT NULL, commit_timestamp TIMESTAMP,
  ingested_ts TIMESTAMP
) USING DELTA
TBLPROPERTIES (
  'delta.enableDeletionVectors'='true',
  'delta.deletedFileRetentionDuration'='interval 7 days',
  'delta.autoOptimize.optimizeWrite'='true',
  'delta.autoOptimize.autoCompact'='true'
)
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {q("pipeline_runs")} (
  run_id STRING NOT NULL, pipeline_id STRING NOT NULL, source_table STRING NOT NULL,
  chunk_source_table STRING NOT NULL, target_table STRING NOT NULL,
  trust_filter STRING NOT NULL, source_start_version BIGINT, source_end_version BIGINT,
  shard_count INT NOT NULL, status STRING NOT NULL, total_events BIGINT,
  completed_events BIGINT, quarantined_events BIGINT, has_more_source BOOLEAN,
  error_message STRING, lease_owner STRING, lease_expires_ts TIMESTAMP,
  created_ts TIMESTAMP, updated_ts TIMESTAMP, processing_completed_ts TIMESTAMP,
  merged_ts TIMESTAMP, files_completed_ts TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors'='true')
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {q("run_events")} (
  run_id STRING NOT NULL, pipeline_id STRING NOT NULL, EVENT_ID BIGINT NOT NULL,
  ADC_UPDT TIMESTAMP, source_commit_version BIGINT NOT NULL, shard_id INT NOT NULL,
  compressed_size BIGINT, chunk_count BIGINT, expected_blob_length BIGINT,
  chunk_integrity_status STRING NOT NULL, chunk_integrity_reason STRING,
  created_ts TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors'='true')
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {q("run_chunks")} (
  run_id STRING NOT NULL, pipeline_id STRING NOT NULL, EVENT_ID BIGINT NOT NULL,
  ADC_UPDT TIMESTAMP, source_commit_version BIGINT NOT NULL,
  BLOB_SEQ_NUM BIGINT NOT NULL, VALID_UNTIL_DT_TM TIMESTAMP,
  VALID_FROM_DT_TM TIMESTAMP, UPDT_DT_TM TIMESTAMP, UPDT_ID BIGINT,
  UPDT_TASK BIGINT, UPDT_CNT BIGINT, UPDT_APPLCTX BIGINT,
  LAST_UTC_TS TIMESTAMP, COMPRESSION_CD BIGINT, BLOB_CONTENTS BINARY,
  BLOB_LENGTH BIGINT, ENCNTR_ID BIGINT, Trust STRING, created_ts TIMESTAMP
) USING DELTA
TBLPROPERTIES (
  'delta.enableDeletionVectors'='true',
  'delta.deletedFileRetentionDuration'='interval 7 days',
  'delta.autoOptimize.optimizeWrite'='true',
  'delta.autoOptimize.autoCompact'='true'
)
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {q("shard_attempts")} (
  run_id STRING NOT NULL, shard_id INT NOT NULL, attempt_id STRING NOT NULL,
  attempt_no INT NOT NULL, status STRING NOT NULL, lease_owner STRING,
  lease_expires_ts TIMESTAMP, selected_events BIGINT, completed_events BIGINT,
  quarantined_events BIGINT, error_class STRING, error_message STRING,
  failure_fingerprint STRING, databricks_trace_id STRING, retryable BOOLEAN,
  started_ts TIMESTAMP, heartbeat_ts TIMESTAMP, ended_ts TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors'='true')
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {q("batch_output")} (
  run_id STRING NOT NULL, shard_id INT NOT NULL, EVENT_ID BIGINT NOT NULL,
  VALID_UNTIL_DT_TM TIMESTAMP, VALID_FROM_DT_TM TIMESTAMP, UPDT_DT_TM TIMESTAMP,
  UPDT_ID BIGINT, UPDT_TASK BIGINT, UPDT_CNT BIGINT, UPDT_APPLCTX BIGINT,
  LAST_UTC_TS TIMESTAMP, ADC_UPDT TIMESTAMP, BLOB_BINARY BINARY,
  CONTENT_TYPE STRING, ENCODING STRING, BLOB_TEXT STRING, BINARY_SIZE BIGINT,
  TEXT_LENGTH BIGINT, STATUS STRING, anon_text STRING, ENCNTR_ID BIGINT,
  Trust STRING, raw_sha256 STRING, decompressor_version INT,
  parser_version INT, post_processor_version INT, metrics STRING, output_ts TIMESTAMP
) USING DELTA
TBLPROPERTIES (
  'delta.enableDeletionVectors'='true',
  'delta.autoOptimize.optimizeWrite'='true',
  'delta.autoOptimize.autoCompact'='true'
)
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {q("history_output")} (
  run_id STRING NOT NULL, shard_id INT NOT NULL, EVENT_ID BIGINT NOT NULL,
  ADC_UPDT TIMESTAMP, raw_sha256 STRING, decompressor_version INT,
  parser_version INT, post_processor_version INT, status STRING,
  truncation_flag BOOLEAN, truncation_reason STRING, ftfy_explain STRING,
  decompression_strategy STRING, source STRING, parse_status STRING,
  metrics STRING, output_ts TIMESTAMP
) USING DELTA
TBLPROPERTIES (
  'delta.enableDeletionVectors'='true',
  'delta.autoOptimize.optimizeWrite'='true',
  'delta.autoOptimize.autoCompact'='true'
)
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {q("quarantine")} (
  run_id STRING NOT NULL, shard_id INT NOT NULL, EVENT_ID BIGINT NOT NULL,
  ADC_UPDT TIMESTAMP, source_commit_version BIGINT, reason_code STRING NOT NULL,
  error_class STRING, error_message STRING, failure_fingerprint STRING,
  nonretryable_attempts INT NOT NULL, raw_reference STRING, status STRING NOT NULL,
  first_failure_ts TIMESTAMP, last_failure_ts TIMESTAMP, resolution STRING,
  reviewed_by STRING, reviewed_ts TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors'='true')
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {q("file_queue")} (
  run_id STRING NOT NULL, shard_id INT, EVENT_ID BIGINT NOT NULL, ADC_UPDT TIMESTAMP,
  raw_sha256 STRING NOT NULL, content_type STRING, status STRING NOT NULL,
  attempt_count INT NOT NULL, lease_owner STRING, lease_expires_ts TIMESTAMP,
  volume_path STRING, file_sha256 STRING, byte_size BIGINT, last_error STRING,
  created_ts TIMESTAMP, updated_ts TIMESTAMP, completed_ts TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors'='true')
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {q("pipeline_events")} (
  event_uuid STRING NOT NULL, event_ts TIMESTAMP NOT NULL, pipeline_id STRING,
  run_id STRING, shard_id INT, EVENT_ID BIGINT, ADC_UPDT TIMESTAMP,
  severity STRING NOT NULL, event_type STRING NOT NULL, message STRING, details STRING
) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors'='true')
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {q("validation_results")} (
  validation_run_id STRING NOT NULL, suite STRING NOT NULL,
  test_name STRING NOT NULL, passed BOOLEAN NOT NULL,
  details STRING, test_ts TIMESTAMP NOT NULL
) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors'='true')
""")

# COMMAND ----------

for table_name, columns in {
    "cdf_inbox": "(pipeline_id, commit_version)",
    "run_events": "(run_id, shard_id)",
    "run_chunks": "(run_id, EVENT_ID)",
    "batch_output": "(run_id, shard_id)",
    "history_output": "(run_id, shard_id)",
    "quarantine": "(run_id, shard_id)",
}.items():
    try:
        spark.sql(f"ALTER TABLE {q(table_name)} CLUSTER BY {columns}")
    except Exception as exc:
        print(f"non-fatal clustering warning for {table_name}: {exc}")

# COMMAND ----------

seed_ingested = int(SEED_INGESTED) if SEED_INGESTED else None
seed_merged = int(SEED_MERGED) if SEED_MERGED else None
checkpoint = (
    spark.table(f"{STATE_SCHEMA}.pipeline_checkpoint")
    .filter(
        (F.col("pipeline_id") == PIPELINE_ID)
        & (F.col("source_table") == SOURCE_TABLE)
        & (F.col("trust_filter") == TRUST_FILTER)
    )
    .limit(2)
    .collect()
)
if len(checkpoint) > 1:
    raise RuntimeError("Duplicate checkpoint rows found")
if not checkpoint:
    schema = spark.table(f"{STATE_SCHEMA}.pipeline_checkpoint").schema
    row = Row(
        pipeline_id=PIPELINE_ID,
        source_table=SOURCE_TABLE,
        trust_filter=TRUST_FILTER,
        cdf_ingested_version=seed_ingested,
        target_merged_version=seed_merged,
        last_ingest_run_id=None,
        last_merge_run_id=None,
        updated_ts=utc_now_naive(),
    )
    spark.createDataFrame([row], schema).write.mode("append").insertInto(
        f"{STATE_SCHEMA}.pipeline_checkpoint"
    )
    print(f"seeded checkpoint ingested={seed_ingested} merged={seed_merged}")
else:
    print("checkpoint already exists; no seed applied")

display(
    spark.table(f"{STATE_SCHEMA}.pipeline_checkpoint")
    .filter(F.col("pipeline_id") == PIPELINE_ID)
)
# Version-grain state migration. Safe to re-run.
for _table_name, _columns in {
    "run_events": [("SOURCE_VERSION_ID", "STRING")],
    "run_chunks": [("SOURCE_VERSION_ID", "STRING")],
    "batch_output": [("SOURCE_VERSION_ID", "STRING"), ("BLOB_VERSION_ID", "STRING")],
    "history_output": [("SOURCE_VERSION_ID", "STRING"), ("BLOB_VERSION_ID", "STRING")],
    "quarantine": [("SOURCE_VERSION_ID", "STRING")],
}.items():
    _fqn = f"{STATE_SCHEMA}.{_table_name}"
    _existing = {field.name.upper() for field in spark.table(_fqn).schema.fields}
    for _column, _type in _columns:
        if _column.upper() not in _existing:
            spark.sql(f"ALTER TABLE {quote_table(_fqn)} ADD COLUMNS ({_column} {_type})")
        spark.sql(
            f"ALTER TABLE {quote_table(_fqn)} ALTER COLUMN `{_column}` "
            "SET TAGS ('ig_risk'='0','ig_severity'='0')"
        )

