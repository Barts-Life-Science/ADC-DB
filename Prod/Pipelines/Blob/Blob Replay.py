# Databricks notebook source
# COMMAND ----------

raise RuntimeError(
    "Blob Replay is disabled because its EVENT_ID+ADC_UPDT grain can combine or overwrite "
    "distinct source versions. Use the v5 forward publisher or a version-aware replay."
)

# COMMAND ----------
# Legacy EVENT_ID+ADC_UPDT replay disabled by version-grain repair 2026-08-25.
# Blob Replay — re-extract rows matching a scope predicate using the current
# shared library. Does NOT use a worklist; input is explicit.
#
# Same orchestrator/shard pattern as blob_processing_v2, but the input is a
# SCOPE_SQL predicate against the bronze table, and raw chunks come from
# c_cold.raw.mill_ce_blob (an unpartitioned 3TB archive).
#
# The orchestrator scans c_cold ONCE and materializes the needed chunks into
# a Delta staging table partitioned by shard_key. Shards read their partition
# from this small staging table instead of re-scanning c_cold 8x in parallel.

# COMMAND ----------

# MAGIC %pip install ftfy>=6.3 mammoth>=1.7 resiliparse>=0.14 pypdfium2>=4.30 loky
# MAGIC %pip install striprtf ocflzw-decompress
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./blob_shared_lib

# COMMAND ----------

from pyspark.sql import functions as F, types as T, Row
import time, json, hashlib
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---- Parameters ----
dbutils.widgets.text("SCOPE_SQL", "STATUS LIKE '%LZW%' OR STATUS = 'Failed to decode'")
dbutils.widgets.text("REPLAY_ID", "")  # auto-generated if blank
dbutils.widgets.text("SHARDS", "8")
dbutils.widgets.text("SHARD_ID", "")
dbutils.widgets.text("TARGET_TABLE", "4_prod.bronze.mill_blob_text")
dbutils.widgets.text("HISTORY_TABLE", "4_prod.logs.mill_blob_extractor_history")
dbutils.widgets.text("STAGING_DB", "4_prod.tmp")
dbutils.widgets.text("METADATA_TABLE", "4_prod.tmp.pipeline_metadata")
dbutils.widgets.text("DRY_RUN", "false")
dbutils.widgets.text("MAX_ROWS", "300000")  # 0 = unbounded
dbutils.widgets.text("SHARD_TIMEOUT_SEC", "14400")  # 4h default; raise for big backfills
dbutils.widgets.text("MAX_CONCURRENT_SHARDS", "2")

SCOPE_SQL = dbutils.widgets.get("SCOPE_SQL")
SHARDS = int(dbutils.widgets.get("SHARDS"))
SHARD_ID_STR = dbutils.widgets.get("SHARD_ID")
TARGET_TABLE = dbutils.widgets.get("TARGET_TABLE")
HISTORY_TABLE = dbutils.widgets.get("HISTORY_TABLE")
STAGING_DB = dbutils.widgets.get("STAGING_DB")
METADATA_TABLE = dbutils.widgets.get("METADATA_TABLE")
DRY_RUN = dbutils.widgets.get("DRY_RUN").lower() == "true"
MAX_ROWS = int(dbutils.widgets.get("MAX_ROWS"))
SHARD_TIMEOUT_SEC = int(dbutils.widgets.get("SHARD_TIMEOUT_SEC"))
MAX_CONCURRENT_SHARDS = max(1, int(dbutils.widgets.get("MAX_CONCURRENT_SHARDS")))

REPLAY_ID = dbutils.widgets.get("REPLAY_ID")
if not REPLAY_ID:
    scope_hash = hashlib.sha1(SCOPE_SQL.encode()).hexdigest()[:6]
    REPLAY_ID = f"replay_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{scope_hash}"

IS_ORCHESTRATOR = (SHARD_ID_STR == "")
SHARD_ID = None if IS_ORCHESTRATOR else int(SHARD_ID_STR)

# ---- Spark config (same as forward path) ----
try:
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "320")
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
except Exception as e:
    print(f"(non-fatal) could not set some Spark configs: {e}")

# ---- Staging table names (used in both orchestrator and shard) ----
scope_table = f"{STAGING_DB}.replay_scope_{REPLAY_ID}"
chunks_table = f"{STAGING_DB}.replay_chunks_{REPLAY_ID}"
orphans_table = f"{STAGING_DB}.replay_orphans_{REPLAY_ID}"

# COMMAND ----------

# ==================== ORCHESTRATOR ====================
if IS_ORCHESTRATOR:
    print(f"REPLAY ORCHESTRATOR  ID={REPLAY_ID}")
    print(f"Scope: {SCOPE_SQL}")
    print(f"Target: {TARGET_TABLE}   History: {HISTORY_TABLE}   Dry-run: {DRY_RUN}")
    print(f"Shard timeout: {SHARD_TIMEOUT_SEC}s ({SHARD_TIMEOUT_SEC/3600:.1f}h)")

    # Acquire lock — refuse to start if another forward run or replay is active
    active = spark.sql(f"""
        SELECT run_id, status FROM {METADATA_TABLE}
        WHERE status IN ('worklist_created', 'processing_complete', 'replay_in_progress')
          AND completed_ts IS NULL
    """).collect()
    # processing_complete with completed_ts NULL means Blob 2 finished but Blob 3
    # hasn't run yet — replay is still safe (different rows). worklist_created or
    # replay_in_progress means another writer is in-flight; abort.
    active_in_flight = [r for r in active if r['status'] in ('worklist_created', 'replay_in_progress')]
    if active_in_flight:
        raise RuntimeError(f"Another run is active: {active_in_flight}. Aborting to avoid conflicts.")

    if not DRY_RUN:
        spark.sql(f"""
            INSERT INTO {METADATA_TABLE}
                (run_id, worklist_table, status, created_ts, batch_tables, processed_events, completed_ts, history_tables)
            VALUES
                ('{REPLAY_ID}', NULL, 'replay_in_progress', current_timestamp(), NULL, NULL, NULL, NULL)
        """)

    # ---- Step 1: Resolve scope ----
    spark.sql(f"""
        CREATE OR REPLACE TABLE {scope_table} AS
        SELECT EVENT_ID, ADC_UPDT, STATUS AS v1_status,
               decompressor_version AS v1_dec_version,
               parser_version AS v1_parser_version,
               post_processor_version AS v1_pp_version
        FROM {TARGET_TABLE}
        WHERE {SCOPE_SQL}
    """)
    scope_count = spark.table(scope_table).count()
    print(f"Scope resolves to {scope_count:,} rows")

    if MAX_ROWS > 0 and scope_count > MAX_ROWS:
        raise RuntimeError(f"Scope ({scope_count:,}) exceeds MAX_ROWS ({MAX_ROWS:,}); abort")

    # ---- Step 2: ONE scan of c_cold -> materialize chunks partitioned by shard_key ----
    # Without this, each shard would re-scan the 3TB/29k-file c_cold.raw.mill_ce_blob
    # (no partitioning, no clustering) looking for its ~1/SHARDS of events.
    # This scan takes minutes; 8 concurrent copies do not finish in 2 hours.
    t0 = time.time()
    spark.sql(f"""
        CREATE OR REPLACE TABLE {chunks_table}
        USING DELTA
        PARTITIONED BY (shard_key)
        AS
        SELECT ce.EVENT_ID, ce.ADC_UPDT, ce.ENCNTR_ID, ce.Trust,
               ce.BLOB_SEQ_NUM, ce.BLOB_CONTENTS, ce.COMPRESSION_CD, ce.BLOB_LENGTH,
               ce.VALID_UNTIL_DT_TM, ce.VALID_FROM_DT_TM, ce.UPDT_DT_TM,
               ce.UPDT_ID, ce.UPDT_TASK, ce.UPDT_CNT, ce.UPDT_APPLCTX,
               ce.LAST_UTC_TS,
               CAST(pmod(xxhash64(ce.EVENT_ID), {SHARDS}) AS INT) AS shard_key
        FROM c_cold.raw.mill_ce_blob ce
        JOIN {scope_table} s
          ON ce.EVENT_ID = s.EVENT_ID AND ce.ADC_UPDT = s.ADC_UPDT
        WHERE ce.Trust = 'Barts'
          AND ce.VALID_UNTIL_DT_TM = TIMESTAMP '2100-12-31 00:00:00'
    """)
    chunks_count = spark.table(chunks_table).count()
    print(f"Materialized {chunks_count:,} raw chunk rows in {time.time()-t0:.1f}s")

    # ---- Step 3: Orphans (scope rows whose raw chunks didn't appear) ----
    spark.sql(f"""
        CREATE OR REPLACE TABLE {orphans_table} AS
        SELECT s.EVENT_ID, s.ADC_UPDT
        FROM {scope_table} s
        LEFT ANTI JOIN {chunks_table} c
          ON c.EVENT_ID = s.EVENT_ID AND c.ADC_UPDT = s.ADC_UPDT
    """)
    orphan_count = spark.table(orphans_table).count()
    print(f"Orphans (no raw found): {orphan_count:,}")

    if DRY_RUN:
        print("--- DRY RUN ---")
        print(f"Would process: {scope_count - orphan_count:,} (events with raw)")
        print(f"Would skip orphans: {orphan_count:,}")
        spark.sql(f"DROP TABLE {scope_table}")
        spark.sql(f"DROP TABLE {chunks_table}")
        spark.sql(f"DROP TABLE {orphans_table}")
        dbutils.notebook.exit(f"Dry run: scope={scope_count:,} orphans={orphan_count:,}")

    # ---- Step 4: Fan out shards ----
    def run_shard(sid):
        path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        try:
            return (sid, "SUCCESS", dbutils.notebook.run(path, SHARD_TIMEOUT_SEC, {
                "SCOPE_SQL": SCOPE_SQL, "REPLAY_ID": REPLAY_ID,
                "SHARDS": str(SHARDS), "SHARD_ID": str(sid),
                "TARGET_TABLE": TARGET_TABLE, "HISTORY_TABLE": HISTORY_TABLE,
                "STAGING_DB": STAGING_DB, "METADATA_TABLE": METADATA_TABLE,
                "DRY_RUN": "false", "MAX_ROWS": "0",
                "SHARD_TIMEOUT_SEC": str(SHARD_TIMEOUT_SEC),
            }))
        except Exception as e:
            return (sid, "FAILED", str(e))

    start = time.time()
    results = []
    with ThreadPoolExecutor(max_workers=min(MAX_CONCURRENT_SHARDS, SHARDS)) as ex:
        futures = {ex.submit(run_shard, i): i for i in range(SHARDS)}
        for fut in as_completed(futures):
            results.append(fut.result())
            print(f"shard {results[-1][0]}: {results[-1][1]}")
    print(f"Shard elapsed: {time.time()-start:.1f}s")

    # ---- Step 5: Collect shard batches and history ----
    batch_tables, history_tables = [], []
    for i in range(SHARDS):
        bt = f"{STAGING_DB}.replay_batch_{REPLAY_ID}_shard_{i:04d}"
        ht = f"{STAGING_DB}.replay_history_{REPLAY_ID}_shard_{i:04d}"
        if spark.catalog.tableExists(bt):
            batch_tables.append(bt)
        if spark.catalog.tableExists(ht):
            history_tables.append(ht)

    # MERGE shard batches -> bronze (idempotent on EVENT_ID + ADC_UPDT)
    if batch_tables:
        batch_union = " UNION ALL ".join(f"SELECT * FROM {t}" for t in batch_tables)
        spark.sql(f"""
            CREATE OR REPLACE TEMPORARY VIEW replay_batch_all AS
            {batch_union}
        """)
        spark.sql(f"""
            MERGE INTO {TARGET_TABLE} target
            USING replay_batch_all source
               ON target.EVENT_ID = source.EVENT_ID AND target.ADC_UPDT = source.ADC_UPDT
            WHEN MATCHED THEN UPDATE SET
                BLOB_TEXT = source.BLOB_TEXT,
                CONTENT_TYPE = source.CONTENT_TYPE,
                ENCODING = source.ENCODING,
                STATUS = source.STATUS,
                BINARY_SIZE = source.BINARY_SIZE,
                TEXT_LENGTH = source.TEXT_LENGTH,
                anon_text = NULL,
                raw_sha256 = source.raw_sha256,
                decompressor_version = source.decompressor_version,
                parser_version = source.parser_version,
                post_processor_version = source.post_processor_version
        """)
        print("MERGE into bronze complete")

    # Append shard histories (schema-aligned, fills extracted_date if target needs it)
    if history_tables:
        hist_target_schema = spark.table(HISTORY_TABLE).schema

        from functools import reduce
        hist_dfs = [spark.table(t) for t in history_tables if spark.catalog.tableExists(t)]
        if hist_dfs:
            combined_hist = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), hist_dfs)
            select_exprs = []
            for field in hist_target_schema:
                if field.name in combined_hist.columns:
                    select_exprs.append(F.col(field.name).cast(field.dataType).alias(field.name))
                elif field.name == "extracted_date":
                    select_exprs.append(F.current_date().cast(field.dataType).alias(field.name))
                elif field.name == "run_id":
                    select_exprs.append(F.lit(REPLAY_ID).cast(field.dataType).alias(field.name))
                else:
                    select_exprs.append(F.lit(None).cast(field.dataType).alias(field.name))
            aligned_hist = combined_hist.select(*select_exprs)
            aligned_hist.write.mode("append").insertInto(HISTORY_TABLE)
            print(f"Appended {aligned_hist.count():,} history rows")

    # Cleanup intermediate tables
    cleanup_targets = [scope_table, chunks_table, orphans_table] + batch_tables + history_tables
    for t in cleanup_targets:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {t}")
        except Exception as e:
            print(f"  Warning: could not drop {t}: {e}")

    # Release lock
    spark.sql(f"""
        UPDATE {METADATA_TABLE}
        SET status = 'replay_complete',
            completed_ts = current_timestamp(),
            batch_tables = '{",".join(batch_tables)}',
            history_tables = '{",".join(history_tables)}'
        WHERE run_id = '{REPLAY_ID}'
    """)
    print(f"Replay {REPLAY_ID} complete")
    dbutils.notebook.exit(f"Replay {REPLAY_ID}: merged {len(batch_tables)} batch tables")

# COMMAND ----------

# ==================== SHARD ====================
print(f"REPLAY SHARD {SHARD_ID}/{SHARDS}  ID={REPLAY_ID}")

# Read this shard's chunks from the materialized staging table (partition-pruned).
events_df = spark.sql(f"""
    SELECT EVENT_ID, ADC_UPDT, COLLECT_LIST(
        NAMED_STRUCT(
            'BLOB_SEQ_NUM', BLOB_SEQ_NUM,
            'BLOB_CONTENTS', BLOB_CONTENTS,
            'COMPRESSION_CD', COMPRESSION_CD,
            'BLOB_LENGTH', BLOB_LENGTH,
            'VALID_UNTIL_DT_TM', VALID_UNTIL_DT_TM,
            'VALID_FROM_DT_TM', VALID_FROM_DT_TM,
            'UPDT_DT_TM', UPDT_DT_TM,
            'UPDT_ID', UPDT_ID,
            'UPDT_TASK', UPDT_TASK,
            'UPDT_CNT', UPDT_CNT,
            'UPDT_APPLCTX', UPDT_APPLCTX,
            'LAST_UTC_TS', LAST_UTC_TS,
            'ADC_UPDT', ADC_UPDT,
            'ENCNTR_ID', ENCNTR_ID,
            'Trust', Trust
        )
    ) AS chunks_data
    FROM {chunks_table}
    WHERE shard_key = {SHARD_ID}
    GROUP BY EVENT_ID, ADC_UPDT
""")

n = events_df.count()
if n == 0:
    dbutils.notebook.exit(f"Shard {SHARD_ID}: no events")

desired = max(16, min(160, (n // 200) or 1))
events_df = events_df.repartition(desired, F.col("EVENT_ID"))

# Project first-chunk fields (same shape as forward path)
first = (events_df
    .select("EVENT_ID", "ADC_UPDT", "chunks_data",
            F.element_at(F.col("chunks_data"), 1).alias("first_chunk"))
    .select("EVENT_ID",
            F.col("first_chunk.ENCNTR_ID").alias("ENCNTR_ID"),
            F.col("first_chunk.Trust").alias("Trust"),
            "chunks_data",
            F.col("first_chunk.VALID_UNTIL_DT_TM").alias("VALID_UNTIL_DT_TM"),
            F.col("first_chunk.VALID_FROM_DT_TM").alias("VALID_FROM_DT_TM"),
            F.col("first_chunk.UPDT_DT_TM").alias("UPDT_DT_TM"),
            F.col("first_chunk.UPDT_ID").alias("UPDT_ID"),
            F.col("first_chunk.UPDT_TASK").alias("UPDT_TASK"),
            F.col("first_chunk.UPDT_CNT").alias("UPDT_CNT"),
            F.col("first_chunk.UPDT_APPLCTX").alias("UPDT_APPLCTX"),
            F.col("first_chunk.LAST_UTC_TS").alias("LAST_UTC_TS"),
            F.col("first_chunk.ADC_UPDT").alias("ADC_UPDT")))

# COMMAND ----------

# ---- UDF: process_blob_udf (verbatim from blob_processing_v2 with two changes:
#      history["run_id"] = REPLAY_ID, history["source"] = "replay") ----

_udf_schema = T.StructType([
    T.StructField("EVENT_ID", T.LongType(), True),
    T.StructField("ENCNTR_ID", T.LongType(), True),
    T.StructField("Trust", T.StringType(), True),
    T.StructField("VALID_UNTIL_DT_TM", T.TimestampType(), True),
    T.StructField("VALID_FROM_DT_TM", T.TimestampType(), True),
    T.StructField("UPDT_DT_TM", T.TimestampType(), True),
    T.StructField("UPDT_ID", T.LongType(), True),
    T.StructField("UPDT_TASK", T.LongType(), True),
    T.StructField("UPDT_CNT", T.LongType(), True),
    T.StructField("UPDT_APPLCTX", T.LongType(), True),
    T.StructField("LAST_UTC_TS", T.TimestampType(), True),
    T.StructField("ADC_UPDT", T.TimestampType(), True),
    T.StructField("BLOB_BINARY", T.BinaryType(), True),
    T.StructField("CONTENT_TYPE", T.StringType(), True),
    T.StructField("ENCODING", T.StringType(), True),
    T.StructField("BLOB_TEXT", T.StringType(), True),
    T.StructField("BINARY_SIZE", T.LongType(), True),
    T.StructField("TEXT_LENGTH", T.LongType(), True),
    T.StructField("STATUS", T.StringType(), True),
    T.StructField("anon_text", T.StringType(), True),
    T.StructField("raw_sha256", T.StringType(), True),
    T.StructField("decompressor_version", T.IntegerType(), True),
    T.StructField("parser_version", T.IntegerType(), True),
    T.StructField("post_processor_version", T.IntegerType(), True),
    T.StructField("history_json", T.StringType(), True),
    T.StructField("metrics", T.StringType(), True),
])

_history_json_schema = T.StructType([
    T.StructField("raw_sha256", T.StringType()),
    T.StructField("decompressor_version", T.IntegerType()),
    T.StructField("parser_version", T.IntegerType()),
    T.StructField("post_processor_version", T.IntegerType()),
    T.StructField("status", T.StringType()),
    T.StructField("truncation_flag", T.BooleanType()),
    T.StructField("truncation_reason", T.StringType()),
    T.StructField("ftfy_explain", T.StringType()),
    T.StructField("decompression_strategy", T.StringType()),
    T.StructField("run_id", T.StringType()),
    T.StructField("source", T.StringType()),
])

def _safe_int(x):
    if x is None or x == "":
        return None
    try:
        return int(x) if isinstance(x, (int, float)) else int(float(str(x)))
    except Exception:
        return None

# Module-level dict sentinel — see blob_processing_v2 for the cloudpickle/lru_cache rationale.
_PARSE_POOL_STATE = {"pool": None}

def _worker_parse_pool():
    if _PARSE_POOL_STATE["pool"] is None:
        _PARSE_POOL_STATE["pool"] = ParsePool(timeout=PARSE_TIMEOUT_SEC)
    return _PARSE_POOL_STATE["pool"]

@F.udf(returnType=_udf_schema)
def process_blob_udf(event_id, encntr_id, trust, chunks_data, valid_until, valid_from,
                     updt_dt, updt_id, updt_task, updt_cnt, updt_applctx,
                     last_utc, adc_updt):
    metrics = {"chunk_count": 0, "error": None, "parse_status": None}
    history = {
        "raw_sha256": None,
        "decompressor_version": DECOMPRESSOR_VERSION,
        "parser_version": PARSER_VERSION,
        "post_processor_version": POST_PROCESSOR_VERSION,
        "status": None, "truncation_flag": False, "truncation_reason": None,
        "ftfy_explain": None, "decompression_strategy": None,
        "run_id": REPLAY_ID, "source": "replay",
    }
    try:
        sorted_chunks = sorted(chunks_data, key=lambda x: x['BLOB_SEQ_NUM'] or 0)
        chunks = [c['BLOB_CONTENTS'] for c in sorted_chunks if c['BLOB_CONTENTS']]
        metrics["chunk_count"] = len(chunks)
        compression_cd = sorted_chunks[0]['COMPRESSION_CD'] if sorted_chunks else None
        blob_length = sorted_chunks[0]['BLOB_LENGTH'] if sorted_chunks else None

        raw_sha = raw_content_sha256([
            {'BLOB_SEQ_NUM': c['BLOB_SEQ_NUM'], 'BLOB_CONTENTS': c['BLOB_CONTENTS']}
            for c in sorted_chunks
        ])
        history["raw_sha256"] = raw_sha

        combined = b"".join(chunks)
        if len(combined) > MAX_BLOB_SIZE:
            status = f"Compressed Too Large: {len(combined)} bytes"
            history["status"] = status
            return Row(
                EVENT_ID=_safe_int(event_id), ENCNTR_ID=_safe_int(encntr_id), Trust=trust,
                VALID_UNTIL_DT_TM=valid_until, VALID_FROM_DT_TM=valid_from, UPDT_DT_TM=updt_dt,
                UPDT_ID=_safe_int(updt_id), UPDT_TASK=_safe_int(updt_task),
                UPDT_CNT=_safe_int(updt_cnt), UPDT_APPLCTX=_safe_int(updt_applctx),
                LAST_UTC_TS=last_utc, ADC_UPDT=adc_updt,
                BLOB_BINARY=None, CONTENT_TYPE=None, ENCODING=None, BLOB_TEXT=None,
                BINARY_SIZE=len(combined), TEXT_LENGTH=None, STATUS=status, anon_text=None,
                raw_sha256=raw_sha,
                decompressor_version=DECOMPRESSOR_VERSION,
                parser_version=PARSER_VERSION,
                post_processor_version=POST_PROCESSOR_VERSION,
                history_json=json.dumps(history, default=str),
                metrics=json.dumps(metrics),
            )

        decompressed, dec_err = decompress(combined, compression_cd, len(chunks), blob_length, metrics)
        history["decompression_strategy"] = metrics.get("decompression_strategy")
        if decompressed is None:
            status = dec_err or "Decompression failed"
            history["status"] = status
            return Row(
                EVENT_ID=_safe_int(event_id), ENCNTR_ID=_safe_int(encntr_id), Trust=trust,
                VALID_UNTIL_DT_TM=valid_until, VALID_FROM_DT_TM=valid_from, UPDT_DT_TM=updt_dt,
                UPDT_ID=_safe_int(updt_id), UPDT_TASK=_safe_int(updt_task),
                UPDT_CNT=_safe_int(updt_cnt), UPDT_APPLCTX=_safe_int(updt_applctx),
                LAST_UTC_TS=last_utc, ADC_UPDT=adc_updt,
                BLOB_BINARY=None, CONTENT_TYPE=None, ENCODING=None, BLOB_TEXT=None,
                BINARY_SIZE=None, TEXT_LENGTH=None, STATUS=status, anon_text=None,
                raw_sha256=raw_sha,
                decompressor_version=DECOMPRESSOR_VERSION,
                parser_version=PARSER_VERSION,
                post_processor_version=POST_PROCESSOR_VERSION,
                history_json=json.dumps(history, default=str),
                metrics=json.dumps(metrics),
            )

        if len(decompressed) > MAX_BLOB_SIZE:
            status = f"Decompressed too large: {len(decompressed)} bytes"
            history["status"] = status
            return Row(
                EVENT_ID=_safe_int(event_id), ENCNTR_ID=_safe_int(encntr_id), Trust=trust,
                VALID_UNTIL_DT_TM=valid_until, VALID_FROM_DT_TM=valid_from, UPDT_DT_TM=updt_dt,
                UPDT_ID=_safe_int(updt_id), UPDT_TASK=_safe_int(updt_task),
                UPDT_CNT=_safe_int(updt_cnt), UPDT_APPLCTX=_safe_int(updt_applctx),
                LAST_UTC_TS=last_utc, ADC_UPDT=adc_updt,
                BLOB_BINARY=None, CONTENT_TYPE=None, ENCODING=None, BLOB_TEXT=None,
                BINARY_SIZE=len(decompressed), TEXT_LENGTH=None, STATUS=status, anon_text=None,
                raw_sha256=raw_sha,
                decompressor_version=DECOMPRESSOR_VERSION,
                parser_version=PARSER_VERSION,
                post_processor_version=POST_PROCESSOR_VERSION,
                history_json=json.dumps(history, default=str),
                metrics=json.dumps(metrics),
            )

        text, content_type, encoding, parse_status = _worker_parse_pool().parse(decompressed, None)
        metrics["parse_status"] = parse_status

        if text and text.startswith("[") and "]" in text[:200]:
            cleaned = text
            pp_meta = {"truncation_flag": False, "truncation_reason": None,
                       "ftfy_explain": None, "ftfy_failed": False}
        else:
            cleaned, pp_meta = post_process(
                text or "", content_type or "", decompressed, blob_length,
                decompressor_flag=metrics.get("blob_length_mismatch", False)
            )
        if cleaned == "":
            cleaned = None
        history["ftfy_explain"] = pp_meta.get("ftfy_explain")
        history["truncation_flag"] = pp_meta.get("truncation_flag", False)
        history["truncation_reason"] = pp_meta.get("truncation_reason")

        if cleaned is None and content_type in BINARY_CONTENT_TYPES:
            status = "Binary content - not text"
        elif cleaned:
            if isinstance(cleaned, str) and cleaned.startswith("[") and "]" in cleaned[:200]:
                status = cleaned.split("]")[0][1:]
            else:
                status = "Decoded"
        else:
            status = "Failed to decode"
        history["status"] = status

        if cleaned:
            cleaned = cleaned.encode('utf-8', errors='ignore').decode('utf-8')
            if len(cleaned) > 1_000_000:
                cleaned = cleaned[:1_000_000]
                if not history["truncation_flag"]:
                    history["truncation_flag"] = True
                    history["truncation_reason"] = "1mb_cap"

        return Row(
            EVENT_ID=_safe_int(event_id), ENCNTR_ID=_safe_int(encntr_id), Trust=trust,
            VALID_UNTIL_DT_TM=valid_until, VALID_FROM_DT_TM=valid_from, UPDT_DT_TM=updt_dt,
            UPDT_ID=_safe_int(updt_id), UPDT_TASK=_safe_int(updt_task),
            UPDT_CNT=_safe_int(updt_cnt), UPDT_APPLCTX=_safe_int(updt_applctx),
            LAST_UTC_TS=last_utc, ADC_UPDT=adc_updt,
            BLOB_BINARY=None, CONTENT_TYPE=content_type, ENCODING=encoding, BLOB_TEXT=cleaned,
            BINARY_SIZE=len(decompressed), TEXT_LENGTH=len(cleaned) if cleaned else None,
            STATUS=status, anon_text=None,
            raw_sha256=raw_sha,
            decompressor_version=DECOMPRESSOR_VERSION,
            parser_version=PARSER_VERSION,
            post_processor_version=POST_PROCESSOR_VERSION,
            history_json=json.dumps(history, default=str),
            metrics=json.dumps(metrics),
        )
    except Exception as e:
        metrics["error"] = str(e)[:500]
        history["status"] = f"Error: {str(e)[:200]}"
        return Row(
            EVENT_ID=_safe_int(event_id), ENCNTR_ID=_safe_int(encntr_id), Trust=trust,
            VALID_UNTIL_DT_TM=valid_until, VALID_FROM_DT_TM=valid_from, UPDT_DT_TM=updt_dt,
            UPDT_ID=_safe_int(updt_id), UPDT_TASK=_safe_int(updt_task),
            UPDT_CNT=_safe_int(updt_cnt), UPDT_APPLCTX=_safe_int(updt_applctx),
            LAST_UTC_TS=last_utc, ADC_UPDT=adc_updt,
            BLOB_BINARY=None, CONTENT_TYPE=None, ENCODING=None, BLOB_TEXT=None,
            BINARY_SIZE=None, TEXT_LENGTH=None, STATUS=history["status"], anon_text=None,
            raw_sha256=None,
            decompressor_version=DECOMPRESSOR_VERSION,
            parser_version=PARSER_VERSION,
            post_processor_version=POST_PROCESSOR_VERSION,
            history_json=json.dumps(history, default=str),
            metrics=json.dumps(metrics),
        )

# COMMAND ----------

# ---- Run UDF, project history_json -> hp before cache, write batch + history ----

raw_stage_table = f"{STAGING_DB}.replay_raw_{REPLAY_ID}_shard_{SHARD_ID:04d}"

(first
      .select(process_blob_udf(
          F.col("EVENT_ID"), F.col("ENCNTR_ID"), F.col("Trust"),
          F.col("chunks_data"),
          F.col("VALID_UNTIL_DT_TM"), F.col("VALID_FROM_DT_TM"), F.col("UPDT_DT_TM"),
          F.col("UPDT_ID"), F.col("UPDT_TASK"), F.col("UPDT_CNT"), F.col("UPDT_APPLCTX"),
          F.col("LAST_UTC_TS"), F.col("ADC_UPDT")
      ).alias("r"))
      .select("r.*")
      .write.mode("overwrite").saveAsTable(raw_stage_table))

  # Read back from disk — now deterministic. delta.optimizeWrite (enabled in the Spark
  # config near the top of this notebook) sizes output files, so no manual .repartition().
raw = (spark.table(raw_stage_table)
      .withColumn("hp", F.from_json("history_json", _history_json_schema))
      .drop("history_json", "metrics"))

batch_table = f"{STAGING_DB}.replay_batch_{REPLAY_ID}_shard_{SHARD_ID:04d}"
history_table_shard = f"{STAGING_DB}.replay_history_{REPLAY_ID}_shard_{SHARD_ID:04d}"

batch_df = raw.drop("hp")
batch_df.write.mode("overwrite").saveAsTable(batch_table)

history_df = (raw
      .filter(F.col("hp").isNotNull())
      .select("EVENT_ID", "ADC_UPDT",
              "hp.raw_sha256", "hp.decompressor_version", "hp.parser_version",
              "hp.post_processor_version", "hp.status", "hp.truncation_flag",
              "hp.truncation_reason", "hp.ftfy_explain", "hp.decompression_strategy",
              "hp.run_id", "hp.source"))
history_df.write.mode("overwrite").saveAsTable(history_table_shard)

  # Drop this shard's raw staging table (the orchestrator's cleanup only knows about
  # scope/chunks/orphans/batch/history tables, not replay_raw_*).
spark.sql(f"DROP TABLE IF EXISTS {raw_stage_table}")


dbutils.notebook.exit(f"Shard {SHARD_ID}: {n} events -> {batch_table}")
