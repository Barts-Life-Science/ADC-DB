# Databricks notebook source
# MAGIC %md
# MAGIC # Pathology Incremental Embed Loop (pathology_embed_increment)
# MAGIC
# MAGIC Serverless weekly loop: discover new free-text CONTEXT KEYS from mill/raw → embed (capped) →
# MAGIC remap into the concept maps → additively back-fill map_pathology. The WHOLE loop runs INLINE in the
# MAGIC Map Pipeline family job via the `CC/mappipeline_map_pathology_section` section, which `%run`s this
# MAGIC module and calls the stage fns directly (see the ## INTEGRATION cell at the end). Authored + tested
# MAGIC against 8_dev; the user promotes to prod. (SUPERSEDED: the earlier paste-into-nomenclature plan.)
# MAGIC
# MAGIC **Changelog**
# MAGIC - v0.1 — config + run-log DDL
# MAGIC - v0.5 — stage 3 remap_keys (serverless Spark cross-join dot-product; NO driver numpy; test + context-scoped result)
# MAGIC - v0.7 — stage 5 apply_to_map_pathology (additive backfill via map re-join, no source re-scan; correction abort; 6-key MERGE)
# MAGIC - v1.0 — Task 11: human-gate AOAI smoke (if False) + paste-ready packaging + handoff.
# MAGIC - v2.3 — lazy openai import (_get_embedding_client) so %run works on any compute; import-time probe removed.

# COMMAND ----------

# ── pathology_embed_increment — config (dev) ────────────────────────────────────
# INTEGRATION: this module is `%run` from the Map Pipeline family job; the
# CC/mappipeline_map_pathology_section section calls the stage fns INLINE (see the
# ## INTEGRATION cell at the end). Authored + tested against 8_dev; user promotes to prod.
ENVIRONMENT       = "prod"
MAP_SCHEMA        = "3_lookup.omop"                         # prod: 3_lookup.omop (loop tables promoted there; see pathology_embed_promote_to_prod)
EMBEDDINGS_TABLE  = "3_lookup.embeddings.terms"
QUEUE             = f"{MAP_SCHEMA}.pathology_embed_queue"
TEST_MAP          = f"{MAP_SCHEMA}.pathology_test_concept_map"
RESULT_MAP        = f"{MAP_SCHEMA}.pathology_result_concept_map"
TEST_INDEX        = f"{MAP_SCHEMA}.concept_index_test"
RESULT_INDEX      = f"{MAP_SCHEMA}.concept_index_result"
ANSWER_LISTS      = f"{MAP_SCHEMA}.concept_answer_lists"
TEST_OVERRIDES    = f"{MAP_SCHEMA}.pathology_test_map_overrides"
RESULT_OVERRIDES  = f"{MAP_SCHEMA}.pathology_result_map_overrides"
EXCL_TBL          = f"{MAP_SCHEMA}.pathology_result_value_exclusions"   # 32 rows, has `pattern` (Task 8 result_status EXCL_REGEX)
UNIT_MAP          = f"{MAP_SCHEMA}.pathology_unit_map"                  # 49 rows, has `unit_source_value` (Task 8 unit backfill)
FREQ_TABLE        = f"{MAP_SCHEMA}.pathology_term_frequency"  # DEPRECATED (rde-seeded); the loop uses FREQ_TABLE_MILLRAW (Task 2)
CONCEPT           = "3_lookup.omop.concept"
RUN_LOG           = f"{MAP_SCHEMA}.pathology_embed_run_log"
# map_pathology coupling — the rebuild gate now lives in the Map-Pipeline-owned flag table
# pathology_map_rebuild_flag (single-row), which REPLACES the retired map_pathology_state table.
MP_TARGET         = "4_prod.bronze.map_pathology"         # prod: 4_prod.bronze.map_pathology
MP_REBUILD_FLAG   = f"{MAP_SCHEMA}.pathology_map_rebuild_flag"   # dev 8_dev.omop.* ; prod 3_lookup.omop.* — single-row (id INT, rebuild_flagged BOOLEAN); the Map Pipeline section owns/creates it

# thresholds (verbatim from the map builds)
SIM_HIGH          = 0.85   # test side
SIM_LOW           = 0.70
SIM_HIGH_LOINC    = 0.80   # result side
SIM_HIGH_SNOMED   = 0.82

# Tiers the incremental RESULT backfill admits into bronze. The open-set arms (auto_anchor/auto_value/
# auto_genpos) were added 2026-06-30 so the loop consumes the SAME result tiers the FULL_REBUILD does
# (promotion runbook §C). Used by apply_to_map_pathology Pass A (rj result join) + Pass B. The TEST-map
# joins stay ('curated','auto_high','auto_low') — open-set arms produce RESULT concepts only.
CONSUMED_TIERS = "('curated','auto_high','auto_low','auto_anchor','auto_value','auto_genpos')"

# cost cap (Task 4)
MAX_EMBED_TERMS   = 20_000     # per-run hard cap on distinct texts sent to AOAI
EST_USD_PER_TERM  = 0.0000013  # text-embedding-3-large ~ $0.13/1M tokens; ~10 tok/term -> rough upper bound

# P0 grain + lossless key (2026-06-30): the RESULT arm now embeds/keys on the BARE result text (the open-set
# scoring grain that Stage 3b scores against), and result_normalized is persisted LOSSLESSLY (no downstream
# substring_index recovery, which truncated values containing ' | '). Two queue columns:
#   result_normalized -- lossless result identity (NULL for kind='test')
#   embed_text        -- the text actually embedded + scored: result_normalized for results, term for tests
# Idempotent ALTER (the prod migration notebook / the fixture create the queue; this just back-adds the cols
# on an existing queue). A missing table here is fine (caught + ignored) -- the creator makes it with the cols.
for _qcol in ("result_normalized STRING", "embed_text STRING"):
    try:
        spark.sql(f"ALTER TABLE {QUEUE} ADD COLUMNS ({_qcol})")
        print(f"{QUEUE}: added {_qcol}")
    except Exception as _e:
        print(f"{QUEUE} add-column note ({_qcol}): {str(_e)[:100]}")  # already present / table absent -> fine

def utcnow():
    # NOTE: import locally (NOT `import datetime` at module scope). When this module is %run from a
    # host notebook that did `from datetime import datetime` (the class), a module-level `import datetime`
    # would REBIND the host's `datetime` global to the module, breaking every downstream datetime.now().
    from datetime import datetime as _dt, timezone as _tz
    return _dt.now(_tz.utc)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RUN_LOG} (
  run_id                 STRING,
  run_ts                 TIMESTAMP,
  since_watermark        TIMESTAMP,
  n_missing_test_keys    BIGINT,
  n_missing_result_keys  BIGINT,
  n_vector_ready         BIGINT,
  n_needs_embed          BIGINT,
  n_embedded             BIGINT,
  n_deferred             BIGINT,
  est_cost_usd           DOUBLE,
  n_additive_keys        BIGINT,
  n_correction_keys      BIGINT,
  n_map_backfilled       BIGINT,
  n_map_inserted         BIGINT,
  n_bbv_result_override_skipped BIGINT,
  rebuild_flagged        BOOLEAN,
  aborted_for_correction BOOLEAN,
  dry_run                BOOLEAN,
  duration_s             DOUBLE
) USING DELTA
""")
print(f"Run-log ready: {RUN_LOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 — pathology_term_frequency repointed to mill/raw (FREQ_TABLE_MILLRAW)
# MAGIC Builds a term/event_count table for embed-priority ordering, sourced from mill/raw (NOT rde).
# MAGIC term shapes are byte-identical to pathology_embed_queue.term so the Task-4 LOWER(term) join matches.

# COMMAND ----------

FREQ_TABLE_MILLRAW = f"{MAP_SCHEMA}.pathology_term_frequency_millraw"

def build_term_frequency_millraw():
    # HEAVY BUILD (~5 min; scans ~4.4B-row mill_clinical_event). Refresh OUT-OF-BAND on a cluster, not
    # every loop run — the loop tolerates a stale/partial freq table (priority-only, Task-4 LEFT JOIN, ec=0 sorts last).
    # term shapes IDENTICAL to the queue/rde builder: result-kind = '<desc> | <lower/trim/collapse result>',
    # test-kind = '<code> | <desc>'. Separator ' | '. Non-numeric gate via RLIKE (== map_pathology's
    # ResultNumeric definition). Used ONLY for embed PRIORITY ordering in Task 4 (LEFT JOIN; ec=0 sorts last).
    spark.sql(f"""
    CREATE OR REPLACE TABLE {FREQ_TABLE_MILLRAW} USING DELTA AS
    WITH
    m1 AS (   -- master dedup: one row per (WkgCode,TFCCode), current by LastUpdateDT
      SELECT WkgCode, TFCCode,
             COALESCE(TFCDesc_Full, TFCDesc_Rep, TFCDesc_WP) AS desc_full
      FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.WkgCode, m.TFCCode
                 ORDER BY m.LastUpdateDT DESC NULLS LAST) AS rn
        FROM 4_prod.raw.path_master_resultable m
      ) WHERE rn = 1
    ),
    raw_results AS (
      SELECT concat_ws(' | ', lower(m1.desc_full), trim(regexp_replace(lower(rl.TFCValue), '\\\\s+', ' '))) AS term,
             COUNT(*) AS n
      FROM 4_prod.raw.path_patient_resultlevel rl
      JOIN m1 ON m1.WkgCode = rl.WkgCode AND m1.TFCCode = rl.TFCCode
      WHERE rl.TFCCode IS NOT NULL AND m1.desc_full IS NOT NULL
        AND rl.TFCValue IS NOT NULL
        AND NOT (rl.TFCValue RLIKE '^\\\\s*[<>]?=?\\\\s*-?[0-9.]+\\\\s*$')
        AND trim(regexp_replace(lower(rl.TFCValue), '\\\\s+', ' ')) <> ''
      GROUP BY 1
    ),
    raw_tests AS (
      SELECT concat_ws(' | ', lower(rl.TFCCode), lower(m1.desc_full)) AS term, COUNT(*) AS n
      FROM 4_prod.raw.path_patient_resultlevel rl
      JOIN m1 ON m1.WkgCode = rl.WkgCode AND m1.TFCCode = rl.TFCCode
      WHERE rl.TFCCode IS NOT NULL AND m1.desc_full IS NOT NULL
      GROUP BY 1
    ),
    linked_base AS (   -- pathology isolation: EVENT_CLASS_CD IN (233,236) AND catalog type 2513
      SELECT o.ORDER_MNEMONIC AS code, cv.DESCRIPTION AS descr, ce.RESULT_VAL
      FROM 4_prod.raw.mill_clinical_event ce
      LEFT JOIN 3_lookup.mill.mill_order_catalog oc ON oc.CATALOG_CD = ce.CATALOG_CD
      LEFT JOIN 4_prod.raw.mill_orders o ON o.ORDER_ID = ce.ORDER_ID
      LEFT JOIN 3_lookup.mill.mill_code_value cv ON cv.CODE_VALUE = ce.EVENT_CD
      WHERE ce.EVENT_CLASS_CD IN (233, 236) AND oc.CATALOG_TYPE_CD = 2513
    ),
    linked_results AS (
      SELECT concat_ws(' | ', lower(descr), trim(regexp_replace(lower(RESULT_VAL), '\\\\s+', ' '))) AS term,
             COUNT(*) AS n
      FROM linked_base
      WHERE descr IS NOT NULL AND RESULT_VAL IS NOT NULL
        AND NOT (RESULT_VAL RLIKE '^\\\\s*[<>]?=?\\\\s*-?[0-9.]+\\\\s*$')
        AND trim(regexp_replace(lower(RESULT_VAL), '\\\\s+', ' ')) <> ''
      GROUP BY 1
    ),
    linked_tests AS (
      SELECT concat_ws(' | ', lower(code), lower(descr)) AS term, COUNT(*) AS n
      FROM linked_base WHERE code IS NOT NULL AND descr IS NOT NULL
      GROUP BY 1
    )
    SELECT term, SUM(n) AS event_count
    FROM (SELECT * FROM raw_results UNION ALL SELECT * FROM raw_tests
          UNION ALL SELECT * FROM linked_results UNION ALL SELECT * FROM linked_tests)
    GROUP BY term
    """)
    n = spark.sql(f"SELECT COUNT(*) c FROM {FREQ_TABLE_MILLRAW}").first()["c"]
    print(f"Built {FREQ_TABLE_MILLRAW}: {n:,} terms")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 — Stage 1: discover_new_keys
# MAGIC Finds missing concept-map CONTEXT KEYS from mill/raw (key-first), classifies each as
# MAGIC vector_ready (text already embedded) or pending (needs AOAI), and enqueues them.
# MAGIC Scoped kind IN ('test','result'). Re-attempts previously-unmapped keys only when vector_ready.

# COMMAND ----------

def discover_new_keys(since_watermark, dry_run=False):
    """Stage 1. Find concept-map context keys present in source (ADC_UPDT > since_watermark) that are
    NOT already mapped to a non-NULL concept AND not already embedded+queued; classify each by whether
    its embedding text already exists. Re-attempts previously-unmapped keys ONLY when their text is
    already embedded (zero-cost remap retry). INSERTs new keys into QUEUE (status vector_ready|pending).
    Idempotent: re-running inserts nothing new. Scoped to kind IN ('test','result')."""
    wm = f"TIMESTAMP'{since_watermark}'"
    discover_sql = f"""
    WITH
    emb AS (SELECT DISTINCT LOWER(term) AS term FROM {EMBEDDINGS_TABLE} WHERE embedding_vector IS NOT NULL),
    -- linked (CERNER) source, pathology-isolated; description = cv.DESCRIPTION (NOT mnemonic)
    linked_base AS (
      SELECT 'CERNER_TESTCODE' AS code_system, o.ORDER_MNEMONIC AS code, cv.DESCRIPTION AS description,
             ce.RESULT_VAL AS result_txt, ce.ADC_UPDT AS adc
      FROM 4_prod.raw.mill_clinical_event ce
      LEFT JOIN 3_lookup.mill.mill_order_catalog oc ON oc.CATALOG_CD = ce.CATALOG_CD
      LEFT JOIN 4_prod.raw.mill_orders o ON o.ORDER_ID = ce.ORDER_ID
      LEFT JOIN 3_lookup.mill.mill_code_value cv ON cv.CODE_VALUE = ce.EVENT_CD
      WHERE ce.EVENT_CLASS_CD IN (233, 236) AND oc.CATALOG_TYPE_CD = 2513 AND ce.ADC_UPDT > {wm}
    ),
    -- raw (TFC) source; description = COALESCE(...). CONTROLLER DECISION: watermark = rl.ADC_UPDT alone
    -- (no samplelevel join) — discovery needs only DISTINCT keys (from rl/m1, never sl), so the deduped
    -- sl1 join map_pathology uses is unnecessary here and an un-deduped sl join would fan out.
    raw_m1 AS (
      SELECT WkgCode, TFCCode, COALESCE(TFCDesc_Full, TFCDesc_Rep, TFCDesc_WP) AS descr
      FROM (SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.WkgCode, m.TFCCode
              ORDER BY m.LastUpdateDT DESC NULLS LAST) rn FROM 4_prod.raw.path_master_resultable m) WHERE rn=1
    ),
    raw_base AS (
      SELECT 'TFC' AS code_system, rl.TFCCode AS code, raw_m1.descr AS description,
             rl.TFCValue AS result_txt, rl.ADC_UPDT AS adc
      FROM 4_prod.raw.path_patient_resultlevel rl
      JOIN raw_m1 ON raw_m1.WkgCode = rl.WkgCode AND raw_m1.TFCCode = rl.TFCCode
      WHERE raw_m1.descr IS NOT NULL AND rl.ADC_UPDT > {wm}
    ),
    src_test AS (
      SELECT DISTINCT code_system, code, description,
             concat_ws(' | ', lower(code), lower(description)) AS term
      FROM (SELECT code_system, code, description FROM linked_base
            UNION ALL SELECT code_system, code, description FROM raw_base)
      WHERE code IS NOT NULL AND description IS NOT NULL
    ),
    src_result AS (
      SELECT DISTINCT code_system, code, description, result_normalized,
             concat_ws(' | ', lower(description), result_normalized) AS term
      FROM (
        SELECT code_system, code, description,
               LOWER(TRIM(REGEXP_REPLACE(result_txt,'\\\\s+',' '))) AS result_normalized
        FROM (SELECT code_system, code, description, result_txt FROM linked_base
              UNION ALL SELECT code_system, code, description, result_txt FROM raw_base)
        WHERE description IS NOT NULL AND result_txt IS NOT NULL
          AND NOT (result_txt RLIKE '^\\\\s*[<>]?=?\\\\s*-?[0-9.]+\\\\s*$')
      )
      -- drop sentinel/punctuation-only values (?, -, :, n/a) that never carry a mappable concept
      WHERE result_normalized <> '' AND result_normalized RLIKE '[a-z0-9]'
    ),
    miss_test AS (
      SELECT s.code_system, s.code, s.description, CAST(NULL AS STRING) AS result_normalized, s.term, 'test' AS kind
      FROM src_test s
      LEFT ANTI JOIN (SELECT code_system, code, description FROM {TEST_MAP} WHERE measurement_concept_id IS NOT NULL) m
        ON m.code_system=s.code_system AND m.code=s.code AND m.description=s.description
      -- Idempotency: exclude keys ALREADY in the queue under ANY status (pending/vector_ready/done/deferred).
      -- A pending/deferred key is already queued for embed; re-discovering it would blind-append a duplicate
      -- (mode=append, no uniqueness constraint). NOTE: re-attempting an already-embedded-but-'unmapped' key
      -- when a better concept candidate later appears is OUT OF SCOPE here (it would be a separate
      -- 'flip done->vector_ready' re-remap pass) — see tracked follow-up.
      LEFT ANTI JOIN (SELECT code_system, code, description FROM {QUEUE} WHERE kind='test') q
        ON q.code_system=s.code_system AND q.code=s.code AND q.description=s.description
    ),
    miss_result AS (
      SELECT s.code_system, s.code, s.description, s.result_normalized, s.term, 'result' AS kind
      FROM src_result s
      LEFT ANTI JOIN (SELECT code_system, code, description, result_normalized FROM {RESULT_MAP} WHERE value_as_concept_id IS NOT NULL) m
        ON m.code_system=s.code_system AND m.code=s.code AND m.description=s.description AND m.result_normalized=s.result_normalized
      -- Idempotency: exclude keys ALREADY in the queue under ANY status (see miss_test note above).
      -- I2 FIX: match on the FULL term (lossless), NOT substring_index(-1) (which is lossy when the
      -- result value contains ' | ' → those keys never matched their own queue row and re-appended
      -- every run). src_result.term = concat_ws(' | ', lower(description), result_normalized), and the
      -- queue stores that exact term, so LOWER(q.term)=LOWER(s.term) is an exact identity match.
      LEFT ANTI JOIN (SELECT code_system, code, description, LOWER(term) AS qterm FROM {QUEUE} WHERE kind='result') q
        ON q.code_system=s.code_system AND q.code=s.code AND q.description=s.description AND q.qterm=LOWER(s.term)
    ),
    all_missing AS (SELECT * FROM miss_test UNION ALL SELECT * FROM miss_result),
    classified AS (
      -- term_norm: legacy column. The loop keys on LOWER(term); the bulk pipeline (Task 14h) populated
      -- term_norm with a richer renormalized form. We write LOWER(term) here (harmless for the loop) —
      -- if any external consumer relies on the Task-14h semantics, reconcile separately (tracked follow-up).
      -- P0: embed_text = the BARE result text for results (open-set scoring grain), the context term for
      -- tests. Status keys on embed_text (the text that will actually be embedded), NOT term.
      SELECT a.*, LOWER(a.term) AS term_norm,
        CASE WHEN a.kind='result' THEN a.result_normalized ELSE a.term END AS embed_text,
        CASE WHEN e.term IS NOT NULL THEN 'vector_ready' ELSE 'pending' END AS status
      FROM all_missing a
      LEFT JOIN emb e ON LOWER(CASE WHEN a.kind='result' THEN a.result_normalized ELSE a.term END) = e.term
    )
    SELECT * FROM classified
    """
    df = spark.sql(discover_sql)
    agg = {(r["kind"], r["status"]): r["n"] for r in
           df.groupBy("kind","status").count().withColumnRenamed("count","n").collect()}
    n_test  = builtins.sum(v for (k,_),v in agg.items() if k=="test")
    n_result= builtins.sum(v for (k,_),v in agg.items() if k=="result")
    n_vr    = builtins.sum(v for (_,s),v in agg.items() if s=="vector_ready")
    n_pend  = builtins.sum(v for (_,s),v in agg.items() if s=="pending")
    if not dry_run:
        # QUEUE now carries result_normalized (lossless result identity) + embed_text (the canonical
        # embed/score key: bare result text for results, context term for tests). Downstream embed_pending_capped
        # + Stage 3b key on embed_text -- never substring_index (which truncated ' | '-bearing result values).
        (df.select("code_system","code","description","term","kind","status","term_norm",
                   "result_normalized","embed_text")
           .write.format("delta").mode("append").saveAsTable(QUEUE))
    return {"n_missing_test_keys": n_test, "n_missing_result_keys": n_result,
            "n_vector_ready": n_vr, "n_needs_embed": n_pend}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 — Stage 2: embed_pending_capped (+ reused AOAI worker)
# MAGIC Worker helpers copied verbatim from pathology_omop_02_bronze_pipeline (serverless-safe secret
# MAGIC fetch; non-fatal probe). embed_pending_capped embeds DISTINCT pending texts (freq-ordered, capped),
# MAGIC flips pending->vector_ready (text now embedded) and capped overflow pending->deferred.

# COMMAND ----------

# ── Reused AOAI worker (from pathology_omop_02_bronze_pipeline) ──────────────────────────────────────
# v2.3 CHANGE: openai is imported LAZILY (inside _get_embedding_client, called only by _embed_one_batch),
# so `%run` of this module succeeds on ANY compute with zero env-prep. `import openai` + the AOAI client +
# the secret fetch happen ONLY when an actual embed runs (where openai + the adc_store secret are required
# anyway). The old import-time client + non-fatal probe are removed (nothing AOAI runs at import).
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys, os, gc, time, glob, uuid, json, socket
import builtins
from pyspark.sql.types import (StructType, StructField, StringType, ArrayType,
                               FloatType, TimestampType)
from pyspark.sql.functions import current_timestamp

AOAI_ENDPOINT    = "https://benja-m6p4lv77-eastus2.cognitiveservices.azure.com"
AOAI_API_VERSION = "2023-05-15"
AOAI_DEPLOYMENT  = "text-embedding-3-large"
EMBEDDING_MODEL  = AOAI_DEPLOYMENT

_EMBED_CLIENT = None   # memoised; built on first real embed via _get_embedding_client()
def _get_embedding_client():
    """Lazily import openai, fetch the AOAI key, and build the AzureOpenAI client. Called ONLY from
    _embed_one_batch (the sole AOAI consumer) — so merely %run-importing this module needs neither
    openai installed nor the secret scope reachable. Memoised: built once per session."""
    global _EMBED_CLIENT
    if _EMBED_CLIENT is not None:
        return _EMBED_CLIENT
    from openai import AzureOpenAI   # lazy: only when an embed actually runs
    try:
        _key = dbutils.secrets.get(scope="adc_store", key="barts_global_key")
    except NameError:
        from databricks.sdk import WorkspaceClient
        _key = WorkspaceClient().dbutils.secrets.get(scope="adc_store", key="barts_global_key")
    _EMBED_CLIENT = AzureOpenAI(api_key=_key, api_version=AOAI_API_VERSION, azure_endpoint=AOAI_ENDPOINT)
    return _EMBED_CLIENT

BATCH_SIZE          = 256
MAX_WORKERS         = 16
MAX_RETRIES         = 6
FETCH_BUFFER_TERMS  = 32_768
PROGRESS_EVERY      = 16
# COMMIT_EVERY_BATCHES removed — commits now fire per-buffer.

MAX_CONSECUTIVE_FAILURES  = 3
STAGING_SCHEMA            = "3_lookup.embeddings"  # OGR_NO_DEV_V1/path_embed
STAGING_VOLUME_NAME       = "staging_pathology"
STAGING_PATH              = f"/Volumes/3_lookup/embeddings/{STAGING_VOLUME_NAME}"
LOCK_FILE                 = f"{STAGING_PATH}/.embed_lock"
LOCK_STALE_SECONDS        = 300

print(f"AOAI endpoint:     {AOAI_ENDPOINT}")
print(f"Deployment:        {AOAI_DEPLOYMENT}")
print(f"Embed config:      batch_size={BATCH_SIZE}, max_workers={MAX_WORKERS}, "
      f"buffer={FETCH_BUFFER_TERMS:,}, commit_per_buffer=True")
print(f"Staging path:      {STAGING_PATH}")
print(f"Lock file:         {LOCK_FILE}")

# ── Staging volume (verbatim) ───────────────────────────────────────────────────────────────────────
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {STAGING_SCHEMA}.{STAGING_VOLUME_NAME}")
os.makedirs(STAGING_PATH, exist_ok=True)
print(f"Staging volume ready: {STAGING_PATH}")
print(f"Files already staged: {len(glob.glob(STAGING_PATH + '/*.parquet'))}")

# ── Lock helpers (verbatim) ─────────────────────────────────────────────────────────────────────────
SESSION_ID = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"

def _read_lock():
    if not os.path.exists(LOCK_FILE):
        return None
    try:
        with open(LOCK_FILE) as f:
            return json.load(f)
    except Exception:
        return None

def acquire_lock():
    existing = _read_lock()
    if existing:
        hb_age = time.time() - existing.get("heartbeat_ts", 0)
        if hb_age < LOCK_STALE_SECONDS:
            raise RuntimeError(
                f"\n*** ANOTHER SESSION IS RUNNING — REFUSING TO START ***\n"
                f"  session_id  : {existing.get('session_id')}\n"
                f"  started_utc : {existing.get('started_utc')}\n"
                f"  last_hb_ago : {hb_age:.0f}s ago (stale = {LOCK_STALE_SECONDS}s)\n"
                f"\nIf you're SURE no other session is active, delete the lock:\n"
                f"  dbutils.fs.rm('{LOCK_FILE}')"
            )
        print(f"[warn] stale lock from {existing.get('session_id')} "
              f"({hb_age:.0f}s ago) — taking over.")
    payload = {
        "session_id":   SESSION_ID,
        "started_utc":  utcnow().isoformat(),
        "heartbeat_ts": time.time(),
        "host":         socket.gethostname(),
    }
    with open(LOCK_FILE, "w") as f:
        json.dump(payload, f)
    print(f"✓ Lock acquired: {SESSION_ID}")

def heartbeat_lock():
    existing = _read_lock()
    if not existing or existing.get("session_id") != SESSION_ID:
        raise RuntimeError(f"Lock no longer owned by this session (now {existing}). Aborting.")
    existing["heartbeat_ts"] = time.time()
    with open(LOCK_FILE, "w") as f:
        json.dump(existing, f)

def release_lock():
    existing = _read_lock()
    if existing and existing.get("session_id") == SESSION_ID:
        try:
            os.remove(LOCK_FILE)
            print(f"✓ Lock released: {SESSION_ID}")
        except Exception as e:
            print(f"[warn] couldn't release lock: {e}")

# ── Transient classification + batch embed + parquet staging (verbatim) ───────────────────────────────
_TRANSIENT_SPARK_ERRORS = (
    "TEMPORARILY_UNAVAILABLE", "UNAVAILABLE", "RETRIES_EXCEEDED",
    "DEADLINE_EXCEEDED", "_InactiveRpcError",
    "channel closed", "Connection reset", "Connection refused", "EOF occurred",
)
def _is_transient(e: Exception) -> bool:
    return any(s in str(e) for s in _TRANSIENT_SPARK_ERRORS)

def _embed_one_batch(chunk: List[str]) -> List[List[float]]:
    delay = 1.0
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = _get_embedding_client().embeddings.create(
                model=AOAI_DEPLOYMENT,
                input=chunk,
                encoding_format="float",
            )
            return [d.embedding for d in resp.data]
        except Exception as e:
            last_exc = e
            time.sleep(delay)
            delay = builtins.min(delay * 2, 30.0)
    raise RuntimeError(f"embedding batch failed after {MAX_RETRIES} retries: {last_exc}")

_PA_SCHEMA = pa.schema([
    pa.field("term",             pa.string()),
    pa.field("embedding_vector", pa.list_(pa.float32())),
    pa.field("model_version",    pa.string()),
    pa.field("created_at",       pa.timestamp("us")),
    pa.field("embedded_at",      pa.timestamp("us")),
    pa.field("ADC_UPDT",         pa.timestamp("us")),
])

def _stage_one_batch_to_parquet(terms: List[str], vectors: List[List[float]]) -> str:
    now = utcnow().replace(tzinfo=None)
    n = len(terms)
    table = pa.table({
        "term":             pa.array(terms, type=pa.string()),
        "embedding_vector": pa.array(vectors, type=pa.list_(pa.float32())),
        "model_version":    pa.array([EMBEDDING_MODEL] * n, type=pa.string()),
        "created_at":       pa.array([now] * n, type=pa.timestamp("us")),
        "embedded_at":      pa.array([now] * n, type=pa.timestamp("us")),
        "ADC_UPDT":         pa.array([now] * n, type=pa.timestamp("us")),
    }, schema=_PA_SCHEMA)
    fname = f"chunk_{int(time.time()*1000)}_{uuid.uuid4().hex[:10]}.parquet"
    fpath = os.path.join(STAGING_PATH, fname)
    pq.write_table(table, fpath, compression="snappy")
    return fname

def _worker_embed_and_stage(batch_terms: List[str]) -> Tuple[int, str, float]:
    # Belt-and-braces canonicalization: lowercase the batch before embed + stage.
    # Fetch queries already lowercase via LOWER(...) but this guarantees the
    # written `term` column is canonical even if a caller bypasses the fetcher.
    batch_terms = [t.lower() for t in batch_terms if t is not None]
    t0 = time.time()
    vectors = _embed_one_batch(batch_terms)
    fname = _stage_one_batch_to_parquet(batch_terms, vectors)
    return len(batch_terms), fname, time.time() - t0

# ── EMBEDDINGS_TABLE_ACTIVE write-probe (verbatim) ────────────────────────────────────────────────────
_probe_schema = StructType([
    StructField("term", StringType(), True),
    StructField("embedding_vector", ArrayType(FloatType()), True),
    StructField("model_version", StringType(), True),
])
def _probe_3_lookup():
    spark.createDataFrame([], _probe_schema) \
        .withColumn("created_at", current_timestamp()) \
        .withColumn("embedded_at", current_timestamp()) \
        .withColumn("ADC_UPDT", current_timestamp()) \
        .write.format("delta").mode("append").saveAsTable("3_lookup.embeddings.terms")
# No development fallback: a failed write probe must fail the run, not divert prod embeddings into 8_dev.
_probe_3_lookup()
EMBEDDINGS_TABLE_ACTIVE = "3_lookup.embeddings.terms"
print(f"Embeddings target: {EMBEDDINGS_TABLE_ACTIVE}")

# ── Staged-file listing + commit-to-delta (verbatim) ──────────────────────────────────────────────────
def _list_staged_files() -> List[str]:
    return sorted(glob.glob(STAGING_PATH + "/*.parquet"))

def commit_staged_to_delta() -> dict:
    staged = _list_staged_files()
    if not staged:
        return {"status": "noop", "files": 0, "rows": 0}
    try:
        df = spark.read.schema(StructType([
            StructField("term",             StringType(),               True),
            StructField("embedding_vector", ArrayType(FloatType()),     True),
            StructField("model_version",    StringType(),               True),
            StructField("created_at",       TimestampType(),            True),
            StructField("embedded_at",       TimestampType(),            True),
            StructField("ADC_UPDT",         TimestampType(),            True),
        ])).parquet(*staged)
        n_rows = df.count()
        df.write.format("delta").mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(EMBEDDINGS_TABLE_ACTIVE)
        for f in staged:
            try: os.remove(f)
            except Exception as rm_e: print(f"[warn] couldn't delete {f}: {rm_e}")
        return {"status": "ok", "files": len(staged), "rows": n_rows}
    except Exception as e:
        return {"status": "failed", "files": len(staged), "rows": 0,
                "error": str(e)[:300], "transient": _is_transient(e)}

# COMMAND ----------

def embed_pending_capped(max_terms=MAX_EMBED_TERMS, max_cost_usd=None, dry_run=False):
    """Stage 2. Embed DISTINCT pending texts (top-N by frequency, within the cap); overflow -> deferred.
    After commit, flip pending->vector_ready for keys whose text now has a vector. Returns counts."""
    # P0: embed + flip on embed_text (bare result text for results / context term for tests) -- the open-set
    # scoring grain Stage 3b looks up. (Was LOWER(term) = the 'desc | result' context string for results,
    # which never matched the bare-text substrate lookup.) freq priority is a best-effort LEFT JOIN: the freq
    # table is keyed on context terms, so result embed_texts may not match -> COALESCE(ec,0) sorts them last;
    # acceptable for the capped weekly volume (a bare-result freq rebuild is a separate, non-blocking follow-up).
    # EMBED_QUEUE_RETRY_PATCH_APPLIED v1
    # Deferred TEST keys are retried every run; before 2026-09-26 nothing read 'deferred' again and 1,443 test
    # keys were stranded. Deferred RESULT keys stay parked: 17.4M near-unique strings (workflow stamps,
    # accession IDs, whole histology reports) that the open-set arm judged not worth embedding, and the
    # result remap can take only 5k per run anyway. Texts are ranked (new pending first, then tests, event
    # frequency, keys sharing the text) and only the top `cap` reach the driver.
    cap = max_terms
    if max_cost_usd is not None:
        cap = builtins.min(cap, int(max_cost_usd / EST_USD_PER_TERM))
    if not dry_run:
        # Keys queued before embed_text existed were never fetched. Tests embed their term, results their
        # result_normalized; legacy results without one have no bare text to embed and stay NULL.
        spark.sql(f"""
          UPDATE {QUEUE} SET embed_text = CASE WHEN kind='result' THEN result_normalized ELSE term END
          WHERE embed_text IS NULL
            AND ((kind='test' AND status IN ('pending','deferred'))
                 OR (kind='result' AND status='pending' AND result_normalized IS NOT NULL))
        """)
    needs_sql = f"""
    WITH pend AS (SELECT LOWER(embed_text) AS term,
                         MAX(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS is_new,
                         MAX(CASE WHEN kind='test' THEN 1 ELSE 0 END) AS is_test,
                         COUNT(*) AS n_keys
                  FROM {QUEUE}
                  WHERE embed_text IS NOT NULL AND (status='pending' OR (status='deferred' AND kind='test'))
                  GROUP BY LOWER(embed_text))
    SELECT p.* FROM pend p
    LEFT ANTI JOIN (SELECT term FROM {EMBEDDINGS_TABLE} WHERE embedding_vector IS NOT NULL) e ON p.term=e.term
    """
    fetch_sql = f"""
    WITH needs AS ({needs_sql}),
         freq AS (SELECT LOWER(term) AS term, SUM(event_count) ec FROM {FREQ_TABLE_MILLRAW} GROUP BY LOWER(term))
    SELECT n.term, COUNT(*) OVER () AS n_total FROM needs n LEFT JOIN freq f ON n.term=f.term
    ORDER BY n.is_new DESC, n.is_test DESC, COALESCE(f.ec,0) DESC, n.n_keys DESC, n.term
    LIMIT {builtins.max(cap, 1)}
    """
    fetched = spark.sql(fetch_sql).collect()
    n_needs_embed = int(fetched[0]["n_total"]) if fetched else 0
    to_embed = [r["term"] for r in fetched][:builtins.max(cap, 0)]
    n_overflow = builtins.max(n_needs_embed - len(to_embed), 0)
    # est_cost_usd is the ATTEMPTED upper-bound (cap * unit), not realized spend (partial failures cost less).
    est_cost = len(to_embed) * EST_USD_PER_TERM
    if dry_run:
        return {"n_needs_embed": n_needs_embed, "n_embedded": 0,
                "n_deferred": n_overflow, "est_cost_usd": est_cost, "would_embed": len(to_embed)}
    n_embedded = 0
    if to_embed:
        acquire_lock()
        try:
            batches = [to_embed[i:i+BATCH_SIZE] for i in range(0, len(to_embed), BATCH_SIZE)]
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futs = [ex.submit(_worker_embed_and_stage, b) for b in batches]
                for fut in as_completed(futs):
                    try:
                        n,_,_ = fut.result(); n_embedded += n
                    except Exception as e:
                        print(f"[warn] batch failed: {str(e)[:200]}")
            print(commit_staged_to_delta())
        finally:
            release_lock()
    # The two MERGEs run AFTER release_lock intentionally: they are pending-guarded + idempotent; the lock
    # guards AOAI spend + parquet staging, not queue state.
    spark.sql(f"""
      MERGE INTO {QUEUE} t
      USING (SELECT DISTINCT LOWER(term) AS term FROM {EMBEDDINGS_TABLE} WHERE embedding_vector IS NOT NULL) s
      ON LOWER(t.embed_text)=s.term
      WHEN MATCHED AND (t.status='pending' OR (t.status='deferred' AND t.kind='test'))
        THEN UPDATE SET t.status='vector_ready'
    """)
    # MERGE #1 above moved every embedded text (pending, or a deferred test) to vector_ready. Whatever is still
    # pending (beyond the cap, or its batch failed) becomes 'deferred': tests are retried next run, results park.
    m = spark.sql(f"UPDATE {QUEUE} SET status='deferred' WHERE embed_text IS NOT NULL AND status='pending'").first()
    n_deferred = int(m["num_affected_rows"]) if m is not None and "num_affected_rows" in m.asDict() else 0
    return {"n_needs_embed": n_needs_embed, "n_embedded": n_embedded,
            "n_deferred": n_deferred, "est_cost_usd": est_cost}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 — Stage 3: remap_keys (serverless Spark cross-join; NO driver numpy)
# MAGIC Chunked (REMAP_CHUNK=500) cross-join dot-product; scored materialised to a Delta scratch table
# MAGIC per chunk; test side (5a) + context-scoped result side (5b, answer-list preferred). Aborts above
# MAGIC REMAP_MAX_SERVERLESS=5000 vector_ready keys (run bulk Task-15 on a cluster instead).

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# ── Task 5 chunking config ───────────────────────────────────────────────────────────────────────────
REMAP_CHUNK         = 500    # vector_ready keys processed per cross-join chunk
REMAP_MAX_SERVERLESS = 5000  # abort guard: above this, run the bulk Task-15 build on a cluster

# DQ4_B2_PROPERTY_GUARD_V1: rule path_property_guard_v1. A count concept ([#/...]) never maps a size/volume test and a
# size index (entitic volume, MCV, MCH, RDW) never maps a count test. Evaluated on every scored candidate before
# ranking; the best compatible candidate wins, else the key is unmapped. The rejected winner is kept as provenance.
PATH_PROPERTY_GUARD_SQL = (
    r"(lower(concept_name) RLIKE '\\[#/' AND lower(description) RLIKE '\\b(mcv|mch|mchc|rdw|mean cell|mean corpuscular|volume|distribution width)\\b')"
    r" OR (lower(concept_name) RLIKE 'entitic|\\bmcv\\b|\\bmch\\b|\\bmchc\\b|mean corpuscular|distribution width'"
    r" AND lower(description) RLIKE '\\bcount\\b|^(rbcs?|wbcs?|red (blood )?cells?|white (blood )?cells?|red blood count|platelets?)$')"
)

def _remap_test(dry_run=False):
    """Stage 3a — map vector_ready TEST keys into TEST_MAP via a serverless Spark cross-join dot-product
    (aggregate(zip_with(qv,cv,*))), chunked at REMAP_CHUNK with the cross-join materialised once per
    chunk to a Delta SCRATCH table (cache()/temp-views banned on serverless). The bbv_override layer is
    inlined as a CTE. Aborts above REMAP_MAX_SERVERLESS vector_ready keys (run the bulk cluster build)."""
    n_vr = spark.sql(
        f"SELECT COUNT(*) c FROM {QUEUE} WHERE kind='test' AND status='vector_ready'").first()["c"]
    if n_vr > REMAP_MAX_SERVERLESS:
        raise RuntimeError(
            f"_remap_test: {n_vr:,} vector_ready test keys exceed REMAP_MAX_SERVERLESS={REMAP_MAX_SERVERLESS:,}. "
            f"This is a backfill/cold-replay-scale remap — run the bulk Task-15 build on a CLASSIC CLUSTER "
            f"(pathology_omop_02c_task15_bulk_cluster), not the serverless cross-join.")
    if dry_run:
        return n_vr  # dry-run: count only, no scratch/merge

    CHUNK  = f"{MAP_SCHEMA}._tmp_remap_chunk_test"
    SCORED = f"{MAP_SCHEMA}._tmp_remap_scored_test"
    n_remapped = 0
    while True:
        # 1) materialise up to REMAP_CHUNK vector_ready test keys to a Delta scratch table (deterministic order)
        spark.sql(f"""
          CREATE OR REPLACE TABLE {CHUNK} USING DELTA AS
          SELECT eq.code_system, eq.code, eq.description, LOWER(eq.term) AS term
          FROM {QUEUE} eq
          WHERE eq.kind='test' AND eq.status='vector_ready'
          ORDER BY eq.code_system, eq.code, eq.description
          LIMIT {REMAP_CHUNK}
        """)
        if spark.sql(f"SELECT COUNT(*) c FROM {CHUNK}").first()["c"] == 0:
            break

        # 2) build `scored` (the expensive cross-join dot-product) ONCE and write it to a Delta scratch
        #    table. BROADCAST the small chunk side. cache()/temp-views are banned on serverless.
        #    The override layer reads the tiny CHUNK table directly (step 3), so only the cross-join
        #    (non-override `remaining`) needs materialising here.
        spark.sql(f"""
          CREATE OR REPLACE TABLE {SCORED} USING DELTA AS
          WITH q AS (SELECT code_system, code, description, term FROM {CHUNK}),
          ov AS (SELECT pattern, concept_id, concept_name, match_mode FROM {TEST_OVERRIDES}),
          override_hits AS (
            SELECT q.code_system, q.code, q.description
            FROM q JOIN ov
              ON (ov.match_mode='exact' AND lower(q.description)=ov.pattern)
              OR (ov.match_mode='contains' AND lower(q.description) LIKE concat('%',ov.pattern,'%'))
            GROUP BY q.code_system, q.code, q.description
          ),
          remaining AS (
            SELECT q.code_system,q.code,q.description, e.embedding_vector AS qv
            FROM q JOIN {EMBEDDINGS_TABLE} e ON q.term=LOWER(e.term)
            LEFT JOIN override_hits o ON q.code_system=o.code_system AND q.code=o.code AND q.description=o.description
            WHERE e.embedding_vector IS NOT NULL AND o.code_system IS NULL
          ),
          cand AS (
            SELECT ci.concept_id, ci.concept_name, ci.vocabulary_id, e.embedding_vector AS cv
            FROM {TEST_INDEX} ci JOIN {EMBEDDINGS_TABLE} e ON ci.term=LOWER(e.term)
            WHERE e.embedding_vector IS NOT NULL
          )
          SELECT /*+ BROADCAST(r) */
                 r.code_system,r.code,r.description,c.concept_id,c.concept_name,c.vocabulary_id,
                 -- dot product == cosine: embedding_vector is L2-unit-normalized (text-embedding-3-large default); the 0.85/0.80/0.82/0.70 thresholds depend on this.
                 aggregate(zip_with(r.qv,c.cv,(x,y)->x*y),CAST(0.0 AS DOUBLE),(a,x)->a+x) AS sim
          FROM remaining r CROSS JOIN cand c
        """)

        # 3) tiering + projection read the scratch `scored` table (cross-join NOT re-evaluated) +
        #    re-derive override_hits from the tiny CHUNK table (no cross-join) -> new_df
        new_df = spark.sql(f"""
          WITH q AS (SELECT code_system, code, description, term FROM {CHUNK}),
          ov AS (SELECT pattern, concept_id, concept_name, match_mode FROM {TEST_OVERRIDES}),
          override_hits AS (
            SELECT q.code_system, q.code, q.description, ov.concept_id, ov.concept_name,
                   'bbv_override' AS source, CAST(1.0 AS DOUBLE) AS sim, 'curated' AS tier
            FROM q JOIN ov
              ON (ov.match_mode='exact' AND lower(q.description)=ov.pattern)
              OR (ov.match_mode='contains' AND lower(q.description) LIKE concat('%',ov.pattern,'%'))
            QUALIFY ROW_NUMBER() OVER (PARTITION BY q.code_system,q.code,q.description
              ORDER BY CASE ov.match_mode WHEN 'exact' THEN 0 ELSE 1 END, length(ov.pattern) DESC)=1
          ),
          scored_all AS (SELECT code_system,code,description,concept_id,concept_name,vocabulary_id,sim,
                     ({PATH_PROPERTY_GUARD_SQL}) AS guard_incompatible
                     FROM {SCORED}),
          guard_rejected AS (SELECT code_system,code,description,vocabulary_id,
                     concept_id AS rejected_candidate_concept_id, sim AS rejected_candidate_similarity
                     FROM scored_all WHERE guard_incompatible QUALIFY ROW_NUMBER() OVER
                     (PARTITION BY code_system,code,description ORDER BY sim DESC, concept_id)=1),
          scored AS (SELECT code_system,code,description,concept_id,concept_name,vocabulary_id,sim
                     FROM scored_all WHERE NOT guard_incompatible),
          per_vocab_top AS (SELECT * FROM scored QUALIFY ROW_NUMBER() OVER
            (PARTITION BY code_system,code,description,vocabulary_id ORDER BY sim DESC)=1),
          loinc_hit AS (SELECT * FROM per_vocab_top WHERE vocabulary_id='LOINC' AND sim>={SIM_HIGH}),
          snomed_hit AS (SELECT * FROM per_vocab_top WHERE vocabulary_id='SNOMED' AND sim>={SIM_HIGH}),
          best_any AS (SELECT * FROM per_vocab_top QUALIFY ROW_NUMBER() OVER
            (PARTITION BY code_system,code,description ORDER BY sim DESC)=1),
          chosen AS (
            SELECT code_system,code,description,concept_id,concept_name,vocabulary_id,sim,
                   'embedding_loinc' AS source,'auto_high' AS tier FROM loinc_hit
            UNION ALL
            SELECT s.code_system,s.code,s.description,s.concept_id,s.concept_name,s.vocabulary_id,s.sim,
                   'embedding_snomed','auto_high' FROM snomed_hit s LEFT JOIN loinc_hit l
              ON s.code_system=l.code_system AND s.code=l.code AND s.description=l.description
              WHERE l.code_system IS NULL
            UNION ALL
            SELECT b.code_system,b.code,b.description,b.concept_id,b.concept_name,b.vocabulary_id,b.sim,
                   CASE WHEN b.vocabulary_id='LOINC' THEN 'embedding_loinc' ELSE 'embedding_snomed' END,
                   CASE WHEN b.sim>={SIM_LOW} THEN 'auto_low' ELSE 'unmapped' END
            FROM best_any b
            LEFT JOIN loinc_hit l ON b.code_system=l.code_system AND b.code=l.code AND b.description=l.description
            LEFT JOIN snomed_hit s ON b.code_system=s.code_system AND b.code=s.code AND b.description=s.description
            WHERE l.code_system IS NULL AND s.code_system IS NULL
          ),
          unioned AS (
            SELECT code_system,code,description,concept_id,concept_name,
                   CAST(NULL AS STRING) AS vocabulary_id, sim, source, tier FROM override_hits
            UNION ALL SELECT code_system,code,description,concept_id,concept_name,vocabulary_id,sim,source,tier FROM chosen
            UNION ALL SELECT g.code_system,g.code,g.description,CAST(NULL AS BIGINT),CAST(NULL AS STRING),g.vocabulary_id,
                   CAST(NULL AS DOUBLE),CASE WHEN g.vocabulary_id='LOINC' THEN 'embedding_loinc' ELSE 'embedding_snomed' END,
                   'unmapped'
            FROM guard_rejected g LEFT ANTI JOIN best_any b
              ON g.code_system=b.code_system AND g.code=b.code AND g.description=b.description
          )
          SELECT u.code_system,u.code,u.description,
                 CASE WHEN u.tier='unmapped' THEN CAST(NULL AS BIGINT) ELSE u.concept_id END AS measurement_concept_id,
                 COALESCE(CASE WHEN u.source='bbv_override' THEN c.vocabulary_id ELSE u.vocabulary_id END, u.vocabulary_id) AS concept_vocabulary_id,
                 u.concept_name, u.source AS test_mapping_source, u.sim AS similarity_score, u.tier AS confidence_tier,
                 g.rejected_candidate_concept_id, g.rejected_candidate_similarity,
                 CASE WHEN g.rejected_candidate_concept_id IS NOT NULL THEN 'path_property_guard_v1' END AS reject_rule_id,
                 CAST(NULL AS STRING) AS mapping_rule_id
          FROM unioned u
          LEFT JOIN guard_rejected g ON g.code_system=u.code_system AND g.code=u.code AND g.description=u.description
            AND (u.sim IS NULL OR g.rejected_candidate_similarity > u.sim)
          LEFT JOIN {CONCEPT} c ON c.concept_id=u.concept_id
        """)
        new_df = (new_df.withColumn("mapping_version", F.lit(1))
                        .withColumn("mapped_at", F.current_timestamp())
                        .withColumn("ADC_UPDT", F.current_timestamp()))
        # 4) MERGE this chunk's keys into TEST_MAP (verbatim merge condition + mapping_version bump)
        tgt = DeltaTable.forName(spark, TEST_MAP)
        (tgt.alias("t").merge(new_df.alias("s"),
            "t.code_system=s.code_system AND t.code=s.code AND t.description=s.description")
         .whenMatchedUpdate(condition=(
            "t.measurement_concept_id IS DISTINCT FROM s.measurement_concept_id "
            "OR t.confidence_tier IS DISTINCT FROM s.confidence_tier "
            "OR t.test_mapping_source IS DISTINCT FROM s.test_mapping_source "
            "OR ABS(COALESCE(t.similarity_score,0)-COALESCE(s.similarity_score,0))>0.001 "
            "OR NOT (t.rejected_candidate_concept_id <=> s.rejected_candidate_concept_id)"),
            set={"measurement_concept_id":"s.measurement_concept_id","concept_vocabulary_id":"s.concept_vocabulary_id",
                 "concept_name":"s.concept_name","test_mapping_source":"s.test_mapping_source",
                 "similarity_score":"s.similarity_score","confidence_tier":"s.confidence_tier",
                 "rejected_candidate_concept_id":"s.rejected_candidate_concept_id",
                 "rejected_candidate_similarity":"s.rejected_candidate_similarity",
                 "reject_rule_id":"s.reject_rule_id","mapping_rule_id":"s.mapping_rule_id",
                 "mapping_version":"t.mapping_version + 1","mapped_at":"current_timestamp()","ADC_UPDT":"current_timestamp()"})
         .whenNotMatchedInsertAll().execute())
        n_chunk = spark.sql(f"SELECT COUNT(*) c FROM {CHUNK}").first()["c"]
        # n_remapped counts PROCESSED chunk keys (incl. those that mapped to 'unmapped'), not rows the map changed; the real map-change counts come from Task 9's run-log via the into-map MERGE.
        n_remapped += n_chunk
        # 5) flip THIS chunk's keys vector_ready->done (kind-scoped), then loop
        spark.sql(f"""
          MERGE INTO {QUEUE} t USING {CHUNK} s
          ON t.kind='test' AND t.code_system=s.code_system AND t.code=s.code AND t.description=s.description
          WHEN MATCHED AND t.status='vector_ready' THEN UPDATE SET t.status='done'
        """)
    spark.sql(f"DROP TABLE IF EXISTS {CHUNK}")
    spark.sql(f"DROP TABLE IF EXISTS {SCORED}")
    return n_remapped

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════════════════════════
# Stage 3b — OPEN-SET + EMBEDDING-PRIMARY-FUSION result mapping (REBUILT 2026-06-30)
# Replaces the legacy nearest-context arm (broad LOINC/SNOMED + answer-list, polarity-BLIND) with the
# SAME arms the bulk build uses: anchor-fusion (embedding-primary polarity) + organism + generic-positive
# + value/morphology FLAG_DICT + LOINC-answer. Serverless throughout (small pools + cross-join dot-product,
# NEVER the 96k driver matrix). Routing/fusion are SINGLE-SOURCED via the canonical rerouter notebooks
# (dbutils.notebook.run) — no fourth copy of the regex/threshold logic in this module.
# ════════════════════════════════════════════════════════════════════════════════════════════════

# Open-set pool tables (small; serverless-safe). organism_vectors carries its embedding inline.
ANCHOR_TABLE   = f"{MAP_SCHEMA}.concept_result_anchors"        # 76 active surface forms
ORG_VECS       = f"{MAP_SCHEMA}.organism_vectors"              # concept_id, concept_name, embedding_vector
ORG_ALLOW      = f"{MAP_SCHEMA}.concept_result_organism_allow" # ~260 allow concepts (allow-subset, NOT 96k)
MORPH_DICT     = f"{MAP_SCHEMA}._morphology_flag_dict"         # value/morphology token -> value_concept_id
FINDING_DICT   = f"{MAP_SCHEMA}._finding_flag_dict"            # finding token_regex -> finding concept (+suborder)
SUBSTRATE      = f"{MAP_SCHEMA}._tmp_openset_substrate"        # the 29-col openset_lookup schema (this run's keys)
SUBSTRATE_FUSED= f"{MAP_SCHEMA}._tmp_openset_substrate_fused"  # after the fusion rerouter
LOINC_PROP     = f"{MAP_SCHEMA}._tmp_loinc_proposal"           # LOINC-answer arm (4-tuple grain)
VALUE_PROP     = f"{MAP_SCHEMA}._tmp_value_proposal"           # value/morphology decode (text grain)
FINDING_PROP   = f"{MAP_SCHEMA}._tmp_finding_proposal"         # finding-axis decode (text + suborder grain)
# F3 grade-strip regex, verbatim from pathology_omop_02d_openset_result_cluster _stage_queries (score_text).
GRADE_STRIP_RE = r"\\s*(\\+{1,4}|\\b[1-4]\\+|\\b(profuse|heavy|moderate|scanty|light|few|occasional)\\b)"
# bulk integrate organism acceptance gate (rerouter A2): FLOOR/MARGIN/DELTA.
OS_FLOOR_VALUE, OS_MARGIN_VALUE, OS_DELTA_VALUE = 0.63, 0.02, 0.04
# LOINC-answer arm thresholds (bulk Phase I).
FLOOR_LOINC, MARGIN_LOINC = 0.62, 0.05

# ── Polarity gate (§B) ── cache-backed veto (populated by polgate_gpu_run) ──
GATE_ENABLED       = True
GATE_TABLE         = f"{MAP_SCHEMA}.pathology_polarity_gate"
GATE_MODEL_VERSION = "deberta-v3-large-mnli-fever-anli-ling-wanli"
GATE_VERSION       = 1

# BAKED CONFIG — generated by _stage3b_config_generator from _openset_routing_config + _fusion_cfg
#   + exclusions. SOURCE-OF-TRUTH copy; regenerate + re-promote if those tables change.
_ROUTE_CFG = {
    'CLEAN_NEG_BLOCK_RE': '\\b(rh ?d|rhd|blood group|kell|duffy|kidd|anti[- ]?[abd])\\b',
    'CLEAN_NEG_MAXLEN': '45',
    'COMPOUND_RE': '[,;]| and | but | however | except |:\\s|[a-z]{2}\\.\\s+[a-z]',
    'FORCED_ANCHOR_MAP': '[["not\\\\s+isolat|no\\\\b.{0,40}isolat", "not isolated"], ["no\\\\s+growth|not\\\\b.{0,40}grow|no\\\\b.{0,40}grow", "no growth"], ["not\\\\s+detect|no\\\\b.{0,40}detect|non[- ]?detect|not\\\\s+detected|pcr negative", "not detected"], ["not\\\\s+seen|no\\\\b.{0,40}seen|not\\\\s+observed|no\\\\s+organisms?\\\\s+seen", "not seen"], ["non[- ]?react|not\\\\s+react", "non-reactive"], ["\\\\babsent\\\\b|not\\\\s+present|no\\\\b.{0,40}presen", "absent"], ["\\\\bnegative\\\\b|\\\\bneg$|[ -]ve$|cannot be excluded|cannot be ruled out|\\\\bsusceptible to\\\\b", "negative"]]',
    'FRAGMENT_RE': '^(this (hiv|hbv|hcv)? ?(antibody )?(assay|test) (is|does|can|only)|this is consistent|this confirms|samples today|weak d reaction|for [a-z]+ antigen, suggesting|insufficient identification|low level of|non-reacting|these results (would|within)|slope neg|immunisation recommended|patients (under|with)|suppression may|and should be followed|please (note|see|contact|repeat)|results within normal limits$|review results$|consider )',
    'GENERIC_POS_RE': 'isolated from .{0,30}bottle|presumptive isolate|^isolated\\.?$|^mixed growth\\b|\\bmixed growth\\.?$|^growth\\.?$|\\bheavy growth\\b|\\bscanty growth\\b|\\blight growth\\b|\\bmoderate growth\\b|\\bprofuse growth\\b',
    'GRADE_RE': '\\b(profuse|heavy|moderate|scanty|light|few|occasional)\\b|\\+{1,4}|\\b[1-4]\\+',
    'HAS_ORGANISM_TOKEN_RE': 'coccus|cocci|bacill|\\brods?\\b|coliform|strepto|staphylo|pseudomon|klebsiella|e\\.?\\s?coli|enterococc|candida|yeast|aspergill|crypto|\\bmyco|salmonella|shigella|proteus|serratia|haemophil|neisseria|clostrid|listeria|enterobact|acinetobacter|morganella|citrobacter|providencia|diphther|\\b[a-z]+ [a-z]+(?:us|um|is|ae|osa|ica)\\b',
    'ISOLATE_POS_RE': '^(isolated|presumptive isolate)',
    'JUNK_MAXLEN': '2',
    'JUNK_RE': '^[^a-z]*$|^[a-z]{1,2}[0-9][a-z0-9]*$',
    'MORPH_RE': 'gram[ -]?(?:negative|positive)|coagulase[ -]?negative|oxidase[ -]?negative|catalase[ -]?negative',
    'NEG_RE': '(?:\\bnot\\b|\\bno\\b|\\bnon[- ]?|\\bnone\\b).{0,40}?(?:detect|seen|isolat|grow|react|presen|ident)|\\bnegative\\b|\\bno growth\\b|\\bnon[- ]?reactive\\b|\\bnot detected\\b|\\bnon detected\\b|\\bnot seen\\b|\\bnot observed\\b|\\bneg$|[ -]ve$|not isolated|pcr negative|cannot be excluded|cannot be ruled out|\\bsusceptible to\\b',
    'ORG_BLOCK_RE': '(\\btest$)|(\\bserology$)|(\\bscreen$)|(\\bscreen )|(\\bset up$)|(\\bsens up$)|(\\btyping\\b)|(\\bplease\\b)|(^\\?\\s)|(antibody test)|(\\borf[0-9])|,.{1,40},.{1,40},|(are part of)|(is widespread)|(are usually)|(are normal$)|(^to )|(^hpv only$)|(^hiv,$)|\\besbl\\b|(^group [a-z] strep$)|(antigen,$)|(^hepatitis b\\.?$)|(^hbs$)|(evidence of past)|(\\bpast (exposure|infection))|(not recent)|(no recent)|(previou.{0,15}(exposure|infection))|(historic)|(coliform/pseudomon)|(shigella/enteroinvasive)|(carbapenemase producing organism)',
    'POSITIVE_OVERRIDE_MAP': '[["shigella ?/ ?(enteroinvasive e ?coli|eiec) dna detected", 21498489, "Shigella/EIEC"]]',
    'POS_RE': '\\bdetected\\b|\\bpositive\\b|\\breactive\\b|\\bisolated\\b|\\bseen\\b|\\bpresent\\b|\\bidentified\\b|\\bgrowth of\\b|\\bgrowth\\b',
    'SUSPECTED_RE': '\\b(possible|possibly|suggest(s|ive)?|presumptive(ly)?|probable|probably|likely|query)\\b',
    'THRESHOLD_DELTA': '0.04',
    'THRESHOLD_FLOOR': '0.63',
    'THRESHOLD_MARGIN': '0.02',
    'WRONG_SPECIES_DENY': '[4216359, 4017991, 37116714, 1244038]',
}

_EXCL_PATTERNS = [
    'not tested',
    'insufficient (sample|serum|specimen|quantity|volume|plasma)',
    'no sample received',
    'sample (haemolysed|not received|leaked|clotted)',
    '\\bhaemolysed\\b',
    '\\bunsuitable\\b',
    '(edta|sample) contamination',
    'duplicate (request|specimen|sample|order)',
    '\\bdeleted\\b',
    '\\bcancelled\\b',
    '(requested|ordered|booked on|check) in error',
    '\\bin error\\b',
    'wrong sample (received|type)',
    'laboratory error|technical (problem|error)|apologies',
    '^\\(?pending\\)?\\.?$',
    '\\bawaiting\\b|to follow|in progress|\\btbc\\b',
    'see (comment|report|below|above|note|attached)',
    'no result available',
    'please see|refer to',
    '(test )?not (indicated|applicable|required|performed)',
    '^n/?a\\.?$',
    '^nil$|^none$',
    'suggest repeat|please repeat|repeat (if necessary|with fresh|sample|requested)',
    'sample too old|too old for analysis|old specimen',
    'no (clotted|labelled|edta|yellow top|suitable|serum)( | [a-z]+ )?(sample|specimen)( received)?',
    'un(labelled|labeled) (sample|specimen)',
    '(booking|registration|ordering) error|error in ordering',
    'wrong patient (bled|sample)|patient mis',
    'amended report|disregard|please ignore|please note',
    'specimen leaked|leaked in transit|unable to perform',
    'inconvenience caused|we apologise|regret',
    '^(test|sample|analysis|required|result|for analysis)\\.?$',
    'analy[sz]ed at|sent (to|away)|performed (at|by) (the )?(pru|tdl|doctors|reference|sheffield|king)',
    'report from (same|the)|see .* report (from|dated)|refer to (comment|result) dated',
]

# fusion thresholds bare numeric; regex SQL literals = repr(sql_str(regex)): backslashes doubled for Spark
# SQL RLIKE + repr for Python-parse safety.
_FC_COS_STRONG   = 0.65
_FC_COS_WEAK     = 0.45
_FC_ANCHOR_FLOOR = 0.45
_FC_ANCHOR_MARGIN= 0.01
_FC_TIE_MARGIN   = 0.02
_FC_CONFIRM_BONUS= 0.05
_FC_BOOST_BONUS  = 0.03
_FC_ABSTAIN_RE_SQL   = "'^(culture|antigen|specimen|sample|microscopy|serology|pcr|histology|cytology|biochemistry|haematology|virology|mc&s|m,c&s|m c s)\\\\s*[.:]?$|^(culture|specimen|sample|microscopy|serology|pcr|test|assay)\\\\s+(complete|completed|received|processed|performed|requested|added|sent|done|awaited|pending|to follow)\\\\b'"
_FC_ADVISORY_RE_SQL  = "'^should not be based on|may represent contamination of the sample|please interpret in the context|negative predictive value'"


# ── Rerouter seams (dbutils.notebook.run) — overridable so a harness/fixture can inject ─────────────
_RR_FLOOR_VALUE, _RR_MARGIN_VALUE, _RR_DELTA_VALUE = 0.63, 0.02, 0.04  # rerouter A2 thresholds (baked)
def _reroute_openset(tbl):
    """Inlined pathology_omop_openset_rerouter — deterministic router over the stored cosines in `tbl`,
    overwriting it in place with route/final_*/tier. Config from the baked _ROUTE_CFG/_EXCL_PATTERNS
    (was spark.table(_openset_routing_config)/exclusions_dev); anchors from concept_result_anchors (data)."""
    import re, json
    from pyspark.sql import functions as F
    cfg = _ROUTE_CFG
    NEG=re.compile(cfg["NEG_RE"]); POS=re.compile(cfg["POS_RE"]); MORPH=re.compile(cfg["MORPH_RE"])
    JUNK=re.compile(cfg["JUNK_RE"]); JUNK_MAX=int(cfg["JUNK_MAXLEN"]); COMPOUND=re.compile(cfg["COMPOUND_RE"]); CLEAN_MAX=int(cfg["CLEAN_NEG_MAXLEN"])
    FORCED=[(re.compile(p),l) for p,l in json.loads(cfg["FORCED_ANCHOR_MAP"])]
    FRAGMENT=re.compile(cfg["FRAGMENT_RE"]) if "FRAGMENT_RE" in cfg else None
    ORG_BLOCK=re.compile(cfg["ORG_BLOCK_RE"]) if "ORG_BLOCK_RE" in cfg else None
    CLEAN_NEG_BLOCK=re.compile(cfg["CLEAN_NEG_BLOCK_RE"]) if "CLEAN_NEG_BLOCK_RE" in cfg else None
    DENY=set(json.loads(cfg["WRONG_SPECIES_DENY"])) if "WRONG_SPECIES_DENY" in cfg else set()
    GENERIC_POS=re.compile(cfg["GENERIC_POS_RE"]) if "GENERIC_POS_RE" in cfg else None
    HAS_ORG_TOKEN=re.compile(cfg["HAS_ORGANISM_TOKEN_RE"]) if "HAS_ORGANISM_TOKEN_RE" in cfg else None
    GRADE=re.compile(cfg["GRADE_RE"]) if "GRADE_RE" in cfg else None
    SUSPECTED=re.compile(cfg["SUSPECTED_RE"]) if "SUSPECTED_RE" in cfg else None
    ISOLATE_POS=re.compile(cfg["ISOLATE_POS_RE"]) if "ISOLATE_POS_RE" in cfg else None
    GROWTH_CID=36032835; ORGANISM_CID=4259632
    POS_OVERRIDE=[(re.compile(p),int(cid),nm) for p,cid,nm in json.loads(cfg["POSITIVE_OVERRIDE_MAP"])] if "POSITIVE_OVERRIDE_MAP" in cfg else []
    def pos_override(s):
        for pat,cid,nm in POS_OVERRIDE:
            if pat.search(s): return cid,nm
        return None
    def grade_and_suspect(s):
        g=None
        if GRADE is not None:
            m=GRADE.search(s); g=m.group(0) if m else None
        susp=bool(s.rstrip().endswith("?")) or bool(SUSPECTED.search(s)) if SUSPECTED else bool(s.rstrip().endswith("?"))
        return susp,g
    EXCL=re.compile("|".join(f"(?:{p})" for p in _EXCL_PATTERNS)) if _EXCL_PATTERNS else None
    ameta={r["anchor_label"]:(int(r["cid"]),r["polarity_class"]) for r in spark.sql(
      f"SELECT DISTINCT anchor_label, polarity_class, COALESCE(snomed_concept_id,loinc_concept_id) cid FROM {ANCHOR_TABLE} WHERE is_active=true AND COALESCE(snomed_concept_id,loinc_concept_id) IS NOT NULL").collect()}
    def intent(s):
        st=MORPH.sub(" ",s)
        if NEG.search(st):
            f="negative"
            for pat,lab in FORCED:
                if pat.search(s): f=lab; break
            return ("negative",f,(len(s)<=CLEAN_MAX) and (COMPOUND.search(s) is None))
        if POS.search(st): return ("positive",None,False)
        return ("neutral",None,False)
    rows=spark.table(tbl).collect()
    out=[]
    for r in rows:
        rn=r["result_normalized"]
        a_lab=r["anchor_label"]; a_pol=r["anchor_polarity"]; a_cid=r["anchor_concept_id"]
        a1=float(r["anchor_top1"] or 0); a2=float(r["anchor_top2"] or 0); am=float(r["anchor_margin"] or 0)
        o_cid=r["org_concept_id"]; o_name=r["org_concept_name"]
        o1=float(r["org_top1"] or 0); o2=float(r["org_top2"] or 0); om=float(r["org_margin"] or 0)
        isj=bool(len(rn)<=JUNK_MAX or JUNK.search(rn)); ise=bool(EXCL.search(rn)) if EXCL else False
        frag_blocked=bool(FRAGMENT.search(rn)) if FRAGMENT else False
        org_blocked=bool(ORG_BLOCK.search(rn)) if ORG_BLOCK else False
        cn_blocked=bool(CLEAN_NEG_BLOCK.search(rn)) if CLEAN_NEG_BLOCK else False
        deny_hit=(o_cid in DENY) if o_cid is not None else False
        intn,forced,clean=intent(rn)
        is_suspected,growth_grade=grade_and_suspect(rn)
        has_org_tok=bool(HAS_ORG_TOKEN.search(rn)) if HAS_ORG_TOKEN else False
        ovr=pos_override(rn) if (intn!="negative") else None
        act="none"; fcid=None; fname=None; ftype=None; win=None
        if isj or ise: route="excluded" if ise else "none"
        elif frag_blocked: route="none"
        elif cn_blocked: route="none"
        elif intn=="negative" and clean and (forced in ameta):
            route="anchor"; win="anchor"; act="clean_negation_forced"; fcid,fname,ftype=ameta[forced][0],forced,"generic_qualifier"
        elif ovr is not None:
            route="organism"; win="positive_override"; act="positive_override"; fcid,fname,ftype=ovr[0],ovr[1],"organism"
        elif (not org_blocked) and (not deny_hit) and (o1-a1)>=_RR_DELTA_VALUE and o1>=_RR_FLOOR_VALUE and om>=_RR_MARGIN_VALUE and o_cid is not None:
            route="organism"
        elif (intn!="negative") and (GENERIC_POS is not None) and GENERIC_POS.search(rn) and not has_org_tok:
            if ISOLATE_POS is not None and ISOLATE_POS.search(rn):
                route="generic_positive"; win="generic_positive"; act="generic_pos_isolate"; fcid,fname,ftype=ORGANISM_CID,"organism","generic_positive"
            else:
                route="generic_positive"; win="generic_positive"; act="generic_pos_growth"; fcid,fname,ftype=GROWTH_CID,"growth","generic_positive"
        elif a1>=0.60 and am>=0.05: route="anchor"
        else: route="none"
        if win is None:
            win=route
            if route=="anchor":
                if (a_pol=="negative" and intn=="positive") or (a_pol=="positive" and intn=="negative"):
                    act="forced_unmap"; route="none"; win="none"
                else: fcid,fname,ftype=a_cid,a_lab,"generic_qualifier"
            elif route=="organism":
                if intn=="negative":
                    act="forced_to_neg_anchor"; fa=forced or "negative"
                    if fa in ameta: fcid,fname,ftype=ameta[fa][0],fa,"generic_qualifier"; win="anchor"
                    else: route="none"; win="none"
                else: fcid,fname,ftype=o_cid,o_name,"organism"
        tier=("excluded" if route=="excluded" else "auto_anchor" if ftype=="generic_qualifier"
              else "auto_value" if ftype=="organism" else "auto_genpos" if ftype=="generic_positive" else "unmapped")
        out.append((rn, int(r["freq"]), a_lab, a_pol, (int(a_cid) if a_cid is not None else None),
                    builtins.round(a1,6),builtins.round(a2,6),builtins.round(am,6),
                    (int(o_cid) if o_cid is not None else None), o_name, builtins.round(o1,6),builtins.round(o2,6),builtins.round(om,6), builtins.round(o1-a1,6),
                    isj, ise, clean, intn, forced, act, win, route,
                    (int(fcid) if fcid is not None else None), fname, ftype, tier, is_suspected, growth_grade))
    SCHEMA=("result_normalized STRING, freq BIGINT, anchor_label STRING, anchor_polarity STRING, anchor_concept_id BIGINT, "
            "anchor_top1 DOUBLE, anchor_top2 DOUBLE, anchor_margin DOUBLE, org_concept_id BIGINT, org_concept_name STRING, "
            "org_top1 DOUBLE, org_top2 DOUBLE, org_margin DOUBLE, cross_pool_margin DOUBLE, is_junk BOOLEAN, is_excluded BOOLEAN, "
            "is_clean_negation BOOLEAN, polarity_intent STRING, forced_anchor_label STRING, polarity_action STRING, "
            "winning_pool STRING, route STRING, final_value_as_concept_id BIGINT, final_concept_name STRING, final_type STRING, "
            "final_tier STRING, is_suspected BOOLEAN, growth_grade STRING")
    spark.createDataFrame(out, SCHEMA).withColumn("scored_at", F.current_timestamp()) \
         .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(tbl)

def _fuse_substrate(in_lookup, out_lookup):
    """Inlined pathology_omop_openset_fusion_rerouter — embedding-primary anchor polarity re-fuse.
    _fusion_cfg values baked as SQL literals (no CROSS JOIN); ABSTAIN/ADVISORY regexes via _FC_*_RE_SQL."""
    spark.sql(f"""
    CREATE OR REPLACE TABLE {out_lookup} AS
    SELECT
      result_normalized, freq, anchor_label, anchor_polarity, anchor_concept_id,
      anchor_top1, anchor_top2, anchor_margin, org_concept_id, org_concept_name,
      org_top1, org_top2, org_margin, cross_pool_margin, is_junk, is_excluded,
      is_clean_negation, polarity_intent, forced_anchor_label,
      CASE WHEN preserve THEN polarity_action
           WHEN abstain_hit THEN 'fusion_abstain_fragment'
           WHEN anchor_accept AND cos_strong AND agree THEN 'fusion_confirm'
           WHEN anchor_accept AND cos_strong AND disagree THEN 'fusion_cosine_override'
           WHEN anchor_accept AND cos_strong AND polarity_intent='neutral' THEN 'fusion_cosine_solo'
           WHEN anchor_accept AND cos_mid AND agree THEN 'fusion_boost'
           ELSE 'fusion_silent' END AS polarity_action,
      winning_pool,
      CASE WHEN preserve THEN route WHEN abstain_hit THEN 'none' WHEN anchor_accept THEN 'anchor' ELSE 'none' END AS route,
      CASE WHEN preserve THEN final_value_as_concept_id WHEN abstain_hit THEN NULL WHEN anchor_accept THEN anchor_concept_id ELSE NULL END AS final_value_as_concept_id,
      CASE WHEN preserve THEN final_concept_name WHEN abstain_hit THEN NULL WHEN anchor_accept THEN anchor_label ELSE NULL END AS final_concept_name,
      CASE WHEN preserve THEN final_type WHEN abstain_hit THEN NULL WHEN anchor_accept THEN 'generic_qualifier' ELSE NULL END AS final_type,
      CASE WHEN preserve THEN final_tier WHEN abstain_hit THEN 'unmapped' WHEN anchor_accept THEN 'auto_anchor' ELSE 'unmapped' END AS final_tier,
      is_suspected, growth_grade,
      result_normalized AS stripped_result_normalized,
      CASE WHEN cos_strong THEN 'strong' WHEN cos_mid THEN 'mid' ELSE 'weak' END AS cos_band,
      CASE WHEN preserve THEN 'preserved'
           WHEN abstain_hit THEN 'abstain_fragment'
           WHEN anchor_accept AND cos_strong AND agree THEN 'confirm'
           WHEN anchor_accept AND cos_strong AND disagree THEN 'cosine_override'
           WHEN anchor_accept AND cos_strong AND polarity_intent='neutral' THEN 'cosine_solo'
           WHEN anchor_accept AND cos_mid AND agree THEN 'boost'
           ELSE 'silent' END AS regex_role,
      CASE WHEN abstain_hit THEN 'abstain_fragment' WHEN NOT preserve AND NOT anchor_accept THEN 'below_fusion_bar' END AS fusion_reason,
      CASE WHEN anchor_accept AND cos_strong AND agree THEN anchor_top1 + {_FC_CONFIRM_BONUS}
           WHEN anchor_accept AND cos_mid AND agree THEN anchor_top1 + {_FC_BOOST_BONUS}
           ELSE anchor_top1 END AS confidence_score,
      scored_at
    FROM (
      SELECT l.*,
        (l.route IN ('organism','generic_positive','excluded')
           OR l.polarity_action IN ('positive_override','generic_pos_isolate','generic_pos_growth','forced_to_neg_anchor')) AS preserve,
        (l.anchor_top1 >= {_FC_COS_STRONG} AND l.anchor_margin >= {_FC_TIE_MARGIN}) AS cos_strong,
        (l.anchor_top1 >= {_FC_COS_WEAK} AND l.anchor_top1 < {_FC_COS_STRONG} AND l.anchor_top1 >= {_FC_ANCHOR_FLOOR} AND l.anchor_margin >= {_FC_ANCHOR_MARGIN}) AS cos_mid,
        (l.polarity_intent IN ('negative','positive') AND l.anchor_polarity = l.polarity_intent) AS agree,
        (l.polarity_intent IN ('negative','positive') AND l.anchor_polarity IN ('negative','positive') AND l.anchor_polarity <> l.polarity_intent) AS disagree,
        ( ({_FC_ABSTAIN_RE_SQL} IS NOT NULL AND l.result_normalized RLIKE {_FC_ABSTAIN_RE_SQL} AND l.polarity_intent='neutral')
          OR ({_FC_ADVISORY_RE_SQL} IS NOT NULL AND l.result_normalized RLIKE {_FC_ADVISORY_RE_SQL}) ) AS abstain_hit,
        ((NOT (l.route IN ('organism','generic_positive','excluded')
            OR l.polarity_action IN ('positive_override','generic_pos_isolate','generic_pos_growth','forced_to_neg_anchor')))
          AND ( (l.anchor_top1 >= {_FC_COS_STRONG} AND l.anchor_margin >= {_FC_TIE_MARGIN} AND
                   ((l.polarity_intent IN ('negative','positive') AND l.anchor_polarity = l.polarity_intent)
                    OR (l.polarity_intent IN ('negative','positive') AND l.anchor_polarity IN ('negative','positive') AND l.anchor_polarity <> l.polarity_intent)
                    OR l.polarity_intent='neutral'))
                OR (l.anchor_top1 >= {_FC_COS_WEAK} AND l.anchor_top1 < {_FC_COS_STRONG} AND l.anchor_top1 >= {_FC_ANCHOR_FLOOR} AND l.anchor_margin >= {_FC_ANCHOR_MARGIN}
                    AND l.polarity_intent IN ('negative','positive') AND l.anchor_polarity = l.polarity_intent) )) AS anchor_accept
      FROM {in_lookup} l
    )
    """)


def _apply_gate_veto(fused_tbl):
    """§B. Cache-backed polarity veto on the net-new fused substrate, IN PLACE. Downgrade-only; no-op if
    GATE_ENABLED is False or the cache is absent (fail-open). Joins the cache POLE-ONLY via a pre-aggregated
    veto set (result_normalized, embedding_pole)."""
    if not GATE_ENABLED or not spark.catalog.tableExists(GATE_TABLE):
        return
    spark.sql(f"""
      CREATE OR REPLACE TABLE {fused_tbl} AS
      WITH veto AS (
        SELECT DISTINCT result_normalized, embedding_pole FROM {GATE_TABLE}
        WHERE verdict='veto' AND model_version={GATE_MODEL_VERSION!r} AND gate_version={GATE_VERSION}
      ),
      gated AS (
        SELECT l.*,
          (v.result_normalized IS NOT NULL AND l.route='anchor' AND l.final_tier<>'unmapped'
           AND l.anchor_polarity IN ('positive','negative')) AS is_vetoed
        FROM {fused_tbl} l
        LEFT JOIN veto v ON l.result_normalized=v.result_normalized AND l.anchor_polarity=v.embedding_pole
      )
      SELECT result_normalized, freq, anchor_label, anchor_polarity, anchor_concept_id,
        anchor_top1, anchor_top2, anchor_margin, org_concept_id, org_concept_name,
        org_top1, org_top2, org_margin, cross_pool_margin, is_junk, is_excluded,
        is_clean_negation, polarity_intent, forced_anchor_label,
        CASE WHEN is_vetoed THEN 'polarity_veto' ELSE polarity_action END AS polarity_action,
        winning_pool,
        CASE WHEN is_vetoed THEN 'none' ELSE route END AS route,
        CASE WHEN is_vetoed THEN CAST(NULL AS bigint) ELSE final_value_as_concept_id END AS final_value_as_concept_id,
        CASE WHEN is_vetoed THEN CAST(NULL AS string) ELSE final_concept_name END AS final_concept_name,
        CASE WHEN is_vetoed THEN CAST(NULL AS string) ELSE final_type END AS final_type,
        CASE WHEN is_vetoed THEN 'unmapped' ELSE final_tier END AS final_tier,
        is_suspected, growth_grade, stripped_result_normalized, cos_band,
        CASE WHEN is_vetoed THEN 'polarity_veto' ELSE regex_role END AS regex_role,
        CASE WHEN is_vetoed THEN 'nli_contradicts_pole' ELSE fusion_reason END AS fusion_reason,
        confidence_score, scored_at
      FROM gated
    """)

def _build_openset_substrate(dry_run=False):
    """Stage 3b-i. Score this run's distinct vector_ready RESULT keys against the SMALL open-set pools
    (anchor 76 surface forms, organism allow-subset ~260) via the serverless cross-join dot-product,
    producing the 29-col pathology_result_openset_lookup substrate. Regex/route columns are placeholder
    defaults — the rerouters (3b-ii/iii) re-derive + overwrite them from result_normalized. Text grain:
    scored on the grade-stripped score_text (COALESCE-fallback to the bare result_normalized vector, which
    Task-3's embed_text grain guarantees is in the store). Aborts above REMAP_MAX_SERVERLESS."""
    n_vr = spark.sql(
        f"SELECT COUNT(DISTINCT result_normalized) c FROM {QUEUE} "
        f"WHERE kind='result' AND status='vector_ready' AND result_normalized IS NOT NULL").first()["c"]
    if n_vr > REMAP_MAX_SERVERLESS:
        raise RuntimeError(
            f"_build_openset_substrate: {n_vr:,} distinct vector_ready result keys exceed "
            f"REMAP_MAX_SERVERLESS={REMAP_MAX_SERVERLESS:,}. Run the bulk cluster build "
            f"(pathology_omop_02d_openset_result_cluster), not the serverless cross-join.")
    if dry_run:
        return SUBSTRATE
    KEYS = f"{MAP_SCHEMA}._tmp_substrate_keys"
    QV   = f"{MAP_SCHEMA}._tmp_substrate_qv"
    AC   = f"{MAP_SCHEMA}._tmp_substrate_anchor"
    OCt  = f"{MAP_SCHEMA}._tmp_substrate_org"
    # 1) distinct keys + grade-stripped score_text
    spark.sql(f"""
      CREATE OR REPLACE TABLE {KEYS} USING DELTA AS
      SELECT DISTINCT result_normalized,
             TRIM(REGEXP_REPLACE(result_normalized, r'{GRADE_STRIP_RE}', ' ')) AS score_text,
             CAST(1 AS BIGINT) AS freq
      FROM {QUEUE}
      WHERE kind='result' AND status='vector_ready' AND result_normalized IS NOT NULL""")
    # 2) query vector: score_text first, fallback to result_normalized
    spark.sql(f"""
      CREATE OR REPLACE TABLE {QV} USING DELTA AS
      SELECT k.result_normalized, k.freq,
             COALESCE(es.embedding_vector, er.embedding_vector) AS qvec
      FROM {KEYS} k
      LEFT JOIN {EMBEDDINGS_TABLE} es ON LOWER(es.term)=k.score_text        AND es.embedding_vector IS NOT NULL
      LEFT JOIN {EMBEDDINGS_TABLE} er ON LOWER(er.term)=k.result_normalized AND er.embedding_vector IS NOT NULL
      WHERE COALESCE(es.embedding_vector, er.embedding_vector) IS NOT NULL""")
    # 3) anchor cosines (per-concept max-pool -> top1/top2 across concepts)
    spark.sql(f"""
      CREATE OR REPLACE TABLE {AC} USING DELTA AS
      WITH a AS (
        SELECT anchor_label, polarity_class,
               COALESCE(snomed_concept_id,loinc_concept_id) AS cid, LOWER(surface_form) AS sf
        FROM {ANCHOR_TABLE} WHERE is_active=true AND COALESCE(snomed_concept_id,loinc_concept_id) IS NOT NULL),
      av AS (SELECT a.anchor_label, a.polarity_class, a.cid, e.embedding_vector AS cvec
             FROM a JOIN {EMBEDDINGS_TABLE} e ON LOWER(e.term)=a.sf AND e.embedding_vector IS NOT NULL),
      sims AS (
        SELECT q.result_normalized, av.anchor_label, av.polarity_class, av.cid,
               MAX(aggregate(zip_with(q.qvec, av.cvec, (x,y)->x*y), CAST(0.0 AS DOUBLE), (s,x)->s+x)) AS sim
        FROM {QV} q CROSS JOIN av GROUP BY q.result_normalized, av.anchor_label, av.polarity_class, av.cid),
      ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY result_normalized ORDER BY sim DESC) rn FROM sims)
      SELECT r1.result_normalized, r1.anchor_label, r1.polarity_class AS anchor_polarity,
             r1.cid AS anchor_concept_id, r1.sim AS anchor_top1, COALESCE(r2.sim,0.0) AS anchor_top2,
             r1.sim - COALESCE(r2.sim,0.0) AS anchor_margin
      FROM (SELECT * FROM ranked WHERE rn=1) r1
      LEFT JOIN (SELECT result_normalized, sim FROM ranked WHERE rn=2) r2 USING (result_normalized)""")
    # 4) organism cosines (allow-subset; per-concept max-pool)
    spark.sql(f"""
      CREATE OR REPLACE TABLE {OCt} USING DELTA AS
      WITH ov AS (
        SELECT v.concept_id, v.concept_name, v.embedding_vector AS cvec
        FROM {ORG_VECS} v JOIN {ORG_ALLOW} al ON al.concept_id=v.concept_id
        WHERE v.embedding_vector IS NOT NULL),
      sims AS (
        SELECT q.result_normalized, ov.concept_id, ov.concept_name,
               MAX(aggregate(zip_with(q.qvec, ov.cvec, (x,y)->x*y), CAST(0.0 AS DOUBLE), (s,x)->s+x)) AS sim
        FROM {QV} q CROSS JOIN ov GROUP BY q.result_normalized, ov.concept_id, ov.concept_name),
      ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY result_normalized ORDER BY sim DESC) rn FROM sims)
      SELECT r1.result_normalized, r1.concept_id AS org_concept_id, r1.concept_name AS org_concept_name,
             r1.sim AS org_top1, COALESCE(r2.sim,0.0) AS org_top2, r1.sim-COALESCE(r2.sim,0.0) AS org_margin
      FROM (SELECT * FROM ranked WHERE rn=1) r1
      LEFT JOIN (SELECT result_normalized, sim FROM ranked WHERE rn=2) r2 USING (result_normalized)""")
    # 5) assemble the 29-col substrate (regex/route cols = placeholder defaults; rerouter overwrites)
    spark.sql(f"""
      CREATE OR REPLACE TABLE {SUBSTRATE} USING DELTA AS
      SELECT k.result_normalized, k.freq,
             a.anchor_label, a.anchor_polarity, a.anchor_concept_id,
             COALESCE(a.anchor_top1,0.0) anchor_top1, COALESCE(a.anchor_top2,0.0) anchor_top2,
             COALESCE(a.anchor_margin,0.0) anchor_margin,
             o.org_concept_id, o.org_concept_name,
             COALESCE(o.org_top1,0.0) org_top1, COALESCE(o.org_top2,0.0) org_top2,
             COALESCE(o.org_margin,0.0) org_margin,
             COALESCE(o.org_top1,0.0)-COALESCE(a.anchor_top1,0.0) AS cross_pool_margin,
             false AS is_junk, false AS is_excluded, false AS is_clean_negation,
             'neutral' AS polarity_intent, CAST(NULL AS STRING) AS forced_anchor_label,
             CAST(NULL AS STRING) AS polarity_action, CAST(NULL AS STRING) AS winning_pool,
             'none' AS route, CAST(NULL AS BIGINT) AS final_value_as_concept_id,
             CAST(NULL AS STRING) AS final_concept_name, CAST(NULL AS STRING) AS final_type,
             'unmapped' AS final_tier, false AS is_suspected, CAST(NULL AS STRING) AS growth_grade,
             current_timestamp() AS scored_at
      FROM {KEYS} k
      LEFT JOIN {AC}  a USING (result_normalized)
      LEFT JOIN {OCt} o USING (result_normalized)""")
    for t in (KEYS, QV, AC, OCt):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    return SUBSTRATE

def _score_loinc_answers(dry_run=False):
    """Stage 3b LOINC arm (4-tuple grain). For each vector_ready result key whose 4-tuple's test maps to a
    test_concept_id with an answer list, score the result ONLY against that test's allowed answer concepts
    (answer-list-constrained, like the bulk LOINC arm). source='embedding_loinc_answer', tier='auto_value'."""
    if dry_run:
        return LOINC_PROP
    spark.sql(f"""
      CREATE OR REPLACE TABLE {LOINC_PROP} USING DELTA AS
      WITH q AS (
        SELECT DISTINCT eq.code_system, eq.code, eq.description, eq.result_normalized
        FROM {QUEUE} eq WHERE eq.kind='result' AND eq.status='vector_ready' AND eq.result_normalized IS NOT NULL),
      qv AS (
        SELECT q.*, e.embedding_vector AS qvec, tm.measurement_concept_id AS test_cid
        FROM q JOIN {EMBEDDINGS_TABLE} e ON LOWER(e.term)=q.result_normalized AND e.embedding_vector IS NOT NULL
        JOIN {TEST_MAP} tm ON tm.code_system=q.code_system AND tm.code=q.code AND tm.description=q.description
          AND tm.confidence_tier IN ('curated','auto_high','auto_low') AND tm.measurement_concept_id IS NOT NULL),
      ans AS (
        SELECT al.test_concept_id, al.answer_concept_id, ci.concept_name, ci.vocabulary_id, e.embedding_vector AS cvec
        FROM {ANSWER_LISTS} al
        JOIN {RESULT_INDEX} ci ON ci.concept_id=al.answer_concept_id
        JOIN {EMBEDDINGS_TABLE} e ON LOWER(e.term)=LOWER(ci.concept_name) AND e.embedding_vector IS NOT NULL),
      sims AS (
        SELECT qv.code_system, qv.code, qv.description, qv.result_normalized,
               ans.answer_concept_id, ans.concept_name, ans.vocabulary_id,
               aggregate(zip_with(qv.qvec, ans.cvec, (x,y)->x*y), CAST(0.0 AS DOUBLE), (s,x)->s+x) AS sim
        FROM qv JOIN ans ON ans.test_concept_id = qv.test_cid),
      ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY code_system,code,description,result_normalized ORDER BY sim DESC) rn,
               (sim - LEAD(sim) OVER (PARTITION BY code_system,code,description,result_normalized ORDER BY sim DESC)) AS marg
        FROM sims)
      SELECT code_system, code, description, result_normalized,
             answer_concept_id AS value_as_concept_id, vocabulary_id AS concept_vocabulary_id, concept_name,
             'embedding_loinc_answer' AS result_mapping_source, sim AS similarity_score, 'auto_value' AS confidence_tier
      FROM ranked
      WHERE rn=1 AND sim >= {FLOOR_LOINC} AND COALESCE(marg,1.0) >= {MARGIN_LOINC}""")
    return LOINC_PROP

# byte-identical to pathology_omop_finding_decode.norm_expr (dual-maintenance hazard — keep in sync)
def _finding_norm_expr(col):
    return (f"regexp_replace(regexp_replace(regexp_replace(lower(trim({col})), "
            f"'\\\\.br\\\\|[\\n/]', ' '), "
            f"'see (further )?guidance.*|http\\\\S+', ' '), "
            f"'\\\\s+', ' ')")
_FIND_ADVISORY_RE = (r"film (hould|should) be con ?idered|(hould|should) be con ?idered if|(hould|should) be interpreted|"
                     r"interpretation for|hba1c interpretation|\\blimitation|"
                     r"plea ?e (end|send|refer|mea ?ure|di ?cu)|please (send|refer|measure|discuss)|"
                     r"particularly rel|\\bif .{0,25}(unexplained|new|per ?i ?t|persist)|"
                     r"^\\?| \\? ?(aki|ckd|viral|racial|ida|therapy)")
_FIND_ADVISORY_KEEP_RE = r"warning,.{0,40}if clinically indicated"
_FIND_NEG_CUE = r'(\\bno\\b|\\bnot\\b|\\bnil\\b|absent|negative for|free of)'

def _decode_value_findings(dry_run=False):
    """Stage 3b-iv. Deterministic FLAG_DICT decode over this run's net-new result keys (NO cosine).
    VALUE: morphology tokens -> value_as_concept_id (auto_value, source flag_decode). FINDING: finding
    tokens -> result_finding_* + suborder (finding axis only; NEVER value_as_concept_id). Logic ported
    from pathology_omop_morphology_normalizer (Cell 2 token explode) + pathology_omop_finding_decode."""
    if dry_run:
        return VALUE_PROP, FINDING_PROP
    from pyspark.sql import functions as _F
    from pyspark.sql.window import Window as _W
    # ---- VALUE (morphology): explode tokens on separators, exact-match the dict ----
    spark.sql(f"""
      CREATE OR REPLACE TABLE {VALUE_PROP} USING DELTA AS
      WITH ks AS (SELECT DISTINCT result_normalized FROM {QUEUE}
                  WHERE kind='result' AND status='vector_ready' AND result_normalized IS NOT NULL),
      clean AS (
        SELECT result_normalized,
               explode(split(regexp_replace(result_normalized, r'\\\\.br\\\\|[\\n/]', ' '), r'[\\s,;]+')) AS tok
        FROM ks),
      m AS (SELECT c.result_normalized, d.value_concept_id, d.concept_name
            FROM clean c JOIN {MORPH_DICT} d ON c.tok = d.flag_token)
      SELECT result_normalized, value_concept_id AS value_as_concept_id, concept_name,
             'flag_decode' AS result_mapping_source, 'auto_value' AS confidence_tier,
             CAST(1.0 AS DOUBLE) AS similarity_score
      FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY result_normalized ORDER BY value_concept_id) rn FROM m)
      WHERE rn=1""")
    # ---- FINDING: norm + advisory drop + dict rlike + finding-adjacent neg guard + suppression + suborder
    keys = spark.sql(f"SELECT DISTINCT result_normalized FROM {QUEUE} "
                     f"WHERE kind='result' AND status='vector_ready' AND result_normalized IS NOT NULL")
    src = (keys.withColumnRenamed("result_normalized","rn_raw")
              .withColumn("result_normalized", _F.expr(_finding_norm_expr("rn_raw")))
              .filter(~(_F.col("result_normalized").rlike(_FIND_ADVISORY_RE)
                        & ~_F.col("result_normalized").rlike(_FIND_ADVISORY_KEEP_RE))))
    dict_df = spark.table(FINDING_DICT)
    # cross-join string x dict; match via Spark-3.5 col-arg rlike (VERBATIM from pathology_omop_finding_decode)
    matched = (src.crossJoin(_F.broadcast(dict_df))
                  .filter(_F.col("result_normalized").rlike(_F.col("token_regex"))))
    # Adjacency neg guard: per-concept driver-collected boolean -- F.when(concept==cid, rlike(NEG_CUE.{0,25}?(?:tok)))
    # (VERBATIM from the bulk decoder; a single generic concat(...) regex is NOT equivalent on multi-finding strings).
    dict_rows = dict_df.select("finding_concept_id", "token_regex").collect()
    neg_col = _F.lit(False)
    for _r in dict_rows:
        _cid = _r["finding_concept_id"]; _tok = _r["token_regex"]
        _pat = f"{_FIND_NEG_CUE}.{{0,25}}?(?:{_tok})"
        neg_col = _F.when(_F.col("finding_concept_id") == _F.lit(_cid),
                          _F.col("result_normalized").rlike(_pat)).otherwise(neg_col)
    matched = matched.withColumn("neg_here", neg_col)
    kept = matched.filter(~_F.col("neg_here")).select(
        "result_normalized","finding_concept_id","concept_name","domain",
        "suppression_group","specificity_rank","priority")
    w_grp = _W.partitionBy("result_normalized","suppression_group")
    sup = (kept.withColumn("mx", _F.max("specificity_rank").over(w_grp))
               .filter(_F.col("specificity_rank")==_F.col("mx")).drop("mx","specificity_rank"))
    w_str = _W.partitionBy("result_normalized").orderBy("priority","finding_concept_id")
    (sup.withColumn("result_finding_suborder", _F.row_number().over(w_str)-1)
        .select(_F.col("result_normalized"),
                _F.col("finding_concept_id").alias("result_finding_concept_id"),
                _F.col("concept_name").alias("result_finding_concept_name"),
                _F.col("domain").alias("result_finding_domain"),
                "result_finding_suborder")
        .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(FINDING_PROP))
    return VALUE_PROP, FINDING_PROP

def _firewall_merge_result(src_tbl):
    """Firewall MERGE of an arm proposal (10-col schema) into RESULT_MAP. Fills only currently-unmapped
    4-tuples (confidence_tier='unmapped' — curated/auto_high/auto_low and already-filled open-set rows are
    NEVER overwritten -> first-writer wins = bulk precedence). Inserts genuinely new 4-tuples. Explicit
    insert column set matching the dev map's 13 columns (NO is_suspected/growth_grade — clone-only)."""
    src = (spark.table(src_tbl)
           .withColumn("mapping_version", F.lit(1))
           .withColumn("mapped_at", F.current_timestamp()).withColumn("ADC_UPDT", F.current_timestamp()))
    if len(src.take(1)) == 0:
        return
    (DeltaTable.forName(spark, RESULT_MAP).alias("t").merge(src.alias("s"),
        "t.code_system=s.code_system AND t.code=s.code AND t.description=s.description "
        "AND t.result_normalized=s.result_normalized")
      .whenMatchedUpdate(condition="t.confidence_tier='unmapped'", set={
        "value_as_concept_id":"s.value_as_concept_id","concept_vocabulary_id":"s.concept_vocabulary_id",
        "concept_name":"s.concept_name","result_mapping_source":"s.result_mapping_source",
        "similarity_score":"s.similarity_score","confidence_tier":"s.confidence_tier",
        "mapping_version":"t.mapping_version + 1","mapped_at":"current_timestamp()","ADC_UPDT":"current_timestamp()"})
      .whenNotMatchedInsert(values={
        "code_system":"s.code_system","code":"s.code","description":"s.description",
        "result_normalized":"s.result_normalized","value_as_concept_id":"s.value_as_concept_id",
        "concept_vocabulary_id":"s.concept_vocabulary_id","concept_name":"s.concept_name",
        "result_mapping_source":"s.result_mapping_source","similarity_score":"s.similarity_score",
        "confidence_tier":"s.confidence_tier",
        "mapping_version":"s.mapping_version","mapped_at":"s.mapped_at","ADC_UPDT":"s.ADC_UPDT"}).execute())

def _compose_and_merge_result(fused, loinc_prop, value_prop):
    """Stage 3b-v. Compose the result-value arms into RESULT_MAP in the bulk integrate order (each MERGE
    fills only still-unmapped 4-tuples -> anchor wins over LOINC over value, first-writer = bulk precedence):
      1. anchor-fusion / organism / generic-positive (from the fused substrate, bulk ACCEPTED_T gate)
      2. LOINC-answer
      3. value/morphology
    Text-grain arms (1,3) fanned over this run's distinct 4-tuples; LOINC (2) is already 4-tuple grain."""
    KEYS4 = f"{MAP_SCHEMA}._tmp_compose_keys"
    arm1  = f"{MAP_SCHEMA}._tmp_arm1"
    arm3  = f"{MAP_SCHEMA}._tmp_arm3"
    spark.sql(f"""CREATE OR REPLACE TABLE {KEYS4} USING DELTA AS
      SELECT DISTINCT code_system, code, description, result_normalized FROM {QUEUE}
      WHERE kind='result' AND status='vector_ready' AND result_normalized IS NOT NULL""")
    # arm 1: fused substrate. ACCEPTANCE GATE COPIED from pathology_omop_openset_map_integrate ACCEPTED_T:
    # final_value_as_concept_id NOT NULL, NOT is_junk, NOT is_excluded, AND route-specific (anchor|genpos|
    # organism with polarity_intent<>'negative' + floor/margin/delta). Without this the loop would promote
    # organism rows the bulk integrate rejects -> breaks parity.
    spark.sql(f"""CREATE OR REPLACE TABLE {arm1} USING DELTA AS
      SELECT k.code_system, k.code, k.description, k.result_normalized,
             s.final_value_as_concept_id AS value_as_concept_id,
             c.vocabulary_id AS concept_vocabulary_id, s.final_concept_name AS concept_name,
             CASE WHEN s.route='organism' THEN 'embedding_organism'
                  WHEN s.route='generic_positive' THEN 'deterministic_generic_positive'
                  ELSE 'embedding_anchor' END AS result_mapping_source,
             CAST(CASE WHEN s.route='organism' THEN s.org_top1
                       WHEN s.route='generic_positive' THEN 1.0
                       ELSE s.anchor_top1 END AS DOUBLE) AS similarity_score,
             s.final_tier AS confidence_tier
      FROM {KEYS4} k JOIN {fused} s USING (result_normalized)
      LEFT JOIN {CONCEPT} c ON c.concept_id = s.final_value_as_concept_id
      WHERE s.final_value_as_concept_id IS NOT NULL
        AND NOT s.is_junk AND NOT s.is_excluded
        AND ( s.route='anchor' OR s.route='generic_positive'
              OR ( s.route='organism' AND s.polarity_intent <> 'negative'
                   AND s.org_top1 >= {OS_FLOOR_VALUE} AND s.org_margin >= {OS_MARGIN_VALUE}
                   AND (s.org_top1 - s.anchor_top1) >= {OS_DELTA_VALUE} ) )""")
    _firewall_merge_result(arm1)
    # arm 2: LOINC (already 4-tuple grain)
    _firewall_merge_result(loinc_prop)
    # arm 3: value/morphology fanned over 4-tuples
    spark.sql(f"""CREATE OR REPLACE TABLE {arm3} USING DELTA AS
      SELECT k.code_system, k.code, k.description, k.result_normalized,
             v.value_as_concept_id, c.vocabulary_id AS concept_vocabulary_id, v.concept_name,
             v.result_mapping_source, v.similarity_score, v.confidence_tier
      FROM {KEYS4} k JOIN {VALUE_PROP} v USING (result_normalized)
      LEFT JOIN {CONCEPT} c ON c.concept_id = v.value_as_concept_id""")
    _firewall_merge_result(arm3)
    for t in (KEYS4, arm1, arm3):
        spark.sql(f"DROP TABLE IF EXISTS {t}")

def _remap_result(dry_run=False):
    """Stage 3b (REBUILT 2026-06-30 — open-set + fusion parity). Score net-new result keys against the
    open-set pools (serverless substrate), route+fuse via the canonical rerouters, decode value/finding
    FLAG_DICTs, compose into RESULT_MAP with the bulk three-MERGE firewall precedence, then flip
    vector_ready->done (keyed on the lossless result_normalized). Returns the finding proposal table name
    for the finding backfill (Stage 5), or None on an empty run. The legacy nearest-context arm is RETIRED."""
    n_vr = spark.sql(f"SELECT COUNT(*) c FROM {QUEUE} WHERE kind='result' AND status='vector_ready'").first()["c"]
    if dry_run:
        return n_vr
    if n_vr == 0:
        # Empty run: return None so apply_finding_to_map_pathology cannot re-apply a stale prior proposal.
        return None
    substrate = _build_openset_substrate(dry_run=False)
    _reroute_openset(substrate)                       # inlined router (was _run_openset_rerouter)
    _fuse_substrate(substrate, SUBSTRATE_FUSED)        # inlined fusion (was _run_fusion_rerouter)
    _apply_gate_veto(SUBSTRATE_FUSED)                  # §B polarity veto (cache-backed; no-op if cache absent)
    loinc_prop = _score_loinc_answers(dry_run=False)
    value_prop, finding_prop = _decode_value_findings(dry_run=False)
    _compose_and_merge_result(SUBSTRATE_FUSED, loinc_prop, value_prop)
    spark.sql(f"UPDATE {QUEUE} SET status='done' WHERE kind='result' AND status='vector_ready'")
    for t in (SUBSTRATE, SUBSTRATE_FUSED, LOINC_PROP, VALUE_PROP):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    return finding_prop

# COMMAND ----------

def remap_keys(dry_run=False):
    """Stage 3 wrapper — remap vector_ready test + result keys into the concept maps. _remap_test is the
    serverless cross-join test arm (unchanged); _remap_result is the REBUILT Stage 3b open-set+fusion arm
    (returns the finding proposal table for the Stage-5 finding backfill, or None). The backstop done-flip
    covers any residual vector_ready test rows (_remap_result already flips its own result rows to done;
    a backstop result-flip would strand the finding proposal's keys, so the sweep is TEST-scoped now)."""
    n_test = _remap_test(dry_run)
    try:
        finding_prop = _remap_result(dry_run)
    finally:
        # Test sweep runs even when the result arm raises (BULK_REMAP_DEFERRED on a large result backlog).
        if not dry_run:
            spark.sql(f"UPDATE {QUEUE} SET status='done' WHERE status='vector_ready' AND kind='test'")
    return {"n_test_remapped": n_test, "n_result_remapped": (finding_prop if dry_run else "done"),
            "finding_prop": (None if dry_run else finding_prop)}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 — Stage 4: classify_map_delta (+ pathology_map_baseline)
# MAGIC Diffs current maps vs the last-reconciled baseline → ADDITIVE (NULL→concept) vs CORRECTION
# MAGIC (concept changed). snapshot_baseline() has a TOCTOU guard: it refuses to snapshot while
# MAGIC rebuild_flagged=TRUE (a late external correction must not be baked into the baseline).

# COMMAND ----------

BASELINE = f"{MAP_SCHEMA}.pathology_map_baseline"
FINDING_BASELINE = f"{MAP_SCHEMA}.pathology_finding_baseline"  # finding-axis reconciliation baseline
def snapshot_baseline(force=False):
    """Persist the current (map, key, concept_id) set as the reconciliation baseline. Called by
    apply_to_map_pathology on the additive path AND by the map_pathology FULL_REBUILD (force=True).
    TOCTOU guard: refuse to snapshot while a correction is outstanding — otherwise a concurrent/late
    external correction could be folded into the baseline and become permanently invisible to
    classify_map_delta. Only FULL_REBUILD (which reconciles the whole table) may force.

    The rebuild flag is read from the Map-Pipeline-owned MP_REBUILD_FLAG table (single-row
    pathology_map_rebuild_flag, which replaced map_pathology_state). A missing table/row is treated
    as FALSE (no correction outstanding) so the snapshot proceeds — the Map Pipeline section creates
    the flag table before the weekly job runs, so a missing table only happens in isolated/first-run
    contexts where there is by definition no outstanding correction."""
    if not force:
        if not spark.catalog.tableExists(MP_REBUILD_FLAG):
            flag = False
        else:
            st = spark.sql(f"SELECT rebuild_flagged FROM {MP_REBUILD_FLAG} WHERE id=1").first()
            flag = bool(st["rebuild_flagged"]) if (st is not None and st["rebuild_flagged"] is not None) else False
        if flag:
            print("snapshot_baseline SKIPPED: rebuild_flagged=TRUE (correction outstanding; await FULL_REBUILD).")
            return
    spark.sql(f"""
      CREATE OR REPLACE TABLE {BASELINE} USING DELTA AS
      SELECT 'test' AS map, code_system, code, description, CAST(NULL AS STRING) AS result_normalized,
             measurement_concept_id AS concept_id FROM {TEST_MAP}
      UNION ALL
      SELECT 'result', code_system, code, description, result_normalized, value_as_concept_id FROM {RESULT_MAP}
    """)
    # FINDING axis baseline (text-grain (result_normalized, suborder) -> finding_concept_id), snapshotted
    # from the bronze finding columns. Only when MP_TARGET carries them (post finding-axis fold); otherwise
    # the finding backfill is a no-op so there is nothing to baseline.
    if _mp_has_finding_cols():
        spark.sql(f"""
          CREATE OR REPLACE TABLE {FINDING_BASELINE} USING DELTA AS
          SELECT DISTINCT
                 LOWER(TRIM(REGEXP_REPLACE(value_source_value,'\\\\s+',' '))) AS result_normalized,
                 result_finding_suborder AS suborder, result_finding_concept_id AS concept_id
          FROM {MP_TARGET} WHERE result_finding_concept_id IS NOT NULL
        """)

# COMMAND ----------

def classify_map_delta(dry_run=False):
    """Diff current maps vs the persisted baseline. additive = key absent/NULL in baseline now non-NULL.
    correction = key had a non-NULL concept in baseline, now a DIFFERENT non-NULL concept (incl. tier
    promotion that changed the concept_id). Covers BOTH the value/test maps AND the finding axis: the
    finding diff compares THIS run's finding decode (FINDING_PROP, text-grain (result_normalized,suborder))
    vs FINDING_BASELINE — a changed finding concept at any suborder is a correction (folds into
    n_correction_keys so apply_* aborts). Returns {n_additive_keys, n_correction_keys, additive_df,
    correction_df, n_finding_correction_keys}."""
    if spark.catalog.tableExists(BASELINE):
        delta_sql = f"""
        WITH cur AS (
          SELECT 'test' AS map, code_system, code, description, CAST(NULL AS STRING) AS result_normalized, measurement_concept_id AS cid FROM {TEST_MAP}
          UNION ALL SELECT 'result', code_system, code, description, result_normalized, value_as_concept_id FROM {RESULT_MAP}
        )
        SELECT cur.*, b.concept_id AS base_cid,
          CASE
            WHEN (b.concept_id IS NULL) AND cur.cid IS NOT NULL THEN 'additive'
            WHEN b.concept_id IS NOT NULL AND cur.cid IS NOT NULL AND b.concept_id <> cur.cid THEN 'correction'
            ELSE 'unchanged' END AS delta_type
        FROM cur LEFT JOIN {BASELINE} b
          ON b.map=cur.map AND b.code_system=cur.code_system AND b.code=cur.code AND b.description=cur.description
         AND b.result_normalized <=> cur.result_normalized
        WHERE NOT ((b.concept_id IS NULL AND cur.cid IS NULL) OR (b.concept_id <=> cur.cid))
        """
    else:
        # No baseline yet (first run): everything currently mapped is treated as additive baseline seed.
        delta_sql = f"""
        SELECT 'test' AS map, code_system, code, description, CAST(NULL AS STRING) AS result_normalized,
               measurement_concept_id AS cid, CAST(NULL AS BIGINT) AS base_cid, 'additive' AS delta_type
        FROM {TEST_MAP} WHERE measurement_concept_id IS NOT NULL
        UNION ALL
        SELECT 'result', code_system, code, description, result_normalized, value_as_concept_id, NULL, 'additive'
        FROM {RESULT_MAP} WHERE value_as_concept_id IS NOT NULL
        """
    d = spark.sql(delta_sql)
    add_df = d.where("delta_type='additive'")
    cor_df = d.where("delta_type='correction'")
    n_corr = cor_df.count()
    # ---- FINDING-axis delta: current run's decode (FINDING_PROP) vs FINDING_BASELINE on (result_normalized, suborder)
    n_find_corr = 0
    if spark.catalog.tableExists(FINDING_PROP) and spark.catalog.tableExists(FINDING_BASELINE):
        fc = spark.sql(f"""
          SELECT COUNT(*) c FROM {FINDING_PROP} l JOIN {FINDING_BASELINE} b
            ON l.result_normalized=b.result_normalized AND l.result_finding_suborder=b.suborder
          WHERE b.concept_id IS NOT NULL AND l.result_finding_concept_id IS NOT NULL
            AND b.concept_id <> l.result_finding_concept_id
        """).first()["c"]
        n_find_corr = int(fc)
    return {"n_additive_keys": add_df.count(), "n_correction_keys": n_corr + n_find_corr,
            "n_finding_correction_keys": n_find_corr,
            "additive_df": add_df, "correction_df": cor_df}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 — Stage 5: apply_to_map_pathology (additive backfill, 6-key MERGE, NO source re-scan)
# MAGIC Re-derives mapped columns by joining EXISTING map_pathology rows back to the maps+concept.
# MAGIC MERGE matches the verified 6-key (source_table,source_event_id,lab_no,code,description,value_source_value)
# MAGIC — the 3-key is NOT unique (1021 raw collisions). Pass A: test-unmapped. Pass B: result-only additive.
# MAGIC Correction → abort + set rebuild_flagged.

# COMMAND ----------

def apply_to_map_pathology(additive_df, correction_present, dry_run=False):
    """Stage 5. correction_present -> abort (set rebuild_flagged, no write). Else two additive passes,
    both re-deriving columns by joining EXISTING map_pathology rows back to the maps (NO source re-scan):
      Pass A: rows with measurement_concept_id IS NULL that the TEST map now covers (full re-projection).
      Pass B: rows already test-mapped but value_as_concept_id IS NULL & non-numeric, whose RESULT key
              now maps (result-only additive). Both are strictly NULL->non-null; never overwrite.

    MERGE matches the VERIFIED 6-key row identity (source_table, source_event_id, lab_no, code,
    description, value_source_value) — the 3-key (source_table,source_event_id,lab_no) is NOT unique
    (1021 raw collisions sharing a TFCResultSeq with distinct result values). code/description/
    value_source_value use IS NOT DISTINCT FROM because all can be NULL (13,849 rows have NULL code).

    I1: the backfill is BOUNDED to additive_df's keys — both passes SEMI-JOIN their NULL-mapped base
    slice to THIS run's additive keys (staged to a Delta scratch table) so only keys that became
    mappable this run are re-scanned, not the whole ~18M-row NULL slice every run. If additive_df is
    None (explicit opt-in for a full backfill), no key filter is applied and both passes full-scan."""
    if correction_present:
        if not dry_run:
            spark.sql(f"UPDATE {MP_REBUILD_FLAG} SET rebuild_flagged = TRUE WHERE id = 1")
        return {"aborted_for_correction": True, "n_map_backfilled": 0, "rebuild_flagged": True}

    # v2 FIX #1: rebuild EXCL_REGEX_SQL in scope EXACTLY as map_pathology does (doubled-backslash),
    # then interpolate with a SINGLE brace below.
    _excl = [r.pattern for r in spark.table(EXCL_TBL).select("pattern").collect()]
    EXCL_REGEX_SQL = ("(" + "|".join(_excl) + ")").replace("\\", "\\\\")

    _NUM = r"^\\s*[<>]?=?\\s*-?[0-9.]+\\s*$"   # map_pathology numeric RLIKE (doubled for the f-string)

    # I1: bound the backfill to THIS run's additive keys (else both passes re-scan ~18M rows every run).
    # Stage the additive keys to a Delta scratch table; each pass SEMI-JOINs its base to the relevant kind.
    ADD_KEYS = f"{MAP_SCHEMA}._tmp_apply_additive_keys"
    if not dry_run and additive_df is not None:
        (additive_df.select("map","code_system","code","description","result_normalized")
            .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(ADD_KEYS))
    _have_keys = (not dry_run) and (additive_df is not None)
    # If additive_df is None (explicit opt-in for a full backfill), _have_keys=False → no key filter →
    # full-scan fallback (preserves the old whole-NULL-slice behavior). The correction path already
    # returned above, so None only reaches here when a caller deliberately requests a full backfill.
    _passA_keyfilter = (f"AND EXISTS (SELECT 1 FROM {ADD_KEYS} ak WHERE ak.map='test' "
                        f"AND ak.code_system=mp.code_system AND ak.code=mp.code AND ak.description=mp.description)") if _have_keys else ""
    _passB_keyfilter = (f"AND EXISTS (SELECT 1 FROM {ADD_KEYS} ak WHERE ak.map='result' "
                        f"AND ak.code_system=mp.code_system AND ak.code=mp.code AND ak.description=mp.description "
                        f"AND ak.result_normalized = LOWER(TRIM(REGEXP_REPLACE(mp.value_source_value,'\\\\s+',' '))))") if _have_keys else ""

    # ── PASS A: test-unmapped rows the test map now covers (full re-projection) ───────────────────
    passA_sql = f"""
      MERGE INTO {MP_TARGET} t
      USING (
        WITH base AS (
          SELECT mp.source_table, mp.source_event_id, mp.lab_no, mp.code_system, mp.code, mp.description,
                 mp.value_source_value AS result_txt, mp.unit_source_value,
                 CASE WHEN mp.value_source_value RLIKE '{_NUM}' THEN 1 ELSE 0 END AS rd_result_numeric,
                 CASE WHEN NOT (mp.value_source_value RLIKE '{_NUM}')
                      THEN LOWER(TRIM(REGEXP_REPLACE(mp.value_source_value,'\\\\s+',' '))) END AS rd_result_normalized
          FROM {MP_TARGET} mp WHERE mp.measurement_concept_id IS NULL {_passA_keyfilter}
        ),
        tj AS (
          SELECT b.*, tm.measurement_concept_id, tm.concept_name AS measurement_concept_name, tm.confidence_tier AS test_confidence_tier
          FROM base b LEFT JOIN {TEST_MAP} tm
            ON tm.code_system=b.code_system AND tm.code=b.code AND tm.description=b.description
           AND tm.confidence_tier IN ('curated','auto_high','auto_low') AND tm.measurement_concept_id IS NOT NULL
        ),
        rj AS (
          SELECT tj.*, rm.value_as_concept_id, rm.concept_name AS result_concept_name, rm.confidence_tier AS result_confidence_tier
          FROM tj LEFT JOIN {RESULT_MAP} rm
            ON rm.code_system=tj.code_system AND rm.code=tj.code AND rm.description=tj.description
           AND rm.result_normalized=tj.rd_result_normalized
           AND rm.confidence_tier IN {CONSUMED_TIERS} AND rm.value_as_concept_id IS NOT NULL
           AND tj.rd_result_numeric = 0
        ),
        uj AS (
          SELECT rj.*, um.unit_concept_id, um.ucum_code
          FROM rj LEFT JOIN {UNIT_MAP} um ON um.unit_source_value=rj.unit_source_value AND rj.unit_source_value IS NOT NULL
        )
        SELECT u.source_table, u.source_event_id, u.lab_no, u.code, u.description, u.result_txt AS value_source_value,
               u.measurement_concept_id, u.measurement_concept_name, u.test_confidence_tier,
               CASE WHEN u.rd_result_numeric=1 THEN NULL ELSE u.value_as_concept_id END AS value_as_concept_id,
               u.value_as_concept_id AS value_as_concept_id_raw,
               u.result_concept_name, u.result_confidence_tier, u.unit_concept_id, u.ucum_code,
               mc.concept_code AS test_code, mc.vocabulary_id AS test_vocab, mc.standard_concept AS test_std,
               rc.concept_code AS result_code, rc.vocabulary_id AS result_vocab, rc.standard_concept AS result_std,
               CASE WHEN u.rd_result_numeric=1 THEN 'numeric'
                    WHEN (CASE WHEN u.rd_result_numeric=1 THEN NULL ELSE u.value_as_concept_id END) IS NOT NULL THEN 'mapped'
                    WHEN u.rd_result_normalized IS NOT NULL AND u.rd_result_normalized RLIKE '{EXCL_REGEX_SQL}' THEN 'excluded'
                    ELSE 'free_text' END AS result_status
        FROM uj u LEFT JOIN {CONCEPT} mc ON mc.concept_id=u.measurement_concept_id
                  LEFT JOIN {CONCEPT} rc ON rc.concept_id=u.value_as_concept_id
        WHERE u.measurement_concept_id IS NOT NULL
      ) s
      ON t.source_table = s.source_table
        AND t.source_event_id = s.source_event_id
        AND t.lab_no IS NOT DISTINCT FROM s.lab_no
        AND t.code IS NOT DISTINCT FROM s.code
        AND t.description IS NOT DISTINCT FROM s.description
        AND t.value_source_value IS NOT DISTINCT FROM s.value_source_value
      WHEN MATCHED AND t.measurement_concept_id IS NULL THEN UPDATE SET
        t.measurement_concept_id=s.measurement_concept_id, t.measurement_concept_name=s.measurement_concept_name,
        t.test_confidence_tier=s.test_confidence_tier, t.value_as_concept_id=s.value_as_concept_id,
        t.result_concept_name=s.result_concept_name, t.result_confidence_tier=s.result_confidence_tier,
        t.unit_concept_id=s.unit_concept_id, t.ucum_code=s.ucum_code,
        t.test_omop_concept_id=s.measurement_concept_id,
        t.test_snomed_code=CASE WHEN s.test_vocab='SNOMED' THEN s.test_code END,
        t.test_loinc_code=CASE WHEN s.test_vocab='LOINC' THEN s.test_code END,
        t.test_omop_standard_concept=s.test_std, t.test_vocabulary_id=s.test_vocab,
        t.result_omop_concept_id=s.value_as_concept_id_raw,
        t.result_snomed_code=CASE WHEN s.result_vocab='SNOMED' THEN s.result_code END,
        t.result_loinc_code=CASE WHEN s.result_vocab='LOINC' THEN s.result_code END,
        t.result_omop_standard_concept=s.result_std, t.result_vocabulary_id=s.result_vocab,
        t.result_status=s.result_status, t.ADC_UPDT=current_timestamp()
      """


    # ── PASS B: test-mapped, result-unmapped, non-numeric rows whose RESULT key now maps ──────────
    # Only value-side columns change; measurement_concept_id and OMOP_MANUAL_* test columns stay.
    passB_sql = f"""
      MERGE INTO {MP_TARGET} t
      USING (
        WITH base AS (
          SELECT mp.source_table, mp.source_event_id, mp.lab_no, mp.code_system, mp.code, mp.description,
                 mp.value_source_value,
                 LOWER(TRIM(REGEXP_REPLACE(mp.value_source_value,'\\\\s+',' '))) AS rd_result_normalized
          FROM {MP_TARGET} mp
          WHERE mp.measurement_concept_id IS NOT NULL AND mp.value_as_concept_id IS NULL
            AND mp.result_status <> 'numeric'
            AND NOT (mp.value_source_value RLIKE '{_NUM}')
            {_passB_keyfilter}
        )
        SELECT b.source_table, b.source_event_id, b.lab_no, b.code, b.description, b.value_source_value,
               rm.value_as_concept_id, rm.concept_name AS result_concept_name, rm.confidence_tier AS result_confidence_tier,
               rc.concept_code AS result_code, rc.vocabulary_id AS result_vocab, rc.standard_concept AS result_std,
               CASE WHEN rm.value_as_concept_id IS NOT NULL THEN 'mapped' END AS result_status
        FROM base b JOIN {RESULT_MAP} rm
          ON rm.code_system=b.code_system AND rm.code=b.code AND rm.description=b.description
         AND rm.result_normalized=b.rd_result_normalized
         AND rm.confidence_tier IN {CONSUMED_TIERS} AND rm.value_as_concept_id IS NOT NULL
        LEFT JOIN {CONCEPT} rc ON rc.concept_id=rm.value_as_concept_id
      ) s
      ON t.source_table = s.source_table
        AND t.source_event_id = s.source_event_id
        AND t.lab_no IS NOT DISTINCT FROM s.lab_no
        AND t.code IS NOT DISTINCT FROM s.code
        AND t.description IS NOT DISTINCT FROM s.description
        AND t.value_source_value IS NOT DISTINCT FROM s.value_source_value
      WHEN MATCHED AND t.value_as_concept_id IS NULL AND s.value_as_concept_id IS NOT NULL THEN UPDATE SET
        t.value_as_concept_id=s.value_as_concept_id, t.result_concept_name=s.result_concept_name,
        t.result_confidence_tier=s.result_confidence_tier,
        t.result_omop_concept_id=s.value_as_concept_id,
        t.result_snomed_code=CASE WHEN s.result_vocab='SNOMED' THEN s.result_code END,
        t.result_loinc_code=CASE WHEN s.result_vocab='LOINC' THEN s.result_code END,
        t.result_omop_standard_concept=s.result_std, t.result_vocabulary_id=s.result_vocab,
        t.result_status=s.result_status, t.ADC_UPDT=current_timestamp()
      """

    if dry_run:
        return {"aborted_for_correction": False, "n_map_backfilled": "(dry-run: see preview query)", "rebuild_flagged": False}
    # n_map_backfilled = REAL rows changed by each pass (Databricks MERGE result exposes num_updated_rows).
    # NOTE: the prior `_mapped_count` OR-predicate undercounted Pass-B (those rows were already measurement-
    # mapped, so a value-only update didn't change the OR). Sum the two passes' actual updated-row counts.
    _ma = spark.sql(passA_sql).first()
    _mb = spark.sql(passB_sql).first()
    spark.sql(f"DROP TABLE IF EXISTS {ADD_KEYS}")   # I1: drop the per-run additive-keys scratch table
    _na = int(_ma["num_updated_rows"]) if _ma is not None and "num_updated_rows" in _ma.asDict() else 0
    _nb = int(_mb["num_updated_rows"]) if _mb is not None and "num_updated_rows" in _mb.asDict() else 0
    return {"aborted_for_correction": False, "n_map_backfilled": _na + _nb,
            "n_map_backfilled_passA": _na, "n_map_backfilled_passB": _nb, "rebuild_flagged": False}

# COMMAND ----------

def _mp_has_finding_cols():
    """True iff MP_TARGET carries the 4 finding-axis columns. They land only when the finding-axis FULL_REBUILD
    CTAS runs (finding-axis runbook §2); until then the incremental finding backfill no-ops."""
    cols = {f.name for f in spark.table(MP_TARGET).schema.fields}
    return {"result_finding_concept_id","result_finding_concept_name",
            "result_finding_domain","result_finding_suborder"}.issubset(cols)

def apply_finding_to_map_pathology(finding_prop, dry_run=False):
    """Stage 5 (finding axis). Write the finding columns on bronze rows whose result decodes to findings.
    suborder 0 -> UPDATE the matching bronze row in place (keeps full payload). suborder>0 -> INSERT a sub-row
    carrying the parent identity but ALL value/test/unit/source OMOP fields NULLed (so the OMOP stage's
    measurement_concept_id IS NOT NULL filter drops it -> zero-OMOP-change). Identity = 7-key incl.
    result_finding_suborder so siblings are distinct + re-runs idempotent. NEVER touches value_as_concept_id.
    Guards: empty/absent proposal -> no-op (None on an empty run cannot re-apply a stale prior proposal);
    MP_TARGET without the finding columns (pre finding-axis fold) -> no-op with a clear message."""
    if dry_run or finding_prop is None or not spark.catalog.tableExists(finding_prop):
        return {"n_finding_updated": 0, "n_finding_inserted": 0}
    if not _mp_has_finding_cols():
        print(f"apply_finding_to_map_pathology SKIPPED: {MP_TARGET} has no result_finding_* columns "
              f"(finding-axis fold not yet applied). No-op.")
        return {"n_finding_updated": 0, "n_finding_inserted": 0, "skipped_no_finding_cols": True}
    BASE = f"{MAP_SCHEMA}._tmp_finding_base"
    spark.sql(f"""CREATE OR REPLACE TABLE {BASE} USING DELTA AS
      SELECT mp.source_table, mp.source_event_id, mp.lab_no, mp.code, mp.description, mp.value_source_value,
             LOWER(TRIM(REGEXP_REPLACE(mp.value_source_value,'\\\\s+',' '))) AS rd
      FROM {MP_TARGET} mp
      WHERE (mp.result_finding_suborder IS NULL OR mp.result_finding_suborder=0)
        AND mp.value_source_value IS NOT NULL""")
    fp = spark.table(finding_prop)
    joined = (spark.table(BASE).alias("b")
              .join(fp.alias("f"), F.col("b.rd")==F.col("f.result_normalized"))
              .select(
                F.col("b.source_table").alias("source_table"),
                F.col("b.source_event_id").alias("source_event_id"),
                F.col("b.lab_no").alias("lab_no"),
                F.col("b.code").alias("code"),
                F.col("b.description").alias("description"),
                F.col("b.value_source_value").alias("value_source_value"),
                F.col("f.result_finding_concept_id").alias("result_finding_concept_id"),
                F.col("f.result_finding_concept_name").alias("result_finding_concept_name"),
                F.col("f.result_finding_domain").alias("result_finding_domain"),
                F.col("f.result_finding_suborder").alias("result_finding_suborder")))
    tgt = DeltaTable.forName(spark, MP_TARGET)
    s0 = joined.filter("result_finding_suborder=0")
    n_upd = s0.count()
    if n_upd:
        (tgt.alias("t").merge(s0.alias("s"),
            "t.source_table=s.source_table AND t.source_event_id=s.source_event_id "
            "AND t.lab_no <=> s.lab_no AND t.code <=> s.code AND t.description <=> s.description "
            "AND t.value_source_value <=> s.value_source_value")
          .whenMatchedUpdate(condition="t.result_finding_suborder IS NULL OR t.result_finding_suborder=0", set={
            "result_finding_concept_id":"s.result_finding_concept_id",
            "result_finding_concept_name":"s.result_finding_concept_name",
            "result_finding_domain":"s.result_finding_domain",
            "result_finding_suborder":"0",
            "ADC_UPDT":"current_timestamp()"}).execute())
    sN = joined.filter("result_finding_suborder>0")
    n_ins = sN.count()
    if n_ins:
        (tgt.alias("t").merge(sN.alias("s"),
            "t.source_table=s.source_table AND t.source_event_id=s.source_event_id "
            "AND t.lab_no <=> s.lab_no AND t.code <=> s.code AND t.description <=> s.description "
            "AND t.value_source_value <=> s.value_source_value "
            "AND t.result_finding_suborder = s.result_finding_suborder")
          .whenNotMatchedInsert(values={
            "source_table":"s.source_table","source_event_id":"s.source_event_id","lab_no":"s.lab_no",
            "code":"s.code","description":"s.description","value_source_value":"s.value_source_value",
            "result_finding_concept_id":"s.result_finding_concept_id",
            "result_finding_concept_name":"s.result_finding_concept_name",
            "result_finding_domain":"s.result_finding_domain","result_finding_suborder":"s.result_finding_suborder",
            "ADC_UPDT":"current_timestamp()"}).execute())
    spark.sql(f"DROP TABLE IF EXISTS {BASE}")
    return {"n_finding_updated": n_upd, "n_finding_inserted": n_ins}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 9 — run_increment orchestrator (DEV/fixture driver; NOT the prod entry path)
# MAGIC Chains discover → embed → remap → classify → apply; writes ONE RUN_LOG row; supports dry_run
# MAGIC (exact for vector_ready keys, "pending embedding" for needs_embed). PROD entry is the
# MAGIC CC/mappipeline_map_pathology_section section (it `%run`s this module and calls the stage fns inline,
# MAGIC owning the gate/flag/baseline) — do NOT use run_increment in prod (it is the DEV/fixture driver).

# COMMAND ----------

def run_increment(since_watermark=None, max_embed_terms=MAX_EMBED_TERMS, max_cost_usd=None, dry_run=False):
    """DEV/fixture end-to-end driver + dev dry-run harness. NOT the prod entry path: in prod the
    CC/mappipeline_map_pathology_section section `%run`s this module and calls the stage fns inline,
    owning the gate/flag/baseline. Do NOT use this function in prod."""
    import uuid as _uuid
    t0 = utcnow()
    run_id = _uuid.uuid4().hex
    if since_watermark is None:
        # DEV/fixture default ONLY: the retired map_pathology_state table held last_source_watermark;
        # the replacement flag table (MP_REBUILD_FLAG) has no watermark column. Derive a family-style
        # watermark from the target instead (matches get_max_timestamp's default-date semantics:
        # full-rebuild-from-scratch when the target is empty). No state-table dependency.
        since_watermark = spark.sql(
            f"SELECT COALESCE(MAX(ADC_UPDT), TIMESTAMP'1980-01-01') FROM {MP_TARGET}").first()[0]
    d1 = discover_new_keys(since_watermark, dry_run=dry_run)
    d2 = embed_pending_capped(max_embed_terms, max_cost_usd, dry_run=dry_run)
    finding_prop = None
    if not dry_run:
        rk = remap_keys(dry_run=False)
        finding_prop = rk.get("finding_prop")   # Stage 3b finding proposal (None on empty run)
    d4 = classify_map_delta(dry_run=dry_run)
    corr = d4["n_correction_keys"] > 0
    d5 = apply_to_map_pathology(d4.get("additive_df"), corr, dry_run=dry_run)
    # finding backfill (same correction gate as the value backfill): skip on a correction; no-op pre-fold
    fr = (apply_finding_to_map_pathology(finding_prop, dry_run=dry_run)
          if (not corr) else {"n_finding_updated": 0, "n_finding_inserted": 0})
    if not dry_run and not corr:
        snapshot_baseline()
    # n_map_inserted = 0 here BY DESIGN: the new-source-row INSERT is the Task-12 MERGE owned by
    # map_pathology_pipeline (not invoked from run_increment); in prod that cell logs its own insert count.
    n_backfilled = int(d5["n_map_backfilled"]) if str(d5.get("n_map_backfilled","")).lstrip("-").isdigit() else 0
    dur = (utcnow() - t0).total_seconds()
    cols = ["run_id","run_ts","since_watermark","n_missing_test_keys","n_missing_result_keys","n_vector_ready",
            "n_needs_embed","n_embedded","n_deferred","est_cost_usd","n_additive_keys","n_correction_keys",
            "n_map_backfilled","n_map_inserted","n_bbv_result_override_skipped",
            "rebuild_flagged","aborted_for_correction","dry_run","duration_s"]
    # n_bbv_result_override_skipped=0 is a PLACEHOLDER pending the override-skip observability hook
    # (the Task-5b decision reserved this column; precise population is a future enhancement, out of
    # scope for this dev driver — but the value must be present so createDataFrame matches RUN_LOG's schema).
    row = [(run_id, t0.replace(tzinfo=None), since_watermark,
            d1["n_missing_test_keys"], d1["n_missing_result_keys"], d1["n_vector_ready"], d1["n_needs_embed"],
            d2["n_embedded"], d2["n_deferred"], float(d2["est_cost_usd"]),
            d4["n_additive_keys"], d4["n_correction_keys"],
            n_backfilled, 0, 0,
            bool(d5.get("rebuild_flagged", False)), bool(d5.get("aborted_for_correction", False)), bool(dry_run), float(dur))]
    spark.createDataFrame(row, cols).write.format("delta").mode("append").saveAsTable(RUN_LOG)
    summary = dict(zip(cols, row[0]))
    # finding-axis counts: kept on the returned summary (RUN_LOG schema unchanged to avoid a migration;
    # the prod section logs finding counts in its own cell if desired).
    summary["n_finding_updated"] = int(fr.get("n_finding_updated", 0))
    summary["n_finding_inserted"] = int(fr.get("n_finding_inserted", 0))
    summary["n_finding_correction_keys"] = int(d4.get("n_finding_correction_keys", 0))
    print(f"run_increment done (dry_run={dry_run}): {summary}")
    return summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## HUMAN-GATE — real-AOAI smoke test (guarded `if False:`; never auto-runs)
# MAGIC The ONLY path that calls real Azure OpenAI. A human flips `if False:` → `if True:` in a
# MAGIC prod-authorised session to do a single 1-term end-to-end embed→remap as a live sanity check.

# COMMAND ----------

if False:  # HUMAN GATE — flip to True only for a deliberate 1-term live AOAI sanity check
    _t = "zzz_smoke_test_term_" + utcnow().strftime("%Y%m%d%H%M%S")
    # 9-col positional INSERT (queue gained result_normalized + embed_text). kind='test' -> result_normalized
    # NULL, embed_text = the context term (tests embed/score on the context string, unchanged).
    spark.sql(f"""INSERT INTO {QUEUE} VALUES
      ('CERNER_TESTCODE','ZZSMOKE','{_t}', LOWER('ZZSMOKE | {_t}'), 'test', 'pending', LOWER('ZZSMOKE | {_t}'),
       NULL, LOWER('ZZSMOKE | {_t}'))""")
    print(embed_pending_capped(max_terms=1))   # real AOAI: ~1 embed call
    n_vec = spark.sql(f"SELECT COUNT(*) c FROM {EMBEDDINGS_TABLE} WHERE LOWER(term)=LOWER('ZZSMOKE | {_t}') AND embedding_vector IS NOT NULL").first()["c"]
    assert n_vec >= 1, "smoke: no vector landed"
    print(remap_keys())
    n_map = spark.sql(f"SELECT COUNT(*) c FROM {TEST_MAP} WHERE code='ZZSMOKE'").first()["c"]
    assert n_map >= 1, "smoke: no test-map row produced"
    print("SMOKE OK — clean up: DELETE the ZZSMOKE queue/map rows + the test embedding.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## INTEGRATION — how this module runs in prod (the Map Pipeline family job)
# MAGIC
# MAGIC **Prod entrypoint = the map-family builder** `/Workspace/Shared/ADC-DB/Prod/Pipelines/Map Pipeline`
# MAGIC (Job 1008776761501829). map_pathology builds there alongside the other `map_*` tables, in ONE compute
# MAGIC run, via the section pasted from `CC/mappipeline_map_pathology_section`. The old standalone
# MAGIC `map_pathology_pipeline` is RETIRED, and the earlier plan to paste stages 1–4 into the nomenclature
# MAGIC notebook (`2313193198223400`) is SUPERSEDED — the whole embed loop now runs INLINE in the section.
# MAGIC
# MAGIC **This module is `%run` from the Map Pipeline notebook** (co-locate it next to that notebook). On the
# MAGIC incremental path the section calls the stage functions inline, in order:
# MAGIC `discover_new_keys → embed_pending_capped → remap_keys → classify_map_delta → apply_to_map_pathology`,
# MAGIC then `snapshot_baseline()`. On FULL_REBUILD it calls `snapshot_baseline(force=True)`. The section OWNS
# MAGIC the gate/flag/baseline decision (see its flag-wiring header): it always calls
# MAGIC `apply_to_map_pathology(..., correction_present=False)` and sets the rebuild flag itself on a correction.
# MAGIC
# MAGIC **Do NOT use `run_increment` in prod** — it is the DEV/fixture end-to-end driver only (the section wires
# MAGIC the stages itself). It derives its watermark from `MAX(ADC_UPDT)` on `MP_TARGET` (no state table).
# MAGIC
# MAGIC **RUN THE MIGRATION NOTEBOOK FIRST.** `pathology_embed_promote_to_prod` must be run once (it clones the
# MAGIC maps/queue + builds the freq table + creates the run-log in `3_lookup.omop`). Only then are the prod loop
# MAGIC tables in place for these constants to point at.
# MAGIC
# MAGIC **PROD constant flips (edit this module's config cell on promotion — AND the section's Cell-3 constants):**
# MAGIC - `MAP_SCHEMA        = "3_lookup.omop"`   (was `8_dev.omop`; loop tables promoted there — NOT `4_prod.omop`)
# MAGIC - `MP_TARGET         = "4_prod.bronze.map_pathology"`
# MAGIC - `MP_REBUILD_FLAG` needs NO flip — it follows `MAP_SCHEMA` (`{MAP_SCHEMA}.pathology_map_rebuild_flag`). The
# MAGIC   Map Pipeline section creates/owns/seeds it; do NOT seed `map_pathology_state` (RETIRED).
# MAGIC - `EMBEDDINGS_TABLE` stays `3_lookup.embeddings.terms`; `CONCEPT` stays `3_lookup.omop.concept` (both global).
# MAGIC - `FREQ_TABLE_MILLRAW` reads the SAME mill/raw `4_prod.raw.*` sources (already prod paths — no change).
# MAGIC - `RUN_LOG` → `3_lookup.omop.pathology_embed_run_log` (follows `MAP_SCHEMA`; migration creates it fresh+empty).
# MAGIC
# MAGIC **ENV PREP (load-bearing — surfaced by the fixture):**
# MAGIC - v2.3: openai + the `adc_store` secret are LAZY (imported/fetched only when an actual embed runs, inside
# MAGIC   `_get_embedding_client`). So `%run`-importing this module needs NO env-prep — it loads on ANY compute.
# MAGIC - The embed path itself (`embed_pending_capped → _embed_one_batch → _get_embedding_client`) STILL requires:
# MAGIC   (a) `openai` installed (`%pip install openai pyarrow` at the top of the Map Pipeline notebook OR a cluster
# MAGIC   library); and (b) the `adc_store` secret scope (key `barts_global_key`) reachable by the job's run identity.
# MAGIC   Without both, the FIRST real embed (not the import) fails.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handoff — done vs human-gated
# MAGIC
# MAGIC **Done & verified in dev (`8_dev`):**
# MAGIC - All 5 stages + the `run_increment` dev driver authored; openai imported LAZILY (`%run`-safe on any compute).
# MAGIC - The fixture harness (`pathology_embed_increment_fixture`) runs end-to-end GREEN — **24/24 assertions**:
# MAGIC   fan-out dedup, Pass A / Pass B additive backfill, excluded branch, answer-list scoping, result-collision
# MAGIC   dedup, correction abort + TOCTOU, idempotency, NULL-safe 6-key uniqueness, source-free column parity.
# MAGIC - Integration deliverables: `CC/mappipeline_map_pathology_section` (the map_pathology section for the Map
# MAGIC   Pipeline family — reuses `update_table`/`get_max_timestamp`, owns the rebuild flag, runs the embed loop
# MAGIC   inline); `CC/mappipeline_update_table_patch` (the backward-compatible `update_condition=` upgrade to the
# MAGIC   family `update_table`); the standalone `map_pathology_pipeline` marked RETIRED.
# MAGIC - The rebuild flag lives in the single-row `{MAP_SCHEMA}.pathology_map_rebuild_flag` (owned by the section;
# MAGIC   read by this module's `snapshot_baseline` guard). `map_pathology_state` is RETIRED.
# MAGIC
# MAGIC **Remains human-gated (prod, in order):**
# MAGIC 1. Run `pathology_embed_promote_to_prod` once (clones loop tables → `3_lookup.omop`, builds freq, fresh run-log).
# MAGIC 2. Co-locate this module next to the Map Pipeline notebook; ensure `%run ./pathology_embed_increment` near its top.
# MAGIC 3. Paste the `update_table` patch (`CC/mappipeline_update_table_patch`) over the family `update_table`, then paste
# MAGIC    the `CC/mappipeline_map_pathology_section` cells after the nomenclature + concept-map builders.
# MAGIC 4. Flip the PROD constants (above) in BOTH this module's config and the section's Cell-3 constants; ensure env prep.
# MAGIC 5. Run the FIRST build as a FULL_REBUILD on a CLUSTER (seeds bronze + the baseline + the flag table).
# MAGIC 6. Schedule the weekly family job; the incremental path auto-selects via the gate. The FIRST weekly run validates
# MAGIC    the RAW incremental path + real AOAI cost (dev used a STUB embed; the RAW branch was UNEXERCISED in dev).
# MAGIC 7. The real-AOAI smoke cell (above, guarded `if False:`) for a deliberate live 1-term sanity check.
# MAGIC
# MAGIC **Cost cap:** `embed_pending_capped` embeds ≤ `MAX_EMBED_TERMS` DISTINCT texts per run; capped overflow is
# MAGIC flipped `pending → deferred` (in-warehouse, logged), picked up by a later run. `est_cost_usd` is the ATTEMPTED
# MAGIC upper bound (cap × unit cost), not realized spend.
# MAGIC
# MAGIC **Correction → rebuild policy:** a concept CORRECTION (an existing key's concept_id changed) sets the rebuild
# MAGIC flag, the section SKIPS the additive backfill, and the NEXT weekly run auto-selects FULL_REBUILD (on a cluster:
# MAGIC reconciles existing rows, clears the flag, re-baselines via `snapshot_baseline(force=True)`). ADDITIVE bumps
# MAGIC (NULL → concept) are absorbed serverless with NO rebuild.
# MAGIC
# MAGIC **Cross-reference:** the `map_pathology_pipeline` promotion runbook (MP Task 14, now retired-but-kept) + its two
# MAGIC tracked parity follow-ups — `result_status` (~513k rows) and the raw NRBC swap (~214k rows).
