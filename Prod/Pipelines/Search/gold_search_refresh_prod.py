# Databricks notebook source
# gold_search_refresh_prod -- standalone production refresh for the gold semantic search index.
# No widgets and no notebook imports. Run All performs the complete production update.

# COMMAND ----------

# MAGIC %pip install --upgrade typing_extensions openai

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, FloatType, TimestampType,
)
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines as pipeline_sdk
import builtins
import json
import time

# Production-only constants.
ENVIRONMENT = "prod"
GOLD_SCHEMA = "3_lookup.gold"
CACHE_WRITE = "3_lookup.embeddings.terms"
CACHE_READ = ["3_lookup.embeddings.terms"]
CORPUS = f"{GOLD_SCHEMA}.search_corpus"
INDEX = f"{GOLD_SCHEMA}.search_index"
RUN_LOG = f"{GOLD_SCHEMA}.run_log"
PROBE_RESULTS = f"{GOLD_SCHEMA}.probe_results"

SRC_CATALOG = "4_prod"
SRC_SCHEMA = "gold"
REGISTRY = "4_prod.gold.reference_concept_registry"
OBJECT_EXCLUDE_SQL = (
    "table_name NOT LIKE '\\_%' AND table_name NOT LIKE 'event\\_log\\_%'"
)

SNOMED_SYSTEMS = ["SNOMED CT", "http://snomed.info/sct"]
CERNER_SYSTEMS = [
    "urn:cerner:code_value", "urn:cerner:nomenclature", "urn:cerner:synonym_id",
    "urn:cerner:scheduling:appointment-type", "urn:cerner:radiology-exam",
    "urn:cerner:order_synonym_id", "CERNER_TESTCODE", "urn:cerner:order-mnemonic",
    "urn:cerner:mill:planned-procedure", "urn:cerner:form_ref_id", "urn:cerner:event_cd",
    "urn:cerner:dcp_input_ref_id", "urn:cerner:blob-content-type",
    "urn:cerner:coded-event:event_result_txt", "urn:cerner:cdf-meaning",
    "urn:cerner:order-comment-type", "urn:cerner:coded-event:event_cd",
    "urn:cerner:research-status", "Cerner",
]
FACETS = [
    "table_name", "table_description", "column_name",
    "column_description", "column_context", "code_display",
]
FACET_MIN_ROWS = {
    "table_name": 160,
    "table_description": 160,
    "column_name": 6_100,
    "column_description": 6_100,
    "column_context": 6_100,
    "code_display": 290_000,
}
MAX_RETIRE_FRACTION = 0.05
MIN_RETIRE_ALLOWANCE = 500

BOILERPLATE_MARKER = "Gold QC twin of the silver product:"
VIEW_BASE_PATTERN = r"Default research view over 4_prod\.gold\.([A-Za-z0-9_]+)"

AOAI_ENDPOINT = "https://benja-m6p4lv77-eastus2.cognitiveservices.azure.com"
AOAI_API_VERSION = "2023-05-15"
AOAI_DEPLOYMENT = "text-embedding-3-large"
EMBEDDING_MODEL = AOAI_DEPLOYMENT
EMBED_DIM = 3072
BATCH_SIZE = 256
MAX_WORKERS = 16
MAX_RETRIES = 6
MAX_EMBED_TERMS = 100_000
MAX_COST_USD = 2.00
EST_USD_PER_TERM = 0.0000039
FLUSH_ROWS = 1_000

STAGING_PATH = "/Volumes/8_dev/embeddings/staging_gold_search"
LOCK_FILE = f"{STAGING_PATH}/.embed_lock"
LOCK_STALE_SECONDS = 300

SCRATCH_OBJ = f"{GOLD_SCHEMA}._scratch_objects"
SCRATCH_COL = f"{GOLD_SCHEMA}._scratch_columns"
SCRATCH_CODE = f"{GOLD_SCHEMA}._scratch_codes"
SCRATCH_ALL = f"{GOLD_SCHEMA}._scratch_corpus"

PIPELINE_NAME = "gold_search_prod"
PIPELINE_SOURCE = (
    "/Workspace/Shared/PM-DE-Misc/Ben/Python/CC/GoldSearch/"
    "gold_search_index_pipeline"
)

_EMBED_CLIENT = None
EMBED_SCHEMA = StructType([
    StructField("term", StringType()),
    StructField("embedding_vector", ArrayType(FloatType())),
    StructField("model_version", StringType()),
    StructField("created_at", TimestampType()),
    StructField("embedded_at", TimestampType()),
    StructField("ADC_UPDT", TimestampType()),
    StructField("updated_at", TimestampType()),
])

print("gold_search_refresh_prod: production targets selected")

# COMMAND ----------

def build_objects():
    """table_name + table_description facets.

    Two things happen here that matter more than anything else in this pipeline:
      1. The QC boilerplate tail is cut from every twin comment. It is ~40 identical tokens
         against a ~15-token semantic lead, so leaving it in collapses all 94 twins together.
      2. The 75 _v_ views have NO semantic comment -- theirs is 100% pipeline boilerplate --
         so they inherit their base twin's stripped description instead.
    locate() is used rather than a regex because Photon and Spark-JVM disagree on whether
    `.` matches CR/LF, and these comments are multi-sentence.
    """
    spark.sql(f"DROP TABLE IF EXISTS {SCRATCH_OBJ}")
    spark.sql(rf"""
    CREATE TABLE {SCRATCH_OBJ} AS
    WITH objs AS (
      SELECT table_name, table_type, comment,
             (table_type = 'VIEW') AS is_view
      FROM {SRC_CATALOG}.information_schema.tables
      WHERE table_schema = '{SRC_SCHEMA}'
        AND {OBJECT_EXCLUDE_SQL}
        AND EXISTS (
          SELECT 1 FROM {SRC_CATALOG}.information_schema.columns c
          WHERE c.table_schema = '{SRC_SCHEMA}' AND c.table_name = tables.table_name
        )
    ),
    stripped AS (
      SELECT o.*,
             CASE WHEN locate('{BOILERPLATE_MARKER}', o.comment) > 0
                  THEN trim(substring(o.comment, 1,
                                      locate('{BOILERPLATE_MARKER}', o.comment) - 1))
                  ELSE trim(o.comment) END AS semantic_comment,
             CASE WHEN locate('{BOILERPLATE_MARKER}', o.comment) > 0
                  THEN 'boilerplate_stripped' ELSE 'literal' END AS strip_source,
             CASE WHEN o.is_view AND o.comment LIKE 'Default research view over%' THEN COALESCE(
                    nullif(regexp_extract(o.comment, '{VIEW_BASE_PATTERN}', 1), ''),
                    regexp_replace(o.table_name, '^([a-z0-9]+)_v_', '$1_'))
                  ELSE NULL END AS base_object_name
      FROM objs o
    ),
    resolved AS (
      SELECT s.table_name, s.table_type, s.is_view, s.comment AS raw_comment,
             s.base_object_name,
             CASE WHEN s.base_object_name IS NOT NULL THEN b.semantic_comment ELSE s.semantic_comment END
               AS description_text,
             CASE WHEN s.base_object_name IS NOT NULL
                  THEN 'inherited_from_base' ELSE s.strip_source END
               AS description_source,
             CASE WHEN s.base_object_name IS NOT NULL
                  THEN b.raw_full ELSE s.comment END AS description_display
      FROM stripped s
      LEFT JOIN (SELECT table_name, semantic_comment, comment AS raw_full
                 FROM stripped WHERE NOT is_view) b
        ON b.table_name = s.base_object_name
    )
    SELECT * FROM resolved
    """)
    n = spark.table(SCRATCH_OBJ).count()
    unresolved = spark.sql(f"""
        SELECT count(*) c FROM {SCRATCH_OBJ}
        WHERE base_object_name IS NOT NULL AND description_text IS NULL
    """).first()["c"]
    # Fail fast: a silently unresolved view would publish an empty or boilerplate description.
    assert unresolved == 0, (
        f"{unresolved} boilerplate research views could not resolve a base twin description. "
        f"Check VIEW_BASE_PATTERN survived notebook import (it must still contain a "
        f"double backslash before the dots) and that the naming convention still holds.")
    print(f"{SCRATCH_OBJ}: {n} objects, 0 unresolved inherited descriptions")
    return n

# COMMAND ----------

def object_facets_sql():
    """Project _scratch_objects into corpus rows for the two object-level facets.

    Names are de-tokenised for embedding ('clinical_v_condition' -> 'clinical condition'):
    a bare snake_case identifier is a poor embedding input, and the view infix carries no
    meaning. The raw identifier is preserved in object_name.
    """
    return rf"""
    SELECT 'table_name' AS facet, table_name AS object_name, table_type AS object_type,
           is_view, base_object_name,
           CAST(NULL AS STRING) AS column_name, CAST(NULL AS INT) AS ordinal_position,
           CAST(NULL AS STRING) AS coding_system, CAST(NULL AS STRING) AS code_system_family,
           CAST(NULL AS STRING) AS code,
           CAST(NULL AS BIGINT) AS source_use_count, CAST(NULL AS BIGINT) AS target_use_count,
           regexp_replace(
             CASE WHEN is_view THEN regexp_replace(table_name, '^([a-z0-9]+)_v_', '$1_')
                  ELSE table_name END, '_', ' ') AS embed_text,
           table_name AS display_text,
           'name_detokenised' AS text_source
    FROM {SCRATCH_OBJ}
    UNION ALL
    SELECT 'table_description', table_name, table_type, is_view, base_object_name,
           NULL, NULL, NULL, NULL, NULL, NULL, NULL,
           description_text, description_display, description_source
    FROM {SCRATCH_OBJ}
    WHERE description_text IS NOT NULL AND trim(description_text) <> ''
    """

# COMMAND ----------

def build_columns():
    """One row per column of every indexed object, carrying the object's type flags."""
    spark.sql(f"DROP TABLE IF EXISTS {SCRATCH_COL}")
    spark.sql(rf"""
    CREATE TABLE {SCRATCH_COL} AS
    SELECT o.table_name, o.table_type, o.is_view, o.base_object_name,
           c.column_name, CAST(c.ordinal_position AS INT) AS ordinal_position,
           c.comment AS column_comment
    FROM {SCRATCH_OBJ} o
    JOIN {SRC_CATALOG}.information_schema.columns c
      ON c.table_schema = '{SRC_SCHEMA}' AND c.table_name = o.table_name
    """)
    n = spark.table(SCRATCH_COL).count()
    print(f"{SCRATCH_COL}: {n} columns")
    return n

# COMMAND ----------

def column_facets_sql():
    """Three column-level facets.

    column_context exists because gold column comments are often non-discriminative on their
    own -- 'Observed code.', 'Source condition code'. Composing object + column + comment into
    one string is what actually answers 'which column holds X?'.
    """
    return rf"""
    SELECT 'column_name' AS facet, table_name AS object_name, table_type AS object_type,
           is_view, base_object_name, column_name, ordinal_position,
           CAST(NULL AS STRING) AS coding_system, CAST(NULL AS STRING) AS code_system_family,
           CAST(NULL AS STRING) AS code,
           CAST(NULL AS BIGINT) AS source_use_count, CAST(NULL AS BIGINT) AS target_use_count,
           regexp_replace(column_name, '_', ' ') AS embed_text,
           column_name AS display_text,
           'name_detokenised' AS text_source
    FROM {SCRATCH_COL}
    UNION ALL
    SELECT 'column_description', table_name, table_type, is_view, base_object_name,
           column_name, ordinal_position, NULL, NULL, NULL, NULL, NULL,
           trim(column_comment), column_comment, 'literal'
    FROM {SCRATCH_COL}
    WHERE column_comment IS NOT NULL AND trim(column_comment) <> ''
    UNION ALL
    SELECT 'column_context', table_name, table_type, is_view, base_object_name,
           column_name, ordinal_position, NULL, NULL, NULL, NULL, NULL,
           concat(regexp_replace(
                    CASE WHEN is_view THEN regexp_replace(table_name, '^([a-z0-9]+)_v_', '$1_')
                         ELSE table_name END, '_', ' '),
                  ' ', regexp_replace(column_name, '_', ' '),
                  ' — ', trim(column_comment)),
           concat(table_name, '.', column_name, ' — ', column_comment),
           'composed'
    FROM {SCRATCH_COL}
    WHERE column_comment IS NOT NULL AND trim(column_comment) <> ''
    """

# COMMAND ----------

def _sql_list(values):
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)

# COMMAND ----------

def build_codes():
    """SNOMED + Cerner codes actually observed in gold, from reference_concept_registry.

    The two SNOMED namespaces are NOT merged: they share only 1,658 codes and represent
    different populations (mapping targets vs verbatim source codings). Both are kept
    verbatim in coding_system and tagged SNOMED in code_system_family so one filter
    reaches both. Rows with no code or no preferred_display are dropped -- there is no usable coded term.
    """
    spark.sql(f"DROP TABLE IF EXISTS {SCRATCH_CODE}")
    spark.sql(rf"""
    CREATE TABLE {SCRATCH_CODE} AS
    SELECT coding_system, code, preferred_display,
           source_use_count, target_use_count,
           CASE WHEN coding_system IN ({_sql_list(SNOMED_SYSTEMS)}) THEN 'SNOMED'
                ELSE 'CERNER' END AS code_system_family
    FROM {REGISTRY}
    WHERE status = 'observed'
      AND coding_system IN ({_sql_list(SNOMED_SYSTEMS + CERNER_SYSTEMS)})
      AND code IS NOT NULL AND trim(code) <> ''
      AND preferred_display IS NOT NULL AND trim(preferred_display) <> ''
    """)
    n = spark.table(SCRATCH_CODE).count()
    print(f"{SCRATCH_CODE}: {n} codes")
    return n

# COMMAND ----------

def code_facets_sql():
    return rf"""
    SELECT 'code_display' AS facet,
           CAST(NULL AS STRING) AS object_name, CAST(NULL AS STRING) AS object_type,
           CAST(NULL AS BOOLEAN) AS is_view, CAST(NULL AS STRING) AS base_object_name,
           CAST(NULL AS STRING) AS column_name, CAST(NULL AS INT) AS ordinal_position,
           coding_system, code_system_family, code,
           source_use_count, target_use_count,
           trim(preferred_display) AS embed_text,
           preferred_display AS display_text,
           'literal' AS text_source
    FROM {SCRATCH_CODE}
    """

# COMMAND ----------

def build_corpus(dry_run=False):
    """Assemble all six facets and MERGE into CORPUS.

    MERGE rather than overwrite so that is_current gives a real lifecycle: a dropped column
    or a code that stops being observed is retired, not silently vanished, and consumers can
    see what changed. corpus_id is deterministic, so re-running changes nothing.
    """
    import uuid
    from datetime import datetime, timezone
    run_id, t0 = str(uuid.uuid4()), datetime.now(timezone.utc)

    build_objects(); build_columns(); build_codes()

    spark.sql(f"DROP TABLE IF EXISTS {SCRATCH_ALL}")
    spark.sql(rf"""
    CREATE TABLE {SCRATCH_ALL} AS
    WITH unioned AS (
      {object_facets_sql()}
      UNION ALL
      {column_facets_sql()}
      UNION ALL
      {code_facets_sql()}
    )
    SELECT sha2(concat_ws('|', facet, coalesce(object_name, ''), coalesce(column_name, ''),
                          coalesce(coding_system, ''), coalesce(code, '')), 256) AS corpus_id,
           facet, object_name, object_type, is_view, base_object_name,
           column_name, ordinal_position, coding_system, code_system_family, code,
           source_use_count, target_use_count,
           embed_text, display_text, text_source,
           TRUE AS is_current, current_timestamp() AS crawled_at,
           current_timestamp() AS ADC_UPDT
    FROM unioned
    WHERE embed_text IS NOT NULL AND trim(embed_text) <> ''
    """)

    n_new = spark.table(SCRATCH_ALL).count()
    dupes = spark.sql(f"""
        SELECT count(*) c FROM (
          SELECT corpus_id FROM {SCRATCH_ALL} GROUP BY corpus_id HAVING count(*) > 1)
    """).first()["c"]
    # A duplicate corpus_id means the key is not identifying -- MERGE would be non-deterministic.
    assert dupes == 0, f"{dupes} duplicate corpus_id values in the staged corpus"

    # ---- Safety gate 1: is the staged corpus actually complete? --------------------
    # WHEN NOT MATCHED BY SOURCE retires everything the source does not contain. If a crawl
    # half-failed -- information_schema flaked, the registry was mid-rebuild, an upstream MV
    # was refreshing -- the staged set is short and the retire-MERGE would mark almost the
    # whole corpus is_current=FALSE, emptying the published index. Neither the duplicate check
    # nor the MERGE itself notices, because a short source is a perfectly valid MERGE source.
    # So prove per-facet completeness BEFORE either MERGE runs.
    staged = {r["facet"]: r["c"] for r in spark.sql(
        f"SELECT facet, count(*) c FROM {SCRATCH_ALL} GROUP BY facet").collect()}
    shortfalls = []
    for facet, floor in FACET_MIN_ROWS.items():
        got = staged.get(facet, 0)
        if got < floor:
            shortfalls.append(f"{facet}: staged {got} < floor {floor}")
    assert not shortfalls, (
        "staged corpus is incomplete -- refusing to MERGE, because retiring against a short "
        "source would empty the index:\n  " + "\n  ".join(shortfalls))

    if dry_run:
        print(f"DRY RUN: staged {n_new} rows, {dupes} duplicates; per-facet {staged}; "
              f"no MERGE performed")
        return {"run_id": run_id, "n_corpus_rows": n_new, "dry_run": True,
                "staged_by_facet": staged}

    before = spark.sql(f"SELECT count(*) c FROM {CORPUS} WHERE is_current").first()["c"]

    # ---- Safety gate 2: how much would this crawl retire? --------------------------
    # Computed against the CURRENT table state, before anything is written. A real gold
    # release drops a handful of columns or codes; it never drops 5% of the corpus at once.
    would_retire = spark.sql(f"""
      SELECT count(*) c FROM {CORPUS} t
      WHERE t.is_current
        AND NOT EXISTS (SELECT 1 FROM {SCRATCH_ALL} s WHERE s.corpus_id = t.corpus_id)
    """).first()["c"]
    retire_cap = builtins.max(MIN_RETIRE_ALLOWANCE, int(before * MAX_RETIRE_FRACTION))
    assert would_retire <= retire_cap, (
        f"this crawl would retire {would_retire} of {before} current rows "
        f"(cap {retire_cap} = max({MIN_RETIRE_ALLOWANCE}, {MAX_RETIRE_FRACTION:.0%} of current)). "
        "That is a partial-crawl signature, not a gold release. Investigate before re-running; "
        "if the drop is genuine, raise MAX_RETIRE_FRACTION deliberately for this one run.")

    spark.sql(f"""
      MERGE INTO {CORPUS} t USING {SCRATCH_ALL} s ON t.corpus_id = s.corpus_id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)
    # Retire anything the crawl no longer sees. Both gates above have already passed.
    spark.sql(f"""
      MERGE INTO {CORPUS} t USING {SCRATCH_ALL} s ON t.corpus_id = s.corpus_id
      WHEN NOT MATCHED BY SOURCE AND t.is_current
        THEN UPDATE SET t.is_current = FALSE, t.ADC_UPDT = current_timestamp()
    """)
    after = spark.sql(f"SELECT count(*) c FROM {CORPUS} WHERE is_current").first()["c"]
    retired = spark.sql(f"SELECT count(*) c FROM {CORPUS} WHERE NOT is_current").first()["c"]
    print(f"corpus: {before} -> {after} current rows, {retired} retired in total")

    spark.createDataFrame(
        [(run_id, "corpus", t0.replace(tzinfo=None),
          datetime.now(timezone.utc).replace(tzinfo=None),
          int(after), int(builtins.max(after - before, 0)), 0, int(retired),
          None, None, None, False, f"staged={n_new}")],
        schema=spark.table(RUN_LOG).schema
    ).write.mode("append").saveAsTable(RUN_LOG)
    return {"run_id": run_id, "n_corpus_rows": after, "n_retired": retired}

# COMMAND ----------

def corpus_keys_sql():
    """One canonical term per LOWER(term) key across the whole current corpus.

    Canonicalisation is what protects the cache's uniqueness invariant. The tiebreak is
    ORDER BY term ASC so the choice is deterministic and reproducible across runs -- picking
    an arbitrary variant would make a re-run embed a *different* casing of the same key and
    append a duplicate.
    """
    return f"""
      SELECT k, term FROM (
        SELECT k, term, ROW_NUMBER() OVER (PARTITION BY k ORDER BY term ASC) AS rn
        FROM (
          SELECT DISTINCT LOWER(embed_text) AS k, embed_text AS term
          FROM {CORPUS}
          WHERE is_current AND embed_text IS NOT NULL AND trim(embed_text) <> ''
        )
      ) WHERE rn = 1
    """

# COMMAND ----------

def pending_texts():
    """Canonical corpus texts with no vector anywhere in the cache.

    Deliberately never touches embedding_vector: existence of a key is the whole question, and
    projecting the vector here would drag 3072 floats per row through a join whose output is
    thrown away.
    """
    have = " UNION ALL ".join(
        f"""SELECT DISTINCT LOWER(c.term) AS k
            FROM {t} c
            LEFT SEMI JOIN want w ON w.k = LOWER(c.term)
            WHERE c.embedding_vector IS NOT NULL"""
        for t in CACHE_READ)
    return spark.sql(f"""
      WITH want AS ({corpus_keys_sql()}),
      have AS ({have})
      SELECT w.term, w.k FROM want w LEFT ANTI JOIN have h ON h.k = w.k
    """)

# COMMAND ----------

def _get_embedding_client():
    """Lazily import openai and build the AOAI client, so %run of this notebook works on any
    compute with no env prep. Only an actual embed needs openai and the adc_store secret."""
    global _EMBED_CLIENT
    if _EMBED_CLIENT is not None:
        return _EMBED_CLIENT
    try:
        from openai import AzureOpenAI
    except ImportError as exc:
        raise RuntimeError(
            "Embedding requires notebook/job libraries 'openai' and a current "
            "'typing_extensions'; restart Python after installation."
        ) from exc
    try:
        _key = dbutils.secrets.get(scope="adc_store", key="barts_global_key")
    except NameError:
        from databricks.sdk import WorkspaceClient
        _key = WorkspaceClient().dbutils.secrets.get(scope="adc_store", key="barts_global_key")
    _EMBED_CLIENT = AzureOpenAI(api_key=_key, api_version=AOAI_API_VERSION,
                                azure_endpoint=AOAI_ENDPOINT)
    return _EMBED_CLIENT

# COMMAND ----------

def _embed_one_batch(texts):
    """Embed up to BATCH_SIZE texts with bounded retry. Returns [(text, vector), ...]."""
    import time
    client = _get_embedding_client()
    for attempt in range(MAX_RETRIES):
        try:
            resp = client.embeddings.create(model=AOAI_DEPLOYMENT, input=texts)
            return [(t, [float(x) for x in d.embedding])
                    for t, d in zip(texts, resp.data)]
        except Exception as exc:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(builtins.min(2 ** attempt, 30))
    return []

# COMMAND ----------

def _acquire_lock(run_id):
    """Advisory single-writer lock on the staging volume.

    Two concurrent top-ups both compute the same pending set and both append it, so the cache
    ends up with two rows per key -- exactly the invariant break this task is guarding. The
    lock is advisory (dbutils.fs has no atomic create-if-absent), so it stops the realistic
    accident -- a re-run of the notebook while the first is still going -- not a determined
    race. The post-append uniqueness assertion is the real backstop.
    """
    import time
    from datetime import datetime, timezone
    try:
        held = dbutils.fs.head(LOCK_FILE, 4096)
    except Exception:
        held = None
    if held:
        meta = json.loads(held)
        age = time.time() - float(meta.get("epoch", 0))
        assert age > LOCK_STALE_SECONDS, (
            f"another gold_search embed run holds the lock (run_id={meta.get('run_id')}, "
            f"age {age:.0f}s < {LOCK_STALE_SECONDS}s). Wait for it, or delete {LOCK_FILE} if "
            "you are certain it is dead. Running both would double-append the same keys.")
        print(f"breaking stale lock from run {meta.get('run_id')} ({age:.0f}s old)")
    dbutils.fs.put(LOCK_FILE, json.dumps({
        "run_id": run_id, "epoch": time.time(),
        "started_at": datetime.now(timezone.utc).isoformat()}), overwrite=True)

# COMMAND ----------

def _release_lock():
    try:
        dbutils.fs.rm(LOCK_FILE)
    except Exception as exc:
        print(f"lock release failed (harmless, it will age out): {str(exc)[:120]}")

# COMMAND ----------

def _flush(buffer, staging_dir):
    """Move one bounded buffer of (term, vector) off the driver and onto the volume."""
    from datetime import datetime, timezone
    if not buffer:
        return 0
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [(t, v, EMBEDDING_MODEL, now, now, now, now) for t, v in buffer]
    # createDataFrame, never parallelize: shared-access UC clusters reject spark.sparkContext.
    (spark.createDataFrame(rows, schema=EMBED_SCHEMA)
          .write.mode("append").parquet(staging_dir))
    n = len(rows)
    buffer.clear()
    return n

# COMMAND ----------

def embed_pending(max_terms=MAX_EMBED_TERMS, max_cost_usd=MAX_COST_USD, dry_run=False):
    """Embed every pending text up to the caps and append to CACHE_WRITE.

    Memory shape matters here. 67,394 texts x 3072 floats is ~207 million Python floats; at 24
    bytes each inside a list that is >6 GiB of driver heap, and the driver dies somewhere past
    half way with an unhelpful message after the AOAI spend has already happened. So completed
    batches are flushed to parquet on the staging volume as they arrive and never accumulate:
    peak driver residency is FLUSH_ROWS vectors, not the whole run.

    The staged parquet is also the crash-recovery artefact -- if the Delta append fails, the
    vectors are on the volume and can be committed without re-embedding.
    """
    import uuid
    from datetime import datetime, timezone
    run_id, t0 = str(uuid.uuid4()), datetime.now(timezone.utc)

    pend = pending_texts().limit(max_terms)
    terms = [r["term"] for r in pend.collect()]     # strings only; the vectors are not here yet
    est = builtins.round(len(terms) * EST_USD_PER_TERM, 4)
    print(f"pending (capped at {max_terms:,}): {len(terms):,};  est ${est}")
    assert est <= max_cost_usd, f"estimated ${est} exceeds cap ${max_cost_usd}"
    if dry_run or not terms:
        return {"run_id": run_id, "n_texts_pending": len(terms), "n_texts_embedded": 0,
                "est_cost_usd": est, "dry_run": True}

    # Belt and braces: the canonicaliser should already guarantee this, but a duplicate key
    # reaching the embedder means a duplicate key reaching the cache.
    lower = [t.lower() for t in terms]
    assert len(set(lower)) == len(lower), (
        f"{len(lower) - len(set(lower))} case-variant duplicates in the pending set -- "
        "corpus_keys_sql() did not canonicalise")

    staging_dir = f"{STAGING_PATH}/run_id={run_id}"
    _acquire_lock(run_id)
    n_staged, failures = 0, 0
    try:
        batches = [terms[i:i + BATCH_SIZE] for i in range(0, len(terms), BATCH_SIZE)]
        buffer = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(_embed_one_batch, b): b for b in batches}
            for fut in as_completed(futures):
                try:
                    buffer.extend(fut.result())
                except Exception as exc:
                    failures += 1
                    print(f"batch failed: {str(exc)[:200]}")
                if len(buffer) >= FLUSH_ROWS:
                    n_staged += _flush(buffer, staging_dir)
                    print(f"  staged {n_staged:,} / {len(terms):,}")
        n_staged += _flush(buffer, staging_dir)
        print(f"staged {n_staged:,} of {len(terms):,}; {failures} batch failures")
        assert failures == 0, (
            f"{failures} batches failed -- refusing to commit a partial top-up. "
            "Most likely causes: openai not installed on this compute, or the adc_store secret "
            f"scope is not reachable by this identity. Staged vectors are at {staging_dir}.")

        # ---- one atomic Delta append, deduped against the cache one last time ----
        staged = spark.read.parquet(staging_dir)
        # TEMP VIEW is created and consumed inside this one function call, so the serverless
        # "TEMP VIEWs do not survive a cell boundary" limitation does not bite.
        staged.createOrReplaceTempView("_gs_staged")
        to_write = spark.sql(f"""
          WITH s AS (
            SELECT term, embedding_vector, model_version, created_at, embedded_at,
                   ADC_UPDT, updated_at, LOWER(term) AS k
            FROM _gs_staged
          ),
          ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY embedded_at DESC NULLS LAST,
                                                              term ASC) AS rn
            FROM s
          ),
          canon AS (SELECT * FROM ranked WHERE rn = 1)
          SELECT c.term, c.embedding_vector, c.model_version, c.created_at, c.embedded_at,
                 c.ADC_UPDT, c.updated_at
          FROM canon c
          LEFT ANTI JOIN (SELECT DISTINCT LOWER(term) AS k FROM {CACHE_WRITE}) e
            ON e.k = c.k
        """)
        n_written = to_write.count()
        to_write.write.format("delta").mode("append").saveAsTable(CACHE_WRITE)

        # The invariant, asserted on the real table after the write. Every consumer of this
        # cache -- including the pathology map loop -- joins on LOWER(term) and assumes one row.
        rows_, keys_ = spark.sql(
            f"SELECT count(*) n, count(DISTINCT LOWER(term)) k FROM {CACHE_WRITE}"
        ).first()
        assert rows_ == keys_, (
            f"{CACHE_WRITE} now has {rows_} rows for {keys_} distinct LOWER(term) keys. "
            "The uniqueness invariant is broken -- do NOT promote this cache. Deduplicate "
            "before doing anything else.")
        print(f"appended {n_written:,} new vectors; {CACHE_WRITE} unique on LOWER(term) ({rows_:,})")
    finally:
        _release_lock()

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    spark.createDataFrame(
        [(run_id, "embed", t0.replace(tzinfo=None), now,
          None, None, None, None,
          int(len(terms)), int(n_written), float(est), False,
          f"target={CACHE_WRITE} staged={staging_dir}")],
        schema=spark.table(RUN_LOG).schema
    ).write.mode("append").saveAsTable(RUN_LOG)
    dbutils.fs.rm(staging_dir, recurse=True)   # vectors are committed; the staging copy is spent
    return {"run_id": run_id, "n_texts_pending": len(terms),
            "n_texts_embedded": int(n_written), "est_cost_usd": est}

# COMMAND ----------



# COMMAND ----------

def _ensure_objects():
    spark.sql("CREATE SCHEMA IF NOT EXISTS 3_lookup.gold")
    spark.sql("CREATE SCHEMA IF NOT EXISTS 8_dev.embeddings")
    spark.sql("CREATE VOLUME IF NOT EXISTS 8_dev.embeddings.staging_gold_search")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS 3_lookup.gold.search_corpus (
      corpus_id          STRING   COMMENT 'Deterministic PK: sha2 of facet, object, column, coding system and code.',
      facet              STRING   COMMENT 'table_name | table_description | column_name | column_description | column_context | code_display.',
      object_name        STRING   COMMENT '4_prod.gold object this row describes; NULL for code rows.',
      object_type        STRING   COMMENT 'MATERIALIZED_VIEW, VIEW or MANAGED.',
      is_view            BOOLEAN  COMMENT 'True for the de-identified _v_ research view layer.',
      base_object_name   STRING   COMMENT 'For views, the base QC twin whose description was inherited.',
      column_name        STRING   COMMENT 'Column this row describes; NULL except column facets.',
      ordinal_position   INT      COMMENT 'Column position within its object.',
      coding_system      STRING   COMMENT 'Verbatim registry coding-system namespace; NULL except code rows.',
      code_system_family STRING   COMMENT 'SNOMED or CERNER -- one filter reaching both SNOMED namespaces.',
      code               STRING   COMMENT 'Observed code value.',
      source_use_count   BIGINT   COMMENT 'Registry source-side observation count; usable as a frequency prior.',
      target_use_count   BIGINT   COMMENT 'Registry target-side mapping observation count.',
      embed_text         STRING   COMMENT 'Exact text sent to the embedding model. Cache key is LOWER(embed_text).',
      display_text       STRING   COMMENT 'Full human-readable text, before boilerplate stripping.',
      text_source        STRING   COMMENT 'literal | boilerplate_stripped | inherited_from_base | name_detokenised | composed.',
      is_current         BOOLEAN  COMMENT 'False once the underlying object, column or code stops being observed.',
      crawled_at         TIMESTAMP COMMENT 'Timestamp of the crawl that last confirmed this row.',
      ADC_UPDT           TIMESTAMP COMMENT 'Pipeline last-update timestamp.'
    ) USING DELTA
    COMMENT 'Searchable-text corpus over 4_prod.gold: object and column metadata plus observed SNOMED and Cerner codes. One row per facet occurrence. Vectors are attached downstream in search_index.'
    """)
    spark.sql("""
    CREATE TABLE IF NOT EXISTS 3_lookup.gold.run_log (
      run_id STRING, stage STRING, started_at TIMESTAMP, finished_at TIMESTAMP,
      n_corpus_rows BIGINT, n_inserted BIGINT, n_updated BIGINT, n_retired BIGINT,
      n_texts_pending BIGINT, n_texts_embedded BIGINT, est_cost_usd DOUBLE,
      dry_run BOOLEAN, notes STRING
    ) USING DELTA
    COMMENT 'One row per gold_search_corpus build and per gold_search_embed_topup run.'
    """)


def _state_name(state):
    return getattr(state, "value", str(state).split(".")[-1])


def _refresh_pipeline():
    w = WorkspaceClient()
    exact = [
        p for p in w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME}'")
        if p.name == PIPELINE_NAME
    ]
    assert len(exact) <= 1, f"multiple pipelines named {PIPELINE_NAME}: {exact}"
    settings = dict(
        name=PIPELINE_NAME,
        catalog="3_lookup",
        schema="gold",
        serverless=True,
        development=False,
        continuous=False,
        configuration={
            "gold_search.corpus": CORPUS,
            "gold_search.cache_read": CACHE_WRITE,
        },
        libraries=[pipeline_sdk.PipelineLibrary(
            notebook=pipeline_sdk.NotebookLibrary(path=PIPELINE_SOURCE)
        )],
    )
    if exact:
        pipeline_id = exact[0].pipeline_id
        w.pipelines.update(pipeline_id, **settings)
    else:
        pipeline_id = w.pipelines.create(**settings).pipeline_id

    update_id = w.pipelines.start_update(pipeline_id).update_id
    deadline = time.time() + 3600
    while True:
        update = w.pipelines.get_update(pipeline_id, update_id).update
        state = _state_name(update.state)
        print(f"pipeline {pipeline_id} update {update_id}: {state}")
        if state == "COMPLETED":
            break
        if state in ("FAILED", "CANCELED"):
            errors = []
            for event in w.pipelines.list_pipeline_events(
                    pipeline_id,
                    filter=f"update_id = '{update_id}'",
                    max_results=100,
                    order_by=["timestamp desc"]):
                data = event.as_dict()
                if data.get("level") in ("ERROR", "FATAL"):
                    errors.append(data.get("message", ""))
                    if data.get("error"):
                        errors.append(json.dumps(data["error"], default=str)[:4000])
            raise RuntimeError(
                f"pipeline update {update_id} {state}: " + " | ".join(errors[:4]))
        assert time.time() < deadline, f"pipeline update timed out: {update_id}"
        time.sleep(10)

    for attempt in range(12):
        try:
            spark.sql(f"SELECT 1 FROM {INDEX} LIMIT 1").collect()
            break
        except Exception:
            if attempt == 11:
                raise
            time.sleep(5)
    return {"pipeline_id": pipeline_id, "update_id": update_id}


def _create_views():
    specs = [
        (
            "3_lookup.gold.v_search_tables",
            "Object-level slice of search_index: 4_prod.gold table names and their descriptions.",
            "facet IN ('table_name', 'table_description')",
        ),
        (
            "3_lookup.gold.v_search_columns",
            "Column-level slice of search_index: column names, descriptions and composed context strings.",
            "facet IN ('column_name', 'column_description', 'column_context')",
        ),
        (
            "3_lookup.gold.v_search_codes",
            "Code slice of search_index: SNOMED and Cerner display terms observed in 4_prod.gold.",
            "facet = 'code_display'",
        ),
    ]
    for view_name, comment, predicate in specs:
        safe_comment = comment.replace("'", "''")
        spark.sql(f"""
          CREATE OR REPLACE VIEW {view_name}
          COMMENT '{safe_comment}'
          AS SELECT * FROM {INDEX} WHERE {predicate}
        """)


def _verify_refresh():
    facets = {
        r["facet"]: int(r["n"])
        for r in spark.sql(
            f"SELECT facet, count(*) n FROM {CORPUS} "
            "WHERE is_current GROUP BY facet"
        ).collect()
    }
    shortfalls = [
        f"{facet}: {facets.get(facet, 0)} < {floor}"
        for facet, floor in FACET_MIN_ROWS.items()
        if facets.get(facet, 0) < floor
    ]
    assert not shortfalls, "corpus completeness failed: " + "; ".join(shortfalls)

    counts = spark.sql(f"""
      SELECT
        (SELECT count(*) FROM {CORPUS} WHERE is_current) AS corpus_rows,
        (SELECT count(*) FROM {INDEX}) AS index_rows,
        (SELECT count(*) FROM {INDEX} WHERE has_vector) AS vector_rows,
        (SELECT count(*) FROM {INDEX}
         WHERE has_vector AND size(embedding_vector) <> {EMBED_DIM}) AS bad_dims,
        (SELECT count(*) FROM {CORPUS}
         WHERE is_current AND facet='code_display' AND code IS NULL) AS null_codes,
        (SELECT count(*) FROM (
           SELECT corpus_id FROM {CORPUS}
           WHERE is_current GROUP BY corpus_id HAVING count(*) > 1
         )) AS duplicate_ids
    """).first().asDict()
    assert counts["corpus_rows"] == counts["index_rows"], counts
    assert counts["index_rows"] > 0, counts
    assert counts["vector_rows"] / counts["index_rows"] >= 0.999, counts
    assert counts["bad_dims"] == 0, counts
    assert counts["null_codes"] == 0, counts
    assert counts["duplicate_ids"] == 0, counts

    cache = spark.sql(f"""
      SELECT count(*) AS rows, count(DISTINCT LOWER(term)) AS keys
      FROM {CACHE_WRITE}
    """).first()
    assert cache["rows"] == cache["keys"], (
        f"cache uniqueness failed: {cache['rows']} rows, {cache['keys']} keys")
    return {**counts, "cache_rows": int(cache["rows"]), "facets": facets}


def _cleanup_scratch():
    for name in ("_scratch_objects", "_scratch_columns", "_scratch_codes", "_scratch_corpus"):
        spark.sql(f"DROP TABLE IF EXISTS {GOLD_SCHEMA}.{name}")
    assert spark.sql(f"SHOW TABLES IN {GOLD_SCHEMA} LIKE '_scratch*'").count() == 0


def _index_size_bytes():
    for row in spark.sql(f"DESCRIBE TABLE EXTENDED {INDEX}").collect():
        if row["col_name"] == "Total Size (bytes)":
            return int(row["data_type"])
    return None

# COMMAND ----------



# COMMAND ----------

# Standalone entrypoint: Run All executes the complete production refresh.
_started = datetime.now(timezone.utc)
print(f"gold search production refresh started at {_started.isoformat()}")

_ensure_objects()
_corpus_result = build_corpus()
_topup_result = embed_pending()
_pipeline_result = _refresh_pipeline()
_create_views()
_verification = _verify_refresh()
_cleanup_scratch()

_result = {
    "started_at": _started.isoformat(),
    "finished_at": datetime.now(timezone.utc).isoformat(),
    "corpus": _corpus_result,
    "topup": _topup_result,
    "pipeline": _pipeline_result,
    "verification": _verification,
    "index_size_bytes": _index_size_bytes(),
}
print("GOLD SEARCH REFRESH COMPLETE")
print(json.dumps(_result, default=str, sort_keys=True))
dbutils.notebook.exit(json.dumps(_result, default=str))
