# Databricks notebook source
# Blob 5:Labels — enrich mill_blob_text with clinical-event labels (S33 Lane A + S34 addendum).
# Additive columns only. Keyed MERGE on EVENT_ID (updates every version row of the event).
# Idempotent: watermark = max(EVENT_ENRICH_ADC_UPDT); scope = rows with missing core labels
# UNION events whose raw clinical_event/blob_result ADC_UPDT passed the watermark. Current
# clinical-event rows are preferred, with a deterministic latest-historical fallback.
# Prod deployment: RDE_Job task AFTER the Blob 4 task — bronze work is never a parallel job.
import json
import time
import uuid


dbutils.widgets.text("target_table", "4_prod.bronze.mill_blob_text")
dbutils.widgets.text("max_events", "5000000")
dbutils.widgets.text("dry_run", "false")
dbutils.widgets.text("keys_table", "")
TARGET = dbutils.widgets.get("target_table")
MAX_EVENTS = int(dbutils.widgets.get("max_events"))
DRY_RUN = dbutils.widgets.get("dry_run").lower() == "true"
KEYS_TABLE = dbutils.widgets.get("keys_table").strip()
KEY_PRED = "true" if not KEYS_TABLE else f"EVENT_ID IN (SELECT EVENT_ID FROM {KEYS_TABLE})"
T_KEY_PRED = "true" if not KEYS_TABLE else f"t.EVENT_ID IN (SELECT EVENT_ID FROM {KEYS_TABLE})"
CE = "4_prod.raw.mill_clinical_event"
BR = "4_prod.raw.mill_ce_blob_result"
CV = "3_lookup.mill.mill_code_value"
SCRATCH_SCHEMA = "8_dev.default" if TARGET.startswith("8_dev") else "4_prod.tmp"
# Each invocation owns its scratch tables. Failed attempts remain inspectable
# without preventing another task attempt from starting.
SCRATCH_TOKEN = uuid.uuid4().hex
SCOPE_STAGE = SCRATCH_SCHEMA + ".blob5_labels_scope_" + SCRATCH_TOKEN
STAGE = SCRATCH_SCHEMA + ".blob5_labels_stage_" + SCRATCH_TOKEN
ALLOWED_TARGETS = {
    "4_prod.bronze.mill_blob_text",
    "8_dev.bronze.mill_blob_text_s33_twin",
    "8_dev.default.blob5_labels_repair_fixture",
    "8_dev.dar064.bronze_repairs",
}

if TARGET not in ALLOWED_TARGETS:
    raise ValueError(f"target_table must be one of {sorted(ALLOWED_TARGETS)}")
if not 100_000 <= MAX_EVENTS <= 5_000_000:
    raise ValueError("max_events must be between 100,000 and 5,000,000")
RUN_TS = spark.sql(
    "SELECT date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSSSSS')"
).first()[0]
RUN_TS_SQL = f"timestamp'{RUN_TS}'"
print(
    f"target={TARGET} scope_stage={SCOPE_STAGE} stage={STAGE} "
    f"max_events={MAX_EVENTS} dry_run={DRY_RUN}"
)

# COMMAND ----------

# DDL guard — add the 30 enrichment columns once (Delta has no ADD COLUMN IF NOT EXISTS).
# EVENT_RESULT_STATUS_* (In-Error axis) and EVENT_RECORD_STATUS_* (RDE 'Status' axis,
# RECORD_STATUS_CD decode) are DISTINCT; neither is the blob-parser STATUS column.
NEW_COLS = [
    ("EVENT_CD", "BIGINT"), ("EVENT_CD_DISPLAY", "STRING"), ("EVENT_CD_DESC", "STRING"),
    ("EVENT_TITLE_TEXT", "STRING"), ("EVENT_TAG", "STRING"),
    ("EVENT_CLASS_CD", "BIGINT"), ("EVENT_CLASS_DISPLAY", "STRING"),
    ("CONTRIBUTOR_SYSTEM_CD", "BIGINT"), ("CONTRIBUTOR_SYSTEM_DISPLAY", "STRING"),
    ("EVENT_RESULT_STATUS_CD", "BIGINT"), ("EVENT_RESULT_STATUS_DISPLAY", "STRING"),
    ("EVENT_RECORD_STATUS_CD", "BIGINT"), ("EVENT_RECORD_STATUS_DISPLAY", "STRING"),
    ("CLINSIG_DT_TM", "TIMESTAMP"),
    ("PARENT_EVENT_ID", "BIGINT"), ("PARENT_EVENT_CD", "BIGINT"),
    ("PARENT_EVENT_CD_DESC", "STRING"), ("PARENT_EVENT_TITLE_TEXT", "STRING"),
    ("PARENT_EVENT_TAG", "STRING"),
    ("EVENT_RELTN_CD", "BIGINT"), ("EVENT_RELTN_DISPLAY", "STRING"),
    ("SUCCESSION_TYPE_CD", "BIGINT"), ("SUCCESSION_TYPE_DISPLAY", "STRING"),
    ("PERFORMED_PRSNL_ID", "BIGINT"), ("PERFORMED_DT_TM", "TIMESTAMP"),
    ("VERIFIED_PRSNL_ID", "BIGINT"), ("VERIFIED_DT_TM", "TIMESTAMP"),
    ("ORGANIZATION_ID", "BIGINT"), ("SERIES_REF_NBR", "STRING"),
    ("EVENT_ENRICH_ADC_UPDT", "TIMESTAMP"),
]
existing = {f.name.upper() for f in spark.table(TARGET).schema.fields}
missing = [(n, t) for n, t in NEW_COLS if n not in existing]
if missing:
    cols_sql = ", ".join(f"{n} {t}" for n, t in missing)
    spark.sql(f"ALTER TABLE {TARGET} ADD COLUMNS ({cols_sql})")
    print(f"added {len(missing)} columns")
else:
    print("all label columns already present")

# COMMAND ----------

# Materialise the bounded event scope ONCE. The prior implementation put an
# unordered LIMIT inside a CTE and referenced that CTE repeatedly; Spark could
# evaluate those references as different 5M-event subsets. Null-stamp backlog is
# explicitly first, then source-watermark refreshes, with EVENT_ID as a stable tie-break.
for scratch in (SCOPE_STAGE, STAGE):
    if spark.catalog.tableExists(scratch):
        raise RuntimeError(
            f"scratch table {scratch} already exists; inspect the failed/concurrent "
            "run before dropping it"
        )

spark.sql(f"""
CREATE TABLE {SCOPE_STAGE} AS
WITH candidates AS (
    SELECT DISTINCT EVENT_ID, 0 AS selection_priority
    FROM {TARGET}
    WHERE (EVENT_ENRICH_ADC_UPDT IS NULL OR EVENT_CD IS NULL
           OR CONTRIBUTOR_SYSTEM_DISPLAY IS NULL OR CLINSIG_DT_TM IS NULL)
      AND {KEY_PRED}
    UNION ALL
    SELECT DISTINCT t.EVENT_ID, 1 AS selection_priority
    FROM {TARGET} t JOIN {CE} ce ON ce.EVENT_ID = t.EVENT_ID
    WHERE ce.VALID_UNTIL_DT_TM > {RUN_TS_SQL}
      AND ce.ADC_UPDT > t.EVENT_ENRICH_ADC_UPDT
      AND {T_KEY_PRED}
    UNION ALL
    SELECT DISTINCT t.EVENT_ID, 1 AS selection_priority
    FROM {TARGET} t JOIN {CE} pce ON pce.EVENT_ID = t.PARENT_EVENT_ID
    WHERE pce.VALID_UNTIL_DT_TM > {RUN_TS_SQL}
      AND pce.ADC_UPDT > t.EVENT_ENRICH_ADC_UPDT
      AND {T_KEY_PRED}
    UNION ALL
    SELECT DISTINCT t.EVENT_ID, 1 AS selection_priority
    FROM {TARGET} t JOIN {BR} br ON br.EVENT_ID = t.EVENT_ID
    WHERE br.VALID_UNTIL_DT_TM > {RUN_TS_SQL}
      AND br.ADC_UPDT > t.EVENT_ENRICH_ADC_UPDT
      AND {T_KEY_PRED}
),
deduplicated AS (
    SELECT EVENT_ID, min(selection_priority) AS selection_priority
    FROM candidates
    GROUP BY EVENT_ID
)
SELECT EVENT_ID, selection_priority
FROM deduplicated
ORDER BY selection_priority, EVENT_ID
LIMIT {MAX_EVENTS}
""")

scope = spark.sql(f"""
SELECT count(*) AS n,
       count(DISTINCT EVENT_ID) AS d,
       sum(CASE WHEN selection_priority = 0 THEN 1 ELSE 0 END) AS unenriched,
       min(EVENT_ID) AS min_event_id,
       max(EVENT_ID) AS max_event_id
FROM {SCOPE_STAGE}
""").first()
assert scope.n == scope.d, (
    f"scope not unique per EVENT_ID: {scope.n} rows / {scope.d} events"
)
assert 0 <= scope.n <= MAX_EVENTS, f"unsafe scope cardinality: {scope.n}"

# Build the enrichment stage exclusively from the physical scope above. Source
# watermarks use the maximum current-row ADC_UPDT while labels use the explicit
# deterministic current-row ordering, preventing an unchosen duplicate source row
# from making the same event eligible forever.
spark.sql(f"""
CREATE TABLE {STAGE} AS
WITH target_state AS (
  SELECT t.EVENT_ID,
         max(CASE WHEN t.EVENT_ENRICH_ADC_UPDT IS NULL OR t.EVENT_CD IS NULL
                       OR t.CONTRIBUTOR_SYSTEM_DISPLAY IS NULL OR t.CLINSIG_DT_TM IS NULL
                  THEN 1 ELSE 0 END) AS was_unenriched,
         min(t.EVENT_ENRICH_ADC_UPDT) AS previous_min_stamp,
         max(t.EVENT_ENRICH_ADC_UPDT) AS previous_max_stamp
  FROM {TARGET} t
  JOIN {SCOPE_STAGE} s ON s.EVENT_ID = t.EVENT_ID
  GROUP BY t.EVENT_ID
),
ce_ranked AS (
  SELECT * FROM (
    SELECT ce.*, ROW_NUMBER() OVER (
      PARTITION BY ce.EVENT_ID
      ORDER BY CASE WHEN ce.VALID_UNTIL_DT_TM > {RUN_TS_SQL} THEN 1 ELSE 0 END DESC,
               ce.UPDT_CNT DESC NULLS LAST, ce.UPDT_DT_TM DESC NULLS LAST,
               ce.VALID_FROM_DT_TM DESC NULLS LAST, ce.ADC_UPDT DESC NULLS LAST
    ) AS _rn
    FROM {CE} ce
    WHERE ce.EVENT_ID IN (SELECT EVENT_ID FROM {SCOPE_STAGE})
  )
),
ce_current AS (
  SELECT * FROM ce_ranked WHERE _rn = 1
),
ce_watermark AS (
  SELECT EVENT_ID, max(ADC_UPDT) AS max_adc_updt
  FROM ce_ranked
  GROUP BY EVENT_ID
),
pce_ranked AS (
  SELECT * FROM (
    SELECT ce.*, ROW_NUMBER() OVER (
      PARTITION BY ce.EVENT_ID
      ORDER BY CASE WHEN ce.VALID_UNTIL_DT_TM > {RUN_TS_SQL} THEN 1 ELSE 0 END DESC,
               ce.UPDT_CNT DESC NULLS LAST, ce.UPDT_DT_TM DESC NULLS LAST,
               ce.VALID_FROM_DT_TM DESC NULLS LAST, ce.ADC_UPDT DESC NULLS LAST
    ) AS _rn
    FROM {CE} ce
    WHERE ce.EVENT_ID IN (
        SELECT DISTINCT PARENT_EVENT_ID
        FROM ce_current
        WHERE PARENT_EVENT_ID IS NOT NULL
      )
  )
),
pce_current AS (
  SELECT * FROM pce_ranked WHERE _rn = 1
),
pce_watermark AS (
  SELECT EVENT_ID, max(ADC_UPDT) AS max_adc_updt
  FROM pce_ranked
  GROUP BY EVENT_ID
),
br_ranked AS (
  SELECT * FROM (
    SELECT br.EVENT_ID, br.SUCCESSION_TYPE_CD, br.ADC_UPDT, ROW_NUMBER() OVER (
      PARTITION BY br.EVENT_ID
      ORDER BY br.VALID_UNTIL_DT_TM DESC NULLS LAST, br.UPDT_CNT DESC NULLS LAST,
               br.UPDT_DT_TM DESC NULLS LAST, br.VALID_FROM_DT_TM DESC NULLS LAST
    ) AS _rn
    FROM {BR} br
    WHERE br.VALID_UNTIL_DT_TM > {RUN_TS_SQL}
      AND br.EVENT_ID IN (SELECT EVENT_ID FROM {SCOPE_STAGE})
  )
),
br_current AS (
  SELECT * FROM br_ranked WHERE _rn = 1
),
br_watermark AS (
  SELECT EVENT_ID, max(ADC_UPDT) AS max_adc_updt
  FROM br_ranked
  GROUP BY EVENT_ID
),
cv AS (
  SELECT CAST(CODE_VALUE AS BIGINT) AS code_value,
         NULLIF(TRIM(DISPLAY), '') AS disp, NULLIF(TRIM(DESCRIPTION), '') AS descr
  FROM {CV} WHERE ACTIVE_IND > 0
)
SELECT
  s.EVENT_ID,
  s.selection_priority AS _SELECTION_PRIORITY,
  ts.was_unenriched AS _WAS_UNENRICHED,
  ts.previous_min_stamp AS _PREVIOUS_MIN_STAMP,
  ts.previous_max_stamp AS _PREVIOUS_MAX_STAMP,
  ce.EVENT_CD,
  ev.disp  AS EVENT_CD_DISPLAY,
  ev.descr AS EVENT_CD_DESC,
  NULLIF(TRIM(ce.EVENT_TITLE_TEXT), '') AS EVENT_TITLE_TEXT,
  NULLIF(TRIM(ce.EVENT_TAG), '')        AS EVENT_TAG,
  ce.EVENT_CLASS_CD,        cls.descr AS EVENT_CLASS_DISPLAY,
  ce.CONTRIBUTOR_SYSTEM_CD, con.descr AS CONTRIBUTOR_SYSTEM_DISPLAY,
  ce.RESULT_STATUS_CD AS EVENT_RESULT_STATUS_CD, res.disp AS EVENT_RESULT_STATUS_DISPLAY,
  ce.RECORD_STATUS_CD AS EVENT_RECORD_STATUS_CD, rec.descr AS EVENT_RECORD_STATUS_DISPLAY,
  ce.CLINSIG_UPDT_DT_TM AS CLINSIG_DT_TM,
  ce.PARENT_EVENT_ID,
  pe.EVENT_CD AS PARENT_EVENT_CD,
  pev.descr AS PARENT_EVENT_CD_DESC,
  NULLIF(TRIM(pe.EVENT_TITLE_TEXT), '') AS PARENT_EVENT_TITLE_TEXT,
  NULLIF(TRIM(pe.EVENT_TAG), '')        AS PARENT_EVENT_TAG,
  ce.EVENT_RELTN_CD, rel.descr AS EVENT_RELTN_DISPLAY,
  br.SUCCESSION_TYPE_CD, suc.disp AS SUCCESSION_TYPE_DISPLAY,
  ce.PERFORMED_PRSNL_ID,
  ce.PERFORMED_DT_TM,
  ce.VERIFIED_PRSNL_ID,
  ce.VERIFIED_DT_TM,
  ce.ORGANIZATION_ID,
  NULLIF(TRIM(ce.SERIES_REF_NBR), '') AS SERIES_REF_NBR,
  greatest(coalesce(cew.max_adc_updt, timestamp'1980-01-01'),
           coalesce(pew.max_adc_updt, timestamp'1980-01-01'),
           coalesce(brw.max_adc_updt, timestamp'1980-01-01')) AS EVENT_ENRICH_ADC_UPDT
FROM {SCOPE_STAGE} s
JOIN target_state ts ON ts.EVENT_ID = s.EVENT_ID
-- LEFT: events with no current clinical-event row STILL get stamped (labels NULL,
-- stamp floors at 1980-01-01) so they never rescope forever; a future CE row
-- rescopes them via the per-row comparison in `scope`.
LEFT JOIN ce_current ce ON ce.EVENT_ID = s.EVENT_ID
LEFT JOIN ce_watermark cew ON cew.EVENT_ID = s.EVENT_ID
LEFT JOIN pce_current pe ON pe.EVENT_ID = ce.PARENT_EVENT_ID
LEFT JOIN pce_watermark pew ON pew.EVENT_ID = ce.PARENT_EVENT_ID
LEFT JOIN br_current br ON br.EVENT_ID = s.EVENT_ID
LEFT JOIN br_watermark brw ON brw.EVENT_ID = s.EVENT_ID
LEFT JOIN cv ev  ON ev.code_value  = ce.EVENT_CD
LEFT JOIN cv cls ON cls.code_value = ce.EVENT_CLASS_CD
LEFT JOIN cv con ON con.code_value = ce.CONTRIBUTOR_SYSTEM_CD
LEFT JOIN cv res ON res.code_value = ce.RESULT_STATUS_CD
LEFT JOIN cv rec ON rec.code_value = ce.RECORD_STATUS_CD
LEFT JOIN cv pev ON pev.code_value = pe.EVENT_CD
LEFT JOIN cv rel ON rel.code_value = ce.EVENT_RELTN_CD
LEFT JOIN cv suc ON suc.code_value = br.SUCCESSION_TYPE_CD
""")

# COMMAND ----------

# Safety assertion BEFORE the MERGE (dedup doctrine): staging must be unique per EVENT_ID.
rows = spark.sql(f"""
SELECT count(*) AS n,
       count(DISTINCT EVENT_ID) AS d,
       sum(CASE WHEN _WAS_UNENRICHED = 1 THEN 1 ELSE 0 END) AS unenriched,
       sum(CASE WHEN _WAS_UNENRICHED = 0 THEN 1 ELSE 0 END) AS refresh,
       sum(CASE WHEN _PREVIOUS_MAX_STAMP IS NOT NULL
                     AND EVENT_ENRICH_ADC_UPDT > _PREVIOUS_MAX_STAMP
                THEN 1 ELSE 0 END) AS advanced,
       sum(CASE WHEN EVENT_CD IS NULL THEN 1 ELSE 0 END) AS no_current_ce
FROM {STAGE}
""").first()
assert rows.n == rows.d, f"staging not unique per EVENT_ID: {rows.n} rows / {rows.d} events"
assert rows.n == scope.n, f"stage/scope mismatch: {rows.n} != {scope.n}"
assert rows.unenriched == scope.unenriched, (
    f"unenriched stage/scope mismatch: {rows.unenriched} != {scope.unenriched}"
)
cov = spark.sql(f"""
SELECT count(*) AS n,
       sum(CASE WHEN EVENT_CD IS NULL THEN 1 ELSE 0 END) AS no_current_ce,
       sum(CASE WHEN PARENT_EVENT_CD_DESC IS NULL THEN 1 ELSE 0 END) AS null_parent_desc,
       sum(CASE WHEN PERFORMED_PRSNL_ID IS NOT NULL AND PERFORMED_PRSNL_ID <> 0
                THEN 1 ELSE 0 END) AS performed_prsnl,
       sum(CASE WHEN PERFORMED_DT_TM IS NOT NULL THEN 1 ELSE 0 END) AS performed_dt,
       sum(CASE WHEN VERIFIED_PRSNL_ID IS NOT NULL AND VERIFIED_PRSNL_ID <> 0
                THEN 1 ELSE 0 END) AS verified_prsnl,
       sum(CASE WHEN VERIFIED_DT_TM IS NOT NULL THEN 1 ELSE 0 END) AS verified_dt,
       sum(CASE WHEN ORGANIZATION_ID IS NOT NULL AND ORGANIZATION_ID <> 0
                THEN 1 ELSE 0 END) AS organization_id,
       sum(CASE WHEN SERIES_REF_NBR IS NOT NULL THEN 1 ELSE 0 END) AS series_ref
FROM {STAGE}""").first()
# no_current_ce = events stamped WITHOUT labels (no live clinical-event row) — expected small.
print(
    f"scoped={scope.n} staged={rows.n} unenriched={rows.unenriched} "
    f"refresh={rows.refresh} advanced={rows.advanced} "
    f"no_current_ce={cov.no_current_ce} null_parent_desc={cov.null_parent_desc}"
)
print(
    "s34_coverage "
    f"performed_prsnl={cov.performed_prsnl} performed_dt={cov.performed_dt} "
    f"verified_prsnl={cov.verified_prsnl} verified_dt={cov.verified_dt} "
    f"organization_id={cov.organization_id} series_ref={cov.series_ref}"
)

# COMMAND ----------

def merge_labels_with_retry(sql, max_attempts=5, sleep=time.sleep):
    """Retry only Delta row-write conflicts, against the same validated stage."""
    retry_classes = {
        "DELTA_CONCURRENT_APPEND", "DELTA_CONCURRENT_DELETE_READ",
        "DELTA_CONCURRENT_DELETE_DELETE",
    }
    for attempt in range(1, max_attempts + 1):
        try:
            # collect also surfaces failures on lazy Spark Connect execution.
            return spark.sql(sql).collect()
        except Exception as exc:
            getter = getattr(exc, "getCondition", None) or getattr(exc, "getErrorClass", None)
            condition = getter() if getter else None
            conflict = bool(condition and condition.split(".")[0] in retry_classes)
            if not conflict:
                conflict = any("[" + name + "]" in str(exc)
                               or "[" + name + "." in str(exc)
                               for name in retry_classes)
            if not conflict or attempt == max_attempts:
                raise
            delay = min(15 * 2 ** (attempt - 1), 60)
            print(f"label MERGE conflict; retry {attempt + 1}/{max_attempts} in {delay}s")
            sleep(delay)


set_clause = ", ".join(f"t.{n} = s.{n}" for n, _ in NEW_COLS)
merge_sql = f"""
MERGE INTO {TARGET} t
USING {STAGE} s
ON t.EVENT_ID = s.EVENT_ID
WHEN MATCHED THEN UPDATE SET {set_clause}
"""
if DRY_RUN:
    print("DRY_RUN — MERGE not executed:")
    print(merge_sql)
    result = {
        "status": "DRY_RUN",
        "target": TARGET,
        "scope_stage": SCOPE_STAGE,
        "stage": STAGE,
        "staged_events": int(rows.n),
        "staged_unenriched_events": int(rows.unenriched or 0),
        "staged_refresh_events": int(rows.refresh or 0),
        "advanced_watermark_events": int(rows.advanced or 0),
        "no_current_ce_events": int(rows.no_current_ce or 0),
    }
    spark.sql(f"DROP TABLE {STAGE}")
    spark.sql(f"DROP TABLE {SCOPE_STAGE}")
    dbutils.notebook.exit(json.dumps(result, sort_keys=True))
else:
    merge_labels_with_retry(merge_sql)
    result = {
        "status": "COMPLETED",
        "target": TARGET,
        "scope_stage": SCOPE_STAGE,
        "stage": STAGE,
        "staged_events": int(rows.n),
        "staged_unenriched_events": int(rows.unenriched or 0),
        "staged_refresh_events": int(rows.refresh or 0),
        "advanced_watermark_events": int(rows.advanced or 0),
        "no_current_ce_events": int(rows.no_current_ce or 0),
        "scope_min_event_id": int(scope.min_event_id) if scope.min_event_id is not None else None,
        "scope_max_event_id": int(scope.max_event_id) if scope.max_event_id is not None else None,
    }
    spark.sql(f"DROP TABLE {STAGE}")
    spark.sql(f"DROP TABLE {SCOPE_STAGE}")
    dbutils.notebook.exit(json.dumps(result, sort_keys=True))

# COMMAND ----------

# Production metadata replay (Task 3; HUMAN-GATED — comments only in this staged notebook).
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_CD COMMENT 'Clinical-event code value used for governed document typing.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_CD SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_CD_DISPLAY COMMENT 'Active code-value display for EVENT_CD.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_CD_DISPLAY SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_CD_DESC COMMENT 'Active code-value description for EVENT_CD.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_CD_DESC SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_TITLE_TEXT COMMENT 'Source clinical-event title text; may contain patient-identifying free text.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_TITLE_TEXT SET TAGS ('ig_risk'='moderate', 'ig_severity'='moderate')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_TAG COMMENT 'Source clinical-event tag text; may contain patient-identifying free text.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_TAG SET TAGS ('ig_risk'='moderate', 'ig_severity'='moderate')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_CLASS_CD COMMENT 'Clinical-event class code value.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_CLASS_CD SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_CLASS_DISPLAY COMMENT 'Active code-value description for EVENT_CLASS_CD.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_CLASS_DISPLAY SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN CONTRIBUTOR_SYSTEM_CD COMMENT 'Clinical-event contributor-system code value.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN CONTRIBUTOR_SYSTEM_CD SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN CONTRIBUTOR_SYSTEM_DISPLAY COMMENT 'Active code-value description for CONTRIBUTOR_SYSTEM_CD.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN CONTRIBUTOR_SYSTEM_DISPLAY SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_RESULT_STATUS_CD COMMENT 'Clinical-event result-status code value, including the In Error axis.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_RESULT_STATUS_CD SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_RESULT_STATUS_DISPLAY COMMENT 'Active code-value display for EVENT_RESULT_STATUS_CD.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_RESULT_STATUS_DISPLAY SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_RECORD_STATUS_CD COMMENT 'Clinical-event record-status code value, distinct from result and parser status.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_RECORD_STATUS_CD SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_RECORD_STATUS_DISPLAY COMMENT 'Active code-value description for EVENT_RECORD_STATUS_CD.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_RECORD_STATUS_DISPLAY SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN CLINSIG_DT_TM COMMENT 'Clinical-significance update timestamp from the source clinical event.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN CLINSIG_DT_TM SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN PARENT_EVENT_ID COMMENT 'Parent clinical-event identifier for document threading.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN PARENT_EVENT_ID SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN PARENT_EVENT_CD COMMENT 'Parent clinical-event code value.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN PARENT_EVENT_CD SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN PARENT_EVENT_CD_DESC COMMENT 'Active code-value description for the parent clinical-event code.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN PARENT_EVENT_CD_DESC SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN PARENT_EVENT_TITLE_TEXT COMMENT 'Source parent clinical-event title text; may contain patient-identifying free text.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN PARENT_EVENT_TITLE_TEXT SET TAGS ('ig_risk'='moderate', 'ig_severity'='moderate')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN PARENT_EVENT_TAG COMMENT 'Source parent clinical-event tag text; may contain patient-identifying free text.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN PARENT_EVENT_TAG SET TAGS ('ig_risk'='moderate', 'ig_severity'='moderate')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_RELTN_CD COMMENT 'Clinical-event relationship code value.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_RELTN_CD SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_RELTN_DISPLAY COMMENT 'Active code-value description for EVENT_RELTN_CD.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_RELTN_DISPLAY SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN SUCCESSION_TYPE_CD COMMENT 'Blob-result succession-type code value.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN SUCCESSION_TYPE_CD SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN SUCCESSION_TYPE_DISPLAY COMMENT 'Active code-value display for SUCCESSION_TYPE_CD.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN SUCCESSION_TYPE_DISPLAY SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN PERFORMED_PRSNL_ID COMMENT 'Internal Millennium personnel identifier for the practitioner who performed or authored the clinical event.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN PERFORMED_PRSNL_ID SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN PERFORMED_DT_TM COMMENT 'Source timestamp when the clinical event was performed or authored.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN PERFORMED_DT_TM SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN VERIFIED_PRSNL_ID COMMENT 'Internal Millennium personnel identifier for the practitioner who verified the clinical event.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN VERIFIED_PRSNL_ID SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN VERIFIED_DT_TM COMMENT 'Source timestamp when the clinical event was verified.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN VERIFIED_DT_TM SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN ORGANIZATION_ID COMMENT 'Internal Millennium organization identifier associated with the clinical event.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN ORGANIZATION_ID SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN SERIES_REF_NBR COMMENT 'Source series reference defining the document replacement or succession thread.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN SERIES_REF_NBR SET TAGS ('ig_risk'='low', 'ig_severity'='low')
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_ENRICH_ADC_UPDT COMMENT 'Greatest source ADC_UPDT consumed by the event-label enrichment.'
# ALTER TABLE 4_prod.bronze.mill_blob_text ALTER COLUMN EVENT_ENRICH_ADC_UPDT SET TAGS ('ig_risk'='low', 'ig_severity'='low')



