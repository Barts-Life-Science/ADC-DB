# Databricks notebook source
# MAGIC %md
# MAGIC # Data Browser usage ledger (daily)
# MAGIC
# MAGIC This builds the billing ledger for the Data Browser warehouse. It splits each hour's **billed** DBUs and list-price
# MAGIC cost across the statements that ran on the warehouse in that hour, weighted by each statement's wall-clock time.
# MAGIC The browser's own metering (`agent/app/metering_dbx.py`) is a provisional estimate; this ledger is what it gets
# MAGIC reconciled against, and the ledger wins.
# MAGIC
# MAGIC **Sources** (all system tables, read-only):
# MAGIC - `system.billing.usage` gives billed DBUs per hour for the warehouse.
# MAGIC - `system.billing.list_prices` gives the price per DBU (list price, `effective_list`; this is not our negotiated rate).
# MAGIC - `system.query.history` gives the statements, their timings, and their attribution.
# MAGIC
# MAGIC **Attribution**, in order of preference:
# MAGIC 1. `query_tags` keys `ctx`, `u` and `feature`, as set by the browser's `dbx.query_tags()` since 2026-11-05.
# MAGIC    A statement counts as tagged when `feature` is present; a missing `ctx` or `u` defaults to `system`. This is the
# MAGIC    same rule as the browser's `dbx.parse_query_tag`. Query tags are not masked, so any principal can read them.
# MAGIC 2. The legacy `/* dbrowse ctx=… u=… feature=… */` comment in `statement_text`. Since the 2026-08-26 platform release
# MAGIC    this text reads `<REDACTED>` unless the job's run-as identity is an account admin or a member of
# MAGIC    `databricks_pii_access`, so this fallback normally finds nothing.
# MAGIC 3. Otherwise the statement is booked as `_untagged`. Cube's JDBC traffic lands here, and so does anything run by hand.
# MAGIC
# MAGIC An hour that was billed but had no statement time (warm-up, or idle before auto-stop) gets one `_idle` row. That
# MAGIC way each hour's ledger rows always sum to exactly what was billed, and no cost is dropped.
# MAGIC
# MAGIC **Window.** Billing lands about 26 hours late and can be restated, so every run recomputes the trailing
# MAGIC `lookback_days` and MERGEs the result. Rows inside the window that no longer exist in the source are deleted (for
# MAGIC example, an `_idle` row replaced once late query history arrives). Rows that have not changed are not rewritten.
# MAGIC Rows less than 72 hours old are flagged `provisional`.
# MAGIC
# MAGIC **Currency.** Costs are in the list-price currency, which is USD for this SKU. No FX conversion is applied here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC
# MAGIC | widget | default | meaning |
# MAGIC |---|---|---|
# MAGIC | `warehouse_id` | `ffa3d5b6eec256f8` | the Data Browser warehouse (`databrowser-wh`) |
# MAGIC | `target_table` | `8_dev.databrowser_ledger.usage_ledger` | the ledger table; prod is `d_databrowser.app.usage_ledger` |
# MAGIC | `lookback_days` | `4` | trailing days to recompute (billing lag is about 26 hours) |
# MAGIC | `dry_run` | `true` | `true` computes and checks but does not MERGE; the run is still logged |

# COMMAND ----------

from datetime import datetime, timedelta, timezone

dbutils.widgets.text("warehouse_id", "ffa3d5b6eec256f8")
dbutils.widgets.text("target_table", "8_dev.databrowser_ledger.usage_ledger")
dbutils.widgets.text("lookback_days", "4")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])

WAREHOUSE_ID = dbutils.widgets.get("warehouse_id").strip()
TARGET_TABLE = dbutils.widgets.get("target_table").strip()
RUN_LOG_TABLE = TARGET_TABLE + "_runs"
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
DRY_RUN = dbutils.widgets.get("dry_run") == "true"

# The window starts on an hour boundary so that it lines up with the hourly billing records.
RUN_AT = datetime.now(timezone.utc).replace(microsecond=0)
WINDOW_START = (RUN_AT - timedelta(days=LOOKBACK_DAYS)).replace(minute=0, second=0)
# Hours newer than this can still be restated by billing, so they are flagged provisional.
SETTLED_BEFORE = (RUN_AT - timedelta(hours=72)).replace(minute=0, second=0)

assert LOOKBACK_DAYS >= 2, "lookback_days must cover the billing lag (about 26 hours); use 2 or more"
assert TARGET_TABLE.count(".") == 2, f"target_table must be catalog.schema.table, got {TARGET_TABLE!r}"

# Regex for the legacy comment tag. It is passed as a SQL parameter, so it needs no SQL string escaping.
# It has no '.', so the Photon and JVM regex engines agree on it.
LEGACY_TAG_REGEX = r"/\*\s*dbrowse\s+ctx=(\S+)\s+u=(\S+)\s+feature=(\S+)\s*\*/"

print(f"warehouse={WAREHOUSE_ID} target={TARGET_TABLE} window_start={WINDOW_START:%Y-%m-%d %H:%M}Z "
      f"settled_before={SETTLED_BEFORE:%Y-%m-%d %H:%M}Z dry_run={DRY_RUN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Target tables (created if missing)

# COMMAND ----------

def create_tables_if_missing():
    """Create the ledger table and its run log, with comments, if they do not exist yet.
    This does not touch existing tables, so it is safe to run every day."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS IDENTIFIER(:t) (
          hour              TIMESTAMP     COMMENT 'Billing hour (UTC, start of hour).',
          warehouse_id      STRING        COMMENT 'SQL warehouse the cost was billed to.',
          statement_id      STRING        COMMENT 'system.query.history statement_id, or _idle for billed time with no statements.',
          executed_by       STRING        COMMENT 'Principal that ran the statement (NULL for _idle).',
          ctx               STRING        COMMENT 'dbrowse context from the query tag; _untagged if none; _idle for idle time.',
          user_hash         STRING        COMMENT 'dbrowse user hash from the query tag (pseudonymous; never reversed here).',
          feature           STRING        COMMENT 'dbrowse feature from the query tag.',
          tag_source        STRING        COMMENT 'Where the attribution came from: query_tags | statement_text | untagged | idle.',
          elapsed_ms        BIGINT        COMMENT 'Wall-clock ms of this statement that fell inside this hour.',
          hour_elapsed_ms   BIGINT        COMMENT 'Sum of elapsed_ms over all statements in this hour (the share denominator).',
          hour_dbus         DECIMAL(38,12) COMMENT 'Total DBUs billed to the warehouse for this hour.',
          attributed_dbus   DECIMAL(38,12) COMMENT 'This row''s share of hour_dbus (elapsed_ms / hour_elapsed_ms).',
          attributed_cost   DECIMAL(38,12) COMMENT 'This row''s share of the hour''s list-price cost.',
          currency          STRING        COMMENT 'Currency of attributed_cost (list-price currency; USD for this SKU).',
          list_price_source STRING        COMMENT 'Which price column was used.',
          provisional       BOOLEAN       COMMENT 'True while the hour is under 72h old and billing may still be restated.',
          created_at        TIMESTAMP     COMMENT 'When this row was first written.',
          updated_at        TIMESTAMP     COMMENT 'When this row last changed.'
        )
        COMMENT 'Data Browser usage ledger: billed warehouse DBUs and list-price cost per hour, split across statements by wall-clock share. Key (warehouse_id, hour, statement_id). Built daily from system.billing + system.query.history by the estate-side usage_ledger job.'
        CLUSTER BY (hour)
    """, args={"t": TARGET_TABLE})
    spark.sql("""
        CREATE TABLE IF NOT EXISTS IDENTIFIER(:t) (
          run_at            TIMESTAMP COMMENT 'Run start (UTC).',
          warehouse_id      STRING    COMMENT 'Warehouse ledgered.',
          window_start      TIMESTAMP COMMENT 'First hour recomputed.',
          dry_run           BOOLEAN   COMMENT 'True if the MERGE was skipped.',
          source_rows       BIGINT    COMMENT 'Ledger rows computed for the window.',
          billed_hours      BIGINT    COMMENT 'Billed hours in the window.',
          window_dbus       DECIMAL(38,12) COMMENT 'DBUs billed in the window.',
          window_cost       DECIMAL(38,12) COMMENT 'List-price cost billed in the window.',
          tagged_share      DOUBLE    COMMENT 'Fraction of window DBUs attributed from query_tags or statement_text.',
          num_inserted_rows BIGINT    COMMENT 'MERGE inserts.',
          num_updated_rows  BIGINT    COMMENT 'MERGE updates.',
          num_deleted_rows  BIGINT    COMMENT 'MERGE deletes (rows in window no longer in source).'
        )
        COMMENT 'One row per usage_ledger job run: window, totals and MERGE counts.'
    """, args={"t": RUN_LOG_TABLE})


create_tables_if_missing()

# COMMAND ----------

# MAGIC %md
# MAGIC ## The ledger for the window
# MAGIC
# MAGIC One SQL statement builds the rows. The MERGE below reuses the same text, so the rows that get checked are the
# MAGIC rows that get written.
# MAGIC
# MAGIC 1. `billed`: DBUs and list-price cost per hour, with each usage record priced at the list price in force when it
# MAGIC    started.
# MAGIC 2. `statement_hours`: each statement split into the hours it overlaps, with the milliseconds it spent in each.
# MAGIC 3. `attributed`: the attribution read from the tags (or the legacy comment), plus each hour's total milliseconds.
# MAGIC 4. Final select: billed hours LEFT JOIN statements. A billed hour with no statements becomes one `_idle` row.

# COMMAND ----------

LEDGER_SQL = """
WITH billed AS (
  -- Billed DBUs and list-price cost per hour for this warehouse.
  -- Retraction and restatement records carry signed quantities, so a plain SUM nets them out.
  -- Decimal sizes are chosen so that no product exceeds 38 digits. Past 38, Spark silently cuts the scale to 6
  -- and the per-statement shares stop adding back to the billed hour.
  SELECT date_trunc('HOUR', u.usage_start_time)                            AS hour,
         SUM(u.usage_quantity)                                            AS hour_dbus,
         SUM(CAST(u.usage_quantity AS DECIMAL(21,12))
             * CAST(p.pricing.effective_list.default AS DECIMAL(16,10)))  AS hour_cost,
         MAX(p.currency_code)                                             AS currency,
         COUNT(DISTINCT p.currency_code)                                  AS currencies,
         COUNT_IF(p.sku_name IS NULL)                                     AS unpriced_records
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices p
    ON  p.sku_name   = u.sku_name
    AND p.cloud      = u.cloud
    AND p.usage_unit = u.usage_unit
    AND u.usage_start_time >= p.price_start_time
    AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
  WHERE u.usage_metadata.warehouse_id = :warehouse_id
    AND u.usage_date       >= to_date(:window_start)
    AND u.usage_start_time >= :window_start
  GROUP BY 1
),
statement_hours AS (
  -- One row per (statement, hour it overlaps), with the wall-clock ms spent in that hour.
  -- Statements that started before the window but ran into it are kept; hours before the window are dropped.
  SELECT statement_id, executed_by, query_tags, statement_text, hour,
         unix_millis(least(end_time, hour + INTERVAL 1 HOUR))
           - unix_millis(greatest(start_time, hour))                      AS elapsed_ms
  FROM (
    SELECT q.statement_id, q.executed_by, q.query_tags, q.statement_text, q.start_time, q.end_time,
           explode(sequence(date_trunc('HOUR', q.start_time),
                            date_trunc('HOUR', q.end_time),
                            INTERVAL 1 HOUR))                             AS hour
    FROM system.query.history q
    WHERE q.compute.warehouse_id = :warehouse_id
      AND q.start_time >= :window_start - INTERVAL 2 DAYS   -- warehouse statements cannot run longer than 2 days
      AND q.end_time   >= :window_start
      AND q.end_time   IS NOT NULL
  )
  WHERE hour >= :window_start
),
attributed AS (
  -- Attribution: query_tags first, then the legacy comment, else _untagged.
  -- Same rule as the browser's dbx.parse_query_tag: tagged when `feature` is present; ctx and u default to 'system'.
  -- try_element_at, because a plain map lookup on a missing key fails under ANSI mode.
  SELECT hour, statement_id, executed_by, elapsed_ms,
         nullif(try_element_at(query_tags, 'feature'), '')                                    AS tag_feature,
         coalesce(nullif(try_element_at(query_tags, 'ctx'), ''), 'system')                    AS tag_ctx,
         coalesce(nullif(try_element_at(query_tags, 'u'), ''), 'system')                      AS tag_u,
         nullif(regexp_extract(statement_text, :tag_regex, 1), '')                            AS text_ctx,
         nullif(regexp_extract(statement_text, :tag_regex, 2), '')                            AS text_u,
         nullif(regexp_extract(statement_text, :tag_regex, 3), '')                            AS text_feature,
         SUM(elapsed_ms) OVER (PARTITION BY hour)                                             AS hour_elapsed_ms
  FROM statement_hours
  WHERE elapsed_ms > 0
)
SELECT b.hour,
       :warehouse_id                                                     AS warehouse_id,
       coalesce(a.statement_id, '_idle')                                 AS statement_id,
       a.executed_by,
       CASE WHEN a.statement_id IS NULL THEN '_idle'
            WHEN a.tag_feature IS NOT NULL THEN a.tag_ctx
            ELSE coalesce(a.text_ctx, '_untagged') END                   AS ctx,
       CASE WHEN a.tag_feature IS NOT NULL THEN a.tag_u
            WHEN a.text_ctx IS NOT NULL THEN a.text_u END                AS user_hash,
       CASE WHEN a.tag_feature IS NOT NULL THEN a.tag_feature
            WHEN a.text_ctx IS NOT NULL THEN a.text_feature END          AS feature,
       CASE WHEN a.statement_id IS NULL THEN 'idle'
            WHEN a.tag_feature IS NOT NULL THEN 'query_tags'
            WHEN a.text_ctx IS NOT NULL THEN 'statement_text'
            ELSE 'untagged' END                                          AS tag_source,
       coalesce(a.elapsed_ms, 0)                                         AS elapsed_ms,
       coalesce(a.hour_elapsed_ms, 0)                                    AS hour_elapsed_ms,
       CAST(b.hour_dbus AS DECIMAL(38,12))                               AS hour_dbus,
       CAST(b.hour_cost AS DECIMAL(38,12))                               AS hour_cost,
       CAST(CASE WHEN a.statement_id IS NULL THEN b.hour_dbus
                 ELSE CAST(b.hour_dbus AS DECIMAL(24,12)) * CAST(a.elapsed_ms AS DECIMAL(13,0))
                                  / CAST(a.hour_elapsed_ms AS DECIMAL(13,0)) END
            AS DECIMAL(38,12))                                           AS attributed_dbus,
       CAST(CASE WHEN a.statement_id IS NULL THEN b.hour_cost
                 ELSE CAST(b.hour_cost AS DECIMAL(24,12)) * CAST(a.elapsed_ms AS DECIMAL(13,0))
                                  / CAST(a.hour_elapsed_ms AS DECIMAL(13,0)) END
            AS DECIMAL(38,12))                                           AS attributed_cost,
       b.currency,
       'system.billing.list_prices.pricing.effective_list.default'       AS list_price_source,
       b.hour >= :settled_before                                         AS provisional,
       -- currencies and unpriced_records are kept for the safety checks only; the MERGE does not write them.
       b.currencies,
       b.unpriced_records
FROM billed b
LEFT JOIN attributed a ON a.hour = b.hour
"""

SQL_ARGS = {
    "warehouse_id": WAREHOUSE_ID,
    "window_start": WINDOW_START.strftime("%Y-%m-%d %H:%M:%S"),
    "settled_before": SETTLED_BEFORE.strftime("%Y-%m-%d %H:%M:%S"),
    "tag_regex": LEGACY_TAG_REGEX,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Safety checks (one aggregate pass, before any write)
# MAGIC
# MAGIC - The key (`warehouse_id`, `hour`, `statement_id`) is unique.
# MAGIC - Every usage record found a list price, and the window has only one currency.
# MAGIC - In every hour the attributed DBUs and cost add back to what was billed (within 1e-6 for decimal rounding).

# COMMAND ----------

def check_ledger():
    """Compute the window's totals and the safety checks in a single aggregate query.
    Raises AssertionError (with a message) on any failure; otherwise returns the totals for the run log."""
    # One pass: aggregate per hour, then over the hours. Deliberately no scalar subqueries. Several of them over the
    # same CTE intermittently failed on serverless with "Task not serializable: SQLMetricSnapshot" (runtime bug,
    # seen 2026-11-05).
    checks = spark.sql(f"""
        WITH ledger AS ({LEDGER_SQL}),
        per_hour AS (
          SELECT hour,
                 COUNT(*)                                       AS n_rows,
                 COUNT(DISTINCT statement_id)                   AS n_keys,        -- warehouse_id is constant
                 SUM(attributed_dbus)                           AS sum_dbus,
                 MAX(hour_dbus)                                 AS hour_dbus,
                 SUM(attributed_cost)                           AS sum_cost,
                 MAX(hour_cost)                                 AS hour_cost,
                 MAX(unpriced_records)                          AS unpriced_records,
                 MAX(currencies)                                AS currencies,
                 MAX(currency)                                  AS currency,
                 SUM(CASE WHEN tag_source IN ('query_tags', 'statement_text')
                          THEN attributed_dbus ELSE 0 END)      AS tagged_dbus
          FROM ledger
          GROUP BY hour
        )
        SELECT SUM(n_rows)                                                           AS source_rows,
               COALESCE(SUM(n_rows - n_keys), 0)                                     AS duplicate_keys,
               COALESCE(MAX(unpriced_records), 0)                                    AS unpriced_records,
               COALESCE(MAX(currencies), 0)                                          AS max_currencies,
               COUNT(DISTINCT currency)                                              AS window_currencies,
               COUNT_IF(ABS(sum_dbus - hour_dbus) > 0.000001
                        OR ABS(sum_cost - hour_cost) > 0.000001)                     AS hours_not_summing,
               COUNT(*)                                                              AS billed_hours,
               SUM(hour_dbus)                                                        AS window_dbus,
               SUM(hour_cost)                                                        AS window_cost,
               SUM(tagged_dbus) / NULLIF(SUM(sum_dbus), 0)                           AS tagged_share
        FROM per_hour
    """, args=SQL_ARGS).first().asDict()

    print(checks)
    assert checks["duplicate_keys"] == 0, f"duplicate ledger keys: {checks['duplicate_keys']}"
    assert checks["unpriced_records"] == 0, f"usage records with no list price: {checks['unpriced_records']}"
    assert checks["max_currencies"] <= 1 and checks["window_currencies"] <= 1, "more than one currency in the window"
    assert checks["hours_not_summing"] == 0, f"hours whose attributed DBUs or cost do not sum to billed: {checks['hours_not_summing']}"
    return checks


CHECKS = check_ledger()

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE into the ledger
# MAGIC
# MAGIC - New keys are inserted.
# MAGIC - Changed rows are updated. A row is only rewritten if a value actually changed, which avoids no-op rewrites.
# MAGIC - Keys in the target that fall inside this window, for this warehouse, and are no longer in the source are deleted.
# MAGIC   Rows outside the window are never touched.

# COMMAND ----------

def merge_ledger():
    """MERGE the window's ledger rows into the target table and return the MERGE's row counts."""
    result = spark.sql(f"""
        MERGE INTO IDENTIFIER(:target) t
        USING ({LEDGER_SQL}) s
        ON  t.warehouse_id = s.warehouse_id
        AND t.hour         = s.hour
        AND t.statement_id = s.statement_id
        WHEN MATCHED AND NOT (
                 t.executed_by     <=> s.executed_by
             AND t.ctx             <=> s.ctx
             AND t.user_hash       <=> s.user_hash
             AND t.feature         <=> s.feature
             AND t.tag_source      <=> s.tag_source
             AND t.elapsed_ms      <=> s.elapsed_ms
             AND t.hour_elapsed_ms <=> s.hour_elapsed_ms
             AND t.hour_dbus       <=> s.hour_dbus
             AND t.attributed_dbus <=> s.attributed_dbus
             AND t.attributed_cost <=> s.attributed_cost
             AND t.currency        <=> s.currency
             AND t.provisional     <=> s.provisional)
          THEN UPDATE SET
             executed_by = s.executed_by, ctx = s.ctx, user_hash = s.user_hash, feature = s.feature,
             tag_source = s.tag_source, elapsed_ms = s.elapsed_ms, hour_elapsed_ms = s.hour_elapsed_ms,
             hour_dbus = s.hour_dbus, attributed_dbus = s.attributed_dbus, attributed_cost = s.attributed_cost,
             currency = s.currency, list_price_source = s.list_price_source, provisional = s.provisional,
             updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT
             (hour, warehouse_id, statement_id, executed_by, ctx, user_hash, feature, tag_source,
              elapsed_ms, hour_elapsed_ms, hour_dbus, attributed_dbus, attributed_cost, currency,
              list_price_source, provisional, created_at, updated_at)
          VALUES
             (s.hour, s.warehouse_id, s.statement_id, s.executed_by, s.ctx, s.user_hash, s.feature, s.tag_source,
              s.elapsed_ms, s.hour_elapsed_ms, s.hour_dbus, s.attributed_dbus, s.attributed_cost, s.currency,
              s.list_price_source, s.provisional, current_timestamp(), current_timestamp())
        WHEN NOT MATCHED BY SOURCE
             AND t.warehouse_id = :warehouse_id
             AND t.hour >= :window_start
          THEN DELETE
    """, args={**SQL_ARGS, "target": TARGET_TABLE}).first().asDict()
    print(result)
    return result


if DRY_RUN:
    print("dry_run=true: MERGE skipped")
    MERGE_COUNTS = {"num_inserted_rows": None, "num_updated_rows": None, "num_deleted_rows": None}
else:
    MERGE_COUNTS = merge_ledger()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run log

# COMMAND ----------

def as_param(value):
    """Turn a value into a SQL string parameter; None becomes '' and is NULLIF'd back to NULL in the SQL."""
    return "" if value is None else str(value)


def log_run():
    """Append one row describing this run (window, totals, MERGE counts) to the run log table."""
    spark.sql("""
        INSERT INTO IDENTIFIER(:t)
        VALUES (CAST(:run_at AS TIMESTAMP), :warehouse_id, CAST(:window_start AS TIMESTAMP), :dry_run,
                :source_rows, :billed_hours, CAST(:window_dbus AS DECIMAL(38,12)), CAST(:window_cost AS DECIMAL(38,12)),
                CAST(NULLIF(:tagged_share, '') AS DOUBLE), CAST(NULLIF(:ins, '') AS BIGINT),
                CAST(NULLIF(:upd, '') AS BIGINT), CAST(NULLIF(:del, '') AS BIGINT))
    """, args={
        "t": RUN_LOG_TABLE,
        "run_at": RUN_AT.strftime("%Y-%m-%d %H:%M:%S"),
        "warehouse_id": WAREHOUSE_ID,
        "window_start": SQL_ARGS["window_start"],
        "dry_run": DRY_RUN,
        "source_rows": CHECKS["source_rows"],
        "billed_hours": CHECKS["billed_hours"],
        "window_dbus": str(CHECKS["window_dbus"] or 0),
        "window_cost": str(CHECKS["window_cost"] or 0),
        "tagged_share": as_param(CHECKS["tagged_share"]),
        "ins": as_param(MERGE_COUNTS["num_inserted_rows"]),
        "upd": as_param(MERGE_COUNTS["num_updated_rows"]),
        "del": as_param(MERGE_COUNTS["num_deleted_rows"]),
    })


log_run()
print("done")

