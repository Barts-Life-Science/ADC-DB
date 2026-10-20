# Databricks notebook source
# MAGIC %run ../Bronze/_bronze_common

# COMMAND ----------

# BRONZE_FIX_946452877034658_V1

# ANON_B1_PERF_20260904_V1
"""Bronze-job task wrapper for the anonymous-text engine.

Actions:
  init      anon_initialize: create/tag control tables, one global identifier refresh.
  run       anon_<feed>: budgeted, resumable redaction of one feed; syncs replace-built state.
  finalize  anon_finalize: per-feed backlog report row for this pipeline run.
Runs as one task per feed in Bronze_Pipeline_Parallel; never as a parallel job.
"""

import json
import re
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

for _name, _default in [
    ("pipeline_run_id", ""),
    ("target_schema", "4_prod.bronze"),
    ("allow_production_write", "false"),
    ("feed", ""),
    ("action", "run"),
    ("max_runtime_seconds", "7200"),
    ("work_cap_rows", "0"),
    ("max_batches", "1000"),
    ("report_backlog", "false"),
    ("report_feeds", ""),
]:
    try:
        dbutils.widgets.text(_name, _default)
    except Exception:
        pass

RUN_ID = bronze_run_id()
TARGET_SCHEMA = bronze_value("target_schema", "4_prod.bronze")
ALLOW_PRODUCTION_WRITE = bronze_bool("allow_production_write", False)
FEED = bronze_value("feed", "")
ACTION = bronze_value("action", "run")
MAX_RUNTIME_SECONDS = int(bronze_value("max_runtime_seconds", "7200"))
WORK_CAP_ROWS = int(bronze_value("work_cap_rows", "0"))
MAX_BATCHES = int(bronze_value("max_batches", "1000"))
REPORT_BACKLOG = bronze_bool("report_backlog", False)
REPORT_FEEDS_RAW = bronze_value("report_feeds", "")
ENGINE_TIMEOUT_GRACE_SECONDS = 900
ENGINE_PATH = bronze_sibling("Anon__anon_engine")
CONTROL_SCHEMA = "6_mgmt.anon" if TARGET_SCHEMA == "4_prod.bronze" else "8_dev.anon"
STATE_TYPES = {
    "anon_status": "STRING",
    "anon_redactor_version": "STRING",
    "anon_source_text_sha": "STRING",
    "anon_identity_fingerprint": "STRING",
    "anon_context_fingerprint": "STRING",
    "anon_redaction_count": "BIGINT",
    "anon_processed_at": "TIMESTAMP",
}
REPLACE_BUILT_FEEDS = {
    "neonatal_episode_narrative": dict(
        table="map_neonatal_episode_narrative",
        keys=["EntityID"],
        outputs=[
            "anon_final_summary_text",
            "anon_birth_summary",
            "anon_episode_summary",
            "anon_diagnosis_during_stay",
            "anon_drugs_during_stay",
            "anon_maternal_medical_notes",
        ],
    ),
    "pacs_report": dict(
        table="map_pacs_report",
        keys=["PACS_REPORT_ID"],
        outputs=["anon_report_text"],
    ),
    "text_event": dict(
        table="map_text_events",
        keys=["EVENT_ID"],
        outputs=["anon_text_result"],
    ),
}
ALL_FEEDS = [
    "neonatal_episode_narrative",
    "elective_access_comment",
    "endobase_exam",
    "order_comment",
    "pacs_report",
    "pacs_report_bridge",
    "pacs_request",
    "mill_blob_text",
    "text_event",
    "pathology_report",
    "safety_text_fragment",
    "prearrival",
    "tracking_location_stay",
    "pending_movement",
    "location_attribute_history",
]
if REPORT_FEEDS_RAW:
    try:
        REPORT_FEEDS = json.loads(REPORT_FEEDS_RAW)
    except Exception:
        REPORT_FEEDS = [item.strip() for item in REPORT_FEEDS_RAW.split(",") if item.strip()]
else:
    REPORT_FEEDS = list(ALL_FEEDS)
if not isinstance(REPORT_FEEDS, list) or not REPORT_FEEDS:
    raise ValueError("report_feeds must be a non-empty JSON list or comma-separated list")
unknown_report_feeds = sorted(set(REPORT_FEEDS) - set(ALL_FEEDS))
if unknown_report_feeds:
    raise ValueError(f"unknown report_feeds: {unknown_report_feeds}")

if ACTION not in {"init", "run", "finalize"}:
    raise ValueError(f"unsupported action {ACTION!r}")
if ACTION == "run" and FEED not in ALL_FEEDS:
    raise ValueError(f"unknown feed {FEED!r}")
if TARGET_SCHEMA == "4_prod.bronze" and not ALLOW_PRODUCTION_WRITE:
    raise RuntimeError("allow_production_write=true is required to write 4_prod.bronze")
if TARGET_SCHEMA != "4_prod.bronze" and not TARGET_SCHEMA.startswith("8_dev."):
    raise RuntimeError(
        f"target_schema {TARGET_SCHEMA!r} must be exactly 4_prod.bronze or under 8_dev."
    )
if MAX_RUNTIME_SECONDS < 1800:
    raise ValueError("max_runtime_seconds must be at least 1800")

STARTED = datetime.now(timezone.utc)
STARTED_MONOTONIC = time.monotonic()
print(bronze_json({
    "task": "anon",
    "action": ACTION,
    "feed": FEED,
    "run_id": RUN_ID,
    "target_schema": TARGET_SCHEMA,
    "control_schema": CONTROL_SCHEMA,
    "max_runtime_seconds": MAX_RUNTIME_SECONDS,
    "work_cap_rows": WORK_CAP_ROWS,
}))


def _qtable(name):
    return ".".join(f"`{part}`" for part in name.replace("`", "").split("."))


def _run_engine(action, feed, suffix, skip_fingerprint_refresh, max_runtime_seconds):
    raw = dbutils.notebook.run(
        ENGINE_PATH,
        max_runtime_seconds + ENGINE_TIMEOUT_GRACE_SECONDS,
        {
            "action": action,
            "feed": feed or "mill_blob_text",
            "control_schema": CONTROL_SCHEMA,
            "target_schema": TARGET_SCHEMA,
            "max_batches": str(MAX_BATCHES),
            "max_runtime_seconds": str(max_runtime_seconds),
            "work_cap_rows": str(WORK_CAP_ROWS),
            "skip_fingerprint_refresh": "true" if skip_fingerprint_refresh else "false",
            "run_id": f"{RUN_ID}_{suffix}",
            "production_confirmation": "RUN_PRODUCTION_ANON_V3_3" if TARGET_SCHEMA == "4_prod.bronze" else "",
        },
    )
    try:
        return json.loads(raw)
    except Exception as exc:
        raise RuntimeError(
            f"anon engine returned invalid JSON for {action}/{feed}: {raw[:1000]}"
        ) from exc


def _last_state_sync(feed):
    log = f"{CONTROL_SCHEMA}.state_sync_log"
    if not spark.catalog.tableExists(log):
        return None
    row = spark.sql(
        f"SELECT max(synced_through) AS ts FROM {log} WHERE feed = '{feed}'"
    ).first()
    return row.ts if row and row.ts else None


def _sync_replace_state(feed):
    """Snapshot anonymous state for replace-built publishers."""
    cfg = REPLACE_BUILT_FEEDS[feed]
    table = f"{TARGET_SCHEMA}.{cfg['table']}"
    target = f"{CONTROL_SCHEMA}.state_{feed}"
    through = STARTED
    since = _last_state_sync(feed)
    source = spark.table(table).where(F.col("anon_status").isNotNull())
    if since is not None and spark.catalog.tableExists(target):
        source = source.where(
            F.col("anon_processed_at") >= F.lit(since) - F.expr("INTERVAL 1 DAY")
        )
    selected = source.select(*cfg["keys"], *cfg["outputs"], *STATE_TYPES)
    stage = (
        f"{CONTROL_SCHEMA}.anon_state_"
        + re.sub(r"[^A-Za-z0-9_]", "_", f"{feed}_{RUN_ID}")
    )
    selected.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(stage)
    if not spark.catalog.tableExists(target):
        spark.table(stage).write.mode("overwrite").saveAsTable(target)
    else:
        # Evolve the state contract before MERGE and column tagging.
        existing = {field.name.lower(): field for field in spark.table(target).schema.fields}
        for field in selected.schema.fields:
            actual = existing.get(field.name.lower())
            if actual is None:
                if field.name in cfg["keys"]:
                    raise RuntimeError(f"{target}: missing state key {field.name}")
                column = field.name.replace("`", "``")
                spark.sql(f"ALTER TABLE {_qtable(target)} ADD COLUMNS (`{column}` {field.dataType.simpleString()})")
            elif actual.dataType != field.dataType:
                raise RuntimeError(f"{target}: incompatible state type for {field.name}")
        join_clause = " AND ".join(
            f"t.`{column}` <=> s.`{column}`" for column in cfg["keys"]
        )
        spark.sql(
            f"MERGE INTO {_qtable(target)} t USING {_qtable(stage)} s ON {join_clause} "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        )
    for name in cfg["outputs"]:
        spark.sql(
            f"ALTER TABLE {_qtable(target)} ALTER COLUMN `{name}` "
            "SET TAGS ('ig_risk'='3','ig_severity'='2')"
        )
    for name in STATE_TYPES:
        spark.sql(
            f"ALTER TABLE {_qtable(target)} ALTER COLUMN `{name}` "
            "SET TAGS ('ig_risk'='1','ig_severity'='1')"
        )
    spark.sql(f"DROP TABLE IF EXISTS {_qtable(stage)}")
    spark.createDataFrame(
        [(feed, through, RUN_ID, datetime.now(timezone.utc))],
        "feed string, synced_through timestamp, run_id string, recorded_at timestamp",
    ).write.mode("append").saveAsTable(f"{CONTROL_SCHEMA}.state_sync_log")
    return int(spark.table(target).count())


INVOCATIONS = f"{CONTROL_SCHEMA}.feed_invocations"
INVOCATION_SCHEMA = (
    "pipeline_run_id string, feed string, status string, eligible_total long, work_rows long, processed long, "
    "touched long, drift_settled long, remaining_estimate long, stop_reason string, recorded_at timestamp"
)


def record_invocation(feed, result):
    """Append one row for this task invocation (every attempt, including failures) from the engine's own result.

    ANON_OUTCOME_V1 (2026-10-18): the finalizer used to combine the latest plan with processed counts summed over
    all attempts (an unfinished retry could look complete), and document-context feeds, which write no plan row,
    looked not run. Outcomes now come from the latest invocation only.
    """
    spark.sql(f"CREATE TABLE IF NOT EXISTS {INVOCATIONS} ({INVOCATION_SCHEMA}) USING DELTA")

    def number(key):
        value = result.get(key)
        return int(value) if value is not None else None

    spark.createDataFrame(
        [(RUN_ID, feed, str(result.get("status")), number("eligible_total"), number("work_rows"), number("processed"),
          number("touched"), number("drift_settled"), number("remaining_estimate"),
          (str(result.get("stop_reason"))[:500] if result.get("stop_reason") is not None else None),
          datetime.now(timezone.utc))],
        INVOCATION_SCHEMA,
    ).write.mode("append").saveAsTable(INVOCATIONS)


def summarize_feed(feed):
    """run_summary row for one feed from its latest invocation in this pipeline run (never summed across attempts)."""
    latest = None
    if spark.catalog.tableExists(INVOCATIONS):
        latest = (
            spark.table(INVOCATIONS)
            .where((F.col("pipeline_run_id") == RUN_ID) & (F.col("feed") == feed))
            .orderBy(F.col("recorded_at").desc())
            .first()
        )
    now = datetime.now(timezone.utc)
    if latest is None:
        return (RUN_ID, feed, None, None, None, None, now, "not_run")
    remaining = latest["remaining_estimate"]
    if latest["status"] == "failed":
        outcome = "failed"
    elif latest["status"] == "partial" or remaining is None or remaining > 0:
        outcome = "partial"
    else:
        outcome = "complete"
    return (RUN_ID, feed, latest["eligible_total"], latest["work_rows"], latest["processed"], remaining, now, outcome)


if ACTION == "init":
    init_result = _run_engine("init_controls", None, "init", False, 1800)
    fingerprint_result = _run_engine(
        "refresh_fingerprints",
        None,
        "fingerprints",
        False,
        max(MAX_RUNTIME_SECONDS - 1800, 1800),
    )
    result = {
        "init": init_result,
        "fingerprints": fingerprint_result,
        "status": "success",
    }

elif ACTION == "run":
    try:
        result = _run_engine("run", FEED, FEED, True, MAX_RUNTIME_SECONDS)
    except Exception as error:
        record_invocation(FEED, {"status": "failed", "stop_reason": repr(error)})
        raise
    record_invocation(FEED, result)
    if result.get("status") not in {"success", "partial"}:
        raise RuntimeError(f"anon engine failed for {FEED}: {result}")
    if FEED in REPLACE_BUILT_FEEDS:
        result["state_rows"] = _sync_replace_state(FEED)

elif ACTION == "finalize":
    rows = [summarize_feed(feed) for feed in REPORT_FEEDS]
    if spark.catalog.tableExists(f"{CONTROL_SCHEMA}.run_summary") and \
            "outcome" not in spark.table(f"{CONTROL_SCHEMA}.run_summary").columns:
        spark.sql(f"ALTER TABLE {CONTROL_SCHEMA}.run_summary ADD COLUMNS (outcome STRING)")
    spark.createDataFrame(
        rows,
        "pipeline_run_id string, feed string, eligible_total long, work_rows long, "
        "processed long, remaining_estimate long, recorded_at timestamp, outcome string",
    ).write.mode("append").saveAsTable(f"{CONTROL_SCHEMA}.run_summary")
    if spark.catalog.tableExists(INVOCATIONS):
        for column in ("pipeline_run_id", "feed", "status", "eligible_total", "work_rows", "processed", "touched",
                       "drift_settled", "remaining_estimate", "stop_reason", "recorded_at"):
            spark.sql(f"ALTER TABLE {INVOCATIONS} ALTER COLUMN {column} SET TAGS ('ig_risk'='1','ig_severity'='1')")
    for column in (
        "pipeline_run_id",
        "feed",
        "eligible_total",
        "work_rows",
        "processed",
        "remaining_estimate",
        "recorded_at",
        "outcome",
    ):
        spark.sql(
            f"ALTER TABLE {CONTROL_SCHEMA}.run_summary ALTER COLUMN {column} "
            "SET TAGS ('ig_risk'='1','ig_severity'='1')"
        )
    result = {
        "status": "success",
        "report_feeds": REPORT_FEEDS,
        "feeds": [
            dict(zip(
                [
                    "pipeline_run_id",
                    "feed",
                    "eligible_total",
                    "work_rows",
                    "processed",
                    "remaining_estimate",
                    "recorded_at",
                    "outcome",
                ],
                row,
            ))
            for row in rows
        ],
    }

payload = {
    "action": ACTION,
    "feed": FEED,
    "run_id": RUN_ID,
    "target_schema": TARGET_SCHEMA,
    "elapsed_seconds": round(time.monotonic() - STARTED_MONOTONIC, 1),
    **result,
}
print(bronze_json(payload))
dbutils.notebook.exit(bronze_json(payload))
