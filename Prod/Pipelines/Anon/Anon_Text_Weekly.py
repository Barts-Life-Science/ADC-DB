# Databricks notebook source
import json
import re
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F


# Declared configuration: edit in source review, never through widgets.
EXECUTE = True
CONFIRM = 'RUN_PRODUCTION_ANON_V3_2'
RUN_LABEL = "anon_weekly_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
FEEDS_TO_RUN = json.loads('["neonatal_episode_narrative", "elective_access_comment", "endobase_exam", "order_comment", "pacs_report", "pacs_report_bridge", "mill_blob_text", "text_event", "pathology_report"]')
ENGINE_PATH = '/Workspace/Shared/ADC-DB/Prod/Pipelines/Anon/Anon__anon_engine'
CONTROL_SCHEMA = '6_mgmt.anon'
SCRATCH_SCHEMA = '6_mgmt.anon'
# RDE_REPAIR_20260901_WEEKLY_DEADLINE
MAX_BATCHES = 1000
MAX_WEEKLY_RUNTIME_SECONDS = 28800
MAX_ENGINE_RUNTIME_SECONDS = 14400
MIN_ENGINE_RUNTIME_SECONDS = 1800
ENGINE_TIMEOUT_GRACE_SECONDS = 900
MILL_BLOB_REDACTION_BACKLOG_LIMIT = 2000000
RUN_STARTED_MONOTONIC = time.monotonic()
STATE_TYPES = json.loads('{"anon_identity_fingerprint": "STRING", "anon_processed_at": "TIMESTAMP", "anon_redaction_count": "BIGINT", "anon_redactor_version": "STRING", "anon_source_text_sha": "STRING", "anon_status": "STRING"}')
FEEDS = json.loads('{"elective_access_comment": {"build": "merge_in_place", "keys": ["SOURCE_SYSTEM_OID", "WAITING_LIST_OID"], "outputs": ["anon_comments"], "table": "4_prod.bronze.map_elective_access_list"}, "endobase_exam": {"build": "merge_in_place", "keys": ["ENDOBASE_EXAM_TERM_ID"], "outputs": ["anon_term_text"], "table": "4_prod.bronze.map_endobase_exam_term"}, "mill_blob_text": {"build": "merge_in_place", "keys": ["BLOB_VERSION_ID"], "outputs": ["anon_text"], "table": "4_prod.bronze.mill_blob_text"}, "neonatal_episode_narrative": {"build": "replace_built", "keys": ["EntityID"], "outputs": ["anon_final_summary_text", "anon_birth_summary", "anon_episode_summary", "anon_diagnosis_during_stay", "anon_drugs_during_stay", "anon_maternal_medical_notes"], "table": "4_prod.bronze.map_neonatal_episode_narrative"}, "order_comment": {"build": "merge_in_place", "keys": ["ORDER_ID", "ACTION_SEQUENCE", "COMMENT_TYPE_CD"], "outputs": ["anon_comment_text"], "table": "4_prod.bronze.map_order_comment"}, "pacs_report": {"build": "replace_built", "keys": ["PACS_REPORT_ID"], "outputs": ["anon_report_text"], "table": "4_prod.bronze.map_pacs_report"}, "pacs_report_bridge": {"build": "merge_in_place", "keys": ["REPORT_ID"], "outputs": ["anon_bridged_text"], "table": "4_prod.bronze.map_pacs_report_text_bridge"}, "pathology_report": {"build": "merge_in_place", "keys": ["report_version_id"], "outputs": ["anon_report_text"], "table": "4_prod.bronze.map_pathology_report"}, "text_event": {"build": "replace_built", "keys": ["EVENT_ID"], "outputs": ["anon_text_result"], "table": "4_prod.bronze.map_text_events"}}')
# RDE_REPAIR_20260830_SKIP_UNREADY_ANON_FEEDS
# These feeds were deployed before their person/encounter routing columns existed in
# production bronze. Skip only while prerequisites are absent; they auto-enable when
# the upstream columns land. Other feed errors remain fatal.
ANON_FEED_REQUIRED_COLUMNS = {
    "endobase_exam": {"ANON_EXAM_PERSON_ID", "ANON_EXAM_ENCNTR_ID"},
    "order_comment": {"ANON_ORDER_PERSON_ID", "ANON_ORDER_ENCNTR_ID"},
    "pathology_report": {"ANON_ACCESSION_PERSON_ID"},
}
SKIPPED_FEEDS = {}
_ready_feeds = []
for _feed in FEEDS_TO_RUN:
    _required = ANON_FEED_REQUIRED_COLUMNS.get(_feed, set())
    _available = set(spark.table(FEEDS[_feed]["table"]).columns)
    _missing = sorted(_required - _available)
    if _missing:
        SKIPPED_FEEDS[_feed] = _missing
        print(json.dumps({
            "event": "anon_feed_skipped_missing_prerequisites",
            "feed": _feed,
            "table": FEEDS[_feed]["table"],
            "missing_columns": _missing,
        }, sort_keys=True))
    else:
        _ready_feeds.append(_feed)
FEEDS_TO_RUN = _ready_feeds

REPLACE_BUILT_FEEDS = set(json.loads('["neonatal_episode_narrative", "pacs_report", "text_event"]'))

if not EXECUTE or CONFIRM != 'RUN_PRODUCTION_ANON_V3_2':
    raise RuntimeError("Production anonymous-text driver is disabled")


def _safe(value):
    return re.sub(r"[^A-Za-z0-9_]", "_", value)


def _qtable(name):
    return ".".join(f"`{part}`" for part in name.replace("`", "").split("."))


def _tag_anon_columns(table, outputs):
    for name in outputs:
        spark.sql(
            f"ALTER TABLE {_qtable(table)} ALTER COLUMN `{name}` "
            "SET TAGS ('ig_risk'='3','ig_severity'='2')"
        )
    for name in STATE_TYPES:
        spark.sql(
            f"ALTER TABLE {_qtable(table)} ALTER COLUMN `{name}` "
            "SET TAGS ('ig_risk'='1','ig_severity'='1')"
        )


def _sync_replace_state(feed, since):
    cfg = FEEDS[feed]
    target = f"{CONTROL_SCHEMA}.state_{_safe(feed)}"
    source = spark.table(cfg["table"]).where(F.col("anon_status").isNotNull())
    if spark.catalog.tableExists(target):
        source = source.where(F.col("anon_processed_at") >= F.lit(since))
    selected = source.select(*cfg["keys"], *cfg["outputs"], *STATE_TYPES)
    stage = f"{SCRATCH_SCHEMA}.anon_state_{_safe(feed)}_{RUN_LABEL}"
    selected.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(stage)
    if not spark.catalog.tableExists(target):
        spark.table(stage).write.mode("overwrite").saveAsTable(target)
    else:
        join_clause = " AND ".join(
            f"t.`{column}` <=> s.`{column}`" for column in cfg["keys"]
        )
        spark.sql(
            f"MERGE INTO {_qtable(target)} t USING {_qtable(stage)} s ON {join_clause} "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        )
    _tag_anon_columns(target, cfg["outputs"])
    spark.sql(f"DROP TABLE IF EXISTS {_qtable(stage)}")


def _remaining_weekly_seconds():
    return max(
        0,
        int(MAX_WEEKLY_RUNTIME_SECONDS - (time.monotonic() - RUN_STARTED_MONOTONIC)),
    )


def _run_engine(action, feed, suffix):
    remaining = _remaining_weekly_seconds()
    minimum_budget = MIN_ENGINE_RUNTIME_SECONDS + ENGINE_TIMEOUT_GRACE_SECONDS
    if remaining < minimum_budget:
        return {
            "status": "partial",
            "stop_reason": "weekly_deadline",
            "action": action,
            "feed": feed,
            "remaining_seconds": remaining,
        }
    engine_budget = min(
        MAX_ENGINE_RUNTIME_SECONDS,
        remaining - ENGINE_TIMEOUT_GRACE_SECONDS,
    )
    timeout_seconds = engine_budget + ENGINE_TIMEOUT_GRACE_SECONDS
    raw = dbutils.notebook.run(
        ENGINE_PATH,
        timeout_seconds,
        {
            "action": action,
            "feed": feed,
            "control_schema": CONTROL_SCHEMA,
            "max_batches": str(MAX_BATCHES),
            "max_runtime_seconds": str(engine_budget),
            "run_id": RUN_LABEL + "_" + suffix,
            "production_confirmation": CONFIRM,
        },
    )
    try:
        return json.loads(raw)
    except Exception as exc:
        raise RuntimeError(
            f"Anon engine returned invalid JSON for action={action}, feed={feed}: {raw[:1000]}"
        ) from exc

results = []
_run_engine("init_controls", "mill_blob_text", "init")
fingerprint_result = _run_engine(
    "refresh_fingerprints", "mill_blob_text", "fingerprints"
)
results.append({"step": "refresh_fingerprints", "engine_result": fingerprint_result})

legacy_result = _run_engine(
    "initialize_legacy_blob_state", "mill_blob_text", "legacy_state"
)
results.append({"step": "initialize_legacy_blob_state", "engine_result": legacy_result})

stop_reason = None
if legacy_result.get("status") != "success":
    stop_reason = legacy_result.get("stop_reason") or "legacy_state_incomplete"
else:
    for feed in FEEDS_TO_RUN:
        if feed == "mill_blob_text":
            count_result = _run_engine(
                "eligible_count", feed, feed + "_eligible_count"
            )
            if count_result.get("status") != "success":
                results.append({"feed": feed, "engine_result": count_result})
                stop_reason = count_result.get("stop_reason") or "eligible_count_incomplete"
                break
            eligible_count = int(count_result.get("eligible_count", 0))
            if eligible_count > MILL_BLOB_REDACTION_BACKLOG_LIMIT:
                stop_reason = "mill_blob_redaction_backlog_guard"
                results.append({
                    "feed": feed,
                    "engine_result": count_result,
                    "backlog_limit": MILL_BLOB_REDACTION_BACKLOG_LIMIT,
                })
                break
        started = datetime.now(timezone.utc).replace(tzinfo=None)
        engine_result = _run_engine("run", feed, feed)
        results.append({"feed": feed, "engine_result": engine_result})
        if engine_result.get("status") != "success":
            stop_reason = engine_result.get("stop_reason") or "feed_incomplete"
            break
        if feed in REPLACE_BUILT_FEEDS:
            _sync_replace_state(feed, started)

status = "partial" if stop_reason else "success"
payload = {
    "run_label": RUN_LABEL,
    "status": status,
    "stop_reason": stop_reason,
    "elapsed_seconds": round(time.monotonic() - RUN_STARTED_MONOTONIC, 1),
    "remaining_seconds": _remaining_weekly_seconds(),
    "feeds": results,
    "skipped_feeds": SKIPPED_FEEDS,
}
print(json.dumps(payload, sort_keys=True))
dbutils.notebook.exit(json.dumps(payload, sort_keys=True))

