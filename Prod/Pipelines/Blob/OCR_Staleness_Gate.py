# Databricks notebook source
# OCR_Staleness_Gate: RDE_Job leaf after Blob_OCR_GPU (run_if ALL_DONE). A failed OCR attempt only
# fails the job when OCR has not finished an APPLY/BOOTSTRAP for STALE_HOURS; the queue is
# lease-driven, so a missed day (no GPU, no cluster) loses nothing.
import json
import re

dbutils.widgets.text("JOB_RUN_ID", "")
dbutils.widgets.text("OCR_TASK_KEY", "Blob_OCR_GPU")
dbutils.widgets.text("STATE_SCHEMA", "4_prod.tmp")
dbutils.widgets.text("STALE_HOURS", "72")

TOLERATED = ("FAILED", "TIMEDOUT", "INTERNAL_ERROR")


def decide(ocr_state, age_hours, stale_hours):
    """(passed, message) from the OCR task's final state and the newest good heartbeat's age."""
    if ocr_state == "SUCCESS":
        return True, "Blob OCR succeeded"
    if ocr_state not in TOLERATED:
        return False, f"Blob OCR ended {ocr_state}: not a GPU/cluster failure, so it is not tolerated"
    if age_hours is None:
        return False, f"Blob OCR ended {ocr_state} and has no successful run on record"
    if age_hours <= stale_hours:
        return True, (f"WARNING: Blob OCR ended {ocr_state}; last successful run {age_hours:.1f} h ago "
                      f"(tolerated up to {stale_hours:g} h, queue work waits for the next run)")
    return False, (f"Blob OCR ended {ocr_state}; last successful run {age_hours:.1f} h ago, "
                   f"beyond {stale_hours:g} h: OCR is stale")


def latest_attempt(tasks, task_key):
    attempts = [t for t in tasks if t.task_key == task_key]
    return max(attempts, key=lambda t: t.attempt_number or 0) if attempts else None


def task_state(run_id, task_key):
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    run = w.jobs.get_run(run_id)
    tasks = list(run.tasks or [])
    while getattr(run, "next_page_token", None):
        run = w.jobs.get_run(run_id, page_token=run.next_page_token)
        tasks += run.tasks or []
    task = latest_attempt(tasks, task_key)
    if task is None or task.state is None:
        return None
    if task.state.result_state is not None:
        return task.state.result_state.value
    return task.state.life_cycle_state.value if task.state.life_cycle_state else None


def heartbeat_age_hours(state_schema):
    if not re.fullmatch(r"[A-Za-z0-9_]+\.[A-Za-z0-9_]+", state_schema):
        raise ValueError("STATE_SCHEMA must be catalog.schema")
    catalog, schema = state_schema.split(".")
    try:
        row = spark.sql(
            f"SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(max(heartbeat_ts))) / 3600.0 AS age "
            f"FROM `{catalog}`.`{schema}`.blob_ocr_heartbeat WHERE status IN ('success', 'bootstrap')"
        ).first()
    except Exception as exc:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc):
            return None
        raise
    return None if row is None or row.age is None else float(row.age)


run_id = int(dbutils.widgets.get("JOB_RUN_ID"))
task_key = dbutils.widgets.get("OCR_TASK_KEY")
state = task_state(run_id, task_key)
age = heartbeat_age_hours(dbutils.widgets.get("STATE_SCHEMA"))
passed, message = decide(state, age, float(dbutils.widgets.get("STALE_HOURS")))
print(message)
if not passed:
    raise RuntimeError(message)
dbutils.notebook.exit(json.dumps({"ocr_state": state, "heartbeat_age_hours": age, "message": message}))

