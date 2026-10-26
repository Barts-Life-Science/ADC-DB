# Databricks notebook source
# MAGIC %md
# MAGIC # Pathology negation outcome (BRONZE_FIX_20261025)
# MAGIC
# MAGIC Leaf task after `pathology_negation` (run if: all done, serverless). Job run status follows leaf tasks, so this
# MAGIC task decides what a negation failure means for the bronze run:
# MAGIC
# MAGIC | pathology_negation | this task | why |
# MAGIC |---|---|---|
# MAGIC | succeeded | succeeds | nothing to do |
# MAGIC | every failed attempt was a cloud capacity failure (GPU stockout, quota, pool full) | succeeds, prints `NEGATION_DEFERRED` | negation scores a bounded batch from a backlog; the next run resumes it |
# MAGIC | any attempt failed for another reason, or it did not run because something upstream failed | fails | a real defect must fail the run |

# COMMAND ----------

import json

from databricks.sdk import WorkspaceClient

dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("watched_task", "pathology_negation")
RUN_ID = int(dbutils.widgets.get("pipeline_run_id"))
# NEGATION_FAILOVER_V1 (BRONZE_FIX_20261025B): watched_task lists the T4 task and its A10 fail-over, comma-separated.
WATCHED_TASKS = [key.strip() for key in dbutils.widgets.get("watched_task").split(",") if key.strip()]

# Text that marks a failure as "no capacity", not a code defect.
CAPACITY_MARKERS = (
    "CLOUD_PROVIDER_RESOURCE_STOCKOUT", "ZonalAllocationFailed", "AllocationFailed",
    "AZURE_QUOTA_EXCEEDED_EXCEPTION", "QuotaExceeded", "INSTANCE_POOL_MAX_CAPACITY_REACHED",
    "SkuNotAvailable", "OverconstrainedAllocationRequest", "OverconstrainedZonalAllocationRequest",
)


def watched_attempts(workspace, run_id, task_key):
    """Every attempt of the watched task in this job run (repairs included), oldest first."""
    run = workspace.jobs.get_run(run_id, include_history=True)
    attempts = [t for t in run.tasks or [] if t.task_key == task_key]
    return sorted(attempts, key=lambda t: t.start_time or 0)


def describe(attempt):
    """(result state, termination code, message) of one attempt, as plain strings.

    Older SDKs (e.g. on serverless) have no RunTask.status; the state message carries the same cluster error text.
    """
    state = attempt.state
    result = state.result_state.value if state and state.result_state else "NONE"
    status = getattr(attempt, "status", None)
    details = status.termination_details if status else None
    code = details.code.value if details and details.code else "NONE"
    message = " ".join(filter(None, [details.message if details else None, state.state_message if state else None]))
    return result, code, message


def is_capacity_failure(code, message):
    """A failure caused by cloud capacity (the cluster never became ready), not by the notebook."""
    return code in ("CLUSTER_ERROR", "NONE") and any(marker in message for marker in CAPACITY_MARKERS)

# COMMAND ----------

workspace = WorkspaceClient()
latest_result = {}
attempts = []
for task_key in WATCHED_TASKS:
    for result, code, message in (describe(a) for a in watched_attempts(workspace, RUN_ID, task_key)):
        print(json.dumps({"task": task_key, "result": result, "code": code, "message": message[:300]}))
        attempts.append((task_key, result, code, message))
        latest_result[task_key] = result
if not attempts:
    raise RuntimeError(f"{WATCHED_TASKS} have no attempt in run {RUN_ID}")

# A fail-over task that did not run because the T4 task succeeded is EXCLUDED; it is not a failure.
failed = [(task_key, code, message) for task_key, result, code, message in attempts
          if result not in ("SUCCESS", "EXCLUDED")]
succeeded = [task_key for task_key, result in latest_result.items() if result == "SUCCESS"]
if succeeded:
    outcome = "NEGATION_SUCCEEDED"
    print(f"NEGATION_SUCCEEDED on {succeeded}")
elif failed and all(is_capacity_failure(code, message) for _, code, message in failed):
    outcome = "NEGATION_DEFERRED"
    print(f"NEGATION_DEFERRED: {len(failed)} attempt(s) found no cloud capacity; the backlog resumes next run.")
else:
    raise RuntimeError(f"negation failed for a reason other than cloud capacity: {failed[-1] if failed else attempts}")
dbutils.notebook.exit(outcome)

