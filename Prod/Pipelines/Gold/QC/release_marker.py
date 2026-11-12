# Databricks notebook source
# MAGIC %md
# MAGIC # Release marker (INSR R4; design R4 "Release boundary", decision 16)
# MAGIC `action=open|close|rollback|check`. Silver_Domains opens a release before S1 and closes it last; every gold
# MAGIC consumer job runs `check` first and skips while a release is open (task value `release_open`).
# MAGIC Exit JSON: `{"fails": [], ...}`.

# COMMAND ----------

import datetime
import json

import release_marker_lib as M


def get_param(name, default):
    """A run parameter if the runner passed one, else the default (never declares a widget)."""
    try:
        value = dbutils.widgets.get(name)
        return value if value not in (None, "") else default
    except Exception:  # noqa: BLE001
        return default


def _utc_now():
    """The driver's UTC clock: the event time the marker SQL records (release_marker_lib takes it as a parameter)."""
    return datetime.datetime.now(datetime.timezone.utc)


ACTION = get_param("action", "check")
SCHEMA = get_param("schema", "`6_mgmt`.data_quality")
CONTEXT = get_param("release_context", "")
assert ACTION in ("open", "close", "rollback", "check"), ACTION
if ACTION != "check":                       # a consumer's check never needs DDL rights
    for statement in M.ddl(SCHEMA):
        spark.sql(statement)
MARKER_EXISTS = spark.catalog.tableExists(SCHEMA.replace("`", "") + ".release_state")

# COMMAND ----------

if ACTION == "open":
    # Silver_Domains passes no id: the run id names the release (SD_<run id>), handed to later tasks.
    release_id = get_param("release_id", "SD_" + get_param("job_run_id", "manual"))
    spark.sql(M.open_sql(SCHEMA, release_id, get_param("job_run_id", "manual"), _utc_now()))
    dbutils.jobs.taskValues.set(key="release_id", value=release_id)
    result = {"action": "open", "release_id": release_id, "fails": []}
elif ACTION in ("close", "rollback"):
    release_id = get_param("release_id", "")
    assert release_id, "close/rollback needs release_id"
    spark.sql(M.close_sql(SCHEMA, release_id, _utc_now(), "closed" if ACTION == "close" else "rolled_back",
                          get_param("note", "")))
    state = spark.sql(f"SELECT status FROM {SCHEMA}.release_state WHERE release_id = '{release_id}'").collect()
    ok = state and state[0]["status"] == ("closed" if ACTION == "close" else "rolled_back")
    result = {"action": ACTION, "release_id": release_id, "fails": [] if ok else [f"{release_id} is {state}"]}
else:
    open_ids = [r["release_id"] for r in spark.sql(M.open_releases_sql(SCHEMA)).collect()] if MARKER_EXISTS else []
    release_open, reason = M.decide(open_ids, CONTEXT)
    if MARKER_EXISTS:
        spark.sql(M.log_sql(SCHEMA, get_param("consumer", "manual"), get_param("run_id", "none"), release_open, open_ids,
                             CONTEXT, _utc_now()))
    dbutils.jobs.taskValues.set(key="release_open", value="true" if release_open else "false")
    print("release gate:", reason)
    result = {"action": "check", "release_open": release_open, "reason": reason, "fails": []}
dbutils.notebook.exit(json.dumps(result))

