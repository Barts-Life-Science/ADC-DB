# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze lane readiness gate (BRONZE_READINESS_V2, 2026-10-18)
# MAGIC
# MAGIC First task on each map lane. It waits up to `capacity_wait_seconds` for the lane cluster to reach its
# MAGIC configured minimum workers, then runs a two-partition shuffle probe. A capacity shortfall is reported, not
# MAGIC failed: the lane still runs on what it has. Only a failed probe fails the task. The Jobs timeout (1800 s)
# MAGIC also covers cluster setup, which took 380-470 s on 2026-10-18.

# COMMAND ----------

import json
import threading
import time

from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

dbutils.widgets.text("capacity_wait_seconds", "900")
CAPACITY_WAIT_SECONDS = int(dbutils.widgets.get("capacity_wait_seconds") or "900")
POLL_SECONDS = 30
STARTED = time.monotonic()


def report(event, **details):
    """Print one structured line; the task output keeps it even if nothing else can be written."""
    payload = {"event": event, "elapsed_seconds": round(time.monotonic() - STARTED, 1), **details}
    print("BRONZE_READINESS " + json.dumps(payload, default=str), flush=True)


def this_cluster_id():
    """Cluster id of the job cluster running this task, or None on serverless."""
    try:
        return spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
    except Exception:
        return None


def lane_capacity(workspace, cluster_id):
    """Return (current workers, configured minimum workers) for a cluster."""
    cluster = workspace.clusters.get(cluster_id)
    minimum = cluster.autoscale.min_workers if cluster.autoscale else (cluster.num_workers or 0)
    return len(cluster.executors or []), int(minimum or 0)


def wait_for_minimum_workers():
    """Wait, up to CAPACITY_WAIT_SECONDS, until the lane has its minimum workers. Never raises."""
    cluster_id = this_cluster_id()
    if not cluster_id:
        report("CAPACITY_SKIPPED", reason="no job cluster id (serverless or unknown)")
        return {"status": "skipped"}
    workspace = WorkspaceClient()
    deadline = time.monotonic() + CAPACITY_WAIT_SECONDS
    while True:
        try:
            workers, minimum = lane_capacity(workspace, cluster_id)
        except Exception as error:
            report("CAPACITY_UNKNOWN", cluster_id=cluster_id, error=str(error)[:300])
            return {"status": "unknown", "cluster_id": cluster_id}
        if workers >= minimum:
            report("CAPACITY_READY", cluster_id=cluster_id, workers=workers, minimum=minimum)
            return {"status": "ready", "cluster_id": cluster_id, "workers": workers, "minimum": minimum}
        if time.monotonic() >= deadline:
            report("CAPACITY_SHORT", cluster_id=cluster_id, workers=workers, minimum=minimum)
            return {"status": "short", "cluster_id": cluster_id, "workers": workers, "minimum": minimum}
        report("CAPACITY_WAITING", cluster_id=cluster_id, workers=workers, minimum=minimum)
        time.sleep(POLL_SECONDS)


def executor_probe():
    """Two-partition shuffle and sum: a real executor round trip (same probe as the V1 gate)."""
    stop = threading.Event()

    def heartbeat():
        while not stop.wait(30):
            report("PROBE_WAITING")

    thread = threading.Thread(target=heartbeat, daemon=True)
    thread.start()
    try:
        total = spark.range(0, 32, 1, 2).repartition(2).agg(F.sum("id").alias("total")).first().total
    finally:
        stop.set()
        thread.join(timeout=1)
    if total != 496:
        raise RuntimeError(f"readiness probe sum {total!r} != 496")
    return total

# COMMAND ----------

capacity = wait_for_minimum_workers()
capacity_seconds = round(time.monotonic() - STARTED, 1)
probe_value = executor_probe()
result = {"status": "BOOTSTRAP_READY", "gate": "BRONZE_READINESS_V2", "capacity": capacity,
          "capacity_wait_seconds": capacity_seconds,
          "probe_seconds": round(time.monotonic() - STARTED - capacity_seconds, 1), "value": probe_value}
report("BOOTSTRAP_READY", **result)
dbutils.notebook.exit(json.dumps(result, default=str))

