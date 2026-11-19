# Databricks notebook source
# MAGIC %md
# MAGIC # Weekly pathology expansion
# MAGIC
# MAGIC Runs the deterministic A12 sidecars after `map_pipeline`. Production writes are
# MAGIC fail-closed and require the orchestrator's explicit `allow_production_write=true`.

# COMMAND ----------

import json
import time
from datetime import datetime, timezone

from pathology_amr import run_amr
from pathology_genetics import run_genetics
from pathology_validation import run_genetics_validation  # GEN4_LANE_V1
from pathology_incremental import run_incremental_core
from pathology_indications import changed_scope, run_indications
from pathology_pipeline import PipelineConfig, create_views
from pathology_runtime import register_runtime_artifact


WEEKLY_RUNNER_VERSION = "2026-08-16-periodic-core-v3"


for _name, _default in {
    "pipeline_run_id": "",
    "target_schema": "8_dev.bronze",
    "force_full_refresh": "false",
    "allow_production_write": "false",
    "full_reconciliation": "false",
    "bootstrap_mode": "false",
    "periodic_lanes": "auto",
}.items():
    try:
        dbutils.widgets.text(_name, _default)
    except Exception:
        pass


def value(name: str, default: str = "") -> str:
    try:
        return (dbutils.widgets.get(name) or default).strip()
    except Exception:
        return default


def flag(name: str) -> bool:
    return value(name).lower() in {"1", "true", "yes", "y"}


def bronze_lookup_schema(target_schema: str) -> str:
    catalog, _schema = target_schema.split(".", 1)
    if catalog == "4_prod":
        return "3_lookup.omop"
    if target_schema == "8_dev.bronze":
        return "8_dev.lookup"
    return target_schema


def periodic_lane_state(force_full_refresh: bool) -> dict[str, object]:
    mode = value("periodic_lanes", "auto").lower()
    if mode not in {"auto", "false", "no", "off", "true", "yes", "on", "indications"}:
        raise ValueError("periodic_lanes must be auto, true, false, or indications")
    now = datetime.now(timezone.utc)
    iso_week = int(now.isocalendar().week)
    if force_full_refresh:
        enabled = True
        reason = "force_full_refresh"
    elif mode == "auto":
        enabled = iso_week % 4 == 0
        reason = "every_fourth_iso_week" if enabled else "weekly_core_only"
    else:
        enabled = mode in {"true", "yes", "on"}
        reason = "explicit_override"
    return {
        "enabled": enabled,
        "mode": mode,
        "iso_week": iso_week,
        "cadence": "every_fourth_iso_week",
        "reason": reason,
    }


# DQ4_B5_INDICATIONS_ON_DIAGNOSIS_V1: rule indications_diagnosis_version_v1. The indications lane reads
# 4_prod.bronze.map_diagnosis, so it runs whenever that table has moved since the lane last ran,
# not only every fourth ISO week. The version it ran against is kept on map_pathology_indication.
INDICATIONS_DIAGNOSIS_SOURCE = "4_prod.bronze.map_diagnosis"
INDICATIONS_VERSION_PROPERTY = "pathology.indications.map_diagnosis_version"
# INDICATIONS_INCREMENTAL_V1: with the accession-source version also stored, a later run can read both change feeds
# from where this one stopped and rebuild only the affected accessions (see pathology_indications.changed_scope).
INDICATIONS_ACCESSION_VERSION_PROPERTY = "pathology.indications.accession_source_version"


def indications_lane_state(periodic: dict[str, object], target_schema: str) -> dict[str, object]:
    current = int(spark.sql(f"DESCRIBE HISTORY {INDICATIONS_DIAGNOSIS_SOURCE} LIMIT 1").first()["version"])
    current_accession = int(spark.sql(
        f"DESCRIBE HISTORY {target_schema}.map_pathology_accession_source LIMIT 1").first()["version"])
    # CONTEXT_LINK_V1 (BRONZE_FIX_20261025C): the lane's versions live on the context link table.
    table = f"{target_schema}.map_pathology_diagnosis_context"
    last = last_accession = None
    if spark.catalog.tableExists(table):
        props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
        last = props.get(INDICATIONS_VERSION_PROPERTY)
        last_accession = props.get(INDICATIONS_ACCESSION_VERSION_PROPERTY)
    if periodic["enabled"]:
        enabled, reason = True, periodic["reason"]
    elif periodic["mode"] in {"false", "no", "off"}:
        enabled, reason = False, "explicit_override"
    elif periodic["mode"] == "indications":
        enabled, reason = True, "explicit_indications"
    elif last is None or int(last) != current:
        enabled, reason = True, "map_diagnosis_version_changed"
    elif last_accession is None or int(last_accession) != current_accession:
        # New or changed accessions need context too; the incremental path keeps this cheap.
        enabled, reason = True, "accession_source_version_changed"
    else:
        enabled, reason = False, "map_diagnosis_unchanged"
    return {"enabled": enabled, "reason": reason, "map_diagnosis_version": current,
            "last_map_diagnosis_version": last, "accession_source_version": current_accession,
            "last_accession_source_version": last_accession}


# STM_SPECIMEN_TYPE_V1/runner: isolates carry map_pathology_specimen_type columns, so the AMR lane also runs whenever the map
# has moved since the lane last ran. The version it ran against is kept on map_pathology_microbiology_isolate.
SPECIMEN_MAP_VERSION_PROPERTY = "pathology.amr.specimen_map_version"


def amr_lane_state(periodic: dict[str, object], target_schema: str) -> dict[str, object]:
    current = int(spark.sql(f"DESCRIBE HISTORY {target_schema}.map_pathology_specimen_type LIMIT 1").first()["version"])
    table = f"{target_schema}.map_pathology_microbiology_isolate"
    last = None
    if spark.catalog.tableExists(table):
        props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
        last = props.get(SPECIMEN_MAP_VERSION_PROPERTY)
    if periodic["enabled"]:
        enabled, reason = True, periodic["reason"]
    elif periodic["mode"] in {"false", "no", "off"}:
        enabled, reason = False, "explicit_override"
    elif last is None or int(last) != current:
        enabled, reason = True, "specimen_map_version_changed"
    else:
        enabled, reason = False, "specimen_map_unchanged"
    return {"enabled": enabled, "reason": reason, "specimen_map_version": current,
            "last_specimen_map_version": last}


# BFIX_SHUFFLE_PROBE_V1 (BRONZE_FIX_20261116). On Sunday weekly runs since 10-18 this task's cluster has come up
# with workers that cannot reach each other's shuffle service. In run 1021647626060126 the driver log shows 570
# fetch failures ("Connecting to /10.0.2.x:4048 timed out (120000 ms)", nearly all involving one worker); every
# stage that touched it failed after about 495 s and was retried, so the task crawled for 20 hours. Healthy runs
# show no fetch failures at all.
#
# Before any real work this probe runs a small shuffle in which every worker sends data to every other worker, and
# proves it from the data: each reducer reports which hosts its rows came from. Verdicts:
#   HEALTHY       every ordered pair of distinct workers exchanged shuffle data; the task goes on.
#   SICK          a round did not finish within PROBE_ROUND_SECONDS, or failed: the task fails now, and its retry
#                 starts on new machines (the task has its own cluster per attempt).
#   INCONCLUSIVE  every round finished but some workers or pairs never took part (a scheduling accident is the
#                 likely cause). Reported, not called healthy; the task goes on, protected by the cluster's
#                 exclude-on-fetch-failure and short shuffle connect timeout settings.
PROBE_ROUND_SECONDS = 90
PROBE_ROUNDS = 3
PROBE_TASK_SLEEP_SECONDS = 0.5      # each probe task waits this long, so the tasks spread over every task slot
PROBE_COALESCE_CONF = "spark.sql.adaptive.coalescePartitions.enabled"
PROBE_EXECUTOR_WAIT_SECONDS = 180   # a fresh cluster's workers register with Spark one by one after it reports RUNNING
PROBE_ROUND_PAUSE_SECONDS = 15      # pause before re-running a round that did not reach every worker


def probe_report(event, **details):
    """Print one structured probe line (the task output keeps it; same style as the bronze readiness gate)."""
    print("SHUFFLE_PROBE " + json.dumps({"event": event, **details}, default=str), flush=True)


def probe_local_ip():
    """This process's IP address on the cluster network (runs on executors). No packet is sent: connecting a UDP
    socket only picks the outgoing interface."""
    import socket
    probe_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe_socket.connect(("10.255.255.255", 1))
        return probe_socket.getsockname()[0]
    finally:
        probe_socket.close()


def probe_cluster_shape():
    """(cluster id, executor count, task count) for this job cluster, or None when there is no job cluster
    (serverless dev runs). The task count is twice the cluster's cores, so the first wave fills every slot."""
    try:
        cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
    except Exception:
        return None
    if not cluster_id:
        return None
    from databricks.sdk import WorkspaceClient
    cluster = WorkspaceClient().clusters.get(cluster_id)
    executors = len(cluster.executors or [])
    tasks = max(32, int(2 * (cluster.cluster_cores or 128)))
    return cluster_id, executors, tasks


def probe_shuffle_pairs(tasks):
    """Run one probe shuffle and return the set of (map host, reduce host) pairs the reducers received data for.

    Map side: `tasks` tasks each emit one row per reduce key, tagged with the host it ran on. Reduce side: the rows
    are hash-partitioned by key into `tasks` partitions, and each reducer records the hosts its rows came from and
    its own host. A pair with two different hosts is a shuffle block fetched over the network.
    The UDFs build Arrow record batches directly (mapInArrow, explicit types): on DBR 18 the pandas route
    (mapInPandas) failed converting string columns ("Cannot convert pyarrow.lib.ChunkedArray to pyarrow.lib.Array",
    dev smoke 2026-11-16)."""
    import pyarrow as pa
    reduce_keys = 4 * tasks          # several keys per partition, so nearly every reduce partition receives rows
    sleep_seconds = PROBE_TASK_SLEEP_SECONDS

    def map_side(batches):
        """One map task: wait briefly, then emit every reduce key tagged with this host."""
        host = probe_local_ip()
        for _batch in batches:
            time.sleep(sleep_seconds)
            yield pa.RecordBatch.from_arrays(
                [pa.array(range(reduce_keys), type=pa.int64()), pa.array([host] * reduce_keys, type=pa.string())],
                names=["reduce_key", "map_host"])

    def reduce_side(batches):
        """One reduce task: collect the map hosts its rows came from, wait briefly, emit (map host, this host)."""
        host = probe_local_ip()
        seen = set()
        for batch in batches:
            seen.update(batch.column(batch.schema.get_field_index("map_host")).to_pylist())
        time.sleep(sleep_seconds)
        hosts = sorted(seen)
        yield pa.RecordBatch.from_arrays(
            [pa.array(hosts, type=pa.string()), pa.array([host] * len(hosts), type=pa.string())],
            names=["map_host", "reduce_host"])

    rows = (spark.range(0, tasks, 1, tasks)
            .mapInArrow(map_side, "reduce_key long, map_host string")
            .repartition(tasks, "reduce_key")
            .mapInArrow(reduce_side, "map_host string, reduce_host string")
            .collect())
    return {(row["map_host"], row["reduce_host"]) for row in rows}


def probe_registered_executors():
    """Executors Spark has registered right now (the driver excluded), or None when Spark cannot say. PySpark's
    StatusTracker has no executor list, so this asks the JVM tracker (available on this task's dedicated cluster)."""
    try:
        return len(spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()) - 1
    except Exception:
        return None


def probe_wait_for_executors(expected):
    """Wait up to PROBE_EXECUTOR_WAIT_SECONDS until Spark has registered `expected` executors. A task that starts the
    moment its cluster is RUNNING can otherwise run every probe task on the first worker to register (dev smoke
    2026-11-16: one host seen, the second worker not yet registered). Returns (registered, seconds waited)."""
    started = time.monotonic()
    registered = probe_registered_executors()
    while registered is not None and registered < expected             and time.monotonic() - started < PROBE_EXECUTOR_WAIT_SECONDS:
        time.sleep(5)
        registered = probe_registered_executors()
    return registered, round(time.monotonic() - started, 1)


def probe_verdict(pairs, expected_executors):
    """HEALTHY when the hosts seen on both sides are the same set, at least as many as the cluster's executors, and
    every ordered pair of distinct hosts exchanged data; otherwise INCONCLUSIVE. (SICK is decided by the caller:
    a round that times out or fails.)"""
    map_hosts = {map_host for map_host, _ in pairs}
    reduce_hosts = {reduce_host for _, reduce_host in pairs}
    hosts = map_hosts | reduce_hosts
    expected_pairs = {(a, b) for a in hosts for b in hosts if a != b}
    missing_pairs = sorted(expected_pairs - set(pairs))
    complete = map_hosts == reduce_hosts and len(hosts) >= max(1, expected_executors)
    return {"status": "HEALTHY" if complete and not missing_pairs else "INCONCLUSIVE",
            "hosts": sorted(hosts), "expected_executors": expected_executors,
            "pairs_seen": len(expected_pairs) - len(missing_pairs), "pairs_expected": len(expected_pairs),
            "missing_pairs": missing_pairs[:10]}


def probe_classic_context():
    """The SparkContext on a classic cluster (this task's dedicated job cluster), or None on Spark Connect."""
    try:
        return spark.sparkContext
    except Exception:
        return None


def probe_tag_jobs(tag):
    """Tag every Spark job this thread starts, so the round can be cancelled as a whole. On a classic cluster the
    SparkContext job tag is used: dev smoke 2026-11-16 showed spark.interruptTag leaving a classic job running."""
    context = probe_classic_context()
    if context is not None:
        context.addJobTag(tag)
    else:
        spark.addTag(tag)


def probe_untag_jobs(tag):
    """Stop tagging this thread's jobs (the round is over; nothing later in the session carries the tag)."""
    context = probe_classic_context()
    if context is not None:
        context.removeJobTag(tag)
    else:
        spark.removeTag(tag)


def probe_cancel_jobs(tag):
    """Cancel every running Spark job carrying the tag."""
    context = probe_classic_context()
    if context is not None:
        context.cancelJobsWithTag(tag)
    else:
        spark.interruptTag(tag)


def probe_round_with_deadline(round_number, tasks, deadline_seconds):
    """Run one probe round on a thread; return ("done", pairs), ("error", exception) or ("timeout", None).
    A round still running at the deadline has its Spark jobs cancelled."""
    import threading
    import uuid
    tag = f"bfix_shuffle_probe_{round_number}_{uuid.uuid4().hex[:8]}"
    outcome = {}

    def run_round():
        """The round itself, on its own thread so the caller can stop waiting."""
        probe_tag_jobs(tag)
        try:
            outcome["pairs"] = probe_shuffle_pairs(tasks)
        except Exception as error:
            outcome["error"] = error
        finally:
            probe_untag_jobs(tag)

    thread = threading.Thread(target=run_round, name=tag, daemon=True)
    thread.start()
    thread.join(deadline_seconds)
    if thread.is_alive():
        probe_cancel_jobs(tag)
        thread.join(30)
        return "timeout", None
    if "error" in outcome:
        return "error", outcome["error"]
    return "done", outcome["pairs"]


def assert_workers_can_shuffle():
    """Run up to PROBE_ROUNDS probe rounds; return the verdict, or raise SICK_CLUSTER (see BFIX_SHUFFLE_PROBE_V1)."""
    shape = probe_cluster_shape()
    if shape is None:
        probe_report("SKIPPED", reason="no job cluster (serverless or unknown)")
        return {"status": "SKIPPED"}
    cluster_id, executors, tasks = shape
    registered, waited = probe_wait_for_executors(executors)
    probe_report("EXECUTORS", cluster_id=cluster_id, expected=executors, registered=registered, waited_seconds=waited)
    # AQE could merge the probe's small reduce partitions into one task, which would hide the cross-worker
    # fetches; it is switched off for the probe only and restored after.
    previous_coalesce = spark.conf.get(PROBE_COALESCE_CONF, None)
    spark.conf.set(PROBE_COALESCE_CONF, "false")
    try:
        verdict = None
        for round_number in range(1, PROBE_ROUNDS + 1):
            round_started = time.monotonic()
            state, value = probe_round_with_deadline(round_number, tasks, PROBE_ROUND_SECONDS)
            seconds = round(time.monotonic() - round_started, 1)
            if state != "done":
                probe_report("SICK", cluster_id=cluster_id, round=round_number, state=state, seconds=seconds,
                             error=repr(value)[:1000] if value is not None else None)
                raise RuntimeError(
                    f"SICK_CLUSTER: worker shuffle probe round {round_number} {state} after {seconds} s on cluster "
                    f"{cluster_id} (BFIX_SHUFFLE_PROBE_V1); the task's retry starts on new machines") from (
                        value if isinstance(value, BaseException) else None)
            verdict = probe_verdict(value, executors)
            probe_report("ROUND", cluster_id=cluster_id, round=round_number, seconds=seconds, tasks=tasks, **verdict)
            if verdict["status"] == "HEALTHY":
                return verdict
            if round_number < PROBE_ROUNDS:
                time.sleep(PROBE_ROUND_PAUSE_SECONDS)
        probe_report("INCONCLUSIVE", cluster_id=cluster_id, rounds=PROBE_ROUNDS, **verdict)
        return verdict
    finally:
        if previous_coalesce is None:
            spark.conf.unset(PROBE_COALESCE_CONF)
        else:
            spark.conf.set(PROBE_COALESCE_CONF, previous_coalesce)


shuffle_probe = assert_workers_can_shuffle()

started = time.monotonic()
target_schema = value("target_schema", "8_dev.bronze")
allow_production_write = flag("allow_production_write")
if "." not in target_schema:
    raise ValueError(f"target_schema must be catalog.schema, got {target_schema!r}")
if not target_schema.startswith("8_dev.") and not allow_production_write:
    raise PermissionError("refusing non-dev target without allow_production_write=true")

register_runtime_artifact(spark)
lookup_schema = bronze_lookup_schema(target_schema)
config = PipelineConfig(bronze_schema=target_schema, lookup_schema=lookup_schema)

force_full_refresh = flag("force_full_refresh")
if force_full_refresh:
    spark.sql(
        f"DELETE FROM {target_schema}.pathology_expansion_state "
        "WHERE source_name IN ('map_pathology','path_patient_samplelevel',"
        "'mill_order_detail','specimen_map','path_master_orderables')"
    )

core = run_incremental_core(spark, config, validate_stage_keys=False)
periodic = periodic_lane_state(force_full_refresh)

# GEN4_LANE_V1: genetics runs on every run. Incremental by report text hash and parser/enrich versions;
# a forced full refresh re-parses everything. Its validation gates fail the task on any ERROR.
if spark.table(f"{lookup_schema}.pathology_hgnc_alias").limit(1).count() > 0:
    genetics = run_genetics(
        spark,
        config,
        include_proposed_profiles=False,
        mode="full" if force_full_refresh else "incremental",
        validate_stage_keys=False,
    )
    genetics = {**genetics, "validation_run_id": run_genetics_validation(spark, config)}
else:
    genetics = "SKIPPED_NO_HGNC"

if periodic["enabled"]:
    amr = run_amr(
        spark,
        config,
        include_proposed_rules=True,
        full_reconcile=True,
        validate_stage_keys=False,
    )
else:
    skipped = {
        "status": "SKIPPED_PERIODIC_CADENCE",
        "cadence": periodic["cadence"],
        "iso_week": periodic["iso_week"],
    }
    amr = skipped
amr_lane = amr_lane_state(periodic, target_schema)
if amr_lane["enabled"] and not periodic["enabled"]:
    amr = run_amr(
        spark,
        config,
        include_proposed_rules=True,
        full_reconcile=True,
        validate_stage_keys=False,
    )
if amr_lane["enabled"] and "status" not in amr:
    spark.sql(
        f"ALTER TABLE {target_schema}.map_pathology_microbiology_isolate SET TBLPROPERTIES "
        f"('{SPECIMEN_MAP_VERSION_PROPERTY}' = '{amr_lane['specimen_map_version']}')"
    )
indications_lane = indications_lane_state(periodic, target_schema)
if indications_lane["enabled"]:
    # Forced and periodic runs reconcile in full; otherwise only the accessions whose inputs changed since the
    # versions stored on the target (changed_scope falls back to full when that is not safe).
    scope, scope_stats = None, {"reason": "full: forced or periodic run"}
    if not force_full_refresh and not periodic["enabled"]:
        diagnosis_from = indications_lane["last_map_diagnosis_version"]
        accession_from = indications_lane["last_accession_source_version"]
        scope, scope_stats = changed_scope(
            spark, config,
            (int(diagnosis_from) + 1, indications_lane["map_diagnosis_version"]) if diagnosis_from is not None else None,
            (int(accession_from) + 1, indications_lane["accession_source_version"]) if accession_from is not None else None,
        )
    print("[INDICATIONS] scope:", scope_stats)
    indications = run_indications(
        spark,
        config,
        include_proposed_rules=True,
        include_diagnosis_context=True,
        full_reconcile=True,
        validate_stage_keys=False,
        accession_scope=scope,
    )
    indications = {**indications, "scope": scope_stats}
    spark.sql(
        f"ALTER TABLE {target_schema}.map_pathology_diagnosis_context SET TBLPROPERTIES "  # CONTEXT_LINK_V1
        f"('{INDICATIONS_VERSION_PROPERTY}' = '{indications_lane['map_diagnosis_version']}', "
        f"'{INDICATIONS_ACCESSION_VERSION_PROPERTY}' = '{indications_lane['accession_source_version']}')"
    )
else:
    indications = {"status": "SKIPPED_DIAGNOSIS_UNCHANGED", **indications_lane}
create_views(spark, config)

result = {
    "status": "SUCCESS",
    "pipeline_run_id": value("pipeline_run_id"),
    "weekly_runner_version": WEEKLY_RUNNER_VERSION,
    "target_schema": target_schema,
    "lookup_schema": lookup_schema,
    "mode": core["mode"],
    "run_id": core["run_id"],
    "core": core.get("metrics", {}),
    "periodic_lanes": periodic,
    "indications_lane": indications_lane,
    "amr_lane": amr_lane,
    "genetics": genetics,
    "indications": indications,
    "amr": amr,
    "elapsed_seconds": round(time.monotonic() - started, 3),
    "shuffle_probe": shuffle_probe,     # BFIX_SHUFFLE_PROBE_V1
    "doctrine_reference": "no-curation doctrine 2026-08-13, master plan v2.4",
}
dbutils.notebook.exit(json.dumps(result, sort_keys=True, default=str))
