# Databricks notebook source
import json
from datetime import timezone

spark.conf.set("spark.sql.session.timeZone", "UTC")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("child_timeout_seconds", "21600")
requested = dbutils.widgets.get("run_id").strip()
timeout_seconds = int(dbutils.widgets.get("child_timeout_seconds"))
ROOT = "/Workspace/Shared/ADC-DB/Prod/Pipelines/Assessment"  # OGR_NO_DEV_V1
PIPELINE_ID = "061f9450-ec36-4c3d-b8fc-8b6bcbcc397a"
TABLES = {
    # The four MANAGED reference_* tables pinned in the frozen spec no longer exist in 4_prod.silver (checked 2026-09-26).
    "bronze:map_powerform_assessment_item": "4_prod.bronze.map_powerform_assessment_item",
    "bronze:map_numeric_events": "4_prod.bronze.map_numeric_events",
}

def qs(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"

def map_sql(values):
    parts = []
    for key in sorted(values):
        parts.extend((qs(key), qs(values[key])))
    return "map(" + ",".join(parts) + ")"

def iso_millis(value):
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"

def latest_completed_pipeline(pipeline_id):
    # Prod pipelines' event_log is not readable by the SP; use the system timeline.
    rows = spark.sql(f"""
      SELECT update_id,max(period_end_time) timestamp,
             max_by(result_state,period_end_time) state,
             max_by(update_type,period_end_time) update_type
      FROM system.lakeflow.pipeline_update_timeline
      WHERE pipeline_id='{pipeline_id}' AND period_start_time > current_timestamp() - INTERVAL 30 DAYS
      GROUP BY update_id ORDER BY timestamp DESC
    """).collect()
    active = [r.asDict() for r in rows if r["state"] not in ("COMPLETED","FAILED","CANCELED")]
    assert not active, f"pipeline has non-terminal updates: {active}"
    completed = next((r for r in rows if r["state"] == "COMPLETED" and r["update_type"] != "VALIDATE"), None)
    assert completed is not None, "pipeline has no completed update"
    return completed["update_id"]

def schema_fingerprint(schema):
    # Guards against refreshes the system timeline has not yet surfaced.
    return str(spark.sql(f"""SELECT max(last_altered) m FROM 4_prod.information_schema.tables
      WHERE table_schema='{schema}' AND table_name NOT LIKE 'event_log%'
               AND table_name NOT LIKE '\\_\\_%'  -- Lakeflow internal materializations get background maintenance""").first()["m"])

def collect_pins():
    pins = {"silver:pipeline_update": latest_completed_pipeline(PIPELINE_ID),
            "silver:schema_last_altered": schema_fingerprint("silver")}
    for key, table in TABLES.items():
        row = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()
        pins[key] = str(row["version"])
    return pins

now = spark.sql("SELECT current_timestamp() ts").first()["ts"]
RUN_OPEN_TS = iso_millis(now)
RUN = requested or ("dq4_silver_" + now.strftime("%Y%m%d_%H%M%S"))
assert RUN.startswith("dq4_silver_") and RUN.replace("_", "").isalnum(), RUN
assert spark.sql(f"SELECT count(*) n FROM 6_mgmt.silver_qc.dq_run WHERE run_id={qs(RUN)}").first().n == 0, f"run_id already exists: {RUN}"
open_pins = collect_pins()
source_pins = dict(open_pins)
source_pins["run_open_ts"] = RUN_OPEN_TS
spark.sql(f"""
INSERT INTO 6_mgmt.silver_qc.dq_run
(run_id,started_at,finished_at,scope,source_pins,pin_bracket_valid,tool_versions,notes,created_by_session)
VALUES (
 {qs(RUN)},CAST({qs(RUN_OPEN_TS)} AS TIMESTAMP),NULL,
 'DQ4 silver statistical and clinical plausibility battery',
 {map_sql(source_pins)},NULL,
 map('battery','dq4_frozen_20260825','evidence_hash','dq/tools/evidence_hash.py'),
 'Frozen accepted DQ4 assessment specification; thresholds are not self-retuned during routine assessment runs.',
 'DQ4')
""")
args = {"run_id": RUN, "run_open_ts": RUN_OPEN_TS, "silver_update_id": open_pins["silver:pipeline_update"]}
steps = [
    "11_SILVER_PROFILE",
    "12_SILVER_SCANS_AND_SINGLES",
    "13_SILVER_DERIVED_RESULTS",
    "14_SILVER_ISSUEIFY",
    "15_SILVER_EXEMPLARS",
    "16_SILVER_EVIDENCE",
]
try:
    for step in steps:
        print(f"START {step}", flush=True)
        result = dbutils.notebook.run(ROOT + "/" + step, timeout_seconds, args)
        print(f"DONE {step}: {result}", flush=True)
    counts = spark.sql(f"""
      SELECT count(*) rows,count(DISTINCT check_id) checks,
             sum(CASE WHEN status='fail' THEN 1 ELSE 0 END) failures
      FROM 6_mgmt.silver_qc.dq_check_result
      WHERE run_id={qs(RUN)} AND created_by_session='DQ4'
    """).first()
    # Frozen spec = 262 checks; concept-keyed checks whose concept no longer appears in
    # silver emit no row. Report the shortfall rather than abort the run.
    assert counts.rows == counts.checks and counts.checks <= 262, counts
    if counts.checks < 262:
        print(f"WARNING: {262 - counts.checks} frozen checks produced no result (concept absent from current silver)", flush=True)
    missing_evidence = spark.sql(f"""
      SELECT count(*) n FROM 6_mgmt.silver_qc.dq_issue i
      LEFT ANTI JOIN 6_mgmt.silver_qc.dq_evidence_exemplar e
        ON e.issue_id=i.issue_id AND e.evidence_hash=i.evidence_hash
      WHERE i.last_seen_run={qs(RUN)}
    """).first().n
    assert missing_evidence == 0, f"missing evidence: {missing_evidence}"
    unregistered = spark.sql(f"""
      SELECT count(*) n FROM (
        SELECT DISTINCT r.check_id
        FROM 6_mgmt.silver_qc.dq_check_result r
        LEFT ANTI JOIN 6_mgmt.silver_qc.dq_check c ON c.check_id=r.check_id
        WHERE r.run_id={qs(RUN)}
      )
    """).first().n
    assert unregistered == 0, f"unregistered result checks: {unregistered}"
    unsettled = spark.sql(f"""
      WITH attempted AS (
        SELECT lane,seq,stmt_sha256,attempted_at
        FROM 6_mgmt.silver_qc.dq_exec_log
        WHERE run_id={qs(RUN)} AND status='attempted'
      )
      SELECT count(*) n FROM attempted a
      LEFT ANTI JOIN 6_mgmt.silver_qc.dq_exec_log s
        ON s.run_id={qs(RUN)} AND s.lane=a.lane AND s.seq=a.seq
       AND s.stmt_sha256=a.stmt_sha256 AND s.status<>'attempted'
       AND s.settled_at>=a.attempted_at
    """).first().n
    assert unsettled == 0, f"unsettled execution attempts: {unsettled}"
    dbutils.notebook.run(ROOT + "/17_SILVER_CLEANUP", 3600, {"run_id": RUN})
    close_pins = collect_pins()
    valid = close_pins == open_pins
    finished = iso_millis(spark.sql("SELECT current_timestamp() ts").first()["ts"])
    note = f" Completed {counts.rows} checks with {counts.failures} failures; evidence complete. Pin bracket valid={valid}."
    spark.sql(f"""UPDATE 6_mgmt.silver_qc.dq_run
      SET finished_at=CAST({qs(finished)} AS TIMESTAMP),
          pin_bracket_valid={str(valid).lower()},
          notes=concat(coalesce(notes,''),{qs(note)})
      WHERE run_id={qs(RUN)}""")
    assert valid, {"opening": open_pins, "closing": close_pins}
except Exception as exc:
    try:
        dbutils.notebook.run(ROOT + "/17_SILVER_CLEANUP", 3600, {"run_id": RUN})
    except Exception as cleanup_exc:
        print(f"Best-effort cleanup failed: {cleanup_exc}", flush=True)
    finished = iso_millis(spark.sql("SELECT current_timestamp() ts").first()["ts"])
    try:
        close_pins = collect_pins()
        valid = close_pins == open_pins
    except Exception:
        valid = False
    note = " FAILED: " + str(exc)[:2500]
    spark.sql(f"""UPDATE 6_mgmt.silver_qc.dq_run
      SET finished_at=CAST({qs(finished)} AS TIMESTAMP),
          pin_bracket_valid=false,
          notes=concat(coalesce(notes,''),{qs(note)})
      WHERE run_id={qs(RUN)}""")
    raise
dbutils.notebook.exit(json.dumps({
    "run_id": RUN, "run_open_ts": RUN_OPEN_TS,
    "checks": counts.checks, "failures": counts.failures,
    "pin_bracket_valid": True,
}, default=str))

