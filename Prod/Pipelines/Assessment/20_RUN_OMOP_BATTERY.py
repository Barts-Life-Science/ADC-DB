# Databricks notebook source
import hashlib
import json
from datetime import timezone

spark.conf.set("spark.sql.session.timeZone", "UTC")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("child_timeout_seconds", "21600")
requested = dbutils.widgets.get("run_id").strip()
timeout_seconds = int(dbutils.widgets.get("child_timeout_seconds"))
ROOT = "/Workspace/Shared/ADC-DB/Prod/Pipelines/Assessment"  # OGR_NO_DEV_V1
OMOP_PIPELINE_ID = "4bd3f582-0551-4c75-9b99-0031207f3417"
SILVER_PIPELINE_ID = "061f9450-ec36-4c3d-b8fc-8b6bcbcc397a"
VOCAB = {
    # 4_prod.omop vocab tables are now SELECT * views over 3_lookup.omop; pin the underlying Delta tables.
    "vocab:concept": "3_lookup.omop.concept",
    "vocab:concept_relationship": "3_lookup.omop.concept_relationship",
    "vocab:concept_ancestor": "3_lookup.omop.concept_ancestor",
    "vocab:concept_class": "3_lookup.omop.concept_class",
    "vocab:concept_synonym": "3_lookup.omop.concept_synonym",
    "vocab:relationship": "3_lookup.omop.relationship",
    "vocab:domain": "3_lookup.omop.domain",
    "vocab:vocabulary": "3_lookup.omop.vocabulary",
    "vocab:drug_strength": "3_lookup.omop.drug_strength",
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
    pins = {
        "omop_cdm:pipeline_update": latest_completed_pipeline(OMOP_PIPELINE_ID),
        "silver:pipeline_update": latest_completed_pipeline(SILVER_PIPELINE_ID),
        "omop_silver:schema_last_altered": schema_fingerprint("omop_silver"),
    }
    for key, table in VOCAB.items():
        pins[key] = str(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])
    pins["vocab:version"] = str(spark.sql("SELECT vocabulary_version FROM 4_prod.omop_silver.cdm_source LIMIT 1").first()[0])
    return pins

now = spark.sql("SELECT current_timestamp() ts").first()["ts"]
RUN_OPEN_TS = iso_millis(now)
RUN = requested or ("dq4_omop_" + now.strftime("%Y%m%d_%H%M%S"))
assert RUN.startswith("dq4_omop_") and RUN.replace("_", "").isalnum(), RUN
assert spark.sql(f"SELECT count(*) n FROM 6_mgmt.silver_qc.dq_run WHERE run_id={qs(RUN)}").first().n == 0, f"run_id already exists: {RUN}"
other_open = spark.sql("SELECT count(*) n FROM 6_mgmt.silver_qc.dq_run WHERE run_id LIKE 'dq4_omop_%' AND finished_at IS NULL").first().n
assert other_open == 0, "another DQ4 OMOP assessment is open; shared Achilles scratch cannot be used concurrently"
open_pins = collect_pins()
source_pins = dict(open_pins)
source_pins["run_open_ts"] = RUN_OPEN_TS
scratch_prefix = "b" + hashlib.sha256(RUN.encode()).hexdigest()[:8]
spark.sql(f"""
INSERT INTO 6_mgmt.silver_qc.dq_run
(run_id,started_at,finished_at,scope,source_pins,pin_bracket_valid,tool_versions,notes,created_by_session)
VALUES (
 {qs(RUN)},CAST({qs(RUN_OPEN_TS)} AS TIMESTAMP),NULL,
 'DQ4 OMOP battery: DQD + Achilles + Heel + Themis over the frozen post-O4 scope',
 {map_sql(source_pins)},NULL,
 map('dqd','2.8.9','sqlrender','1.19.6','achilles','1.8@113433da2ce33ec26463a49f45278ba297a75987','heel_rules','achilles v1.6.3'),
 'Frozen accepted post-O4 DQ4 assessment specification. Refuses to open while either source pipeline is non-terminal.',
 'DQ4')
""")
args = {
    "run_id": RUN,
    "run_open_ts": RUN_OPEN_TS,
    "source_update_id": open_pins["omop_cdm:pipeline_update"],
    "silver_update_id": open_pins["silver:pipeline_update"],
    "scratch_prefix": scratch_prefix,
}
steps = [
    "21_DQD",
    "22_ACHILLES",
    "23_ACHILLES_METADATA",
    "24_ACHILLES_RECONCILE",
    "25_THEMIS",
    "26_HEEL",
    "27_ISSUEIFY",
    "28_EVIDENCE",
]
try:
    for step in steps:
        print(f"START {step}", flush=True)
        result = dbutils.notebook.run(ROOT + "/" + step, timeout_seconds, args)
        print(f"DONE {step}: {result}", flush=True)
    dqd = spark.sql(f"SELECT count(*) n,count(DISTINCT check_id) d FROM 6_mgmt.silver_qc.v_dqd_canonical WHERE run_id={qs(RUN)}").first()
    ach = spark.sql(f"SELECT count(*) n,count(DISTINCT analysis_id) d FROM 6_mgmt.silver_qc.achilles_results WHERE run_id={qs(RUN)}").first()
    meta = spark.sql(f"SELECT count(*) n FROM 6_mgmt.silver_qc.achilles_analysis WHERE run_id={qs(RUN)}").first().n
    heel = spark.sql(f"SELECT count(*) n FROM 6_mgmt.silver_qc.heel_rule_disposition WHERE run_id={qs(RUN)}").first().n
    themis = spark.sql(f"""
      SELECT count(*) n FROM 6_mgmt.silver_qc.dq_check_result r
      JOIN 6_mgmt.silver_qc.dq_check c ON c.check_id=r.check_id
      WHERE r.run_id={qs(RUN)} AND c.rule_source='themis'
    """).first().n
    assert dqd.n == dqd.d and dqd.n > 0, dqd
    assert ach.n > 0 and ach.d > 0 and meta == 218, (ach, meta)
    assert heel == 41 and themis == 21, (heel, themis)
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
    dbutils.notebook.run(ROOT + "/29_OMOP_CLEANUP", 3600, {"run_id": RUN, "scratch_prefix": scratch_prefix})
    close_pins = collect_pins()
    valid = close_pins == open_pins
    finished = iso_millis(spark.sql("SELECT current_timestamp() ts").first()["ts"])
    note = f" DQD={dqd.n}; Achilles rows={ach.n}; Heel rules={heel}; Themis={themis}; evidence complete. Pin bracket valid={valid}."
    spark.sql(f"""UPDATE 6_mgmt.silver_qc.dq_run
      SET finished_at=CAST({qs(finished)} AS TIMESTAMP),
          pin_bracket_valid={str(valid).lower()},
          notes=concat(coalesce(notes,''),{qs(note)})
      WHERE run_id={qs(RUN)}""")
    assert valid, {"opening": open_pins, "closing": close_pins}
except Exception as exc:
    try:
        dbutils.notebook.run(ROOT + "/29_OMOP_CLEANUP", 3600, {"run_id": RUN, "scratch_prefix": scratch_prefix})
    except Exception as cleanup_exc:
        print(f"Best-effort cleanup failed: {cleanup_exc}", flush=True)
    finished = iso_millis(spark.sql("SELECT current_timestamp() ts").first()["ts"])
    note = " FAILED: " + str(exc)[:2500]
    spark.sql(f"""UPDATE 6_mgmt.silver_qc.dq_run
      SET finished_at=CAST({qs(finished)} AS TIMESTAMP),
          pin_bracket_valid=false,
          notes=concat(coalesce(notes,''),{qs(note)})
      WHERE run_id={qs(RUN)}""")
    raise
dbutils.notebook.exit(json.dumps({
    "run_id": RUN, "run_open_ts": RUN_OPEN_TS,
    "dqd_checks": dqd.n, "achilles_rows": ach.n,
    "heel_rules": heel, "themis_checks": themis,
    "pin_bracket_valid": True,
}, default=str))

