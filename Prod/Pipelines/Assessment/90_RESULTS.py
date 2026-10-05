# Databricks notebook source
spark.conf.set("spark.sql.session.timeZone", "UTC")
dbutils.widgets.text("silver_run_id", "")
dbutils.widgets.text("omop_run_id", "")
silver_run = dbutils.widgets.get("silver_run_id").strip()
omop_run = dbutils.widgets.get("omop_run_id").strip()

def qs(value):
    return "'" + str(value).replace("'", "''") + "'"

if not silver_run:
    row = spark.sql("""
      SELECT run_id FROM 6_mgmt.silver_qc.dq_run
      WHERE run_id LIKE 'dq4_silver_%' AND pin_bracket_valid=true
      ORDER BY finished_at DESC LIMIT 1
    """).first()
    silver_run = row["run_id"] if row else ""
if not omop_run:
    row = spark.sql("""
      SELECT run_id FROM 6_mgmt.silver_qc.dq_run
      WHERE run_id LIKE 'dq4_omop_%' AND pin_bracket_valid=true
      ORDER BY finished_at DESC LIMIT 1
    """).first()
    omop_run = row["run_id"] if row else ""
assert silver_run and omop_run, "provide run IDs or complete at least one valid silver and OMOP run"
print({"silver_run_id": silver_run, "omop_run_id": omop_run})

display(spark.sql(f"""
SELECT run_id,started_at,finished_at,
       round((unix_timestamp(finished_at)-unix_timestamp(started_at))/60.0,1) elapsed_minutes,
       pin_bracket_valid,scope,source_pins,tool_versions,notes
FROM 6_mgmt.silver_qc.dq_run
WHERE run_id IN ({qs(silver_run)},{qs(omop_run)})
ORDER BY started_at
"""))

display(spark.sql(f"""
SELECT c.family,r.status,count(*) checks,
       sum(r.measured_rows) measured_rows,
       sum(r.total_rows) total_rows
FROM 6_mgmt.silver_qc.dq_check_result r
JOIN 6_mgmt.silver_qc.dq_check c ON c.check_id=r.check_id
WHERE r.run_id={qs(silver_run)} AND r.created_by_session='DQ4'
GROUP BY c.family,r.status
ORDER BY c.family,r.status
"""))

display(spark.sql(f"""
SELECT 'DQD checks' metric,count(*) value,count(DISTINCT check_id) secondary
FROM 6_mgmt.silver_qc.v_dqd_canonical WHERE run_id={qs(omop_run)}
UNION ALL
SELECT 'DQD failures',sum(CASE WHEN failed=1 THEN 1 ELSE 0 END),sum(CASE WHEN passed=1 THEN 1 ELSE 0 END)
FROM 6_mgmt.silver_qc.v_dqd_canonical WHERE run_id={qs(omop_run)}
UNION ALL
SELECT 'Achilles regular',count(*),count(DISTINCT analysis_id)
FROM 6_mgmt.silver_qc.achilles_results WHERE run_id={qs(omop_run)}
UNION ALL
SELECT 'Achilles distributions',count(*),count(DISTINCT analysis_id)
FROM 6_mgmt.silver_qc.achilles_results_dist WHERE run_id={qs(omop_run)}
UNION ALL
SELECT 'Achilles metadata',count(*),count(DISTINCT analysis_id)
FROM 6_mgmt.silver_qc.achilles_analysis WHERE run_id={qs(omop_run)}
UNION ALL
SELECT 'Heel warnings',count(*),count(DISTINCT rule_id)
FROM 6_mgmt.silver_qc.heel_results WHERE run_id={qs(omop_run)}
UNION ALL
SELECT 'Themis checks',count(*),sum(CASE WHEN r.status='fail' THEN 1 ELSE 0 END)
FROM 6_mgmt.silver_qc.dq_check_result r
JOIN 6_mgmt.silver_qc.dq_check c ON c.check_id=r.check_id
WHERE r.run_id={qs(omop_run)} AND c.rule_source='themis'
"""))

display(spark.sql(f"""
SELECT last_seen_run run_id,c.engine,c.family,i.severity_tier,i.status,count(*) issues,
       sum(i.prevalence.measured_rows) measured_rows
FROM 6_mgmt.silver_qc.dq_issue i
JOIN 6_mgmt.silver_qc.dq_check c ON c.check_id=i.check_id
WHERE i.last_seen_run IN ({qs(silver_run)},{qs(omop_run)})
GROUP BY last_seen_run,c.engine,c.family,i.severity_tier,i.status
ORDER BY run_id,severity_tier,c.engine,c.family,i.status
"""))

display(spark.sql(f"""
SELECT i.last_seen_run run_id,i.severity_tier,c.engine,c.family,i.title,
       i.target_table,i.target_column,i.lane_qualifier,
       i.prevalence.measured_rows,i.prevalence.total_rows,i.status,i.issue_id
FROM 6_mgmt.silver_qc.dq_issue i
JOIN 6_mgmt.silver_qc.dq_check c ON c.check_id=i.check_id
WHERE i.last_seen_run IN ({qs(silver_run)},{qs(omop_run)})
ORDER BY CASE i.severity_tier WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 ELSE 3 END,
         i.prevalence.measured_rows DESC NULLS LAST
LIMIT 200
"""))

display(spark.sql(f"""
WITH attempts AS (
  SELECT run_id,lane,seq,stmt_sha256,attempted_at
  FROM 6_mgmt.silver_qc.dq_exec_log
  WHERE run_id IN ({qs(silver_run)},{qs(omop_run)}) AND status='attempted'
), unsettled AS (
  SELECT a.* FROM attempts a
  LEFT ANTI JOIN 6_mgmt.silver_qc.dq_exec_log s
    ON s.run_id=a.run_id AND s.lane=a.lane AND s.seq=a.seq
   AND s.stmt_sha256=a.stmt_sha256 AND s.status<>'attempted'
   AND s.settled_at>=a.attempted_at
), unsettled_counts AS (
  SELECT run_id,count(*) n FROM unsettled GROUP BY run_id
)
SELECT r.run_id,r.pin_bracket_valid,
       count(DISTINCT i.issue_id) current_issues,
       count(DISTINCT CASE WHEN e.issue_id IS NULL THEN i.issue_id END) missing_evidence,
       coalesce(u.n,0) unsettled_statements
FROM 6_mgmt.silver_qc.dq_run r
LEFT JOIN 6_mgmt.silver_qc.dq_issue i ON i.last_seen_run=r.run_id
LEFT JOIN 6_mgmt.silver_qc.dq_evidence_exemplar e
  ON e.issue_id=i.issue_id AND e.evidence_hash=i.evidence_hash
LEFT JOIN unsettled_counts u ON u.run_id=r.run_id
WHERE r.run_id IN ({qs(silver_run)},{qs(omop_run)})
GROUP BY r.run_id,r.pin_bracket_valid,u.n
ORDER BY r.run_id
"""))

