# Databricks notebook source
import hashlib
import json

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("run_open_ts", "")
RUN = dbutils.widgets.get("run_id").strip()
RUN_OPEN_TS = dbutils.widgets.get("run_open_ts").strip()
assert RUN.startswith("dq4_silver_") and RUN.replace("_", "").isalnum(), RUN
assert RUN_OPEN_TS
LANE = "dq4_evidence"
SESSION = "DQ4"
DETAIL_ONLY = {"code_rate_shift", "digit_preference", "unit_mixture", "reconciliation"}
APPROXIMATE = {"numeric_range", "univariate_outlier", "sentinel_spike"}
ISSUE_FIELDS = (
    "issue_id", "title", "target_table", "target_column", "lane_qualifier",
    "severity_tier", "g1_rule_id", "g1_action", "prevalence",
)

def qs(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"

def arr(values):
    return "array(" + ",".join(qs(x) for x in values) + ")"

def core_json(issue, check, exemplars, lineage, contexts):
    return json.dumps({
        "check_id": check.get("check_id"),
        "check_params": check.get("params"),
        "issue": {k: issue.get(k) for k in ISSUE_FIELDS},
        "exemplars": exemplars,
        "lineage": lineage,
        "contexts": sorted(
            ({"context_id": c["context_id"], "fact": c["fact"]} for c in contexts),
            key=lambda c: c["context_id"],
        ),
    }, sort_keys=True, default=str)

seq = 0
def execute(name, sql):
    global seq
    sha = hashlib.sha256(sql.encode()).hexdigest()
    spark.sql(f"""INSERT INTO 6_mgmt.silver_qc.dq_exec_log VALUES
      ({qs(RUN)},{qs(LANE)},{qs(name)},{seq},{qs(sha)},'attempted',
       NULL,NULL,current_timestamp(),NULL,{qs(SESSION)})""")
    try:
        spark.sql(sql).collect()
        spark.sql(f"""INSERT INTO 6_mgmt.silver_qc.dq_exec_log VALUES
          ({qs(RUN)},{qs(LANE)},{qs(name)},{seq},{qs(sha)},'ok',
           NULL,NULL,current_timestamp(),current_timestamp(),{qs(SESSION)})""")
    except Exception as exc:
        msg = str(exc)[:4000]
        spark.sql(f"""INSERT INTO 6_mgmt.silver_qc.dq_exec_log VALUES
          ({qs(RUN)},{qs(LANE)},{qs(name)},{seq},{qs(sha)},'error',
           {qs(msg)},NULL,current_timestamp(),current_timestamp(),{qs(SESSION)})""")
        raise
    seq += 1

bundle_table = f"6_mgmt.silver_qc_tmp.dq4_bundle_{RUN}"
bundles = spark.sql(f"""
SELECT b.*, k.family
FROM {bundle_table} b
JOIN 6_mgmt.silver_qc.dq_check k
  ON k.check_id = get_json_object(b.check_json, '$.check_id')
ORDER BY b.issue_id
""").collect()
assert bundles, "no current silver issue bundles"

tables = {r.table_name for r in spark.sql("""
SELECT table_name FROM 6_mgmt.information_schema.tables
WHERE table_schema='silver_qc_tmp'
""").collect()}
existing = {
    (r.issue_id, r.evidence_hash)
    for r in spark.sql("SELECT issue_id,evidence_hash FROM 6_mgmt.silver_qc.dq_evidence_exemplar").collect()
}
evidence_map = []
new_rows = []
for row in bundles:
    issue = json.loads(row["issue_json"])
    check = json.loads(row["check_json"])
    detail = json.loads(row["detail_json"] or "{}")
    lineage = json.loads(row["lineage_json"] or "{}")
    contexts = json.loads(row["contexts_json"] or "[]")
    family = row["family"]
    product = (issue.get("target_table") or "").rsplit(".", 1)[-1]
    exemplars = []
    if family in DETAIL_ONLY:
        exemplars = [detail] if detail else []
    else:
        table = f"dq4_ex_{product}__{family}_{RUN}"
        if table in tables:
            rows = spark.sql(f"""
              SELECT kind,value_1,value_2,value_3,metric
              FROM 6_mgmt.silver_qc_tmp.`{table}`
              WHERE check_id={qs(check["check_id"])}
              ORDER BY metric DESC NULLS LAST,value_1 ASC NULLS LAST,
                       value_2 ASC NULLS LAST,value_3 ASC NULLS LAST
              LIMIT 20
            """).collect()
            exemplars = [r.asDict(recursive=True) for r in rows]
        if not exemplars and detail:
            exemplars = [detail]
        if family in APPROXIMATE:
            exemplars = [{"sampling": "TABLESAMPLE 1 PERCENT", "approximate_exemplars": True}] + exemplars
    digest = hashlib.sha256(core_json(issue, check, exemplars, lineage, contexts).encode()).hexdigest()
    evidence_map.append((row["issue_id"], digest))
    if (row["issue_id"], digest) not in existing:
        new_rows.append((
            row["issue_id"], digest, json.dumps(exemplars, sort_keys=True, default=str),
            json.dumps(lineage, sort_keys=True, default=str),
            list(row["context_ids"] or []),
        ))

scratch = f"6_mgmt.silver_qc_tmp.dq4_ehash_{RUN}"
execute("create_ehash.sql", f"CREATE OR REPLACE TABLE {scratch} (issue_id STRING, evidence_hash STRING)")
for start in range(0, len(evidence_map), 100):
    values = ",".join(f"({qs(i)},{qs(h)})" for i,h in evidence_map[start:start+100])
    execute(f"insert_ehash_{start//100:04d}.sql", f"INSERT INTO {scratch} VALUES {values}")
for start in range(0, len(new_rows), 20):
    values = ",".join(
        f"({qs(i)},{qs(h)},{qs(ex)},{qs(lin)},{arr(ctx)},CAST({qs(RUN_OPEN_TS)} AS TIMESTAMP),{qs(SESSION)})"
        for i,h,ex,lin,ctx in new_rows[start:start+20]
    )
    execute(f"merge_evidence_{start//20:04d}.sql", f"""
      MERGE INTO 6_mgmt.silver_qc.dq_evidence_exemplar t
      USING (SELECT * FROM VALUES {values}
             AS v(issue_id,evidence_hash,exemplar_json,lineage_json,context_ids,built_at,created_by_session)) s
      ON t.issue_id=s.issue_id AND t.evidence_hash=s.evidence_hash
      WHEN NOT MATCHED THEN INSERT *
    """)
execute("stamp_ehash.sql", f"""
MERGE INTO 6_mgmt.silver_qc.dq_issue t USING {scratch} s ON t.issue_id=s.issue_id
WHEN MATCHED THEN UPDATE SET t.evidence_hash=s.evidence_hash,t.updated_at=current_timestamp()
""")
gate = spark.sql(f"""
SELECT
  (SELECT count(*) FROM {bundle_table}) current_issues,
  (SELECT count(*) FROM {scratch}) mapped,
  (SELECT count(*) FROM {bundle_table} b LEFT ANTI JOIN {scratch} h ON h.issue_id=b.issue_id) missing_map,
  (SELECT count(*) FROM {scratch} h LEFT ANTI JOIN 6_mgmt.silver_qc.dq_evidence_exemplar e
     ON e.issue_id=h.issue_id AND e.evidence_hash=h.evidence_hash) missing_evidence,
  (SELECT count(*) FROM (
     SELECT issue_id,evidence_hash,count(*) n FROM 6_mgmt.silver_qc.dq_evidence_exemplar
     GROUP BY issue_id,evidence_hash HAVING count(*)>1)) duplicate_pairs
""").first().asDict()
assert gate["current_issues"] == gate["mapped"], gate
assert gate["missing_map"] == 0 and gate["missing_evidence"] == 0, gate
assert gate["duplicate_pairs"] == 0, gate
print({"gate": gate, "new_evidence_rows": len(new_rows)})

