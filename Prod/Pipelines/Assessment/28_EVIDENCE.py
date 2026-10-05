# Databricks notebook source
import hashlib
import json

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("run_open_ts", "")
RUN = dbutils.widgets.get("run_id")
RUN_OPEN_TS = dbutils.widgets.get("run_open_ts")
SESSION = "DQ4"
LANE = "omop_evidence"
assert RUN.startswith("dq4_omop_")
assert RUN_OPEN_TS

ISSUE_FIELDS = (
    "issue_id", "title", "target_table", "target_column", "lane_qualifier",
    "severity_tier", "g1_rule_id", "g1_action", "prevalence",
)

def normalize_lineage(issue, lineage):
    target = issue.get("target_table") or ""
    if not target.startswith("4_prod.omop_silver.") or not isinstance(lineage, dict):
        return lineage
    out = dict(lineage)
    out.setdefault("omop_table", target.rsplit(".", 1)[-1])
    out.setdefault("lineage_keys", sorted(out.get("silver_products") or []))
    return out

def core_json(issue, check, exemplars, lineage, contexts):
    return json.dumps(
        {
            "check_id": check.get("check_id"),
            "check_params": check.get("params"),
            "issue": {k: issue.get(k) for k in ISSUE_FIELDS},
            "exemplars": exemplars,
            "lineage": normalize_lineage(issue, lineage),
            "contexts": sorted(
                ({"context_id": c["context_id"], "fact": c["fact"]}
                 for c in contexts),
                key=lambda c: c["context_id"],
            ),
        },
        sort_keys=True,
        default=str,
    )

def qs(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"

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

def arr(values):
    return "array(" + ",".join(qs(x) for x in values) + ")"

bundle_table = f"6_mgmt.silver_qc_tmp.dq4_bundle_{RUN}"
bundle_rows = spark.sql(f"""
SELECT issue_id, issue_json, check_json, lineage_json, context_ids
FROM {bundle_table}
ORDER BY issue_id
""").collect()
assert bundle_rows, "no current OMOP issue bundles"

all_context_ids = sorted({x for row in bundle_rows for x in (row["context_ids"] or [])})
context_by_id = {}
if all_context_ids:
    ids = ",".join(qs(x) for x in all_context_ids)
    for row in spark.sql(f"""
      SELECT context_id,fact FROM 6_mgmt.silver_qc.dq_known_context
      WHERE context_id IN ({ids})
    """).collect():
        context_by_id[row["context_id"]] = {"context_id": row["context_id"], "fact": row["fact"]}

existing = {
    (row["issue_id"], row["evidence_hash"])
    for row in spark.sql("""
      SELECT issue_id,evidence_hash
      FROM 6_mgmt.silver_qc.dq_evidence_exemplar
    """).collect()
}

evidence_map = []
new_rows = []
for row in bundle_rows:
    issue = json.loads(row["issue_json"])
    check = json.loads(row["check_json"])
    lineage = json.loads(row["lineage_json"] or "{}")
    context_ids = list(row["context_ids"] or [])
    contexts = [context_by_id[x] for x in context_ids if x in context_by_id]
    exemplars = []
    digest = hashlib.sha256(
        core_json(issue, check, exemplars, lineage, contexts).encode()
    ).hexdigest()
    evidence_map.append((row["issue_id"], digest))
    if (row["issue_id"], digest) not in existing:
        new_rows.append((
            row["issue_id"], digest, json.dumps(exemplars, sort_keys=True),
            json.dumps(lineage, sort_keys=True), context_ids
        ))

scratch = f"6_mgmt.silver_qc_tmp.dq4_ehash_{RUN}"
execute("create_ehash.sql", f"""CREATE OR REPLACE TABLE {scratch}
(issue_id STRING, evidence_hash STRING)""")
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
MERGE INTO 6_mgmt.silver_qc.dq_issue t
USING {scratch} s ON t.issue_id=s.issue_id
WHEN MATCHED THEN UPDATE SET
  t.evidence_hash=s.evidence_hash,
  t.updated_at=current_timestamp()
""")

gate = spark.sql(f"""
SELECT
  (SELECT count(*) FROM {bundle_table}) AS current_issues,
  (SELECT count(*) FROM {scratch}) AS mapped,
  (SELECT count(*) FROM {bundle_table} b
   LEFT ANTI JOIN {scratch} h ON h.issue_id=b.issue_id) AS missing_map,
  (SELECT count(*) FROM {scratch} h
   LEFT ANTI JOIN 6_mgmt.silver_qc.dq_evidence_exemplar e
     ON e.issue_id=h.issue_id AND e.evidence_hash=h.evidence_hash) AS missing_evidence,
  (SELECT count(*) FROM (
     SELECT issue_id,evidence_hash,count(*) n
     FROM 6_mgmt.silver_qc.dq_evidence_exemplar
     GROUP BY issue_id,evidence_hash HAVING count(*)>1)) AS duplicate_pairs
""").first().asDict()
assert gate["current_issues"] == gate["mapped"], gate
assert gate["missing_map"] == 0 and gate["missing_evidence"] == 0, gate
assert gate["duplicate_pairs"] == 0, gate
print({"gate": gate, "new_evidence_rows": len(new_rows)})

