# Databricks notebook source
import base64
import gzip
import hashlib
import io
import tarfile

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("run_open_ts", "")
dbutils.widgets.text("silver_update_id", "")
RUN = dbutils.widgets.get("run_id").strip()
RUN_OPEN_TS = dbutils.widgets.get("run_open_ts").strip()
SILVER_UPDATE_ID = dbutils.widgets.get("silver_update_id").strip()
assert RUN.startswith("dq4_silver_") and RUN.replace("_", "").isalnum(), RUN
assert RUN_OPEN_TS and SILVER_UPDATE_ID
LANE = "dq4_issueify"
PAYLOAD = """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"""

def qs(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"

def adapt(value):
    return (value
        .replace("dq4_silver_20260825", RUN)
        .replace("2026-08-25T10:50:49.897Z", RUN_OPEN_TS)
        .replace("f5c7c7ab-e37d-4a31-b9c2-b7631becb16a", SILVER_UPDATE_ID)
        .replace("CAST('2026-08-25' AS DATE)", f"CAST('{RUN_OPEN_TS[:10]}' AS DATE)"))

def unpack():
    archive = tarfile.open(fileobj=io.BytesIO(base64.b64decode(PAYLOAD)), mode="r:gz")
    items = []
    for member in archive.getmembers():
        if member.isfile() and member.name.endswith(".sql"):
            items.append((member.name, adapt(archive.extractfile(member).read().decode("utf-8"))))
    return sorted(items, key=lambda x: x[0])

def execute(seq, name, sql):
    sha = hashlib.sha256(sql.encode()).hexdigest()
    spark.sql(f"""INSERT INTO 6_mgmt.silver_qc.dq_exec_log
      (run_id,lane,stmt_file,seq,stmt_sha256,status,error_text,rows_affected,attempted_at,settled_at,created_by_session)
      VALUES ({qs(RUN)},{qs(LANE)},{qs(name)},{seq},{qs(sha)},'attempted',NULL,NULL,current_timestamp(),NULL,'DQ4')""")
    try:
        spark.sql(sql).collect()
        spark.sql(f"""INSERT INTO 6_mgmt.silver_qc.dq_exec_log
          (run_id,lane,stmt_file,seq,stmt_sha256,status,error_text,rows_affected,attempted_at,settled_at,created_by_session)
          VALUES ({qs(RUN)},{qs(LANE)},{qs(name)},{seq},{qs(sha)},'ok',NULL,NULL,current_timestamp(),current_timestamp(),'DQ4')""")
    except Exception as exc:
        msg = str(exc)[:4000]
        spark.sql(f"""INSERT INTO 6_mgmt.silver_qc.dq_exec_log
          (run_id,lane,stmt_file,seq,stmt_sha256,status,error_text,rows_affected,attempted_at,settled_at,created_by_session)
          VALUES ({qs(RUN)},{qs(LANE)},{qs(name)},{seq},{qs(sha)},'error',{qs(msg)},NULL,current_timestamp(),current_timestamp(),'DQ4')""")
        raise

run_row = spark.sql(f"SELECT count(*) n FROM 6_mgmt.silver_qc.dq_run WHERE run_id={qs(RUN)} AND finished_at IS NULL").first().n
assert run_row == 1, "run_id must identify one open dq_run row"

items = unpack()
# 0004 is the evidence-hash stamping MERGE; 16_SILVER_EVIDENCE creates dq4_ehash_<RUN> and runs it.
items = [(name, sql) for name, sql in items if not name.endswith("dq4_issueify_0004.sql")]
for seq, (name, sql) in enumerate(items):
    execute(seq, name, sql)

print({"run_id": RUN, "lane": LANE, "statements": len(items), "status": "ok"})

