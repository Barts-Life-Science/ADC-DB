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
LANE = "dq4_profile"
PAYLOAD = """H4sIAFWLt2oC/+1a23LbNhDVs74Cb6Q6GpVkRClqks74wjqaceVUYibTJ4YmYYVT3kyAStzpx3cBUhRJ3WrH9ozTPQ+8YpeLxWL3ABK7DX9m3OXM8cIgDjw33FwwL8nogN2Gne+DBhiNhuKsj02tfhYYakOjo5vG8JUxNgxt1IG347HRIVrnGZBD7zMwhbo8ia/3tzv2/oXibG6d2Ba5mpO59eHy5Mwi9snppUU+v3Z8uvo8YEG4oplz6zk8Sgf+7dA5GC2ObFHIGJox0l4bJjlZdD9N7ffk2mUUbojaJWRhXVpnNvESN6TMo6qXxJ7LVSWJkvQXpU/OTha2GudhGNyohWbxxhHNaMqdwO8TrSeULez5dHbR6/XJWgXLPKGhUs2SPPMoiPpBvHTYHeM06hPFceIkpo6jgKjyz24B2mjXkx9cm/AXvetDR0q0hf2ApaF715CvS6/f79BQ9jpMvtJM5VkQqXkccKdUvHLDnIruKtLwpnbRsKZSytVctt/cG0r9LW31dxtROTK+y6nDszz2VCVKYv4F/EdXNIaOwRseRFRqOIfYkheyTVuJ7IoT59E1zWTrq48QerL9Cpr+Nr/6nQydNEv8MgwHe6Ku2+tW8bQZHBK539SWv3eOQdNtQqrluMq3u5zpJXnM1fPpwp7OwIJdorFTf8oqUfCCRT69t2ZkmSV5CuFZH48eeUc0YovXirhXNu4DNMWkf+sCxaA0JKxL+JpSGqEQa3YubAth2ML7W1R7uta0K1x26GubKu/XOppxUrj2p8KHraer0rMQPjQLvOqtm0LAfHNSCrbEPAipuuoTN8vcO1UbiMoDiWOwPsmjYYqjPIzlcTIpjuvTpJj4t5tvrJbl9yPqxn3CuA/JsnxU3FRtWR6pGy+soN83YZJksjH02b1m4vItmVDdLByiFyOlCZ+UnQxi/iCFjfGXEE3SKPFVOQPLxvCN0+nFdGZDRtG12tjsMIX72vOaYh4wxdxrytsDXYjpcq/cdEZUOfKT6jjp7dcUxJQdcMd+G/6mWdKVGU5Uxe7F/OrjB3L6J5EXUNCgQNpQKdVGQhNZpNff9mSJ7cb9+qS8p2QxT3tvOogfGGyb/6cu/5KEyfLOySjLQ/7dK4Bj/F/X9Sb/N2BFgPz/ZfD/drQ8yQqAU8b/1wsAL4+kORXxx0XC8UVCOzRxmYDLBFwm4DIBlwm4TEDs5f+rgIuNtWAZP8Le/3/a/3+1tf+vm2Pk/y+C/2+i5UmYf6Ee9/6R1u+IOCT0SOiR0COhR0KPhB7xYP4PifMGEiZkG0Yz/vh/BDrC/7WROWzt/+ujkYn8/zkwnS2suQ3pz77a5vzA9wvO6ZQhsqZbyg6qDyxQ2RM6Sr9J0A4TsTbh2qJRLe7SoLEFkQDWUKMIUMEbBbuSLApsWdzKwlLWiSrJlzm7klHO/xiK/uRZJiivoLsQQVGq9oq0/oh/nOpCPZlbJCa/voOaqL157vn/WD8EHp3/I601/41XOs7/lzz/26GDGeCBP50+dQ44NP8fayPw6PwfbtV/zcTf/1/y/N+EDs78e2+aPkfdRyAQCAQCgUAgEAgEAoFAIBAIBAKBQCAQCAQC8ePgX47NKPMAUAAA"""

def qs(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"

def adapt(value):
    return (value
        .replace("dq4_silver_20260825", RUN)
        .replace("2026-08-25T10:50:49.897Z", RUN_OPEN_TS)
        .replace("f5c7c7ab-e37d-4a31-b9c2-b7631becb16a", SILVER_UPDATE_ID))

def unpack():
    archive = tarfile.open(fileobj=io.BytesIO(base64.b64decode(PAYLOAD)), mode="r:gz")
    items = []
    for member in archive.getmembers():
        if member.isfile() and member.name.endswith(".sql"):
            items.append((member.name, adapt(archive.extractfile(member).read().decode("utf-8"))))
    return sorted(items, key=lambda x: (0 if "/stats_" in x[0] else 1, x[0]))

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

existing = spark.sql(f"SELECT count(*) n FROM 6_mgmt.silver_qc.dq_value_profile WHERE run_id={qs(RUN)}").first().n
assert existing == 0, f"dq_value_profile already contains {existing} rows for {RUN}"

items = unpack()
for seq, (name, sql) in enumerate(items):
    execute(seq, name, sql)

print({"run_id": RUN, "lane": LANE, "statements": len(items), "status": "ok"})

