# Databricks notebook source
import hashlib, re

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("run_open_ts", "")
dbutils.widgets.text("source_update_id", "")
dbutils.widgets.text("silver_update_id", "")
dbutils.widgets.text("scratch_prefix", "")
RUN = dbutils.widgets.get("run_id").strip()
RUN_OPEN_TS = dbutils.widgets.get("run_open_ts").strip()
SOURCE_UPDATE_ID = dbutils.widgets.get("source_update_id").strip()
SILVER_UPDATE_ID = dbutils.widgets.get("silver_update_id").strip()
SCRATCH_PREFIX = dbutils.widgets.get("scratch_prefix").strip()
assert RUN.startswith("dq4_omop_") and RUN.replace("_", "").isalnum(), RUN
assert RUN_OPEN_TS and SOURCE_UPDATE_ID and SILVER_UPDATE_ID and SCRATCH_PREFIX
PREFIX = SCRATCH_PREFIX + "s_tmpach_"
SESSION = "DQ4"

def qs(v):
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"

def execute(seq, name, sql):
    sha = hashlib.sha256(sql.encode()).hexdigest()
    spark.sql(f"""INSERT INTO 6_mgmt.silver_qc.dq_exec_log VALUES
      ({qs(RUN)},'achilles_reconcile',{qs(name)},{seq},{qs(sha)},'attempted',
       NULL,NULL,current_timestamp(),NULL,{qs(SESSION)})""")
    try:
        result = spark.sql(sql)
        result.collect()
        spark.sql(f"""INSERT INTO 6_mgmt.silver_qc.dq_exec_log VALUES
          ({qs(RUN)},'achilles_reconcile',{qs(name)},{seq},{qs(sha)},'ok',
           NULL,NULL,current_timestamp(),current_timestamp(),{qs(SESSION)})""")
    except Exception as exc:
        msg = str(exc)[:4000]
        spark.sql(f"""INSERT INTO 6_mgmt.silver_qc.dq_exec_log VALUES
          ({qs(RUN)},'achilles_reconcile',{qs(name)},{seq},{qs(sha)},'error',
           {qs(msg)},NULL,current_timestamp(),current_timestamp(),{qs(SESSION)})""")
        raise

names = [r.table_name for r in spark.sql("""
SELECT table_name FROM 6_mgmt.information_schema.tables
WHERE table_schema='silver_qc_tmp'
""").collect()]
regular = sorted(
    (n for n in names if re.fullmatch(re.escape(PREFIX) + r"[0-9]+", n)),
    key=lambda n: int(n.rsplit("_", 1)[1])
)
dist = sorted(
    (n for n in names if re.fullmatch(re.escape(PREFIX) + r"dist_[0-9]+", n)),
    key=lambda n: int(n.rsplit("_", 1)[1])
)
assert regular, "no materialized regular Achilles analysis tables"
assert dist, "no materialized Achilles distribution tables"

reg_select = """SELECT cast(analysis_id as int) analysis_id,
 cast(stratum_1 as STRING) stratum_1, cast(stratum_2 as STRING) stratum_2,
 cast(stratum_3 as STRING) stratum_3, cast(stratum_4 as STRING) stratum_4,
 cast(stratum_5 as STRING) stratum_5, cast(count_value as BIGINT) count_value
 FROM 6_mgmt.silver_qc_tmp.{}"""
dist_select = """SELECT cast(analysis_id as int) analysis_id,
 cast(stratum_1 as STRING) stratum_1, cast(stratum_2 as STRING) stratum_2,
 cast(stratum_3 as STRING) stratum_3, cast(stratum_4 as STRING) stratum_4,
 cast(stratum_5 as STRING) stratum_5, cast(count_value as BIGINT) count_value,
 cast(min_value as DOUBLE) min_value, cast(max_value as DOUBLE) max_value,
 cast(avg_value as DOUBLE) avg_value, cast(stdev_value as DOUBLE) stdev_value,
 cast(median_value as DOUBLE) median_value, cast(p10_value as DOUBLE) p10_value,
 cast(p25_value as DOUBLE) p25_value, cast(p75_value as DOUBLE) p75_value,
 cast(p90_value as DOUBLE) p90_value
 FROM 6_mgmt.silver_qc_tmp.{}"""

execute(0, "reconcile_achilles_results.sql",
        "CREATE OR REPLACE TABLE 6_mgmt.silver_qc_tmp.achilles_results AS SELECT * FROM (" +
        " UNION ALL ".join(reg_select.format(n) for n in regular) +
        ") q WHERE count_value > 5")
execute(1, "reconcile_achilles_results_dist.sql",
        "CREATE OR REPLACE TABLE 6_mgmt.silver_qc_tmp.achilles_results_dist AS SELECT * FROM (" +
        " UNION ALL ".join(dist_select.format(n) for n in dist) +
        ") q WHERE count_value > 5")

meta = spark.sql("""SELECT count(*) n,count(DISTINCT analysis_id) d
FROM 6_mgmt.silver_qc_tmp.achilles_analysis""").first()
assert meta.n == 218 and meta.d == 218, meta
assert RUN.startswith("dq4_omop_")
for table in ("achilles_results","achilles_results_dist","achilles_analysis"):
    prior = spark.sql(f"SELECT count(*) n FROM 6_mgmt.silver_qc.{table} WHERE run_id={qs(RUN)}").first().n
    assert prior >= 0
    execute(10 + ["achilles_results","achilles_results_dist","achilles_analysis"].index(table),
            f"delete_{table}.sql",
            f"DELETE FROM 6_mgmt.silver_qc.{table} WHERE run_id={qs(RUN)}")
execute(20, "append_achilles_results.sql",
        f"INSERT INTO 6_mgmt.silver_qc.achilles_results SELECT {qs(RUN)},* FROM 6_mgmt.silver_qc_tmp.achilles_results")
execute(21, "append_achilles_results_dist.sql",
        f"INSERT INTO 6_mgmt.silver_qc.achilles_results_dist SELECT {qs(RUN)},* FROM 6_mgmt.silver_qc_tmp.achilles_results_dist")
execute(22, "append_achilles_analysis.sql",
        f"INSERT INTO 6_mgmt.silver_qc.achilles_analysis SELECT {qs(RUN)},* FROM 6_mgmt.silver_qc_tmp.achilles_analysis")

summary = {}
for table in ("achilles_results","achilles_results_dist","achilles_analysis"):
    row = spark.sql(f"""SELECT count(*) n,count(DISTINCT analysis_id) analyses
                        FROM 6_mgmt.silver_qc.{table} WHERE run_id={qs(RUN)}""").first()
    summary[table] = {"rows": row.n, "analyses": row.analyses}
print({"materialized_regular":len(regular),"materialized_dist":len(dist),"canonical":summary})

