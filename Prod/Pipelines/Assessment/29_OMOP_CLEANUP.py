# Databricks notebook source
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("scratch_prefix", "")
RUN = dbutils.widgets.get("run_id").strip()
SCRATCH_PREFIX = dbutils.widgets.get("scratch_prefix").strip()
assert RUN.startswith("dq4_omop_") and RUN.replace("_", "").isalnum(), RUN
assert SCRATCH_PREFIX
common = {"achilles_results", "achilles_results_dist", "achilles_analysis"}
rows = spark.sql("""
SELECT table_schema,table_name,table_type
FROM 6_mgmt.information_schema.tables
WHERE table_schema IN ('silver_qc','silver_qc_tmp')
""").collect()
targets = [
    r for r in rows
    if RUN.lower() in r.table_name.lower()
       or r.table_name.lower().startswith(SCRATCH_PREFIX.lower())
       or r.table_name.lower().startswith(("heel_" + RUN + "_").lower())
       or (r.table_schema == "silver_qc_tmp" and r.table_name in common)
]
for row in sorted(targets, key=lambda r: (0 if r.table_type == "VIEW" else 1, r.table_schema, r.table_name)):
    kind = "VIEW" if row.table_type == "VIEW" else "TABLE"
    schema = row.table_schema.replace("`", "``")
    name = row.table_name.replace("`", "``")
    spark.sql(f"DROP {kind} IF EXISTS `6_mgmt`.`{schema}`.`{name}`")
remaining = [
    r for r in spark.sql("""
    SELECT table_schema,table_name FROM 6_mgmt.information_schema.tables
    WHERE table_schema IN ('silver_qc','silver_qc_tmp')
    """).collect()
    if RUN.lower() in r.table_name.lower()
       or r.table_name.lower().startswith(SCRATCH_PREFIX.lower())
       or r.table_name.lower().startswith(("heel_" + RUN + "_").lower())
]
assert not remaining, remaining
print({"dropped": len(targets), "run_id": RUN})

