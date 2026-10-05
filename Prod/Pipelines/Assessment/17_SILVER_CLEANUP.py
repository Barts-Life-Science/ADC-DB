# Databricks notebook source
dbutils.widgets.text("run_id", "")
RUN = dbutils.widgets.get("run_id").strip()
assert RUN.startswith("dq4_silver_") and RUN.replace("_", "").isalnum(), RUN
rows = spark.sql("""
SELECT table_schema,table_name,table_type
FROM 6_mgmt.information_schema.tables
WHERE table_schema IN ('silver_qc','silver_qc_tmp')
""").collect()
targets = [r for r in rows if RUN.lower() in r.table_name.lower()]
for row in sorted(targets, key=lambda r: (0 if r.table_type == "VIEW" else 1, r.table_schema, r.table_name)):
    kind = "VIEW" if row.table_type == "VIEW" else "TABLE"
    schema = row.table_schema.replace("`", "``")
    name = row.table_name.replace("`", "``")
    spark.sql(f"DROP {kind} IF EXISTS `6_mgmt`.`{schema}`.`{name}`")
remaining = [r for r in spark.sql("""
SELECT table_schema,table_name FROM 6_mgmt.information_schema.tables
WHERE table_schema IN ('silver_qc','silver_qc_tmp')
""").collect() if RUN.lower() in r.table_name.lower()]
assert not remaining, remaining
print({"dropped": len(targets), "run_id": RUN})

