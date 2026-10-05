# Databricks notebook source
import json

spark.conf.set("spark.sql.session.timeZone", "UTC")
dbutils.widgets.text("run_suffix", "")
dbutils.widgets.dropdown("run_silver", "true", ["true", "false"])
dbutils.widgets.dropdown("run_omop", "true", ["true", "false"])
dbutils.widgets.text("child_timeout_seconds", "28800")
ROOT = "/Workspace/Shared/ADC-DB/Prod/Pipelines/Assessment"  # OGR_NO_DEV_V1
suffix = dbutils.widgets.get("run_suffix").strip()
if not suffix:
    suffix = spark.sql("SELECT date_format(current_timestamp(),'yyyyMMdd_HHmmss') value").first()["value"]
assert suffix.replace("_", "").isalnum(), suffix
timeout_seconds = int(dbutils.widgets.get("child_timeout_seconds"))
silver_run = "dq4_silver_" + suffix
omop_run = "dq4_omop_" + suffix
outputs = {}
if dbutils.widgets.get("run_silver") == "true":
    print(f"Starting silver battery as {silver_run}", flush=True)
    outputs["silver"] = json.loads(dbutils.notebook.run(
        ROOT + "/10_RUN_SILVER_BATTERY", timeout_seconds,
        {"run_id": silver_run, "child_timeout_seconds": str(timeout_seconds)}
    ))
if dbutils.widgets.get("run_omop") == "true":
    print(f"Starting OMOP battery as {omop_run}", flush=True)
    outputs["omop"] = json.loads(dbutils.notebook.run(
        ROOT + "/20_RUN_OMOP_BATTERY", timeout_seconds,
        {"run_id": omop_run, "child_timeout_seconds": str(timeout_seconds)}
    ))
ids = [x["run_id"] for x in outputs.values()]
if ids:
    quoted = ",".join("'" + x.replace("'", "''") + "'" for x in ids)
    display(spark.sql(f"""
      SELECT run_id,started_at,finished_at,pin_bracket_valid,scope,notes
      FROM 6_mgmt.silver_qc.dq_run WHERE run_id IN ({quoted})
      ORDER BY started_at
    """))
print(json.dumps(outputs, indent=2, default=str))
dbutils.notebook.exit(json.dumps(outputs, default=str))

