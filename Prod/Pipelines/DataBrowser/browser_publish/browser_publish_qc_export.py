# Databricks notebook source
# BrowserPublish / qc_export — Lakeflow expectation metrics for one release.
#
# MUST be run by the dataset's QC pipeline owner: event_log('<id>') is owner-only
# (the publishing SP gets PERMISSION_DENIED). Alternatively set
# event_log_relation to a published event-log table the runner can read.
# It reads the update recorded in lookup.pipeline_config for run_id, applies
# publication-time disclosure, and writes lookup.qc_result. Rules whose
# implementation emits no metric simply have no row: the enrichment reports
# them as "unavailable".

# COMMAND ----------
# MAGIC %run ./browser_publish_config

# COMMAND ----------
# MAGIC %run ./browser_publish_disclosure

# COMMAND ----------
import re

from pyspark.sql import functions as F

dbutils.widgets.dropdown("dataset", "gold", sorted(PROFILES))
DATASET = dbutils.widgets.get("dataset")
PROFILE = PROFILES[DATASET]
assert PROFILE.qc_role, f"{DATASET} names no QC pipeline yet"
dbutils.widgets.dropdown("env", "dev", ["dev", "prod"])
dbutils.widgets.text("run_id", "", "run_id")
dbutils.widgets.text("event_log_relation",
                     f"event_log('{PROFILE.pipelines[PROFILE.qc_role]}')",
                     "event log")

ENV = dbutils.widgets.get("env")
RUN_ID = dbutils.widgets.get("run_id")
EVENT_LOG = dbutils.widgets.get("event_log_relation")
_T = targets_for(PROFILE, ENV)
LOOKUP = f"{_T['catalog']}.{_T['lookup']}"
TAG = (f"/* dbrowse ctx=system u={DATASET}-publish "
       f"feature={DATASET}-qc-export */ ")

# COMMAND ----------
assert re.fullmatch(r"[0-9a-fA-F]{32}", RUN_ID), f"invalid run_id {RUN_ID!r}"
config = spark.sql(TAG + f"""
    SELECT pipeline_id, update_id FROM {LOOKUP}.pipeline_config
    WHERE run_id = '{RUN_ID}' AND role = '{PROFILE.qc_role}'""").first()
assert config is not None and config["update_id"], \
    f"no {PROFILE.qc_role} pipeline_config row for {RUN_ID}; run the publish first"

rows = spark.sql(TAG + qc_events_sql(event_log_relation=EVENT_LOG,
                                     update_id=config["update_id"])).collect()
records = []
for row in rows:
    result = disclose_qc(row["passed"], row["failed"])
    records.append((RUN_ID, config["pipeline_id"], config["update_id"],
                    row["dataset"], row["expectation"], result["passed"],
                    result["passed_band"], result["failed"],
                    result["failed_band"]))

table = f"{LOOKUP}.qc_result"
spark.sql(TAG + QC_RESULT_DDL.format(table=table))
spark.sql(TAG + f"DELETE FROM {table} WHERE run_id = '{RUN_ID}'")
if records:
    (spark.createDataFrame(records,
         "run_id string, pipeline_id string, update_id string, "
         "dataset string, expectation string, passed bigint, "
         "passed_band string, failed bigint, failed_band string")
          .withColumn("computed_at", F.current_timestamp().cast("string"))
          .write.mode("append").saveAsTable(table))
print(f"qc_result: {len(records)} expectation rows for update "
      f"{config['update_id']}")

