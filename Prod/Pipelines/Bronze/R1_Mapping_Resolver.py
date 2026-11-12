# Databricks notebook source
# INSR R1: mapping resolver (Bronze_Pipeline_Parallel task after the landings; INSR_Apply P2 initial build).
# Widgets: env prod|dev; mode incremental|initial|parity; run_id; lanes (comma list, empty = all);
# source_map_json / silver_map_json (dev samples, P2 private physical copies); read_as_of (parity only);
# lookup_overrides_json (dev only: INSR-new lookups -> their dev builds, see the plan's conventions).
import json
import os
import sys
import uuid

import pyspark.sql.functions as F

dbutils.widgets.text("env", "prod")
dbutils.widgets.text("mode", "incremental")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("lanes", "")
dbutils.widgets.text("source_map_json", "{}")
dbutils.widgets.text("silver_map_json", "{}")
dbutils.widgets.text("read_as_of", "")
dbutils.widgets.text("lookup_overrides_json", "{}")
ENV, MODE = dbutils.widgets.get("env"), dbutils.widgets.get("mode")
assert ENV == "dev" or json.loads(dbutils.widgets.get("lookup_overrides_json")) == {}, "overrides are dev-only"
os.environ["R1_LOOKUP_OVERRIDES"] = dbutils.widgets.get("lookup_overrides_json")   # before mapping_chain imports
RUN_ID = dbutils.widgets.get("run_id") or f"r1-{uuid.uuid4()}"
assert ENV in ("prod", "dev") and MODE in ("incremental", "initial", "parity"), (ENV, MODE)
SHARED = ("/Workspace/Shared/ADC-DB/Prod/Pipelines/_shared" if ENV == "prod"
          else "/Workspace/Shared/PM-DE-Misc/Ben/Python/CC/INSR_20261029/R1/src/_shared")
sys.path.append(SHARED)
import mapping_contract as M  # noqa: E402
import mapping_resolver as R  # noqa: E402

NAMES = M.table_names(M.schemas_for("4_prod.bronze.x" if ENV == "prod" else "8_dev.bronze.x"))
READ_AS_OF = dbutils.widgets.get("read_as_of") or None
R.prepare_environment(spark, NAMES, READ_AS_OF)
import mapping_lanes as L  # noqa: E402  (imports mapping_chain with the environment set above)

# COMMAND ----------

# Crosswalk copy first: every crosswalk step reads it. Rebuilt when the concept content hash moved for any lane that
# tracks the concept table (lanes without it in input_maps are ignored), always outside incremental mode.
concept_hash = R.map_hashes(spark, ["3_lookup.omop.concept"], READ_AS_OF)
stored = {r["lane_id"]: json.loads(r["input_maps_hash_json"] or "{}")
          for r in spark.read.table(NAMES["lane"]).collect()}
concept_changed = MODE != "incremental" or R.concept_changed_since(stored, concept_hash["3_lookup.omop.concept"])
crosswalk_rebuilt = R.refresh_crosswalk(spark, NAMES, concept_changed, READ_AS_OF)

# COMMAND ----------

# Lanes, one at a time (the 35M-key pathology result lane resolved in 125 s in the R1 sizing probe).
wanted = [l for l in dbutils.widgets.get("lanes").split(",") if l] or list(L.LANES)
results = []
for lane_id in wanted:
    results.append(R.resolve_lane(spark, NAMES, lane_id, RUN_ID, MODE,
                                  json.loads(dbutils.widgets.get("source_map_json")),
                                  json.loads(dbutils.widgets.get("silver_map_json")), READ_AS_OF))
    print(results[-1])

# COMMAND ----------

# Native arms (registry usage only): new coded values of the feeds no lane maps. Skipped when `lanes` names lanes.
if not dbutils.widgets.get("lanes"):
    for arm in L.native_arms():
        results.append(R.resolve_native(spark, NAMES, arm, RUN_ID, json.loads(dbutils.widgets.get("source_map_json"))))

# One audit row for the run, written through the DataFrame API (the metrics JSON carries quotes).
metrics = json.dumps({"mode": MODE, "crosswalk_rebuilt": crosswalk_rebuilt, "lanes": results}, default=str)
(spark.range(1).select(F.lit(RUN_ID).alias("run_id"), F.lit("resolver").alias("step"),
                       F.lit(None).cast("string").alias("lane_id"), F.lit(None).cast("string").alias("table_name"),
                       F.lit("OK").alias("status"), F.lit(metrics).alias("metrics_json"),
                       F.current_timestamp().alias("at"))
 .write.mode("append").saveAsTable(NAMES["audit"]))
dbutils.notebook.exit(json.dumps({"run_id": RUN_ID, "mode": MODE, "lanes": results, "fails": []}, default=str))

