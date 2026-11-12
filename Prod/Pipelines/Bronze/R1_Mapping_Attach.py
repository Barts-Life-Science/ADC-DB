# Databricks notebook source
# INSR R1: post-landing attach, then the change-driven backfill, then the usage recount
# (Bronze_Pipeline_Parallel: after R1_Mapping_Resolver). Widgets: env prod|dev; run_id; tables (empty = all targets);
# target_schema (dev: 8_dev.insr_r1 sample tables); silver_map_json (dev); skip_backfill (dev unit runs).
import json
import sys
import uuid

dbutils.widgets.text("env", "prod")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("tables", "")
dbutils.widgets.text("target_schema", "`4_prod`.bronze")
dbutils.widgets.text("silver_map_json", "{}")
dbutils.widgets.text("skip_backfill", "false")
dbutils.widgets.text("lookup_overrides_json", "{}")      # dev only (plan conventions)
dbutils.widgets.text("initial_tables", "")      # INSR P2 on a job cluster: initial rewrites only, then exit
INITIAL = [t for t in dbutils.widgets.get("initial_tables").split(",") if t]
ENV = dbutils.widgets.get("env")
assert ENV == "dev" or json.loads(dbutils.widgets.get("lookup_overrides_json")) == {}, "overrides are dev-only"
import os  # noqa: E402
os.environ["R1_LOOKUP_OVERRIDES"] = dbutils.widgets.get("lookup_overrides_json")
RUN_ID = dbutils.widgets.get("run_id") or f"r1-{uuid.uuid4()}"
TARGET_SCHEMA = dbutils.widgets.get("target_schema")


def canonical_schema(name):
    """`catalog`.schema, the form every attach_state / audit key was written with (INSR P2, R1FIX).

    Bronze_Pipeline_Parallel passes its job parameter target_schema=4_prod.bronze (no backticks) to every task; the
    keys then read 4_prod.bronze.map_x and miss the stored `4_prod`.bronze.map_x rows ("no attach watermark",
    catch-up repair 2026-11-09 23:51). Normalising here keeps one key form whatever the caller passes."""
    catalog, schema = name.replace("`", "").split(".")
    return f"`{catalog}`.{schema}"


TARGET_SCHEMA = canonical_schema(TARGET_SCHEMA)   # INSR_FU_ATTACH_SCHEMA_KEY_V1
assert ENV == "prod" or TARGET_SCHEMA.replace("`", "").startswith("8_dev."), "dev runs write only 8_dev"
SHARED = ("/Workspace/Shared/ADC-DB/Prod/Pipelines/_shared" if ENV == "prod"
          else "/Workspace/Shared/PM-DE-Misc/Ben/Python/CC/INSR_20261029/R1/src/_shared")
sys.path.append(SHARED)
import mapping_contract as M  # noqa: E402
import mapping_resolver as R  # noqa: E402

NAMES = M.table_names(M.schemas_for(TARGET_SCHEMA + ".x"))
R.prepare_environment(spark, NAMES)
import mapping_attach as A  # noqa: E402
import mapping_backfill as B  # noqa: E402
import mapping_lanes as L  # noqa: E402

# COMMAND ----------

# 0. INSR P2 (r1_steps.initial_attach_on_cluster): the initial rewrite of the >1bn-row targets on a job cluster, then exit.
if INITIAL:
    done = [A.initial_rewrite(spark, f"{TARGET_SCHEMA}.{t}", RUN_ID) for t in INITIAL]
    dbutils.notebook.exit(json.dumps({"run_id": RUN_ID, "initial": done, "fails": []}, default=str))

# COMMAND ----------

# 1. New and changed bronze rows get their keys and resolutions (heals a table a full rebuild left without columns).
tables = [t for t in dbutils.widgets.get("tables").split(",") if t] or sorted(L.ROW_KEYS)
attached = [A.attach_changed(spark, f"{TARGET_SCHEMA}.{t}", RUN_ID) for t in tables
            if spark.catalog.tableExists(f"{TARGET_SCHEMA}.{t}".replace("`", ""))]
heal_deferred = [a["table"] for a in attached if a.get("healed") == "deferred"]   # over 1bn rows: job cluster later
for row in attached:
    print(row)

# COMMAND ----------

# 2. Pending lane changes: apply whole lanes within 200M rows / $50, defer the rest (cost alert = DEFERRED rows).
backfill = [] if dbutils.widgets.get("skip_backfill") == "true" else [
    B.apply_lane(spark, NAMES, lane_id, RUN_ID, TARGET_SCHEMA) for lane_id in L.LANES]
deferred = [b for b in backfill if b.get("status") == "DEFERRED"]
after_defer = [b for b in backfill if b.get("status") == "APPLIED_AFTER_DEFER"]
held = [b["lane"] for b in backfill if b.get("status") == "HELD_RELEASE_OPEN"]   # applied after the release closes
for row in backfill:
    print(row)

# COMMAND ----------

# 3. Usage recount (registry input and next run's prices).
usage_rows = B.recount_usage(spark, NAMES, TARGET_SCHEMA, json.loads(dbutils.widgets.get("silver_map_json")))
dbutils.notebook.exit(json.dumps({"run_id": RUN_ID, "attached": attached, "backfill": backfill,
                                  "deferred_lanes": [d["lane"] for d in deferred],
                                  "applied_after_defer": [{k: d[k] for k in ("lane", "rows", "usd")} for d in after_defer],
                                  "held_release_open": held, "heal_deferred": heal_deferred, "usage_rows": usage_rows,
                                  "fails": []}, default=str))

