# Databricks notebook source
# Map split initializer. Job-task use only.

# MAGIC %pip install openai pyarrow

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import json

for _name, _default in {
    "pipeline_run_id": "",
    "force_full_refresh": "false",
    "create_cutover_backups": "false",
    "run_post_deployment_checks": "true",
    "map_common_bootstrap": "true",
}.items():
    try:
        dbutils.widgets.get(_name)
    except Exception:
        dbutils.widgets.text(_name, _default)

# COMMAND ----------

# MAGIC %run "/Workspace/Shared/ADC-DB/Prod/Pipelines/Bronze/_bronze_common"

# COMMAND ----------

# MAGIC %run "/Workspace/Shared/ADC-DB/Prod/Pipelines/Bronze/_map_common"

# COMMAND ----------

_result = {
    "status": "INITIALIZED",
    "pipeline": "map_pipeline_split",
    "run_id": _PIPELINE_RUN_ID,
    "full_refresh": _PIPELINE_FULL_REFRESH,
    "source_manifest_hash": _PIPELINE_SOURCE_MANIFEST_HASH,
    "source_manifest_table_count": len(_PIPELINE_SOURCE_MANIFEST),
}
dbutils.jobs.taskValues.set(key="map_run_id", value=_PIPELINE_RUN_ID)
dbutils.jobs.taskValues.set(
    key="source_manifest_hash",
    value=_PIPELINE_SOURCE_MANIFEST_HASH,
)
dbutils.jobs.taskValues.set(
    key="source_manifest_json",
    value=json.dumps(_PIPELINE_SOURCE_MANIFEST, sort_keys=True),
)
dbutils.jobs.taskValues.set(
    key="effective_full_refresh",
    value=str(bool(_PIPELINE_FULL_REFRESH)).lower(),
)
print(json.dumps(_result, sort_keys=True))
dbutils.notebook.exit(json.dumps(_result, sort_keys=True))

