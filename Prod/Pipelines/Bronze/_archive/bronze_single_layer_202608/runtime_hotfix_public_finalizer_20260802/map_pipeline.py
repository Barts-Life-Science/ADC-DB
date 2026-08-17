# Databricks notebook source
# MAGIC %md
# MAGIC # Map Pipeline — Single Layer
# MAGIC
# MAGIC Builds the 23 unsuffixed Map primary tables directly. Each component
# MAGIC projects its output to the pinned retained-column contract before a
# MAGIC primary-table write. No canonical or compatibility publication phase exists.
#
# COMMAND ----------
#
# MAGIC %pip install openai pyarrow
#
# COMMAND ----------
#
# MAGIC %restart_python
#
# COMMAND ----------
#
_MAP_WIDGET_DEFAULTS = {
    "pipeline_run_id": "",
    "force_full_refresh": "false",
    "create_cutover_backups": "false",
    "run_post_deployment_checks": "true",
}
for _map_widget_name, _map_widget_default in _MAP_WIDGET_DEFAULTS.items():
    try:
        dbutils.widgets.text(_map_widget_name, _map_widget_default)
    except Exception:
        pass
#
# COMMAND ----------
#
# MAGIC %run "./_bronze_common"
#
# COMMAND ----------
#
# MAGIC %run "./_map_common"
#
# COMMAND ----------
#
# MAGIC %run "./map_10_foundation"
#
# COMMAND ----------
#
# MAGIC %run "./map_20_clinical"
#
# COMMAND ----------
#
# MAGIC %run "./map_30_events"
#
# COMMAND ----------
#
# MAGIC %run "./map_40_maternity_journey"
#
# COMMAND ----------
#
# MAGIC %run "./map_50_pathology"
#
# COMMAND ----------
#
_pipeline_post_deployment_report = None
if _pipeline_optional_bool_parameter("run_post_deployment_checks", True):
    _pipeline_post_deployment_report = _pipeline_post_deployment_checks()
else:
    print("[INFO] Post-deployment checks disabled by run_post_deployment_checks")
#
# COMMAND ----------
#
# Source checkpoints are committed only after every primary write and all
# single-layer assertions succeed.
_pipeline_commit_all_checkpoints()
#
# COMMAND ----------
#
_pipeline_run_summary = {
    "pipeline": "map_pipeline",
    "release": _PIPELINE_RELEASE_ID,
    "run_id": _PIPELINE_RUN_ID,
    "mode": "single_layer",
    "full_refresh": _PIPELINE_FULL_REFRESH,
    "updated_targets": sorted(set(_PIPELINE_UPDATED_TARGETS)),
    "post_deployment_checks": _pipeline_post_deployment_report,
    "finished_at": _pipeline_utc_now(),
}
_pipeline_audit(
    None,
    "RUN_SUCCESS",
    {
        "updated_targets": sorted(set(_PIPELINE_UPDATED_TARGETS)),
        "post_deployment_checks": _pipeline_post_deployment_report,
    },
)
print(_pipeline_json.dumps(_pipeline_run_summary, default=str, sort_keys=True))
dbutils.notebook.exit(
    _pipeline_json.dumps(_pipeline_run_summary, default=str, sort_keys=True)
)

