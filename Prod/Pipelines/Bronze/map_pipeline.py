# Databricks notebook source
# MAGIC %md
# MAGIC # Map Pipeline — Single Layer
# MAGIC
# MAGIC Builds the 23 unsuffixed Map primary tables directly. Each component
# MAGIC projects its output to the pinned retained-column contract before a
# MAGIC primary-table write. No canonical or compatibility publication phase exists.
# COMMAND ----------
# MAGIC %pip install openai pyarrow
# COMMAND ----------
# MAGIC %restart_python
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
# COMMAND ----------
# MAGIC %run "./_bronze_common"
# COMMAND ----------
# MAGIC %run "./_map_common"
# COMMAND ----------
# MAGIC %run "./map_10_foundation"
# COMMAND ----------
# MAGIC %run "./map_20_clinical"
# COMMAND ----------
# MAGIC %run "./map_30_events"
# COMMAND ----------
# MAGIC %run "./map_40_maternity_journey"
# COMMAND ----------
# MAGIC %run "./map_50_pathology"
# COMMAND ----------
#
# Finalization completed inside map_50_pathology before its %run returned.
# This command deliberately depends only on Databricks' injected dbutils handle.
dbutils.notebook.exit(
    '{"finalized_in":"map_50_pathology","pipeline":"map_pipeline","status":"SUCCESS"}'
)

