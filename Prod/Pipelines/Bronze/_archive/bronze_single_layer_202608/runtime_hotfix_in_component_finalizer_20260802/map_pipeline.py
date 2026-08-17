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
# _map_common registers the finalizer on Python's process-wide builtins
# module. Real component notebooks can remove both private and public common
# names from this caller command, but they cannot remove that durable handle.
import builtins as _map_pipeline_builtins

_map_pipeline_finalizer_name = "_adc_bronze_map_pipeline_finalizer_20260726_v1"
_map_pipeline_finalizer = getattr(
    _map_pipeline_builtins,
    _map_pipeline_finalizer_name,
    None,
)
if not callable(_map_pipeline_finalizer):
    raise RuntimeError(
        "_map_common did not register the durable map-pipeline finalizer"
    )

try:
    _map_pipeline_result = _map_pipeline_finalizer()
finally:
    if (
        getattr(
            _map_pipeline_builtins,
            _map_pipeline_finalizer_name,
            None,
        )
        is _map_pipeline_finalizer
    ):
        delattr(
            _map_pipeline_builtins,
            _map_pipeline_finalizer_name,
        )

dbutils.notebook.exit(_map_pipeline_result)

