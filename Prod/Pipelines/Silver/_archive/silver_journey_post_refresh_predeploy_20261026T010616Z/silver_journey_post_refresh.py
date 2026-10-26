# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Journey post-refresh maintenance
# MAGIC Run after a successful Silver Journey pipeline update. This notebook only
# MAGIC verifies or repairs patient-event clustering. It does not create or replace
# MAGIC form tables: `6_silver_forms` owns those materialized views inside Lakeflow.
# MAGIC
# MAGIC Keep this notebook outside the pipeline library. It is a post-refresh task,
# MAGIC not a dataset definition. The previous form-building code has been removed.

# COMMAND ----------

import json
from datetime import datetime, timezone


def _qident(value):
    """Quote one SQL identifier, escaping embedded backticks."""
    return "`" + value.replace("`", "``") + "`"


def _qtable(name):
    """Quote each part of a fully qualified SQL table name."""
    return ".".join(_qident(value) for value in name.split("."))


# EIR_20261021_V1: ensure_patient_event_clustering retired with events_patient_event;
# the per-fact-table event feeds declare cluster_by in the pipeline.

# COMMAND ----------

result = {
    "started_at": datetime.now(timezone.utc).isoformat(),
    "form_instruments": {"status": "managed_by_pipeline", "notebook": "6_silver_forms"},
    "patient_event_clustering": "retired: events_patient_event removed by EIR_20261021_V1",
    "completed_at": datetime.now(timezone.utc).isoformat(),
}

print(json.dumps(result, indent=2, sort_keys=True))

