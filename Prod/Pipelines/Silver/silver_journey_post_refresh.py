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


def ensure_patient_event_clustering():
    """Preserve the existing patient-event clustering check and repair, failing on ambiguous targets."""
    catalog = _qident("4_prod")
    candidates = spark.sql(f"""
        SELECT table_name
        FROM {catalog}.information_schema.tables
        WHERE table_schema = 'silver'
          AND table_name RLIKE
              '^__materialization_mat_[0-9a-f_]+_events_patient_event_[0-9]+$'
        ORDER BY table_name
    """).collect()

    if len(candidates) != 1:
        names = [row.table_name for row in candidates]
        raise RuntimeError(
            "expected exactly one physical events_patient_event materialization; "
            f"found {names}"
        )

    physical = f"4_prod.silver.{candidates[0].table_name}"
    detail = spark.sql(f"DESCRIBE DETAIL {_qtable(physical)}").first()
    current = [str(value).lower() for value in (detail.clusteringColumns or [])]
    expected = ["person_id", "event_datetime"]

    if current != expected:
        spark.sql(
            f"ALTER TABLE {_qtable(physical)} SET TBLPROPERTIES "
            "('delta.dataSkippingStatsColumns'='person_id,event_datetime')"
        )
        spark.sql(f"ANALYZE TABLE {_qtable(physical)} COMPUTE DELTA STATISTICS")
        spark.sql(
            f"ALTER TABLE {_qtable(physical)} "
            "CLUSTER BY (person_id, event_datetime)"
        )
        detail = spark.sql(f"DESCRIBE DETAIL {_qtable(physical)}").first()
        current = [
            str(value).lower() for value in (detail.clusteringColumns or [])
        ]

    if current != expected:
        raise RuntimeError(
            f"patient-event clustering verification failed for {physical}: {current}"
        )

    return {"table": physical, "columns": current}

# COMMAND ----------

result = {
    "started_at": datetime.now(timezone.utc).isoformat(),
    "form_instruments": {"status": "managed_by_pipeline", "notebook": "6_silver_forms"},
    "patient_event_clustering": ensure_patient_event_clustering(),
    "completed_at": datetime.now(timezone.utc).isoformat(),
}

print(json.dumps(result, indent=2, sort_keys=True))

