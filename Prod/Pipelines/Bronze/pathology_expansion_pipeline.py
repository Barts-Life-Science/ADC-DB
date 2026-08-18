# Databricks notebook source
# MAGIC %md
# MAGIC # Weekly pathology expansion
# MAGIC
# MAGIC Runs the deterministic A12 sidecars after `map_pipeline`. Production writes are
# MAGIC fail-closed and require the orchestrator's explicit `allow_production_write=true`.

# COMMAND ----------

import json
import time
from datetime import datetime, timezone

from pathology_amr import run_amr
from pathology_genetics import run_genetics
from pathology_incremental import run_incremental_core
from pathology_indications import run_indications
from pathology_pipeline import PipelineConfig, create_views
from pathology_runtime import register_runtime_artifact


WEEKLY_RUNNER_VERSION = "2026-08-16-periodic-core-v3"


for _name, _default in {
    "pipeline_run_id": "",
    "target_schema": "8_dev.bronze",
    "force_full_refresh": "false",
    "allow_production_write": "false",
    "full_reconciliation": "false",
    "bootstrap_mode": "false",
    "periodic_lanes": "auto",
}.items():
    try:
        dbutils.widgets.text(_name, _default)
    except Exception:
        pass


def value(name: str, default: str = "") -> str:
    try:
        return (dbutils.widgets.get(name) or default).strip()
    except Exception:
        return default


def flag(name: str) -> bool:
    return value(name).lower() in {"1", "true", "yes", "y"}


def bronze_lookup_schema(target_schema: str) -> str:
    catalog, _schema = target_schema.split(".", 1)
    if catalog == "4_prod":
        return "3_lookup.omop"
    if target_schema == "8_dev.bronze":
        return "8_dev.lookup"
    return target_schema


def periodic_lane_state(force_full_refresh: bool) -> dict[str, object]:
    mode = value("periodic_lanes", "auto").lower()
    if mode not in {"auto", "false", "no", "off", "true", "yes", "on"}:
        raise ValueError("periodic_lanes must be auto, true, or false")
    now = datetime.now(timezone.utc)
    iso_week = int(now.isocalendar().week)
    if force_full_refresh:
        enabled = True
        reason = "force_full_refresh"
    elif mode == "auto":
        enabled = iso_week % 4 == 0
        reason = "every_fourth_iso_week" if enabled else "weekly_core_only"
    else:
        enabled = mode in {"true", "yes", "on"}
        reason = "explicit_override"
    return {
        "enabled": enabled,
        "mode": mode,
        "iso_week": iso_week,
        "cadence": "every_fourth_iso_week",
        "reason": reason,
    }


started = time.monotonic()
target_schema = value("target_schema", "8_dev.bronze")
allow_production_write = flag("allow_production_write")
if "." not in target_schema:
    raise ValueError(f"target_schema must be catalog.schema, got {target_schema!r}")
if not target_schema.startswith("8_dev.") and not allow_production_write:
    raise PermissionError("refusing non-dev target without allow_production_write=true")

register_runtime_artifact(spark)
lookup_schema = bronze_lookup_schema(target_schema)
config = PipelineConfig(bronze_schema=target_schema, lookup_schema=lookup_schema)

force_full_refresh = flag("force_full_refresh")
if force_full_refresh:
    spark.sql(
        f"DELETE FROM {target_schema}.pathology_expansion_state "
        "WHERE source_name IN ('map_pathology','path_patient_samplelevel')"
    )

core = run_incremental_core(spark, config, validate_stage_keys=False)
periodic = periodic_lane_state(force_full_refresh)

if periodic["enabled"]:
    if spark.table(f"{lookup_schema}.pathology_hgnc_alias").limit(1).count() > 0:
        genetics = run_genetics(
            spark,
            config,
            include_proposed_profiles=True,
            full_reconcile=True,
            validate_stage_keys=False,
        )
    else:
        genetics = "SKIPPED_NO_HGNC"

    indications = run_indications(
        spark,
        config,
        include_proposed_rules=True,
        include_diagnosis_context=True,
        full_reconcile=True,
        validate_stage_keys=False,
    )
    amr = run_amr(
        spark,
        config,
        include_proposed_rules=True,
        full_reconcile=True,
        validate_stage_keys=False,
    )
else:
    skipped = {
        "status": "SKIPPED_PERIODIC_CADENCE",
        "cadence": periodic["cadence"],
        "iso_week": periodic["iso_week"],
    }
    genetics = skipped
    indications = skipped
    amr = skipped
create_views(spark, config)

result = {
    "status": "SUCCESS",
    "pipeline_run_id": value("pipeline_run_id"),
    "weekly_runner_version": WEEKLY_RUNNER_VERSION,
    "target_schema": target_schema,
    "lookup_schema": lookup_schema,
    "mode": core["mode"],
    "run_id": core["run_id"],
    "core": core.get("metrics", {}),
    "periodic_lanes": periodic,
    "genetics": genetics,
    "indications": indications,
    "amr": amr,
    "elapsed_seconds": round(time.monotonic() - started, 3),
    "doctrine_reference": "no-curation doctrine 2026-08-13, master plan v2.4",
}
dbutils.notebook.exit(json.dumps(result, sort_keys=True, default=str))


