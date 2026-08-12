# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Pipeline
# MAGIC
# MAGIC Single weekly entry point for the Bronze layer. Runs every domain pipeline in
# MAGIC dependency order and reports one consolidated result.
# MAGIC
# MAGIC Release `20260811_luna_wl_v1`.
# MAGIC
# MAGIC ## Defaults
# MAGIC
# MAGIC The defaults are the ordinary weekly ones: production target, incremental
# MAGIC (`force_full_refresh=false`), no cutover backups, and post-deployment checks on.
# MAGIC Scheduling, theatre, medication orders and PACS are enabled after their initial
# MAGIC production backfills complete. Referral/RTT and waiting-list are registered but
# MAGIC disabled by default until their production deployment/backfill gate is approved.
# MAGIC
# MAGIC ## Dependencies and failure handling
# MAGIC
# MAGIC Device, JAC, BloodTrack and Community read `map_*` outputs, so a Map failure blocks
# MAGIC them rather than letting them build on stale or partially published tables. Pipelines
# MAGIC that do not read Map still run. Every eligible step is attempted, all outcomes are
# MAGIC collected, and the run fails at the end if anything failed.
# MAGIC
# MAGIC ## Paths
# MAGIC
# MAGIC Sibling notebooks are resolved from this notebook's own folder at runtime, so the
# MAGIC release behaves identically in the staging folder and in production.

# COMMAND ----------

# MAGIC %run "./_bronze_common"

# COMMAND ----------

import builtins
import traceback
from datetime import datetime, timezone

_bronze_run_id = bronze_run_id()
_bronze_folder = bronze_notebook_folder()
_bronze_target_schema = bronze_value("target_schema", "4_prod.bronze")
_bronze_force_full_refresh = "true" if bronze_bool("force_full_refresh", False) else "false"
_bronze_create_cutover_backups = "true" if bronze_bool("create_cutover_backups", False) else "false"
_bronze_run_post_deployment_checks = "true" if bronze_bool("run_post_deployment_checks", True) else "false"
_bronze_run_snapshots = "true" if bronze_bool("run_snapshots", False) else "false"
_bronze_refresh_decodes = "true" if bronze_bool("refresh_decodes", False) else "false"

print(f"[BRONZE] release={_BRONZE_RELEASE_ID}")
print(f"[BRONZE] run_id={_bronze_run_id}")
print(f"[BRONZE] folder={_bronze_folder or '(unresolved; using relative paths)'}")
print(f"[BRONZE] target_schema={_bronze_target_schema}")
print(f"[BRONZE] force_full_refresh={_bronze_force_full_refresh}")
print(f"[BRONZE] create_cutover_backups={_bronze_create_cutover_backups}")
print(f"[BRONZE] run_post_deployment_checks={_bronze_run_post_deployment_checks}")
print(f"[BRONZE] run_snapshots={_bronze_run_snapshots}")
print(f"[BRONZE] refresh_decodes={_bronze_refresh_decodes}")

# A failed or in-progress production cutover leaves this lock active so the scheduled
# weekly orchestrator cannot enter a partially bootstrapped release. Direct feed runs
# made by the cutover notebook are intentionally unaffected.
_bronze_deployment_lock_table = (
    f"{bronze_control_schema(_bronze_target_schema)}.activity_bronze_release_lock"
)
if _bronze_target_schema.lower() == "4_prod.bronze" and bronze_table_exists(
    _bronze_deployment_lock_table
):
    _bronze_deployment_lock_qname = ".".join(
        "`" + part.replace("`", "``") + "`"
        for part in _bronze_deployment_lock_table.split(".")
    )
    _bronze_active_deployment_locks = spark.sql(
        f"""
        SELECT release_id, owner_run_id, acquired_at, expires_at
        FROM {_bronze_deployment_lock_qname}
        WHERE lock_name = 'activity_bronze_production_cutover'
          AND status = 'ACTIVE'
          AND expires_at > current_timestamp()
        LIMIT 1
        """
    ).collect()
    if _bronze_active_deployment_locks:
        _bronze_lock = _bronze_active_deployment_locks[0].asDict(recursive=True)
        raise RuntimeError(
            "The production activity-bronze deployment/backfill lock is active; "
            f"weekly run is held until cutover completes: {bronze_json(_bronze_lock)}"
        )

# COMMAND ----------

# Argument contracts, taken from each pipeline's own widget declarations.
#
# `map` and `device` share the full Map-style control set. Everything else takes a
# target schema; the pipelines that assert against an unguarded production write need
# `allow_production_write`, and the ones that support a bootstrap need
# `force_full_refresh`. PowerForms defaults to `define_only`, which is a silent no-op,
# so it is given an explicit `run_mode`.

_bronze_map_args = {
    "pipeline_run_id": _bronze_run_id,
    "force_full_refresh": _bronze_force_full_refresh,
    "create_cutover_backups": _bronze_create_cutover_backups,
    "run_post_deployment_checks": _bronze_run_post_deployment_checks,
}

_bronze_schema_args = {
    "pipeline_run_id": _bronze_run_id,
    "target_schema": _bronze_target_schema,
}

_bronze_schema_refresh_args = dict(
    _bronze_schema_args,
    force_full_refresh=_bronze_force_full_refresh,
)

_bronze_production_write_args = dict(
    _bronze_schema_refresh_args,
    allow_production_write="true",
)

_bronze_new_feed_args = dict(
    _bronze_schema_refresh_args,
    allow_production_write=(
        "true" if _bronze_target_schema.lower() == "4_prod.bronze" else "false"
    ),
    full_reconciliation="false",
    bootstrap_mode="false",
)

_bronze_waiting_list_args = dict(
    _bronze_new_feed_args,
    run_snapshots=_bronze_run_snapshots,
    refresh_decodes=_bronze_refresh_decodes,
)

_BRONZE_STEPS = [
    {
        "name": "map_pipeline",
        "notebook": "map_pipeline",
        "enabled_widget": "run_map_pipeline",
        "enabled_default": True,
        "arguments": _bronze_map_args,
        "requires": [],
        "timeout_seconds": 8 * 60 * 60,
    },
    {
        "name": "registry_pipeline",
        "notebook": "registry_pipeline",
        "enabled_widget": "run_registry_pipeline",
        "enabled_default": True,
        "arguments": dict(_bronze_schema_args, expect_idempotent="false"),
        "requires": [],
        "timeout_seconds": 4 * 60 * 60,
    },
    {
        "name": "endobase_pipeline",
        "notebook": "endobase_pipeline",
        "enabled_widget": "run_endobase_pipeline",
        "enabled_default": True,
        "arguments": _bronze_schema_refresh_args,
        "requires": [],
        "timeout_seconds": 4 * 60 * 60,
    },
    {
        "name": "slam_finance_pipeline",
        "notebook": "slam_finance_pipeline",
        "enabled_widget": "run_slam_finance_pipeline",
        "enabled_default": True,
        "arguments": _bronze_schema_refresh_args,
        "requires": [],
        "timeout_seconds": 4 * 60 * 60,
    },
    {
        "name": "cancer_pipeline",
        "notebook": "cancer_pipeline",
        "enabled_widget": "run_cancer_pipeline",
        "enabled_default": True,
        "arguments": _bronze_production_write_args,
        "requires": [],
        "timeout_seconds": 4 * 60 * 60,
    },
    {
        "name": "powerform_pipeline",
        "notebook": "powerform_pipeline",
        "enabled_widget": "run_powerform_pipeline",
        "enabled_default": False,
        "arguments": dict(_bronze_schema_refresh_args, run_mode="incremental"),
        "requires": [],
        "timeout_seconds": 6 * 60 * 60,
    },
    {
        "name": "device_pipeline",
        "notebook": "device_pipeline",
        "enabled_widget": "run_device_pipeline",
        "enabled_default": True,
        "arguments": _bronze_map_args,
        "requires": ["map_pipeline"],
        "timeout_seconds": 4 * 60 * 60,
    },
    {
        "name": "bloodtrack_pipeline",
        "notebook": "bloodtrack_pipeline",
        "enabled_widget": "run_bloodtrack_pipeline",
        "enabled_default": True,
        "arguments": _bronze_schema_args,
        "requires": ["map_pipeline"],
        "timeout_seconds": 4 * 60 * 60,
    },
    {
        "name": "jac_pipeline",
        "notebook": "jac_pipeline",
        "enabled_widget": "run_jac_pipeline",
        "enabled_default": True,
        "arguments": _bronze_schema_refresh_args,
        "requires": ["map_pipeline"],
        "timeout_seconds": 4 * 60 * 60,
    },
    {
        "name": "community_pipeline",
        "notebook": "community_pipeline",
        "enabled_widget": "run_community_pipeline",
        "enabled_default": True,
        "arguments": _bronze_production_write_args,
        "requires": ["map_pipeline"],
        "timeout_seconds": 4 * 60 * 60,
    },
    {
        "name": "scheduling_pipeline",
        "notebook": "scheduling_pipeline",
        "enabled_widget": "run_scheduling_pipeline",
        "enabled_default": False,
        "arguments": _bronze_production_write_args,
        "requires": ["map_pipeline"],
        "timeout_seconds": 4 * 60 * 60,
    },
    {
        "name": "theatre_pipeline",
        "notebook": "theatre_pipeline",
        "enabled_widget": "run_theatre_pipeline",
        "enabled_default": False,
        "arguments": _bronze_production_write_args,
        "requires": ["map_pipeline"],
        "timeout_seconds": 4 * 60 * 60,
    },
    {
        "name": "medication_order_pipeline",
        "notebook": "medication_order_pipeline",
        "enabled_widget": "run_medication_order_pipeline",
        "enabled_default": False,
        "arguments": _bronze_production_write_args,
        "requires": ["map_pipeline"],
        "timeout_seconds": 6 * 60 * 60,
    },
    {
        "name": "pacs_pipeline",
        "notebook": "pacs_pipeline",
        "enabled_widget": "run_pacs_pipeline",
        "enabled_default": False,
        "arguments": _bronze_production_write_args,
        "requires": [],
        "timeout_seconds": 6 * 60 * 60,
    },
    {
        "name": "referral_rtt_pipeline",
        "notebook": "referral_rtt_pipeline",
        "enabled_widget": "run_referral_rtt_pipeline",
        "enabled_default": False,
        "arguments": _bronze_new_feed_args,
        "requires": [],
        "timeout_seconds": 4 * 60 * 60,
    },
    {
        "name": "waiting_list_pipeline",
        "notebook": "waiting_list_pipeline",
        "enabled_widget": "run_waiting_list_pipeline",
        "enabled_default": False,
        "arguments": _bronze_waiting_list_args,
        "requires": [],
        "timeout_seconds": 8 * 60 * 60,
    },
]

_bronze_step_names = [step["name"] for step in _BRONZE_STEPS]
_bronze_duplicate_names = {
    name for name in _bronze_step_names if _bronze_step_names.count(name) > 1
}
if _bronze_duplicate_names:
    raise RuntimeError(f"Duplicate bronze step names: {sorted(_bronze_duplicate_names)}")

_bronze_unknown_requirements = {
    requirement
    for step in _BRONZE_STEPS
    for requirement in step["requires"]
    if requirement not in _bronze_step_names
}
if _bronze_unknown_requirements:
    raise RuntimeError(
        f"Bronze steps declare unknown dependencies: {sorted(_bronze_unknown_requirements)}"
    )

# COMMAND ----------

_bronze_results = []
_bronze_unavailable = set()
_bronze_started_at = datetime.now(timezone.utc)


def _bronze_step_path(step) -> str:
    return bronze_sibling(step["notebook"])


for _bronze_step in _BRONZE_STEPS:
    _bronze_name = _bronze_step["name"]
    _bronze_path = _bronze_step_path(_bronze_step)
    _bronze_enabled = bronze_bool(
        _bronze_step["enabled_widget"],
        _bronze_step["enabled_default"],
    )

    if not _bronze_enabled:
        print(f"[SKIP] {_bronze_name}: disabled by {_bronze_step['enabled_widget']}")
        _bronze_results.append({
            "step": _bronze_name,
            "status": "SKIPPED_DISABLED",
            "notebook": _bronze_path,
        })
        # A disabled prerequisite makes its consumers unrunnable too.
        _bronze_unavailable.add(_bronze_name)
        continue

    _bronze_blockers = sorted(
        requirement
        for requirement in _bronze_step["requires"]
        if requirement in _bronze_unavailable
    )
    if _bronze_blockers:
        print(f"[SKIP] {_bronze_name}: blocked by {_bronze_blockers}")
        _bronze_results.append({
            "step": _bronze_name,
            "status": "SKIPPED_BLOCKED",
            "notebook": _bronze_path,
            "blocked_by": _bronze_blockers,
        })
        _bronze_unavailable.add(_bronze_name)
        continue

    print(f"[RUN] {_bronze_name}: {_bronze_path}")
    _bronze_step_started = datetime.now(timezone.utc)
    try:
        _bronze_output = dbutils.notebook.run(
            _bronze_path,
            _bronze_step["timeout_seconds"],
            _bronze_step["arguments"],
        )
        _bronze_elapsed = (
            datetime.now(timezone.utc) - _bronze_step_started
        ).total_seconds()
        print(f"[SUCCESS] {_bronze_name}: {_bronze_elapsed:.0f}s")
        _bronze_results.append({
            "step": _bronze_name,
            "status": "SUCCESS",
            "notebook": _bronze_path,
            "elapsed_seconds": builtins.round(_bronze_elapsed, 1),
            "output": (_bronze_output or "")[:4000],
        })
    except Exception as _bronze_exc:
        _bronze_elapsed = (
            datetime.now(timezone.utc) - _bronze_step_started
        ).total_seconds()
        print(f"[FAILED] {_bronze_name}: {type(_bronze_exc).__name__}: {_bronze_exc}")
        print(traceback.format_exc()[-4000:])
        _bronze_results.append({
            "step": _bronze_name,
            "status": "FAILED",
            "notebook": _bronze_path,
            "elapsed_seconds": builtins.round(_bronze_elapsed, 1),
            "exception_type": type(_bronze_exc).__name__,
            "message": str(_bronze_exc)[:4000],
        })
        _bronze_unavailable.add(_bronze_name)

# COMMAND ----------

_bronze_failed = [
    result["step"] for result in _bronze_results if result["status"] == "FAILED"
]
_bronze_blocked = [
    result["step"] for result in _bronze_results if result["status"] == "SKIPPED_BLOCKED"
]
_bronze_succeeded = [
    result["step"] for result in _bronze_results if result["status"] == "SUCCESS"
]

_bronze_summary = {
    "pipeline": "bronze_pipeline",
    "release": _BRONZE_RELEASE_ID,
    "run_id": _bronze_run_id,
    "started_at": _bronze_started_at.isoformat(),
    "finished_at": bronze_utc_now(),
    "elapsed_seconds": builtins.round(
        (datetime.now(timezone.utc) - _bronze_started_at).total_seconds(), 1
    ),
    "force_full_refresh": _bronze_force_full_refresh,
    "run_snapshots": _bronze_run_snapshots,
    "refresh_decodes": _bronze_refresh_decodes,
    "target_schema": _bronze_target_schema,
    "succeeded": _bronze_succeeded,
    "failed": _bronze_failed,
    "blocked": _bronze_blocked,
    "steps": _bronze_results,
}

print(bronze_json(_bronze_summary))

if _bronze_failed or _bronze_blocked:
    raise RuntimeError(
        "bronze_pipeline completed with failures. "
        f"failed={_bronze_failed} blocked={_bronze_blocked}. "
        f"summary={bronze_json(_bronze_summary)}"
    )

dbutils.notebook.exit(bronze_json(_bronze_summary))


