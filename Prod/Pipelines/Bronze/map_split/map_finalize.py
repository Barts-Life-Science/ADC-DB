# Databricks notebook source
# Map split final validation. Job-task use only.

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
    "map_common_bootstrap": "false",
    "expected_components_json": "",
    "source_manifest_json": "",
    "source_manifest_hash": "",
    "effective_full_refresh": "",
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

import hashlib as _map_split_hashlib

_map_split_manifest_raw = str(dbutils.widgets.get("source_manifest_json") or "").strip()
_map_split_manifest_hash = str(dbutils.widgets.get("source_manifest_hash") or "").strip()
if not _map_split_manifest_raw or not _map_split_manifest_hash:
    raise ValueError("Map split finalizer is missing the initializer-pinned source manifest")
_map_split_manifest = {
    str(key): int(value)
    for key, value in json.loads(_map_split_manifest_raw).items()
}
_map_split_computed_hash = _map_split_hashlib.sha256(
    json.dumps(_map_split_manifest, sort_keys=True).encode("utf-8")
).hexdigest()[:16]
if _map_split_computed_hash != _map_split_manifest_hash:
    raise ValueError({
        "message": "Map split finalizer source manifest hash mismatch",
        "expected": _map_split_manifest_hash,
        "actual": _map_split_computed_hash,
    })
_PIPELINE_SOURCE_MANIFEST = _map_split_manifest
_PIPELINE_SOURCE_MANIFEST_HASH = _map_split_manifest_hash
_map_split_effective_full_refresh = str(
    dbutils.widgets.get("effective_full_refresh") or ""
).strip().lower()
if _map_split_effective_full_refresh not in {"true", "false"}:
    raise ValueError("effective_full_refresh must be supplied by map_initialize")
_PIPELINE_FORCE_FULL_REFRESH = _map_split_effective_full_refresh == "true"
_PIPELINE_FULL_REFRESH = _PIPELINE_FORCE_FULL_REFRESH

_COMPONENT_TARGETS = {
    "map_address": ["4_prod.bronze.map_address", "4_prod.bronze.map_address_epc"],
    "map_person": ["4_prod.bronze.map_person"],
    "map_care_site": ["4_prod.bronze.map_care_site"],
    "map_medical_personnel": ["4_prod.bronze.map_medical_personnel"],
    "map_encounter": ["4_prod.bronze.map_encounter"],
    "map_diagnosis": ["4_prod.bronze.map_diagnosis"],
    "map_problem": ["4_prod.bronze.map_problem"],
    "map_med_admin": ["4_prod.bronze.map_med_admin"],
    "map_procedure": ["4_prod.bronze.map_procedure"],
    "map_death": ["4_prod.bronze.map_death"],
    "map_numeric_events": ["4_prod.bronze.map_numeric_events"],
    "map_text_events": ["4_prod.bronze.map_text_events"],
    "map_date_events": ["4_prod.bronze.map_date_events"],
    "map_nomen_events": ["4_prod.bronze.map_nomen_events"],
    "map_coded_events": ["4_prod.bronze.map_coded_events"],
    "map_mat_pregnancy": ["4_prod.bronze.map_mat_pregnancy"],
    "map_mat_birth": ["4_prod.bronze.map_mat_birth"],
    "map_mat_vte_assessment": ["4_prod.bronze.map_mat_vte_assessment"],
    "map_family_history": ["4_prod.bronze.map_family_history"],
    "map_patient_journey": ["4_prod.bronze.map_patient_journey"],
    "map_implant_details": ["4_prod.bronze.map_implant_details"],
    "map_pathology": ["4_prod.bronze.map_pathology"],
}
_expected_raw = str(dbutils.widgets.get("expected_components_json") or "").strip()
_EXPECTED_COMPONENTS = (
    list(_COMPONENT_TARGETS)
    if not _expected_raw
    else list(json.loads(_expected_raw))
)
_unknown = sorted(set(_EXPECTED_COMPONENTS) - set(_COMPONENT_TARGETS))
if _unknown:
    raise ValueError(f"Unknown expected Map components: {_unknown}")

_failures = []
_states = {}
for _component in _EXPECTED_COMPONENTS:
    for _target in _COMPONENT_TARGETS[_component]:
        _state = _pipeline_target_state(_target)
        _states[_target] = _state
        if not _state:
            _failures.append({"target": _target, "reason": "missing_target_state"})
            continue
        if _state.get("component") != _component:
            _failures.append({
                "target": _target,
                "reason": "wrong_component",
                "expected": _component,
                "actual": _state.get("component"),
            })
        if _state.get("run_id") != _PIPELINE_RUN_ID:
            _failures.append({
                "target": _target,
                "reason": "wrong_run_id",
                "expected": _PIPELINE_RUN_ID,
                "actual": _state.get("run_id"),
            })
if _failures:
    raise RuntimeError(
        "Map split completion-state validation failed: "
        + json.dumps(_failures, sort_keys=True)
    )

_post_checks = None
if _pipeline_optional_bool_parameter("run_post_deployment_checks", True):
    _post_checks = _pipeline_post_deployment_checks()
_pipeline_audit(
    None,
    "RUN_SUCCESS",
    {
        "execution_mode": "split_components",
        "components": _EXPECTED_COMPONENTS,
        "target_count": len(_states),
        "post_deployment_checks": _post_checks,
    },
)
_result = {
    "status": "SUCCESS",
    "pipeline": "map_pipeline_split",
    "run_id": _PIPELINE_RUN_ID,
    "full_refresh": _PIPELINE_FULL_REFRESH,
    "components": _EXPECTED_COMPONENTS,
    "target_count": len(_states),
    "post_deployment_checks": _post_checks,
}
print(json.dumps(_result, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(_result, sort_keys=True, default=str))

