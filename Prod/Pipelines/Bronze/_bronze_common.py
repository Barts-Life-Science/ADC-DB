# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Pipeline Shared Helpers
# MAGIC
# MAGIC Lightweight widget, JSON and table helpers shared by `bronze_pipeline` and the
# MAGIC domain pipelines it orchestrates. No Spark cache or persistence is used.
# MAGIC
# MAGIC Release `20260811_luna_wl_v2`.
# MAGIC
# MAGIC Defaults are the **weekly incremental** defaults: `force_full_refresh=false` and
# MAGIC `create_cutover_backups=false`. Backups are taken once by the release cutover
# MAGIC notebook, not on every weekly run.

# COMMAND ----------

import json
import re
import uuid
from datetime import datetime, timezone

_BRONZE_RELEASE_ID = "20260811_luna_wl_v2"

_BRONZE_WIDGET_DEFAULTS = {
    "pipeline_run_id": "",
    "force_full_refresh": "false",
    "create_cutover_backups": "false",
    "run_post_deployment_checks": "true",
    "target_schema": "4_prod.bronze",
    "run_map_pipeline": "true",
    "run_device_pipeline": "true",
    "run_registry_pipeline": "true",
    "run_bloodtrack_pipeline": "true",
    "run_jac_pipeline": "true",
    "run_endobase_pipeline": "true",
    "run_slam_finance_pipeline": "true",
    "run_cancer_pipeline": "true",
    "run_community_pipeline": "true",
    "run_scheduling_pipeline": "true",
    "run_theatre_pipeline": "true",
    "run_medication_order_pipeline": "true",
    "run_pacs_pipeline": "true",
    "run_referral_rtt_pipeline": "true",
    "run_waiting_list_pipeline": "true",
    "run_snapshots": "false",
    "refresh_decodes": "false",
    "run_powerform_pipeline": "true",
}

for _bronze_widget_name, _bronze_widget_default in _BRONZE_WIDGET_DEFAULTS.items():
    try:
        dbutils.widgets.text(_bronze_widget_name, _bronze_widget_default)
    except Exception:
        pass


def bronze_value(name: str, default: str = "") -> str:
    try:
        value = dbutils.widgets.get(name)
        return default if value is None else str(value).strip()
    except Exception:
        return default


def bronze_bool(name: str, default: bool = False) -> bool:
    return bronze_value(name, str(default).lower()).lower() in {"1", "true", "yes", "y"}


def bronze_control_schema(target_schema: str = "4_prod.bronze") -> str:
    """Control-plane schema for state, audit, manifests, queues and run logs."""
    target = str(target_schema).strip()
    return "6_mgmt.bronze" if target.lower() == "4_prod.bronze" else target


def bronze_lookup_schema(target_schema: str = "4_prod.bronze") -> str:
    """Reference-plane schema for governed mappings used by Bronze pipelines."""
    target = str(target_schema).strip()
    return "3_lookup.omop" if target.lower() == "4_prod.bronze" else target


def bronze_active_spark():
    """Return Databricks' injected Spark session captured by this notebook."""
    return _BRONZE_SPARK


def bronze_table_exists(table_name: str) -> bool:
    return bronze_active_spark().catalog.tableExists(table_name)


def bronze_json(value) -> str:
    return json.dumps(value, default=str, sort_keys=True)


def bronze_run_id() -> str:
    requested = bronze_value("pipeline_run_id", "")
    return requested or str(uuid.uuid4())


def bronze_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def bronze_notebook_path() -> str:
    """Absolute workspace path of the notebook currently executing."""
    try:
        return (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
    except Exception:
        return ""


def bronze_notebook_folder() -> str:
    """Folder containing the notebook currently executing.

    `bronze_pipeline` uses this so the release runs identically from the staging
    folder and from production without any absolute path edits.
    """
    path = bronze_notebook_path()
    return path.rsplit("/", 1)[0] if "/" in path else ""


def bronze_sibling(name: str) -> str:
    """Resolve a notebook name to an absolute path in the caller's own folder."""
    folder = bronze_notebook_folder()
    return f"{folder}/{name}" if folder else f"./{name}"

# COMMAND ----------

# Single-layer Bronze contract helpers.
#
# The migration notebook materialises this registry in the target schema from the
# approved canonical-column manifest. Primary-table writes fail closed if the
# registry is absent or if a retained column cannot be produced.
from pyspark.sql import functions as _bronze_F
from pyspark.sql import types as _bronze_T

# Capture Databricks' injected session in a notebook-owned name. Functions loaded
# through %run can retain their module namespace after the injected `spark` name
# is no longer present in that namespace.
_BRONZE_SPARK = spark

_BRONZE_PRIMARY_TABLES = {
    "map_address",
    "map_address_epc",
    "map_care_site",
    "map_coded_events",
    "map_community_care_activity",
    "map_community_care_contact",
    "map_community_patient_link",
    "map_date_events",
    "map_death",
    "map_diagnosis",
    "map_encounter",
    "map_family_history",
    "map_implant_details",
    "map_mat_birth",
    "map_mat_pregnancy",
    "map_mat_vte_assessment",
    "map_med_admin",
    "map_medical_personnel",
    "map_nomen_events",
    "map_numeric_events",
    "map_pathology",
    "map_patient_journey",
    "map_person",
    "map_problem",
    "map_procedure",
    "map_text_events",
}
_BRONZE_CONTRACT_CACHE = {}


_BRONZE_LEGACY_CANONICAL_SUFFIX = "__" + "canonical"


def bronze_base_table_name(table_name: str) -> str:
    base_name = (
        str(table_name).rsplit(".", 1)[-1]
        .removesuffix(_BRONZE_LEGACY_CANONICAL_SUFFIX)
    )
    # Release candidates use the same retained-column contract as their live table.
    # The suffix is deliberately stripped only at the end, so auxiliary tables such
    # as map_x__candidate_release_pipeline_state are never mistaken for primaries.
    return re.sub(r"__candidate_[A-Za-z0-9_-]+$", "", base_name)


def bronze_is_primary_table(table_name: str) -> bool:
    return bronze_base_table_name(table_name).lower() in _BRONZE_PRIMARY_TABLES


def bronze_contract_table(table_name: str) -> str:
    parts = str(table_name).split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected a three-part table name, got {table_name!r}")
    return f"{bronze_control_schema(f'{parts[0]}.{parts[1]}')}.bronze_single_layer_contract"


def bronze_contract_rows(table_name: str):
    if not bronze_is_primary_table(table_name):
        return []
    key = str(table_name).lower()
    if key in _BRONZE_CONTRACT_CACHE:
        return _BRONZE_CONTRACT_CACHE[key]

    base_name = bronze_base_table_name(table_name).lower()
    registry = bronze_contract_table(table_name)
    if bronze_table_exists(registry):
        rows = (
            bronze_active_spark().table(registry)
            .filter(_bronze_F.lower(_bronze_F.col("target_table")) == base_name)
            .orderBy("ordinal_position")
            .select(
                "column_name",
                "ordinal_position",
                "data_type",
                "column_comment",
            )
            .collect()
        )
    elif bronze_table_exists("8_dev.bronze.canonical_column_manifest"):
        # Development fallback. Production deliberately has no fallback: the
        # one-shot migration must install its pinned contract before deployment.
        if str(table_name).lower().startswith("4_prod."):
            raise RuntimeError(
                f"Missing production single-layer contract registry {registry}"
            )
        rows = (
            bronze_active_spark().table("8_dev.bronze.canonical_column_manifest")
            .filter(
                (_bronze_F.lower(_bronze_F.regexp_replace(
                    _bronze_F.col("table_name"),
                    _BRONZE_LEGACY_CANONICAL_SUFFIX + "$",
                    ""
                )) == base_name)
                & (_bronze_F.upper(_bronze_F.col("recommendation")) == "KEEP")
            )
            .orderBy("ordinal_position")
            .select(
                "column_name",
                "ordinal_position",
                "data_type",
                "column_comment",
            )
            .collect()
        )
    else:
        raise RuntimeError(
            f"No single-layer contract is available for {table_name}"
        )

    if not rows:
        raise RuntimeError(f"Empty single-layer contract for {table_name}")
    _BRONZE_CONTRACT_CACHE[key] = rows
    return rows


def bronze_contract_column_names(table_name: str):
    return [str(row["column_name"]) for row in bronze_contract_rows(table_name)]


def bronze_contract_column_set(table_name: str):
    return {
        column_name.lower()
        for column_name in bronze_contract_column_names(table_name)
    }


def bronze_contract_schema(table_name: str):
    fields = []
    for row in bronze_contract_rows(table_name):
        metadata = {}
        if row["column_comment"]:
            metadata["comment"] = str(row["column_comment"])
        fields.append(
            _bronze_T.StructField(
                str(row["column_name"]),
                _bronze_T._parse_datatype_string(str(row["data_type"])),
                True,
                metadata,
            )
        )
    return _bronze_T.StructType(fields)


def bronze_project_contract(frame, table_name: str):
    """Project a primary-table frame to its exact retained-column contract."""
    if not bronze_is_primary_table(table_name):
        return frame
    source_columns = {}
    for column_name in frame.columns:
        lowered = column_name.lower()
        if lowered in source_columns and source_columns[lowered] != column_name:
            raise RuntimeError(
                f"Case-insensitive duplicate source columns for {table_name}: "
                f"{source_columns[lowered]!r}, {column_name!r}"
            )
        source_columns[lowered] = column_name

    expressions = []
    missing = []
    for row in bronze_contract_rows(table_name):
        expected = str(row["column_name"])
        actual = source_columns.get(expected.lower())
        if actual is None:
            missing.append(expected)
            continue
        expressions.append(
            _bronze_F.col(f"`{actual}`")
            .cast(str(row["data_type"]))
            .alias(expected)
        )
    if missing:
        raise RuntimeError(
            f"{table_name} cannot satisfy its retained-column contract; "
            f"missing columns: {missing}"
        )
    return frame.select(*expressions)


def bronze_merge_assignments(frame, table_name: str, source_alias: str = "s"):
    """Return explicit Delta MERGE assignments for every retained column."""
    projected = bronze_project_contract(frame, table_name)
    assignments = {
        column_name: f"{source_alias}.`{column_name}`"
        for column_name in projected.columns
    }
    return projected, assignments


def bronze_assert_primary_contract(table_name: str):
    if not bronze_table_exists(table_name):
        raise RuntimeError(f"Missing primary Bronze table {table_name}")
    expected = [
        (str(row["column_name"]), str(row["data_type"]).lower())
        for row in bronze_contract_rows(table_name)
    ]
    actual = [
        (field.name, field.dataType.simpleString().lower())
        for field in bronze_active_spark().table(table_name).schema.fields
    ]
    if actual != expected:
        raise RuntimeError(
            f"Single-layer schema drift for {table_name}: "
            f"expected={expected}, actual={actual}"
        )
    return {
        "table": table_name,
        "columns": len(actual),
        "status": "PASS",
    }


def bronze_assert_all_primary_contracts(catalog_schema: str = "4_prod.bronze"):
    results = []
    for base_name in sorted(_BRONZE_PRIMARY_TABLES):
        table_name = f"{catalog_schema}.{base_name}"
        results.append(bronze_assert_primary_contract(table_name))
    return results


# Schema evolution is intentionally disabled for steady-state writes. The
# migration/release notebooks perform explicit schema changes under their own
# guarded modes.
try:
    bronze_active_spark().conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
except Exception:
    pass


