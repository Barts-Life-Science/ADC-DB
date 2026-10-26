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


# COMMAND ----------

# METADATA_KEY_UNIQUENESS_V1 (2026-10-25)
# Every internal <schema>._*_metadata MV must hold one row per join key: gold _qc() left-joins
# it on those keys, so a duplicated key copies the gold row. A ROW_BASED silver update on
# 2026-10-17 left 5.47M duplicates in _clinical_pathology_order_metadata and 5.42M duplicated
# rows reached gold. This cell is read-only. It counts duplicate keys in a hash-bucket sample:
# both copies of a key share a bucket, so the sample is unbiased and the estimate is
# sample duplicates x modulus. Every row is still read (key columns only); modulus 1 makes
# the count exact at the price of a much larger distinct-count.
import ast as _ast

dbutils.widgets.text("metadata_schema", "4_prod.tmp")
dbutils.widgets.text("metadata_check_modulus", "100")
dbutils.widgets.dropdown("metadata_check_action", "fail", ["fail", "warn", "skip"])
_METADATA_SCHEMA = dbutils.widgets.get("metadata_schema")
_METADATA_MODULUS = int(dbutils.widgets.get("metadata_check_modulus"))
_METADATA_ACTION = dbutils.widgets.get("metadata_check_action")
_GOLD_SHARED_FILE = "/Workspace/Shared/ADC-DB/Prod/Pipelines/Gold/gold_journey_shared.py"


def _gold_lifecycle_join_keys(path):
    """Read GOLD_V2_LIFECYCLE join keys from the gold shared file by AST; {} if unreadable."""
    try:
        with open(path) as handle:
            tree = _ast.parse(handle.read())
    except Exception as error:
        print(f"gold lifecycle keys unavailable ({error}); falling back to table comments")
        return {}
    keys = {}
    for node in tree.body:
        if not isinstance(node, _ast.Assign):
            continue
        target = node.targets[0]
        if isinstance(target, _ast.Name) and target.id == "GOLD_V2_LIFECYCLE":
            for table, spec in _ast.literal_eval(node.value).items():
                keys[table] = spec["join_keys"]
        elif (isinstance(target, _ast.Subscript) and isinstance(target.value, _ast.Name)
              and target.value.id == "GOLD_V2_LIFECYCLE"):
            keys[_ast.literal_eval(target.slice)] = _ast.literal_eval(node.value)["join_keys"]
    return keys


def _metadata_join_keys(schema):
    """Map every _*_metadata MV in the schema to the keys gold joins it on.

    Order of preference: gold GOLD_V2_LIFECYCLE keys; the first key named in the table
    comment ("Join keys: a, b."); the table's only *_key column. Tables with none are
    reported as unchecked rather than guessed.
    """
    catalog, schema_name = schema.split(".")
    gold_keys = _gold_lifecycle_join_keys(_GOLD_SHARED_FILE)
    tables = spark.sql(f"""
        SELECT table_name, regexp_extract(comment, 'Join keys: ([^.]*)', 1) AS comment_keys
        FROM {_qtable(catalog)}.information_schema.tables
        WHERE table_schema = '{schema_name}'
          AND startswith(table_name, '_') AND endswith(table_name, '_metadata')
    """).collect()
    key_columns = {}
    for row in spark.sql(f"""
        SELECT table_name, column_name FROM {_qtable(catalog)}.information_schema.columns
        WHERE table_schema = '{schema_name}'
          AND startswith(table_name, '_') AND endswith(table_name, '_metadata')
          AND endswith(column_name, '_key')
    """).collect():
        key_columns.setdefault(row.table_name, []).append(row.column_name)
    chosen = {}
    for row in tables:
        product = row.table_name[1:-len("_metadata")]
        if product in gold_keys:
            chosen[row.table_name] = gold_keys[product]
        elif row.comment_keys:
            chosen[row.table_name] = [row.comment_keys.split(",")[0].strip()]
        elif len(key_columns.get(row.table_name, [])) == 1:
            chosen[row.table_name] = key_columns[row.table_name]
        else:
            chosen[row.table_name] = None
    return chosen


def _metadata_duplicate_scan(schema, modulus):
    """One read-only UNION ALL query: sampled rows and distinct keys per metadata MV."""
    chosen = _metadata_join_keys(schema)
    parts = []
    for table, keys in sorted(chosen.items()):
        if not keys:
            continue
        key_list = ", ".join(_qident(column) for column in keys)
        not_null = " AND ".join(f"{_qident(column)} IS NOT NULL" for column in keys)
        parts.append(
            f"SELECT '{table}' AS table_name, '{'+'.join(keys)}' AS join_keys, count(*) AS sample_rows, "
            f"count(DISTINCT struct({key_list})) AS sample_keys "
            f"FROM {_qtable(schema)}.{_qident(table)} "
            f"WHERE {not_null} AND pmod(xxhash64({key_list}), {modulus}) = 0"
        )
    rows = spark.sql(" UNION ALL ".join(parts)).collect() if parts else []
    findings = []
    for row in rows:
        duplicates = int(row.sample_rows) - int(row.sample_keys)
        if duplicates:
            findings.append({
                "table": f"{schema}.{row.table_name}",
                "join_keys": row.join_keys,
                "sample_duplicate_rows": duplicates,
                "estimated_duplicate_rows": duplicates * modulus,
            })
    return {
        "schema": schema,
        "modulus": modulus,
        "tables_checked": len(rows),
        "tables_unchecked": sorted(table for table, keys in chosen.items() if not keys),
        "tables_with_duplicate_keys": findings,
    }


if _METADATA_ACTION == "skip":
    print("metadata key uniqueness check skipped by widget")
else:
    _metadata_result = _metadata_duplicate_scan(_METADATA_SCHEMA, _METADATA_MODULUS)
    print(json.dumps({"metadata_key_uniqueness": _metadata_result}, indent=2, sort_keys=True))
    if _metadata_result["tables_with_duplicate_keys"] and _METADATA_ACTION == "fail":
        raise AssertionError(
            "Internal metadata MVs hold duplicate join keys; gold _qc() would copy rows. "
            "Full-refresh the named MVs (and their gold twins) before the gold update: "
            + json.dumps(_metadata_result["tables_with_duplicate_keys"])
        )

