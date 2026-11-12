# Databricks notebook source
# BrowserPublish / stats — one meta.column_stats row per published column.
#
# One moments aggregation per table. Serverless compute does not support
# cache/persist/rdd, so each relation is profiled without intermediate state.

# COMMAND ----------
# MAGIC %run ./browser_publish_config

# COMMAND ----------
import json

from pyspark.sql import functions as F

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"])
dbutils.widgets.dropdown("action", "none", ["none", "build"])
dbutils.widgets.text("run_id", "", "run_id")

ENV = dbutils.widgets.get("env")
ACTION = dbutils.widgets.get("action")
RUN_ID = dbutils.widgets.get("run_id")

dbutils.widgets.dropdown("dataset", "gold", sorted(PROFILES))
DATASET = dbutils.widgets.get("dataset")
PROFILE = PROFILES[DATASET]
_TARGETS = targets_for(PROFILE, ENV)
TARGET_CATALOG, TARGET_SCHEMA = _TARGETS["catalog"], _TARGETS["schema"]
META_SCHEMA = _TARGETS["meta"]
TAG = f"/* dbrowse ctx=system u={DATASET}-publish feature={DATASET}-stats */ "

EXACT_MAX_ROWS = 100_000_000
SAMPLE_ROWS = 5_000_000

# Match the existing d_databrowser.meta.column_stats contract so every dataset's rows
# coexist with current datasets. Distinct count remains inside the stats JSON.
COLUMN_STATS_DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{META_SCHEMA}.column_stats (
    dataset                  STRING,
    run_id                   STRING,
    table_name               STRING,
    column_name              STRING,
    basis                    STRING,
    suppression_threshold    INT,
    source_row_count_rounded BIGINT,
    computed_at              STRING,
    stats                    STRING
) USING DELTA
"""


def row_count(object_name, schema=None):
    return spark.sql(
        TAG + "SELECT COUNT(*) AS n FROM "
        f"{TARGET_CATALOG}.{schema or TARGET_SCHEMA}.{qi(object_name)}"
    ).first()["n"]


def frame_for(object_name, rows, schema=None):
    """Return the frame to profile and its precision basis label."""
    df = spark.table(
        f"{TARGET_CATALOG}.{schema or TARGET_SCHEMA}.{object_name}")
    if rows <= EXACT_MAX_ROWS:
        return df, "exact"
    fraction = min(1.0, SAMPLE_ROWS / max(rows, 1))
    return (df.sample(withReplacement=False, fraction=fraction, seed=20260901),
            f"sampled:{fraction:.6f}")


def profile(object_name, rows, schema=None):
    """Collect null/distinct/min/max statistics in one aggregation."""
    df, basis = frame_for(object_name, rows, schema)
    columns = [field.name for field in df.schema.fields]
    aggs = []
    for column in columns:
        col = F.col(f"`{column}`")
        aggs += [
            F.count(F.when(col.isNull(), 1)).alias(f"{column}__nulls"),
            F.approx_count_distinct(col).alias(f"{column}__distinct"),
            F.min(col).cast("string").alias(f"{column}__min"),
            F.max(col).cast("string").alias(f"{column}__max"),
        ]
    agg = df.agg(F.count(F.lit(1)).alias("__n"), *aggs).first().asDict()
    return [{
        "table_name": object_name,
        "column_name": column,
        "basis": basis,
        "stats": json.dumps({
            "profiled_rows": agg["__n"],
            "null_count": agg[f"{column}__nulls"],
            "distinct_count": agg[f"{column}__distinct"],
            "min": agg[f"{column}__min"],
            "max": agg[f"{column}__max"],
        }),
        "source_row_count_rounded": rounded_row_count(
            rows, person_level="person_id" in columns),
    } for column in columns]


def build_stats(objects, run_id, schema=None):
    assert run_id, "run_id is required"
    spark.sql(TAG + f"CREATE SCHEMA IF NOT EXISTS "
              f"{TARGET_CATALOG}.{META_SCHEMA}")
    spark.sql(TAG + COLUMN_STATS_DDL)
    spark.sql(TAG + f"DELETE FROM {TARGET_CATALOG}.{META_SCHEMA}.column_stats "
              f"WHERE dataset = '{DATASET}' AND run_id = '{run_id}'")
    records = []
    for object_name in objects:
        rows = row_count(object_name, schema)
        object_records = profile(object_name, rows, schema)
        records += object_records
        print(f"{object_name}: {rows:,} rows, {len(object_records)} columns")
    frame = spark.createDataFrame([
        (DATASET, run_id, record["table_name"], record["column_name"],
         record["basis"], 10, record["source_row_count_rounded"],
         record["stats"])
        for record in records
    ], "dataset string, run_id string, table_name string, column_name string, "
       "basis string, suppression_threshold int, "
       "source_row_count_rounded bigint, stats string")
    (frame.withColumn("computed_at", F.current_timestamp().cast("string"))
          .select("dataset", "run_id", "table_name", "column_name", "basis",
                  "suppression_threshold", "source_row_count_rounded",
                  "computed_at", "stats")
          .write.mode("append")
          .saveAsTable(f"{TARGET_CATALOG}.{META_SCHEMA}.column_stats"))
    print(f"wrote {len(records)} column_stats rows for run {run_id}")
    return records

