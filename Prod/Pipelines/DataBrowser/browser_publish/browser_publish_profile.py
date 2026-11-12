# Databricks notebook source
# BrowserPublish / profile — one suppressed lookup.column_profile row per
# published column.
#
# Measures each staged view in at most three passes (one aggregate, one month
# count over date columns, one top-value count over 'values' columns) and
# writes only what browser_publish_disclosure allows. Tables over
# PROFILE_EXACT_MAX_ROWS are sampled; thresholds are checked on the sample
# before scaling, and sampled profiles carry sample_fraction.
# Serverless has no cache(), so each pass re-reads the (seeded) sample.
# browser_publish_config must already be %run (INTERNAL_COLUMNS, policy_key, qi)
# and TARGET_CATALOG defined; browser_publish_registry does both.

# COMMAND ----------
# MAGIC %run ./browser_publish_disclosure

# COMMAND ----------
import json

from pyspark.sql import Window
from pyspark.sql import functions as F

# Distinct names: this notebook shares the %run namespace with
# browser_publish_stats, whose EXACT_MAX_ROWS / SAMPLE_ROWS must not change.
PROFILE_EXACT_MAX_ROWS = 100_000_000
PROFILE_SAMPLE_ROWS = 20_000_000
PROFILE_TOP_K = 25

COLUMN_PROFILE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    dataset     STRING,
    run_id      STRING,
    table_name  STRING,
    column_name STRING,
    level       STRING,
    basis       STRING,
    profile     STRING,
    computed_at STRING
) USING DELTA
"""


def _stacked(frame, names, value):
    """One (c, v) row per named column per input row, so a single grouped
    count covers every column of the pass."""
    return (frame.select(F.explode(F.array(*[
                F.struct(F.lit(name).alias("c"), value(name).alias("v"))
                for name in names])).alias("s"))
            .select("s.c", "s.v").where("v IS NOT NULL"))


def measure(relation, columns, levels, rows):
    """Raw measurements for one published relation.

    columns: [(name, kind)]; levels: {name: level}; rows: the stats pass's
    row count, used only to decide whether to sample. Returns
    (raw_profiles dict, basis label, scale)."""
    frame, basis, scale = spark.table(relation), "exact", 1.0
    if rows > PROFILE_EXACT_MAX_ROWS:
        fraction = PROFILE_SAMPLE_ROWS / rows
        frame = frame.sample(withReplacement=False, fraction=fraction,
                             seed=20261014)
        basis, scale = f"sampled:{fraction:.6f}", 1.0 / fraction

    # Pass 1: counts, distincts, and numeric moments/percentiles.
    aggs = [F.count(F.lit(1)).alias("__rows")]
    for name, kind in columns:
        col = F.col(f"`{name}`")
        aggs.append(F.count(col).alias(f"{name}__nn"))
        if kind != "complex":
            aggs.append(F.approx_count_distinct(col).alias(f"{name}__distinct"))
        if kind == "numeric" and levels[name] in ("summary", "values"):
            number = col.cast("double")
            aggs.append(F.avg(number).alias(f"{name}__mean"))
            aggs.append(F.percentile_approx(
                number, [0.05, 0.25, 0.5, 0.75, 0.95], 10000
            ).alias(f"{name}__pct"))
    agg = frame.agg(*aggs).first().asDict()

    # Pass 2: monthly counts for date columns.
    dates = [name for name, kind in columns
             if kind == "date" and levels[name] in ("summary", "values")]
    month_rows = []
    if dates:
        months = _stacked(frame, dates, lambda name: F.date_format(
            F.col(f"`{name}`"), "yyyy-MM"))
        month_rows = [(r["c"], r["v"], r["n"]) for r in months.groupBy("c", "v")
                      .agg(F.count(F.lit(1)).alias("n")).collect()]

    # Pass 3: top values, only for columns the policy approves at 'values'.
    tops = [name for name, _ in columns if levels[name] == "values"]
    top_rows = []
    if tops:
        values = _stacked(frame, tops,
                          lambda name: F.col(f"`{name}`").cast("string"))
        counted = values.groupBy("c", "v").agg(F.count(F.lit(1)).alias("n"))
        ranked = (counted.withColumn("r", F.row_number().over(
                      Window.partitionBy("c").orderBy(F.desc("n"), F.asc("v"))))
                  .where(F.col("r") <= PROFILE_TOP_K))
        top_rows = [(r["c"], r["v"], r["n"]) for r in ranked.collect()]

    return raw_profiles(columns, agg, month_rows, top_rows), basis, scale


def build_profiles(objects, run_id, *, schema, column_types, policy, rows,
                   lookup, tag, dataset):
    """Profile every published column of the staged views and append the
    suppressed result to <lookup>.column_profile. Returns the column count."""
    table = f"{lookup}.column_profile"
    spark.sql(tag + COLUMN_PROFILE_DDL.format(table=table))
    spark.sql(tag + f"DELETE FROM {table} "
              f"WHERE dataset = '{dataset}' AND run_id = '{run_id}'")
    written = 0
    for object_name in objects:
        published = []
        for column, source_type in column_types[object_name].items():
            if column in INTERNAL_COLUMNS:
                continue
            entry = policy[policy_key(object_name, column)]
            if entry["action"] == "describe":
                continue
            published.append((column, published_type(entry, source_type),
                              entry))
        columns = [(c, column_kind(t)) for c, t, _ in published]
        levels = {c: publish_level(entry, t) for c, t, entry in published}
        raw, basis, scale = measure(
            f"{TARGET_CATALOG}.{schema}.{qi(object_name)}", columns, levels,
            rows[object_name])
        records = [(dataset, run_id, object_name, c, levels[c], basis,
                    json.dumps(disclose_profile(raw[c], levels[c], scale),
                               sort_keys=True))
                   for c, _ in columns]
        (spark.createDataFrame(records,
             "dataset string, run_id string, table_name string, "
             "column_name string, level string, basis string, profile string")
              .withColumn("computed_at", F.current_timestamp().cast("string"))
              .write.mode("append").saveAsTable(table))
        written += len(records)
        print(f"{object_name}: profiled {len(records)} columns ({basis})")
    return written

