# Databricks notebook source
# orders_spine_cdf_recovery
# One-off bounded recovery for map_orders after full-rebuild checkpoints were cleared.
# Reads Delta CDF from the last known-good source versions and rebuilds only affected ORDER_IDs.

import json
import re
import traceback
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.window import Window

for name, default in {
    "recovery_job_run_id": "",
}.items():
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass

RECOVERY_JOB_RUN_ID = dbutils.widgets.get("recovery_job_run_id").strip()
assert RECOVERY_JOB_RUN_ID, "recovery_job_run_id is required"

TARGET = "4_prod.bronze.map_orders"
ORDERS_SOURCE = "4_prod.raw.mill_orders"
MED_SOURCE = "4_prod.bronze.map_medication_order"
CATALOG_SOURCE = "3_lookup.mill.mill_order_catalog"
CV_SOURCE = "3_lookup.mill.mill_code_value"
SOURCES = [ORDERS_SOURCE, MED_SOURCE, CATALOG_SOURCE, CV_SOURCE]
PIPELINE = "a8a_orders_spine"
WATERMARK_TABLE = "6_mgmt.bronze.s3_watermarks"
AUDIT_TABLE = "8_dev.bronze.orders_spine_cdf_recovery_audit"

BASELINE_SOURCE_VERSIONS = {
    ORDERS_SOURCE: 4224,
    MED_SOURCE: 143,
    CATALOG_SOURCE: 872,
    CV_SOURCE: 893,
}
BASELINE_WATERMARKS = {
    ORDERS_SOURCE: "2026-08-21 02:29:57.196",
    MED_SOURCE: "2026-08-21 02:29:57.196",
    CATALOG_SOURCE: "2026-08-21 02:01:56.790",
    CV_SOURCE: "2026-08-21 02:18:24.436",
}
BASELINE_TARGET_VERSION = 50
CANCELED_RUN_ID = "149022305474578"
SOURCE_STATE_PROPERTY = "bronze_completeness.source_versions_json"
MAX_LOOKUP_CHANGE_KEYS = 5000
MAX_AFFECTED_ORDER_IDS = 50000000

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "4096")

operation_id = uuid.uuid4().hex
suffix = re.sub(r"[^0-9a-zA-Z]", "", RECOVERY_JOB_RUN_ID)[-18:] + "_" + operation_id[:8]
KEYS_TABLE = f"8_dev.bronze.orders_spine_cdf_keys_{suffix}"
OUTPUT_TABLE = f"8_dev.bronze.orders_spine_cdf_output_{suffix}"

result = {
    "operation_id": operation_id,
    "recovery_job_run_id": RECOVERY_JOB_RUN_ID,
    "status": "STARTING",
    "target": TARGET,
    "baseline_source_versions": BASELINE_SOURCE_VERSIONS,
    "keys_table": KEYS_TABLE,
    "output_table": OUTPUT_TABLE,
    "started_at_utc": datetime.now(timezone.utc).isoformat(),
}

def table_version(table_name):
    return int(spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").collect()[0]["version"])

def source_at(table_name, version):
    return spark.read.option("versionAsOf", int(version)).table(table_name)

def empty_bigint(name):
    return spark.range(0).select(F.lit(None).cast("bigint").alias(name)).limit(0)

def cdf(table_name, baseline_version, current_version):
    if current_version <= baseline_version:
        return None
    return (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", int(baseline_version) + 1)
        .option("endingVersion", int(current_version))
        .table(table_name)
    )

def changed_bigint_keys(table_name, source_column, output_column, baseline_version, current_version):
    changes = cdf(table_name, baseline_version, current_version)
    if changes is None:
        return empty_bigint(output_column)
    return (
        changes.select(F.col(source_column).cast("bigint").alias(output_column))
        .where(F.col(output_column).isNotNull())
        .distinct()
    )

def changed_lookup_values(table_name, source_column, baseline_version, current_version):
    changes = cdf(table_name, baseline_version, current_version)
    if changes is None:
        return []
    rows = (
        changes.select(F.col(source_column).cast("bigint").alias("value"))
        .where(F.col("value").isNotNull())
        .distinct()
        .limit(MAX_LOOKUP_CHANGE_KEYS + 1)
        .collect()
    )
    assert len(rows) <= MAX_LOOKUP_CHANGE_KEYS, {
        "message": "Lookup CDF unexpectedly large; refusing to embed a giant IN predicate",
        "source": table_name,
        "distinct_keys_seen": len(rows),
        "limit": MAX_LOOKUP_CHANGE_KEYS,
    }
    return sorted({int(row["value"]) for row in rows})

def cdf_boundary(table_name, clock_column, baseline_version, current_version, baseline_value):
    baseline = datetime.fromisoformat(baseline_value).replace(tzinfo=None)
    changes = cdf(table_name, baseline_version, current_version)
    if changes is None:
        return baseline
    row = (
        changes.where(F.col("_change_type").isin("insert", "update_postimage"))
        .agg(F.max(F.col(clock_column)).alias("boundary"))
        .collect()[0]
    )
    observed = row["boundary"]
    if observed is None:
        return baseline
    return max(baseline, observed)

def latest_per_key(df, key_columns, order_columns):
    window = Window.partitionBy(*key_columns).orderBy(*order_columns)
    return df.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")

def dq_columns(df, date_columns):
    out = df
    for column in date_columns:
        future = F.col(column) > F.current_timestamp()
        sentinel = F.col(column) < F.lit("1901-01-01").cast("timestamp")
        out = (
            out.withColumn(
                f"{column}_FUTURE_IND",
                F.when(F.col(column).isNull(), F.lit(None)).otherwise(future),
            )
            .withColumn(
                f"{column}_SENTINEL_IND",
                F.when(F.col(column).isNull(), F.lit(None)).otherwise(sentinel),
            )
            .withColumn(
                f"{column}_CLEAN",
                F.when(future | sentinel, F.lit(None).cast("timestamp")).otherwise(F.col(column)),
            )
        )
    return out

def set_watermark(source, value):
    stamp = value.strftime("%Y-%m-%d %H:%M:%S.%f")
    source_sql = source.replace("'", "''")
    spark.sql(
        f"""
        MERGE INTO {WATERMARK_TABLE} t
        USING (
          SELECT '{PIPELINE}' pipeline,
                 '{source_sql}' source,
                 TIMESTAMP'{stamp}' max_adc_updt,
                 current_timestamp() updated_at
        ) s
        ON t.pipeline=s.pipeline AND t.source=s.source
        WHEN MATCHED THEN UPDATE SET
          t.max_adc_updt=s.max_adc_updt,
          t.updated_at=s.updated_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )

def write_audit(payload):
    spark.createDataFrame(
        [(
            operation_id,
            RECOVERY_JOB_RUN_ID,
            payload["status"],
            json.dumps(payload.get("current_source_versions"), sort_keys=True),
            int(payload.get("affected_order_ids") or 0),
            int(payload.get("output_rows") or 0),
            json.dumps(payload, sort_keys=True, default=str),
        )],
        """
        operation_id STRING,
        recovery_job_run_id STRING,
        status STRING,
        current_source_versions_json STRING,
        affected_order_ids BIGINT,
        output_rows BIGINT,
        result_json STRING
        """,
    ).withColumn("recorded_at", F.current_timestamp()).write.mode("append").saveAsTable(AUDIT_TABLE)

try:
    assert spark.catalog.tableExists(TARGET), f"Missing existing target {TARGET}"

    target_history = spark.sql(f"DESCRIBE HISTORY {TARGET}")
    bad_commits = (
        target_history.where(F.col("version") > BASELINE_TARGET_VERSION)
        .where(F.upper(F.col("operation")).isin("MERGE", "WRITE", "DELETE", "UPDATE", "TRUNCATE", "RESTORE"))
        .select("version", "timestamp", "operation", "job")
        .collect()
    )
    assert not bad_commits, {
        "message": "map_orders changed after the known-good baseline; re-baseline before recovery",
        "commits": [row.asDict(recursive=True) for row in bad_commits],
    }
    canceled_commits = (
        target_history.where(F.col("job.jobRunId").cast("string") == CANCELED_RUN_ID)
        .where(F.upper(F.col("operation")).isin("MERGE", "WRITE", "DELETE", "UPDATE", "TRUNCATE", "RESTORE"))
        .select("version", "timestamp", "operation")
        .collect()
    )
    assert not canceled_commits, {
        "message": "Canceled orders_spine run committed data; do not apply the bounded recovery blindly",
        "commits": [row.asDict(recursive=True) for row in canceled_commits],
    }

    current_versions = {source: table_version(source) for source in SOURCES}
    result["current_source_versions"] = current_versions
    for source, baseline in BASELINE_SOURCE_VERSIONS.items():
        assert current_versions[source] >= baseline, {
            "source": source,
            "baseline_version": baseline,
            "current_version": current_versions[source],
        }

    orders_changed = changed_bigint_keys(
        ORDERS_SOURCE, "ORDER_ID", "ORDER_ID",
        BASELINE_SOURCE_VERSIONS[ORDERS_SOURCE], current_versions[ORDERS_SOURCE],
    )
    medication_changed = changed_bigint_keys(
        MED_SOURCE, "ORDER_ID", "ORDER_ID",
        BASELINE_SOURCE_VERSIONS[MED_SOURCE], current_versions[MED_SOURCE],
    )
    changed_catalog_codes = changed_lookup_values(
        CATALOG_SOURCE, "CATALOG_CD",
        BASELINE_SOURCE_VERSIONS[CATALOG_SOURCE], current_versions[CATALOG_SOURCE],
    )
    changed_code_values = changed_lookup_values(
        CV_SOURCE, "CODE_VALUE",
        BASELINE_SOURCE_VERSIONS[CV_SOURCE], current_versions[CV_SOURCE],
    )
    result["changed_catalog_codes"] = len(changed_catalog_codes)
    result["changed_code_values"] = len(changed_code_values)

    target_keys = spark.table(TARGET).select(
        "ORDER_ID", "CATALOG_CD", "ACTIVITY_TYPE_CD",
        "ORDER_STATUS_CD", "DEPT_STATUS_CD", "CATALOG_TYPE_CD",
    )
    lookup_condition = F.lit(False)
    if changed_catalog_codes:
        lookup_condition = lookup_condition | F.col("CATALOG_CD").isin(*changed_catalog_codes)
    if changed_code_values:
        lookup_condition = lookup_condition | (
            F.col("ACTIVITY_TYPE_CD").isin(*changed_code_values)
            | F.col("ORDER_STATUS_CD").isin(*changed_code_values)
            | F.col("DEPT_STATUS_CD").isin(*changed_code_values)
            | F.col("CATALOG_TYPE_CD").isin(*changed_code_values)
        )
    lookup_affected = (
        target_keys.where(lookup_condition).select("ORDER_ID")
        if changed_catalog_codes or changed_code_values
        else empty_bigint("ORDER_ID")
    )

    affected = orders_changed.unionByName(medication_changed).unionByName(lookup_affected).distinct()
    spark.sql(f"DROP TABLE IF EXISTS {KEYS_TABLE}")
    affected.write.format("delta").mode("overwrite").saveAsTable(KEYS_TABLE)
    keys = spark.table(KEYS_TABLE)
    affected_count = keys.count()
    result["affected_order_ids"] = affected_count
    assert affected_count <= MAX_AFFECTED_ORDER_IDS, {
        "message": "Affected-key set is unexpectedly large; refusing accidental full rebuild",
        "affected_order_ids": affected_count,
        "limit": MAX_AFFECTED_ORDER_IDS,
    }

    boundaries = {
        ORDERS_SOURCE: cdf_boundary(
            ORDERS_SOURCE, "ADC_UPDT",
            BASELINE_SOURCE_VERSIONS[ORDERS_SOURCE], current_versions[ORDERS_SOURCE],
            BASELINE_WATERMARKS[ORDERS_SOURCE],
        ),
        MED_SOURCE: cdf_boundary(
            MED_SOURCE, "SOURCE_ADC_UPDT",
            BASELINE_SOURCE_VERSIONS[MED_SOURCE], current_versions[MED_SOURCE],
            BASELINE_WATERMARKS[MED_SOURCE],
        ),
        CATALOG_SOURCE: cdf_boundary(
            CATALOG_SOURCE, "ADC_UPDT",
            BASELINE_SOURCE_VERSIONS[CATALOG_SOURCE], current_versions[CATALOG_SOURCE],
            BASELINE_WATERMARKS[CATALOG_SOURCE],
        ),
        CV_SOURCE: cdf_boundary(
            CV_SOURCE, "ADC_UPDT",
            BASELINE_SOURCE_VERSIONS[CV_SOURCE], current_versions[CV_SOURCE],
            BASELINE_WATERMARKS[CV_SOURCE],
        ),
    }
    result["watermark_boundaries"] = {key: str(value) for key, value in boundaries.items()}

    merge_metrics = {}
    output_count = 0
    if affected_count:
        orders = source_at(ORDERS_SOURCE, current_versions[ORDERS_SOURCE]).alias("source")
        base = (
            orders.join(
                keys.alias("keys"),
                F.col("source.ORDER_ID").cast("bigint") == F.col("keys.ORDER_ID"),
                "inner",
            )
            .select(
                F.col("source.ORDER_ID").cast("bigint").alias("ORDER_ID"),
                F.col("source.PERSON_ID").cast("bigint").alias("PERSON_ID"),
                F.col("source.ENCNTR_ID").cast("bigint").alias("ENCNTR_ID"),
                F.col("source.ACTIVITY_TYPE_CD").cast("bigint").alias("ACTIVITY_TYPE_CD"),
                F.col("source.CATALOG_CD").cast("bigint").alias("CATALOG_CD"),
                F.col("source.CATALOG_TYPE_CD").cast("bigint").alias("CATALOG_TYPE_CD"),
                F.col("source.ORDER_MNEMONIC"),
                F.col("source.HNA_ORDER_MNEMONIC"),
                F.col("source.ORDERED_AS_MNEMONIC"),
                F.col("source.ORDER_STATUS_CD").cast("bigint").alias("ORDER_STATUS_CD"),
                F.col("source.DEPT_STATUS_CD").cast("bigint").alias("DEPT_STATUS_CD"),
                F.col("source.ACTIVE_IND").cast("bigint").alias("ACTIVE_IND"),
                F.col("source.ORIG_ORDER_DT_TM"),
                F.col("source.CURRENT_START_DT_TM"),
                F.col("source.STATUS_DT_TM"),
                F.col("source.PROJECTED_STOP_DT_TM"),
                F.col("source.DISCONTINUE_EFFECTIVE_DT_TM"),
                F.col("source.ADC_UPDT").alias("SOURCE_ADC_UPDT"),
            )
        )
        base = latest_per_key(
            base,
            ["ORDER_ID"],
            [F.col("SOURCE_ADC_UPDT").desc_nulls_last()],
        )

        catalog = F.broadcast(
            source_at(CATALOG_SOURCE, current_versions[CATALOG_SOURCE]).select(
                F.col("CATALOG_CD").cast("bigint").alias("_CATALOG_CD"),
                F.col("DESCRIPTION").alias("CATALOG_DISPLAY"),
            )
        )
        base = base.join(
            catalog,
            F.col("CATALOG_CD") == F.col("_CATALOG_CD"),
            "left",
        ).drop("_CATALOG_CD")

        code_values = source_at(CV_SOURCE, current_versions[CV_SOURCE]).select(
            F.col("CODE_VALUE").cast("bigint").alias("_CD"),
            F.col("DISPLAY").alias("_DESC"),
        )
        for code_column, output_column in [
            ("ACTIVITY_TYPE_CD", "ACTIVITY_TYPE_DESC"),
            ("ORDER_STATUS_CD", "ORDER_STATUS_DESC"),
            ("DEPT_STATUS_CD", "DEPT_STATUS_DESC"),
            ("CATALOG_TYPE_CD", "CATALOG_TYPE_DESC"),
        ]:
            lookup = F.broadcast(
                code_values.withColumnRenamed("_CD", f"_{output_column}_CD")
                .withColumnRenamed("_DESC", output_column)
            )
            base = base.join(
                lookup,
                F.col(code_column) == F.col(f"_{output_column}_CD"),
                "left",
            ).drop(f"_{output_column}_CD")

        medication_keys = (
            source_at(MED_SOURCE, current_versions[MED_SOURCE])
            .select(F.col("ORDER_ID").cast("bigint").alias("_MED_ORDER_ID"))
            .join(
                keys,
                F.col("_MED_ORDER_ID") == F.col("ORDER_ID"),
                "inner",
            )
            .select("_MED_ORDER_ID")
            .distinct()
        )
        base = (
            base.join(
                medication_keys,
                F.col("ORDER_ID") == F.col("_MED_ORDER_ID"),
                "left",
            )
            .withColumn("MED_FAMILY_IND", F.col("_MED_ORDER_ID").isNotNull())
            .drop("_MED_ORDER_ID")
        )

        base = dq_columns(
            base,
            [
                "ORIG_ORDER_DT_TM",
                "CURRENT_START_DT_TM",
                "STATUS_DT_TM",
                "PROJECTED_STOP_DT_TM",
                "DISCONTINUE_EFFECTIVE_DT_TM",
            ],
        )
        admin = {"SOURCE_ADC_UPDT", "PIPELINE_UPDT_DT_TM", "ROW_HASH"}
        hash_columns = [
            column for column in base.columns
            if column not in admin
            and not column.endswith(("_FUTURE_IND", "_SENTINEL_IND", "_CLEAN"))
        ]
        out = (
            base.withColumn(
                "ROW_HASH",
                F.xxhash64(F.to_json(F.struct(*[F.col(column) for column in hash_columns]))),
            )
            .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
        )
        target_columns = spark.table(TARGET).columns
        assert set(out.columns) == set(target_columns), {
            "missing_from_output": sorted(set(target_columns) - set(out.columns)),
            "extra_in_output": sorted(set(out.columns) - set(target_columns)),
        }
        out = out.select(*target_columns)

        spark.sql(f"DROP TABLE IF EXISTS {OUTPUT_TABLE}")
        out.write.format("delta").mode("overwrite").saveAsTable(OUTPUT_TABLE)
        output_count = spark.table(OUTPUT_TABLE).count()
        result["output_rows"] = output_count
        assert output_count == affected_count, {
            "message": "Some affected ORDER_IDs were absent from the pinned raw orders snapshot",
            "affected_order_ids": affected_count,
            "output_rows": output_count,
        }

        spark.sql(
            f"""
            MERGE INTO {TARGET} t
            USING {OUTPUT_TABLE} s
              ON t.ORDER_ID=s.ORDER_ID
            WHEN MATCHED AND NOT (t.ROW_HASH <=> s.ROW_HASH)
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
            """
        )
        merge_rows = (
            spark.sql(f"DESCRIBE HISTORY {TARGET}")
            .where(
                (F.col("operation") == "MERGE")
                & (F.col("job.jobRunId").cast("string") == RECOVERY_JOB_RUN_ID)
            )
            .orderBy(F.col("version").desc())
            .limit(1)
            .collect()
        )
        assert merge_rows, "Could not find the recovery MERGE in target history"
        merge_metrics = merge_rows[0]["operationMetrics"] or {}
        result["merge_version"] = int(merge_rows[0]["version"])
        result["merge_metrics"] = merge_metrics

        expected = spark.table(OUTPUT_TABLE).select(
            "ORDER_ID", F.col("ROW_HASH").alias("expected_hash")
        )
        actual = (
            spark.table(TARGET)
            .join(keys, "ORDER_ID", "inner")
            .select("ORDER_ID", F.col("ROW_HASH").alias("actual_hash"))
        )
        mismatch = (
            expected.join(actual, "ORDER_ID", "full")
            .where(~F.col("expected_hash").eqNullSafe(F.col("actual_hash")))
            .limit(1)
            .count()
        )
        assert mismatch == 0, "Post-merge target hash validation failed"

    for source, boundary in boundaries.items():
        set_watermark(source, boundary)

    source_payload = json.dumps(current_versions, sort_keys=True, separators=(",", ":")).replace("'", "''")
    spark.sql(
        f"ALTER TABLE {TARGET} SET TBLPROPERTIES "
        f"('{SOURCE_STATE_PROPERTY}'='{source_payload}')"
    )

    final_properties = spark.sql(f"DESCRIBE DETAIL {TARGET}").collect()[0]["properties"] or {}
    assert json.loads(final_properties[SOURCE_STATE_PROPERTY]) == current_versions

    result.update({
        "status": "RECOVERY_SUCCESS",
        "output_rows": output_count,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
    })
    write_audit(result)

    spark.sql(f"DROP TABLE IF EXISTS {OUTPUT_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {KEYS_TABLE}")

except Exception as exc:
    result.update({
        "status": "RECOVERY_FAILED",
        "error_type": type(exc).__name__,
        "error": str(exc),
        "traceback": traceback.format_exc()[-20000:],
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
    })
    write_audit(result)
    raise

payload = json.dumps(result, sort_keys=True, default=str)
print(json.dumps(result, indent=2, sort_keys=True, default=str))
dbutils.notebook.exit(payload)

