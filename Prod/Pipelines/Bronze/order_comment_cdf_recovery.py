# Databricks notebook source
# order_comment_cdf_recovery
# One-off bounded recovery for map_order_comment after full-rebuild checkpoints were cleared.
# Reads Delta CDF from the last known-good source versions and rebuilds all comments for affected orders.

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

TARGET = "4_prod.bronze.map_order_comment"
COMMENT_SOURCE = "4_prod.raw.mill_order_comment"
TEXT_SOURCE = "4_prod.raw.mill_long_text"
CV_SOURCE = "3_lookup.mill.mill_code_value"
SOURCES = [COMMENT_SOURCE, TEXT_SOURCE, CV_SOURCE]
PIPELINE = "a8b_order_comment"
WATERMARK_TABLE = "6_mgmt.bronze.s3_watermarks"
AUDIT_TABLE = "8_dev.bronze.order_comment_cdf_recovery_audit"

BASELINE_SOURCE_VERSIONS = {
    COMMENT_SOURCE: 1518,
    TEXT_SOURCE: 1160,
    CV_SOURCE: 893,
}
BASELINE_WATERMARKS = {
    COMMENT_SOURCE: "2026-08-21 01:57:16.573",
    TEXT_SOURCE: "2025-09-23 07:18:35.897",
    CV_SOURCE: "2026-08-21 02:18:24.436",
}
BASELINE_TARGET_VERSION = 35
CANCELED_RUN_ID = "149022305474578"
SOURCE_STATE_PROPERTY = "bronze_completeness.source_versions_json"
MAX_LOOKUP_CHANGE_KEYS = 5000
MAX_AFFECTED_ORDER_IDS = 20000000

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "2048")

operation_id = uuid.uuid4().hex
suffix = re.sub(r"[^0-9a-zA-Z]", "", RECOVERY_JOB_RUN_ID)[-18:] + "_" + operation_id[:8]
KEYS_TABLE = f"8_dev.bronze.order_comment_cdf_keys_{suffix}"
OUTPUT_TABLE = f"8_dev.bronze.order_comment_cdf_output_{suffix}"

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

def changed_bigint_keys(table_name, source_column, output_column, baseline_version, current_version, extra_filter=None):
    changes = cdf(table_name, baseline_version, current_version)
    if changes is None:
        return empty_bigint(output_column)
    if extra_filter is not None:
        changes = changes.where(extra_filter)
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

def cdf_boundary(table_name, clock_column, baseline_version, current_version, baseline_value, extra_filter=None):
    baseline = datetime.fromisoformat(baseline_value).replace(tzinfo=None)
    changes = cdf(table_name, baseline_version, current_version)
    if changes is None:
        return baseline
    if extra_filter is not None:
        changes = changes.where(extra_filter)
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
        "message": "map_order_comment changed after the known-good baseline; re-baseline before recovery",
        "commits": [row.asDict(recursive=True) for row in bad_commits],
    }
    canceled_commits = (
        target_history.where(F.col("job.jobRunId").cast("string") == CANCELED_RUN_ID)
        .where(F.upper(F.col("operation")).isin("MERGE", "WRITE", "DELETE", "UPDATE", "TRUNCATE", "RESTORE"))
        .select("version", "timestamp", "operation")
        .collect()
    )
    assert not canceled_commits, {
        "message": "Canceled run committed order-comment data; do not apply bounded recovery blindly",
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

    comment_changed = changed_bigint_keys(
        COMMENT_SOURCE, "ORDER_ID", "ORDER_ID",
        BASELINE_SOURCE_VERSIONS[COMMENT_SOURCE], current_versions[COMMENT_SOURCE],
    )
    text_filter = F.upper(F.trim(F.col("PARENT_ENTITY_NAME"))) == F.lit("ORDER_COMMENT")
    text_changed = changed_bigint_keys(
        TEXT_SOURCE, "PARENT_ENTITY_ID", "ORDER_ID",
        BASELINE_SOURCE_VERSIONS[TEXT_SOURCE], current_versions[TEXT_SOURCE],
        extra_filter=text_filter,
    )
    changed_code_values = changed_lookup_values(
        CV_SOURCE, "CODE_VALUE",
        BASELINE_SOURCE_VERSIONS[CV_SOURCE], current_versions[CV_SOURCE],
    )
    result["changed_code_values"] = len(changed_code_values)

    lookup_affected = (
        spark.table(TARGET)
        .where(F.col("COMMENT_TYPE_CD").isin(*changed_code_values))
        .select("ORDER_ID")
        if changed_code_values
        else empty_bigint("ORDER_ID")
    )
    affected = comment_changed.unionByName(text_changed).unionByName(lookup_affected).distinct()
    spark.sql(f"DROP TABLE IF EXISTS {KEYS_TABLE}")
    affected.write.format("delta").mode("overwrite").saveAsTable(KEYS_TABLE)
    keys = spark.table(KEYS_TABLE)
    affected_count = keys.count()
    result["affected_order_ids"] = affected_count
    assert affected_count <= MAX_AFFECTED_ORDER_IDS, {
        "message": "Affected order-comment keyset is unexpectedly large",
        "affected_order_ids": affected_count,
        "limit": MAX_AFFECTED_ORDER_IDS,
    }

    boundaries = {
        COMMENT_SOURCE: cdf_boundary(
            COMMENT_SOURCE, "ADC_UPDT",
            BASELINE_SOURCE_VERSIONS[COMMENT_SOURCE], current_versions[COMMENT_SOURCE],
            BASELINE_WATERMARKS[COMMENT_SOURCE],
        ),
        TEXT_SOURCE: cdf_boundary(
            TEXT_SOURCE, "ADC_UPDT",
            BASELINE_SOURCE_VERSIONS[TEXT_SOURCE], current_versions[TEXT_SOURCE],
            BASELINE_WATERMARKS[TEXT_SOURCE],
            extra_filter=text_filter,
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
        comments = (
            source_at(COMMENT_SOURCE, current_versions[COMMENT_SOURCE])
            .alias("source")
            .join(
                keys.alias("keys"),
                F.col("source.ORDER_ID").cast("bigint") == F.col("keys.ORDER_ID"),
                "inner",
            )
            .select(
                F.col("source.ORDER_ID").cast("bigint").alias("ORDER_ID"),
                F.col("source.ACTION_SEQUENCE").cast("bigint").alias("ACTION_SEQUENCE"),
                F.col("source.COMMENT_TYPE_CD").cast("bigint").alias("COMMENT_TYPE_CD"),
                F.col("source.LONG_TEXT_ID").cast("bigint").alias("LONG_TEXT_ID"),
                F.col("source.UPDT_DT_TM").alias("COMMENT_UPDT_DT_TM"),
                F.col("source.UPDT_CNT").cast("bigint").alias("COMMENT_UPDT_CNT"),
                F.col("source.COMMENT_DT_TM"),
                F.col("source.ADC_UPDT").alias("SOURCE_COMMENT_ADC_UPDT"),
            )
            .dropDuplicates()
        )
        base = latest_per_key(
            comments,
            ["ORDER_ID", "ACTION_SEQUENCE", "COMMENT_TYPE_CD"],
            [
                F.col("SOURCE_COMMENT_ADC_UPDT").desc_nulls_last(),
                F.col("COMMENT_UPDT_CNT").desc_nulls_last(),
                F.col("COMMENT_UPDT_DT_TM").desc_nulls_last(),
                F.col("LONG_TEXT_ID").desc_nulls_last(),
            ],
        )

        code_values = F.broadcast(
            source_at(CV_SOURCE, current_versions[CV_SOURCE]).select(
                F.col("CODE_VALUE").cast("bigint").alias("_COMMENT_TYPE_CD"),
                F.col("DISPLAY").alias("COMMENT_TYPE_DESC"),
            )
        )
        base = base.join(
            code_values,
            F.col("COMMENT_TYPE_CD") == F.col("_COMMENT_TYPE_CD"),
            "left",
        ).drop("_COMMENT_TYPE_CD")

        text = (
            source_at(TEXT_SOURCE, current_versions[TEXT_SOURCE])
            .where(F.upper(F.trim(F.col("PARENT_ENTITY_NAME"))) == F.lit("ORDER_COMMENT"))
            .alias("source")
            .join(
                keys.alias("keys"),
                F.col("source.PARENT_ENTITY_ID").cast("bigint") == F.col("keys.ORDER_ID"),
                "inner",
            )
            .select(
                F.col("source.LONG_TEXT_ID").cast("bigint").alias("_TEXT_ID"),
                F.col("source.PARENT_ENTITY_ID").cast("bigint").alias("_TEXT_ORDER_ID"),
                F.col("source.LONG_TEXT").alias("_LONG_TEXT"),
                F.col("source.ACTIVE_IND").cast("bigint").alias("TEXT_ACTIVE_IND"),
                F.col("source.UPDT_DT_TM").alias("TEXT_UPDT_DT_TM"),
                F.col("source.UPDT_CNT").cast("bigint").alias("_TEXT_UPDT_CNT"),
                F.col("source.ADC_UPDT").alias("SOURCE_TEXT_ADC_UPDT"),
            )
        )
        text = latest_per_key(
            text,
            ["_TEXT_ID", "_TEXT_ORDER_ID"],
            [
                F.col("SOURCE_TEXT_ADC_UPDT").desc_nulls_last(),
                F.col("_TEXT_UPDT_CNT").desc_nulls_last(),
                F.col("TEXT_UPDT_DT_TM").desc_nulls_last(),
            ],
        )
        base = (
            base.join(
                text,
                (F.col("LONG_TEXT_ID") == F.col("_TEXT_ID"))
                & (F.col("ORDER_ID") == F.col("_TEXT_ORDER_ID")),
                "left",
            )
            .withColumn("TEXT_AVAILABLE_IND", F.col("_TEXT_ID").isNotNull())
            .withColumn("TEXT_TRUNCATED_IND", F.length(F.col("_LONG_TEXT")) > F.lit(1048576))
            .withColumn("COMMENT_TEXT", F.substring(F.col("_LONG_TEXT"), 1, 1048576))
            .drop("_TEXT_ID", "_TEXT_ORDER_ID", "_LONG_TEXT", "_TEXT_UPDT_CNT")
        )

        eligible = base.where(
            (F.col("COMMENT_TYPE_CD") == 66)
            & F.col("TEXT_AVAILABLE_IND")
            & (F.col("TEXT_ACTIVE_IND") == 1)
        )
        winner = (
            latest_per_key(
                eligible,
                ["ORDER_ID"],
                [
                    F.col("ACTION_SEQUENCE").desc(),
                    F.col("TEXT_UPDT_DT_TM").desc_nulls_last(),
                    F.col("COMMENT_UPDT_DT_TM").desc_nulls_last(),
                    F.col("LONG_TEXT_ID").desc_nulls_last(),
                ],
            )
            .select("ORDER_ID", "ACTION_SEQUENCE", "COMMENT_TYPE_CD")
            .withColumn("LATEST_IND", F.lit(True))
        )
        base = (
            base.join(winner, ["ORDER_ID", "ACTION_SEQUENCE", "COMMENT_TYPE_CD"], "left")
            .fillna(False, ["LATEST_IND", "TEXT_AVAILABLE_IND", "TEXT_TRUNCATED_IND"])
        )
        base = dq_columns(base, ["COMMENT_UPDT_DT_TM", "COMMENT_DT_TM", "TEXT_UPDT_DT_TM"])

        admin = {
            "SOURCE_COMMENT_ADC_UPDT",
            "SOURCE_TEXT_ADC_UPDT",
            "PIPELINE_UPDT_DT_TM",
            "ROW_HASH",
        }
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
        output = spark.table(OUTPUT_TABLE)
        output_count = output.count()
        result["output_rows"] = output_count

        duplicate = (
            output.groupBy("ORDER_ID", "ACTION_SEQUENCE", "COMMENT_TYPE_CD")
            .count()
            .where(F.col("count") != 1)
            .limit(1)
            .count()
        )
        assert duplicate == 0, "Recovery output contains duplicate comment keys"

        spark.sql(
            f"""
            MERGE INTO {TARGET} t
            USING {OUTPUT_TABLE} s
              ON t.ORDER_ID=s.ORDER_ID
             AND t.ACTION_SEQUENCE=s.ACTION_SEQUENCE
             AND t.COMMENT_TYPE_CD=s.COMMENT_TYPE_CD
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
        assert merge_rows, "Could not find the order-comment recovery MERGE in target history"
        merge_metrics = merge_rows[0]["operationMetrics"] or {}
        result["merge_version"] = int(merge_rows[0]["version"])
        result["merge_metrics"] = merge_metrics

        expected = output.select(
            "ORDER_ID", "ACTION_SEQUENCE", "COMMENT_TYPE_CD",
            F.col("ROW_HASH").alias("expected_hash"),
        )
        actual = (
            spark.table(TARGET)
            .join(keys, "ORDER_ID", "inner")
            .select(
                "ORDER_ID", "ACTION_SEQUENCE", "COMMENT_TYPE_CD",
                F.col("ROW_HASH").alias("actual_hash"),
            )
        )
        mismatch = (
            expected.join(
                actual,
                ["ORDER_ID", "ACTION_SEQUENCE", "COMMENT_TYPE_CD"],
                "full",
            )
            .where(~F.col("expected_hash").eqNullSafe(F.col("actual_hash")))
            .limit(1)
            .count()
        )
        assert mismatch == 0, (
            "Post-merge order-comment validation failed; source deletions or stale target rows may exist"
        )

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

