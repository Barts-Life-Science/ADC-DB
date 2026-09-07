# Databricks notebook source
# MAGIC %md
# MAGIC # Millennium Medication Order Bronze Pipeline
# MAGIC
# MAGIC Separates prescribing/order activity from the existing JAC dispensing/supply feed.
# MAGIC Publishes pharmacy orders, their action history, ingredients, and current-action detail.
# MAGIC
# MAGIC Contract decisions verified 2026-08-08:
# MAGIC - Pharmacy domain is decoded `ACTIVITY_TYPE_CD=705` (`Pharmacy`); Pharmacy Consults are excluded.
# MAGIC - `mill_order_action` is a declared HISTORICAL lane capped at 2024-09-17. A later watermark
# MAGIC   deliberately fails until a controlled history backfill is approved.
# MAGIC - `map_medication_order_detail` is latest-`ACTION_SEQUENCE` long format. Historical action-level
# MAGIC   order detail is an explicit bronze-contract exclusion, not an implicit downstream raw read.
# MAGIC - Exact scale profile: 363,043,691 pharmacy orders; 457,718,185 pharmacy actions;
# MAGIC   29,944,771 pharmacy ingredients; 6,815,038,919 all-action pharmacy detail rows versus
# MAGIC   2,822,380,791 latest-action source rows before natural-key deduplication. The 2.82B-row
# MAGIC   latest-action lane is the bounded v1 choice. Exact duplicate ingredient/detail raw rows
# MAGIC   are collapsed deterministically at their declared natural keys.
# MAGIC - Cancelled, inactive, suspended and discontinued orders remain represented.
# MAGIC - Curation `2026.08.v2.0`: S10 `medication_order` and the Journey standard block are the
# MAGIC   named consumers. Measured lifecycle sequences/dates and all three display lines stay
# MAGIC   (the display trios are materially different); source update counters and parent-order
# MAGIC   contributor stamps are cut while each child keeps its own source-row `SOURCE_ADC_UPDT`.

# COMMAND ----------

for _name, _default in {
    "target_schema": "8_dev.bronze",
    "allow_production_write": "false",
    "force_full_refresh": "false",
    "full_reconciliation": "false",
    "bootstrap_mode": "false",
    "bootstrap_min_order_id": "",
    "bootstrap_max_order_id": "",
    "recovery_control_table": "8_dev.bronze.medication_order_recovery_control",
}.items():
    try:
        dbutils.widgets.get(_name)
    except Exception:
        dbutils.widgets.text(_name, _default)

# COMMAND ----------

# MAGIC %run ./_bronze_common

# COMMAND ----------

import builtins
import json
from datetime import datetime, timedelta, timezone
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# S4-C1a BEGIN
# S4/C1a drop-in contract: ADC_UPDT remains the bronze BUILD clock and
# SOURCE_ADC_UPDT remains the raw source clock. This enrichment must not alter
# source watermarks or checkpoint semantics.
S4_MED_LOOKUP_TABLE = "3_lookup.mill.map_med_lookup"
S4_OMOP_CONCEPT_TABLE = "4_prod.omop.concept"
S4_OMOP_CONCEPT_RELATIONSHIP_TABLE = "4_prod.omop.concept_relationship"
S4_MED_CONCEPT_COLUMNS = [
    "SNOMED_CODE",
    "MULTUM_CODE",
    "RXNORM_CODE",
    "OMOP_CONCEPT_ID",
    "OMOP_CONCEPT_NAME",
    "OMOP_MAPPING_METHOD",
    "OMOP_MAPPING_CONFIDENCE",
    "OMOP_STANDARD_CONCEPT_ID",
    "OMOP_STANDARD_CONCEPT_NAME",
    "OMOP_STANDARD_CANDIDATE_COUNT",
    "OMOP_STANDARD_MAPPING_METHOD",
    "LOOKUP_SOURCE_ROW_HASH",
]
# S4-C1a END
TARGET_SCHEMA = bronze_value("target_schema", "8_dev.bronze")
ALLOW_PRODUCTION_WRITE = bronze_bool("allow_production_write", False)
FORCE_FULL_REFRESH = bronze_bool("force_full_refresh", False)
FULL_RECONCILIATION = bronze_bool("full_reconciliation", False)
BOOTSTRAP_MODE = bronze_bool("bootstrap_mode", False)
BOOTSTRAP_MIN_ORDER_ID = bronze_value("bootstrap_min_order_id", "")
BOOTSTRAP_MAX_ORDER_ID = bronze_value("bootstrap_max_order_id", "")
RUN_ID = bronze_run_id()
RECOVERY_CONTROL_TABLE = bronze_value(
    "recovery_control_table",
    "8_dev.bronze.medication_order_recovery_control",
)
PIPELINE_LOGIC_VERSION = "2026.08.s4s3a11.recovery-finalize-v2"
LOGIC_VERSION_INT = 2026080801
LOGIC_SOURCE = "__PIPELINE_LOGIC__"
RUN_FUTURE_HORIZON = None

assert not TARGET_SCHEMA.lower().startswith("4_prod") or ALLOW_PRODUCTION_WRITE, (
    "Production writes are disabled. Only the approved orchestrator may pass allow_production_write=true."
)
if BOOTSTRAP_MODE:
    assert BOOTSTRAP_MIN_ORDER_ID and BOOTSTRAP_MAX_ORDER_ID, (
        "bootstrap_mode=true requires bootstrap_min_order_id and bootstrap_max_order_id"
    )
    assert int(BOOTSTRAP_MIN_ORDER_ID) <= int(BOOTSTRAP_MAX_ORDER_ID)

RAW = "4_prod.raw"
CODE_VALUE = "3_lookup.mill.mill_code_value"
SRC_ORDER = f"{RAW}.mill_orders"
SRC_ACTION = f"{RAW}.mill_order_action"
SRC_INGREDIENT = f"{RAW}.mill_order_ingredient"
SRC_DETAIL = f"{RAW}.mill_order_detail"

ORDER = f"{TARGET_SCHEMA}.map_medication_order"
ACTION = f"{TARGET_SCHEMA}.map_medication_order_action"
INGREDIENT = f"{TARGET_SCHEMA}.map_medication_order_ingredient"
DETAIL = f"{TARGET_SCHEMA}.map_medication_order_detail"
STATE_TABLE = f"{bronze_control_schema(TARGET_SCHEMA)}.medication_order_pipeline_state"
LOCK_KEY = "__RUN_LOCK__"
LOCK_TTL_HOURS = 12

PHARMACY_ACTIVITY_TYPE_CD = 705
ACTION_HISTORY_CEILING = "2024-09-17 23:59:59.999999"

SOURCE_SLA = {
    SRC_ORDER: (2, "LIVE"),
    SRC_ACTION: (2, "HISTORICAL"),
    SRC_INGREDIENT: (2, "LIVE"),
    SRC_DETAIL: (2, "LIVE"),
    CODE_VALUE: (365, "REFERENCE"),
}
LOOKUP_SOURCES = {CODE_VALUE}
EXPECTED_COLUMNS = {
    SRC_ORDER: {"ORDER_ID", "PERSON_ID", "ENCNTR_ID", "ACTIVITY_TYPE_CD", "LAST_ACTION_SEQUENCE", "ORDER_STATUS_CD", "ADC_UPDT"},
    SRC_ACTION: {"ORDER_ID", "ACTION_SEQUENCE", "ACTION_TYPE_CD", "ACTION_DT_TM", "ORDER_STATUS_CD", "ADC_UPDT"},
    SRC_INGREDIENT: {"ORDER_ID", "ACTION_SEQUENCE", "COMP_SEQUENCE", "STRENGTH", "VOLUME", "DOSE_QUANTITY", "ADC_UPDT"},
    SRC_DETAIL: {"ORDER_ID", "ACTION_SEQUENCE", "DETAIL_SEQUENCE", "OE_FIELD_ID", "OE_FIELD_MEANING", "ADC_UPDT"},
    CODE_VALUE: {"CODE_VALUE", "DESCRIPTION", "DISPLAY", "ADC_UPDT"},
}
HASH_EXCLUDE = {
    "ROW_HASH", "ADC_UPDT", "SOURCE_PRESENT_IND", "PIPELINE_RUN_ID",
    "PIPELINE_PROCESSED_TS", "SOURCE_ABSENT_DETECTED_TS", "CURATED_STATUS", "CURATED_NOTES",
}

def qident(value: str) -> str:
    return "`" + str(value).replace("`", "``") + "`"


def qname(value: str) -> str:
    return ".".join(qident(part) for part in str(value).split("."))


def sql_escape(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "''")


def assert_expected_columns(table: str, expected: set[str]) -> None:
    missing = sorted(expected - set(spark.table(table).columns))
    assert not missing, f"{table}: missing expected columns {missing}"


STATE_CACHE: dict[tuple[str, str], dict] = {}
SOURCE_VERSIONS: dict[str, int] = {}


def source_version(table: str) -> int:
    if table == LOGIC_SOURCE:
        return LOGIC_VERSION_INT
    return int(spark.sql(f"DESCRIBE HISTORY {qname(table)} LIMIT 1").collect()[0]["version"])


def _staleness_days(watermark) -> float | None:
    if watermark is None:
        return None
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    raw = watermark.replace(tzinfo=None) if watermark.tzinfo else watermark
    return (now - raw).total_seconds() / 86400.0


def source_health(table: str) -> dict:
    row = spark.table(table).agg(
        F.count(F.lit(1)).alias("rows"), F.max("ADC_UPDT").alias("watermark")
    ).collect()[0]
    watermark = row["watermark"]
    return {
        "source_table": table,
        "version": SOURCE_VERSIONS[table],
        "rows": int(row["rows"]),
        "watermark": watermark,
        "source_staleness_days": _staleness_days(watermark),
        "scan": "FULL",
    }


def source_health_for_mode(table: str, mode: str, checkpoint: dict | None) -> dict:
    live_version = SOURCE_VERSIONS[table]
    if table == LOGIC_SOURCE:
        return {
            "source_table": table,
            "version": live_version,
            "rows": None,
            "watermark": None,
            "source_staleness_days": None,
            "scan": "SYNTHETIC",
        }
    if mode == "UNCHANGED_SKIP" and checkpoint and checkpoint.get("source_watermark") is not None:
        watermark = checkpoint["source_watermark"]
        return {
            "source_table": table,
            "version": live_version,
            "rows": checkpoint.get("source_rows"),
            "watermark": watermark,
            "source_staleness_days": _staleness_days(watermark),
            "scan": "REUSED",
        }
    if mode == "INCREMENTAL" and checkpoint and checkpoint.get("source_watermark") is not None:
        base = checkpoint["source_watermark"]
        window_max = (
            spark.table(table)
            .where(F.col("ADC_UPDT") >= F.lit(base).cast("timestamp") - F.expr("INTERVAL 24 HOURS"))
            .agg(F.max("ADC_UPDT").alias("watermark"))
            .collect()[0]["watermark"]
        )
        watermark = builtins.max(value for value in (base, window_max) if value is not None)
        return {
            "source_table": table,
            "version": live_version,
            "rows": checkpoint.get("source_rows"),
            "watermark": watermark,
            "source_staleness_days": _staleness_days(watermark),
            "scan": "OVERLAP_WINDOW",
        }
    return source_health(table)


def load_state_cache() -> None:
    STATE_CACHE.clear()
    for row in spark.table(STATE_TABLE).where(F.col("target_table") != LOCK_KEY).collect():
        item = row.asDict(recursive=True)
        STATE_CACHE[(item["target_table"], item["source_table"])] = item


def last_checkpoint(target: str, source: str) -> dict | None:
    return STATE_CACHE.get((target, source))


def choose_mode(target: str, sources: list[str]) -> str:
    if BOOTSTRAP_MODE:
        return "BOOTSTRAP"
    if FORCE_FULL_REFRESH or FULL_RECONCILIATION or not bronze_table_exists(target):
        return "FULL"
    changed = False
    for source in sources:
        previous = last_checkpoint(target, source)
        if previous is None:
            return "FULL"
        if int(previous["source_version"]) != int(SOURCE_VERSIONS[source]):
            if source == LOGIC_SOURCE:
                return "FULL"
            if source in LOOKUP_SOURCES:
                return "FULL_LOOKUP_CHANGE"
            changed = True
    return "INCREMENTAL" if changed else "UNCHANGED_SKIP"


def changed_rows(table: str, target: str) -> DataFrame:
    previous = last_checkpoint(target, table)
    frame = spark.table(table)
    if previous is None or previous["source_watermark"] is None:
        return frame
    return frame.where(
        F.col("ADC_UPDT") >= F.lit(previous["source_watermark"]).cast("timestamp") - F.expr("INTERVAL 24 HOURS")
    )


def union_order_ids(frames: list[DataFrame]) -> DataFrame:
    selected = [
        frame.select(F.col("ORDER_ID").alias("ORDER_ID_RAW"), F.col("ORDER_ID").cast("long").alias("ORDER_ID"))
        for frame in frames
    ]
    return (
        reduce(lambda left, right: left.unionByName(right), selected)
        .where(F.col("ORDER_ID").isNotNull())
        .groupBy("ORDER_ID")
        .agg(F.min("ORDER_ID_RAW").alias("ORDER_ID_RAW"))
        .select("ORDER_ID_RAW", "ORDER_ID")
    )


def scope_by_order_ids(frame: DataFrame, order_ids: DataFrame | None) -> DataFrame:
    if order_ids is None:
        return frame
    return (
        frame.alias("source")
        .join(
            order_ids.select("ORDER_ID_RAW").alias("scope"),
            F.col("source.ORDER_ID") == F.col("scope.ORDER_ID_RAW"),
            "inner",
        )
        .drop(F.col("scope.ORDER_ID_RAW"))
    )


def with_row_hash(df: DataFrame) -> DataFrame:
    columns = sorted(column for column in df.columns if column not in HASH_EXCLUDE)
    payload = F.concat_ws(
        "\u0001",
        *[F.coalesce(F.col(column).cast("string"), F.lit("<NULL>")) for column in columns],
    )
    return df.withColumn("ROW_HASH", F.sha2(payload, 256))


def assert_unique_non_null(df: DataFrame, keys: list[str], label: str) -> None:
    null_condition = reduce(lambda left, right: left | right, [F.col(key).isNull() for key in keys])
    assert df.where(null_condition).limit(1).count() == 0, f"{label}: NULL key detected"
    assert df.groupBy(*keys).count().where(F.col("count") > 1).limit(1).count() == 0, f"{label}: duplicate key detected"


def ensure_table_features(table: str) -> None:
    spark.sql(
        f"ALTER TABLE {qname(table)} SET TBLPROPERTIES ("
        "'delta.enableChangeDataFeed'='true',"
        "'delta.enableRowTracking'='true',"
        "'delta.enableDeletionVectors'='true')"
    )


CLUSTER_KEYS = {ORDER: ["ORDER_ID"], ACTION: ["ORDER_ID"], DETAIL: ["ORDER_ID"]}


def materialize_stage(df: DataFrame, target: str, keys: list[str]) -> DataFrame:
    staging_table = f"{target}_stg"
    staged = (
        with_row_hash(df)
        .withColumn("PIPELINE_RUN_ID", F.lit(RUN_ID))
        .withColumn("SOURCE_PRESENT_IND", F.lit(True))
        .withColumn("SOURCE_ABSENT_DETECTED_TS", F.lit(None).cast("timestamp"))
        .withColumn("ADC_UPDT", F.current_timestamp())
    )
    (
        staged.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(staging_table)
    )
    snapshot = spark.table(staging_table)
    assert_unique_non_null(snapshot, keys, f"staged {target}")
    return snapshot


def merge_target(
    staged: DataFrame,
    target: str,
    keys: list[str],
    full_compare: bool,
    tombstones: DataFrame | None = None,
) -> dict:
    staged_rows = staged.count()
    if not bronze_table_exists(target):
        (
            staged.write.format("delta")
            .option("mergeSchema", "true")
            .option("delta.enableChangeDataFeed", "true")
            .option("delta.enableRowTracking", "true")
            .option("delta.enableDeletionVectors", "true")
            .saveAsTable(target)
        )
        if target in CLUSTER_KEYS:
            cluster_columns = ", ".join(qident(column) for column in CLUSTER_KEYS[target])
            spark.sql(f"ALTER TABLE {qname(target)} CLUSTER BY ({cluster_columns})")
        operation = "CREATE"
    else:
        condition = " AND ".join(f"t.{qident(key)} <=> s.{qident(key)}" for key in keys)
        values = {column: f"s.{qident(column)}" for column in staged.columns}
        merge = (
            DeltaTable.forName(spark, target)
            .alias("t")
            .merge(staged.alias("s"), condition)
            .whenMatchedUpdate(condition="t.ROW_HASH <> s.ROW_HASH OR t.SOURCE_PRESENT_IND = false", set=values)
            .whenNotMatchedInsert(values=values)
        )
        if full_compare:
            merge = merge.whenNotMatchedBySourceUpdate(
                condition="t.SOURCE_PRESENT_IND = true",
                set={
                    "SOURCE_PRESENT_IND": "false",
                    "SOURCE_ABSENT_DETECTED_TS": "current_timestamp()",
                    "PIPELINE_RUN_ID": f"'{sql_escape(RUN_ID)}'",
                    "ADC_UPDT": "current_timestamp()",
                },
            )
        merge.execute()
        operation = "MERGE"
    tombstone_count = 0
    if tombstones is not None:
        if bronze_table_exists(target):
            present_keys = spark.table(target).where(F.col("SOURCE_PRESENT_IND")).select(*keys)
            tombstones = tombstones.select(*keys).dropDuplicates(keys).join(present_keys, keys, "inner")
        else:
            tombstones = tombstones.select(*keys).where(F.lit(False))
        tombstone_count = tombstones.count()
        if tombstone_count:
            condition = " AND ".join(f"t.{qident(key)} <=> s.{qident(key)}" for key in keys)
            (
                DeltaTable.forName(spark, target)
                .alias("t")
                .merge(tombstones.alias("s"), condition)
                .whenMatchedUpdate(
                    condition="t.SOURCE_PRESENT_IND = true",
                    set={
                        "SOURCE_PRESENT_IND": "false",
                        "SOURCE_ABSENT_DETECTED_TS": "current_timestamp()",
                        "PIPELINE_RUN_ID": f"'{sql_escape(RUN_ID)}'",
                        "ADC_UPDT": "current_timestamp()",
                    },
                )
                .execute()
            )
    ensure_table_features(target)
    return {
        "operation": operation,
        "staged_rows": int(staged_rows),
        "tombstones_staged": int(tombstone_count),
    }


def replace_target_full(df: DataFrame, target: str, keys: list[str]) -> dict:
    """Atomically replace one full target without a many-billion-row MERGE."""
    assert bronze_table_exists(target), f"Recovery replacement requires existing target {target}"
    replacement = (
        with_row_hash(df)
        .withColumn("PIPELINE_RUN_ID", F.lit(RUN_ID))
        .withColumn("SOURCE_PRESENT_IND", F.lit(True))
        .withColumn("SOURCE_ABSENT_DETECTED_TS", F.lit(None).cast("timestamp"))
        .withColumn("ADC_UPDT", F.current_timestamp())
    )
    target_columns = spark.table(target).columns
    missing = sorted(set(target_columns) - set(replacement.columns))
    extra = sorted(set(replacement.columns) - set(target_columns))
    assert not missing and not extra, {
        "target": target,
        "missing_columns": missing,
        "extra_columns": extra,
    }
    view_name = "__medication_detail_full_replace"
    replacement.select(*target_columns).createOrReplaceTempView(view_name)
    try:
        spark.sql(
            f"INSERT OVERWRITE TABLE {qname(target)} "
            f"SELECT * FROM {qident(view_name)}"
        )
    finally:
        spark.catalog.dropTempView(view_name)

    ensure_table_features(target)
    if target in CLUSTER_KEYS:
        cluster_columns = ", ".join(qident(column) for column in CLUSTER_KEYS[target])
        spark.sql(f"ALTER TABLE {qname(target)} CLUSTER BY ({cluster_columns})")

    null_condition = reduce(
        lambda left, right: left | right,
        [F.col(key).isNull() for key in keys],
    )
    assert spark.table(target).where(null_condition).limit(1).count() == 0, (
        f"{target}: NULL key detected after full replacement"
    )
    history = _run_overwrite_history(target, RUN_ID)
    assert history is not None, {
        "target": target,
        "message": "No successful overwrite from this pipeline run was found",
        "run_id": RUN_ID,
    }
    return {
        "operation": "INSERT_OVERWRITE",
        "staged_rows": int(history["output_rows"]),
        "tombstones_staged": 0,
        "target_version": int(history["version"]),
        "output_rows": int(history["output_rows"]),
        "write_timestamp": history["timestamp"],
    }


def add_decode(
    df: DataFrame,
    code_column: str,
    output_column: str,
    decode_lookup: DataFrame,
) -> DataFrame:
    key = f"__cv_{output_column}_code"
    lookup = F.broadcast(
        decode_lookup.select(
            F.col("__CODE_VALUE").alias(key),
            F.col("__CODE_DESCRIPTION").alias(output_column),
        )
    )
    return df.join(lookup, F.col(code_column).cast("long") == F.col(key), "left").drop(key)


# S4-C1a BEGIN
def add_s4_medication_concepts(df: DataFrame, target_keys: list[str]) -> DataFrame:
    """Add the S4 medication concept stack at the target increment's key grain."""
    missing = [column for column in [*target_keys, "SYNONYM_ID"] if column not in df.columns]
    assert not missing, f"S4 medication enrichment: missing target columns {missing}"

    lookup = spark.table(S4_MED_LOOKUP_TABLE).select(
        F.col("SYNONYM_ID").cast("long").alias("__S4_LOOKUP_SYNONYM_ID"),
        F.col("SNOMED_CODE").cast("string").alias("SNOMED_CODE"),
        F.col("MULTUM_CODE").cast("string").alias("MULTUM_CODE"),
        F.col("RXNORM_CODE").cast("string").alias("RXNORM_CODE"),
        F.col("MAPPED_OMOP_CONCEPT_ID").cast("long").alias("MAPPED_OMOP_CONCEPT_ID"),
        F.col("MAPPED_OMOP_CONCEPT_TERM").cast("string").alias("MAPPED_OMOP_CONCEPT_TERM"),
        F.col("STANDARDIZED_SIMILARITY_OMOP_CONCEPT_ID").cast("long").alias(
            "STANDARDIZED_SIMILARITY_OMOP_CONCEPT_ID"
        ),
        F.col("STANDARDIZED_SIMILARITY_OMOP_CONCEPT_TERM").cast("string").alias(
            "STANDARDIZED_SIMILARITY_OMOP_CONCEPT_TERM"
        ),
        F.col("SIMILARITY_OMOP_CONCEPT_ID").cast("long").alias("SIMILARITY_OMOP_CONCEPT_ID"),
        F.col("SIMILARITY_OMOP_CONCEPT_TERM").cast("string").alias("SIMILARITY_OMOP_CONCEPT_TERM"),
        F.col("RAW_SIMILARITY_SCORE").cast("double").alias("RAW_SIMILARITY_SCORE"),
        F.col("SIMILARITY_SCORE").cast("double").alias("SIMILARITY_SCORE"),
        F.col("SOURCE_ROW_HASH").cast("string").alias("LOOKUP_SOURCE_ROW_HASH"),
    )

    # Binding S4 enrich-frame contract: the increment at its target-key grain is
    # the LEFT side. Missing/removed lookup rows therefore emit NULL enrichment.
    enriched = (
        df.alias("target_keys")
        .join(
            F.broadcast(lookup).alias("lookup"),
            F.col("target_keys.SYNONYM_ID").cast("long") == F.col("lookup.__S4_LOOKUP_SYNONYM_ID"),
            "left",
        )
        .select(
            "target_keys.*",
            F.col("lookup.SNOMED_CODE").alias("SNOMED_CODE"),
            F.col("lookup.MULTUM_CODE").alias("MULTUM_CODE"),
            F.col("lookup.RXNORM_CODE").alias("RXNORM_CODE"),
            F.coalesce(
                F.col("lookup.MAPPED_OMOP_CONCEPT_ID"),
                F.col("lookup.STANDARDIZED_SIMILARITY_OMOP_CONCEPT_ID"),
                F.col("lookup.SIMILARITY_OMOP_CONCEPT_ID"),
            ).cast("long").alias("OMOP_CONCEPT_ID"),
            F.coalesce(
                F.col("lookup.MAPPED_OMOP_CONCEPT_TERM"),
                F.col("lookup.STANDARDIZED_SIMILARITY_OMOP_CONCEPT_TERM"),
                F.col("lookup.SIMILARITY_OMOP_CONCEPT_TERM"),
            ).alias("OMOP_CONCEPT_NAME"),
            F.when(
                F.col("lookup.MAPPED_OMOP_CONCEPT_ID").isNotNull(),
                F.lit("LOOKUP_MAPPED"),
            ).when(
                F.col("lookup.STANDARDIZED_SIMILARITY_OMOP_CONCEPT_ID").isNotNull(),
                F.lit("LOOKUP_SIMILARITY_STANDARDIZED"),
            ).when(
                F.col("lookup.SIMILARITY_OMOP_CONCEPT_ID").isNotNull(),
                F.lit("LOOKUP_SIMILARITY"),
            ).alias("OMOP_MAPPING_METHOD"),
            F.when(
                F.col("lookup.MAPPED_OMOP_CONCEPT_ID").isNotNull(),
                F.lit(None).cast("double"),
            ).when(
                F.col("lookup.STANDARDIZED_SIMILARITY_OMOP_CONCEPT_ID").isNotNull(),
                F.col("lookup.RAW_SIMILARITY_SCORE"),
            ).otherwise(F.col("lookup.SIMILARITY_SCORE")).alias("OMOP_MAPPING_CONFIDENCE"),
            F.col("lookup.LOOKUP_SOURCE_ROW_HASH").alias("LOOKUP_SOURCE_ROW_HASH"),
        )
    )

    source_ids = (
        enriched.select(F.col("OMOP_CONCEPT_ID").cast("long").alias("concept_id"))
        .where(F.col("concept_id").isNotNull())
        .distinct()
    )
    concepts = spark.table(S4_OMOP_CONCEPT_TABLE).select(
        F.col("concept_id").cast("long").alias("concept_id"),
        F.col("concept_name").cast("string").alias("concept_name"),
        F.col("standard_concept").cast("string").alias("standard_concept"),
    )
    classified = (
        source_ids.alias("source")
        .join(
            concepts.alias("source_concept"),
            F.col("source.concept_id") == F.col("source_concept.concept_id"),
            "left",
        )
        .select(
            F.col("source.concept_id").alias("concept_id"),
            F.col("source_concept.standard_concept").alias("source_standard_concept"),
        )
    )
    valid_standard = concepts.where(F.col("standard_concept") == F.lit("S")).select(
        F.col("concept_id").alias("standard_concept_id"),
        F.col("concept_name").alias("standard_concept_name"),
    )
    maps_to = (
        spark.table(S4_OMOP_CONCEPT_RELATIONSHIP_TABLE)
        .where(
            (F.col("relationship_id") == F.lit("Maps to"))
            & F.col("invalid_reason").isNull()
        )
        .select(
            F.col("concept_id_1").cast("long").alias("concept_id"),
            F.col("concept_id_2").cast("long").alias("standard_concept_id"),
        )
        .join(source_ids, "concept_id", "inner")
        .join(valid_standard.select("standard_concept_id"), "standard_concept_id", "inner")
        .dropDuplicates(["concept_id", "standard_concept_id"])
    )
    maps_to_agg = maps_to.groupBy("concept_id").agg(
        F.countDistinct("standard_concept_id").cast("long").alias("maps_to_count"),
        F.min("standard_concept_id").cast("long").alias("only_standard_concept_id"),
    )
    resolved = classified.join(maps_to_agg, "concept_id", "left")
    maps_to_count = F.coalesce(F.col("maps_to_count"), F.lit(0).cast("long"))
    already_standard = F.col("source_standard_concept") == F.lit("S")
    resolved = resolved.select(
        F.col("concept_id"),
        F.when(already_standard, F.col("concept_id"))
        .when(maps_to_count == 1, F.col("only_standard_concept_id"))
        .cast("long")
        .alias("OMOP_STANDARD_CONCEPT_ID"),
        F.when(already_standard, F.lit(1).cast("long"))
        .otherwise(maps_to_count)
        .cast("long")
        .alias("OMOP_STANDARD_CANDIDATE_COUNT"),
        F.when(already_standard, F.lit("ALREADY_STANDARD"))
        .when(maps_to_count == 1, F.lit("MAPS_TO_EXACT"))
        .when(maps_to_count > 1, F.lit("MAPS_TO_MULTI"))
        .otherwise(F.lit("NO_STANDARD_MAP"))
        .alias("OMOP_STANDARD_MAPPING_METHOD"),
    )
    resolved = (
        resolved.alias("resolution")
        .join(
            valid_standard.alias("standard_concept"),
            F.col("resolution.OMOP_STANDARD_CONCEPT_ID")
            == F.col("standard_concept.standard_concept_id"),
            "left",
        )
        .select(
            "resolution.*",
            F.col("standard_concept.standard_concept_name").alias("OMOP_STANDARD_CONCEPT_NAME"),
        )
    )

    return (
        enriched.alias("target_keys")
        .join(
            F.broadcast(resolved).alias("standard_resolution"),
            F.col("target_keys.OMOP_CONCEPT_ID") == F.col("standard_resolution.concept_id"),
            "left",
        )
        .select(
            "target_keys.*",
            F.col("standard_resolution.OMOP_STANDARD_CONCEPT_ID").cast("long").alias(
                "OMOP_STANDARD_CONCEPT_ID"
            ),
            F.col("standard_resolution.OMOP_STANDARD_CONCEPT_NAME").alias(
                "OMOP_STANDARD_CONCEPT_NAME"
            ),
            F.col("standard_resolution.OMOP_STANDARD_CANDIDATE_COUNT").cast("long").alias(
                "OMOP_STANDARD_CANDIDATE_COUNT"
            ),
            F.col("standard_resolution.OMOP_STANDARD_MAPPING_METHOD").alias(
                "OMOP_STANDARD_MAPPING_METHOD"
            ),
        )
    )
# S4-C1a END
def apply_order_id_slice(df: DataFrame) -> DataFrame:
    if not BOOTSTRAP_MODE:
        return df
    return df.where(
        F.col("ORDER_ID").cast("long").between(int(BOOTSTRAP_MIN_ORDER_ID), int(BOOTSTRAP_MAX_ORDER_ID))
    )


def add_timestamp_quality(df: DataFrame, column: str, scheduled_future_ok: bool = False) -> DataFrame:
    upper = (
        F.lit("2100-01-01").cast("timestamp")
        if scheduled_future_ok
        else F.lit(RUN_FUTURE_HORIZON).cast("timestamp")
    )
    return df.withColumn(
        f"{column}_QUALITY",
        F.when(F.col(column).isNull(), F.lit("MISSING"))
        .when(F.col(column) < F.lit("1900-01-01").cast("timestamp"), F.lit("INVALID_EARLY"))
        .when(F.col(column) > upper, F.lit("INVALID_FUTURE"))
        .otherwise(F.lit("VALID_OR_PLANNED" if scheduled_future_ok else "VALID")),
    )


def deduplicate_source_rows(df: DataFrame, keys: list[str]) -> DataFrame:
    """Select one deterministic latest technical row for a repeated source natural key."""
    source_hash_columns = sorted(df.columns)
    source_hash = F.sha2(
        F.concat_ws(
            "\u0001",
            *[F.coalesce(F.col(column).cast("string"), F.lit("<NULL>")) for column in source_hash_columns],
        ),
        256,
    )
    source_rank = Window.partitionBy(*[F.col(key).cast("long") for key in keys]).orderBy(
        F.col("UPDT_CNT").desc_nulls_last(),
        F.col("UPDT_DT_TM").desc_nulls_last(),
        F.col("LAST_UTC_TS").desc_nulls_last(),
        F.col("ADC_UPDT").desc_nulls_last(),
        F.col("__SOURCE_ROW_HASH").desc_nulls_last(),
    )
    return (
        df.withColumn("__SOURCE_ROW_HASH", source_hash)
        .withColumn("__SOURCE_ROW_RANK", F.row_number().over(source_rank))
        .where(F.col("__SOURCE_ROW_RANK") == 1)
        .drop("__SOURCE_ROW_HASH", "__SOURCE_ROW_RANK")
    )


def existing_tombstones(target: str, affected_orders: DataFrame | None, staged: DataFrame, keys: list[str]) -> DataFrame | None:
    if affected_orders is None or not bronze_table_exists(target):
        return None
    existing = (
        spark.table(target)
        .join(affected_orders, "ORDER_ID", "inner")
        .select(*keys)
    )
    return existing.join(staged.select(*keys), keys, "left_anti")


def apply_comments(
    table: str,
    table_comment: str,
    comments: dict[str, str],
    consumer: str,
) -> None:
    catalog, schema, table_name = table.split(".")
    table_rows = spark.sql(
        f"SELECT comment FROM {qident(catalog)}.information_schema.tables "
        f"WHERE table_schema = '{sql_escape(schema)}' AND table_name = '{sql_escape(table_name)}'"
    ).collect()
    if not table_rows or (table_rows[0]["comment"] or "") != table_comment:
        spark.sql(f"COMMENT ON TABLE {qname(table)} IS '{sql_escape(table_comment)}'")
    existing = {
        row["column_name"]: row["comment"] or ""
        for row in spark.sql(
            f"SELECT column_name, comment FROM {qident(catalog)}.information_schema.columns "
            f"WHERE table_schema = '{sql_escape(schema)}' AND table_name = '{sql_escape(table_name)}'"
        ).collect()
    }
    actual = set(spark.table(table).columns)
    for column in sorted(actual):
        if column == "ADC_UPDT":
            comment = "Journey standard block: bronze material-change timestamp set by this pipeline."
        elif column == "SOURCE_ADC_UPDT":
            comment = "Journey standard block: raw source-row ADC update timestamp; never the bronze modification stamp."
        else:
            comment = comments.get(
                column,
                f"{consumer}; retained as a source-faithful bronze input or Journey standard-block field.",
            )
        if existing.get(column, "") != comment:
            spark.sql(f"ALTER TABLE {qname(table)} ALTER COLUMN {qident(column)} COMMENT '{sql_escape(comment)}'")


def commit_checkpoints(target_sources: dict[str, list[str]]) -> None:
    rows = []
    for target, sources in target_sources.items():
        for source in sources:
            health = SOURCE_HEALTH[source]
            rows.append(
                (
                    target,
                    source,
                    int(health["version"]),
                    health["watermark"],
                    int(health["rows"]) if health["rows"] is not None else None,
                    RUN_ID,
                )
            )
    if not rows:
        return
    updates = spark.createDataFrame(
        rows,
        "target_table string, source_table string, source_version long, source_watermark timestamp, source_rows long, run_id string",
    ).withColumn("committed_at", F.current_timestamp())
    (
        DeltaTable.forName(spark, STATE_TABLE)
        .alias("t")
        .merge(updates.alias("s"), "t.target_table = s.target_table AND t.source_table = s.source_table")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    for target, sources in target_sources.items():
        declared = "', '".join(sql_escape(source) for source in sources)
        spark.sql(
            f"DELETE FROM {qname(STATE_TABLE)} WHERE target_table = '{sql_escape(target)}' "
            f"AND source_table NOT IN ('{declared}')"
        )


def acquire_run_lock() -> None:
    lock = spark.createDataFrame(
        [(LOCK_KEY, LOCK_KEY, 0, None, None, RUN_ID)],
        "target_table string, source_table string, source_version long, "
        "source_watermark timestamp, source_rows long, run_id string",
    ).withColumn("committed_at", F.current_timestamp())
    (
        DeltaTable.forName(spark, STATE_TABLE)
        .alias("t")
        .merge(lock.alias("s"), "t.target_table = s.target_table AND t.source_table = s.source_table")
        .whenMatchedUpdate(
            condition=f"t.committed_at < current_timestamp() - INTERVAL {LOCK_TTL_HOURS} HOURS",
            set={"run_id": "s.run_id", "committed_at": "current_timestamp()"},
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    owner = spark.table(STATE_TABLE).where(F.col("target_table") == LOCK_KEY).collect()[0]
    assert owner["run_id"] == RUN_ID, (
        f"Another medication_order_pipeline run holds the lock (run_id={owner['run_id']}, acquired "
        f"{owner['committed_at']}, expires after {LOCK_TTL_HOURS}h). Wait, or delete the "
        f"{LOCK_KEY} row from {STATE_TABLE} to break a dead lock."
    )


def release_run_lock() -> None:
    spark.sql(
        f"DELETE FROM {qname(STATE_TABLE)} WHERE target_table = '{LOCK_KEY}' "
        f"AND run_id = '{sql_escape(RUN_ID)}'"
    )


def cleanup_stages() -> None:
    for target in (ORDER, ACTION, INGREDIENT, DETAIL):
        spark.sql(f"DROP TABLE IF EXISTS {qname(f'{target}_stg')}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qname(TARGET_SCHEMA)}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qname(bronze_control_schema(TARGET_SCHEMA))}")
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {qname(STATE_TABLE)} (
      target_table STRING NOT NULL,
      source_table STRING NOT NULL,
      source_version BIGINT NOT NULL,
      source_watermark TIMESTAMP,
      source_rows BIGINT,
      run_id STRING NOT NULL,
      committed_at TIMESTAMP NOT NULL
    ) USING DELTA
    COMMENT 'Medication-order bronze two-phase source version and watermark checkpoints.'
    """
)
for _table, _columns in EXPECTED_COLUMNS.items():
    assert bronze_table_exists(_table), f"Missing source {_table}"
    assert_expected_columns(_table, _columns)

_action_ceiling = datetime.fromisoformat("2024-09-17 23:59:59.999999")

# COMMAND ----------

def pharmacy_orders(order_ids: DataFrame | None = None) -> DataFrame:
    orders = apply_order_id_slice(spark.table(SRC_ORDER))
    orders = scope_by_order_ids(orders, order_ids)
    return orders.where(F.col("ACTIVITY_TYPE_CD").cast("long") == PHARMACY_ACTIVITY_TYPE_CD)


def order_parent(order_ids: DataFrame | None = None) -> DataFrame:
    return pharmacy_orders(order_ids).select(
        F.col("ORDER_ID").alias("ORDER_ID_RAW"),
        F.col("ORDER_ID").cast("long").alias("ORDER_ID"),
        F.when(F.col("PERSON_ID").cast("long") != 0, F.col("PERSON_ID").cast("long")).alias("PARENT_PERSON_ID"),
        F.when(F.col("ENCNTR_ID").cast("long") != 0, F.col("ENCNTR_ID").cast("long")).alias("PARENT_ENCNTR_ID"),
        F.col("ORGANIZATION_ID").cast("long").alias("PARENT_ORGANIZATION_ID"),
        F.col("LAST_ACTION_SEQUENCE").cast("long").alias("LAST_ACTION_SEQUENCE"),
        F.col("ADC_UPDT").alias("ORDER_SOURCE_ADC_UPDT"),
    )


def build_order(order_ids: DataFrame | None, decode_lookup: DataFrame) -> DataFrame:
    result = pharmacy_orders(order_ids).select(
        F.col("ORDER_ID").cast("long").alias("ORDER_ID"),
        F.when(F.col("PERSON_ID").cast("long") != 0, F.col("PERSON_ID").cast("long")).alias("PERSON_ID"),
        F.when(F.col("ENCNTR_ID").cast("long") != 0, F.col("ENCNTR_ID").cast("long")).alias("ENCNTR_ID"),
        F.col("ORGANIZATION_ID").cast("long").alias("ORGANIZATION_ID"),
        F.lit("PHARMACY").alias("ORDER_DOMAIN"),
        F.col("ACTIVITY_TYPE_CD").cast("long").alias("ACTIVITY_TYPE_CD"),
        F.col("CATALOG_CD").cast("long").alias("CATALOG_CD"),
        F.col("CATALOG_TYPE_CD").cast("long").alias("CATALOG_TYPE_CD"),
        F.col("SYNONYM_ID").cast("long").alias("SYNONYM_ID"),
        "ORDER_MNEMONIC", "ORDERED_AS_MNEMONIC", "HNA_ORDER_MNEMONIC",
        "ORDER_DETAIL_DISPLAY_LINE", "CLINICAL_DISPLAY_LINE", "SIMPLIFIED_DISPLAY_LINE",
        F.col("ORDER_STATUS_CD").cast("long").alias("ORDER_STATUS_CD"),
        F.col("DEPT_STATUS_CD").cast("long").alias("DEPT_STATUS_CD"),
        F.col("ACTIVE_STATUS_CD").cast("long").alias("ACTIVE_STATUS_CD"),
        F.col("STOP_TYPE_CD").cast("long").alias("STOP_TYPE_CD"),
        F.col("DISCONTINUE_TYPE_CD").cast("long").alias("DISCONTINUE_TYPE_CD"),
        F.col("MED_ORDER_TYPE_CD").cast("long").alias("MED_ORDER_TYPE_CD"),
        F.col("FREQUENCY_ID").cast("long").alias("FREQUENCY_ID"),
        F.col("LAST_ACTION_SEQUENCE").cast("long").alias("LAST_ACTION_SEQUENCE"),
        F.col("LAST_CORE_ACTION_SEQUENCE").cast("long").alias("LAST_CORE_ACTION_SEQUENCE"),
        F.col("LAST_INGRED_ACTION_SEQUENCE").cast("long").alias("LAST_INGREDIENT_ACTION_SEQUENCE"),
        F.col("PRN_IND").cast("long").alias("PRN_IND"),
        F.col("IV_IND").cast("long").alias("IV_IND"),
        F.col("SUSPEND_IND").cast("long").alias("SUSPEND_IND"),
        F.col("RESUME_IND").cast("long").alias("RESUME_IND"),
        F.col("DISCONTINUE_IND").cast("long").alias("DISCONTINUE_IND"),
        F.when(F.col("ACTIVE_IND").cast("long") == 0, F.lit(1)).otherwise(F.lit(0)).alias("INACTIVE_ORDER_FLAG"),
        F.col("ACTIVE_IND").cast("long").alias("ACTIVE_IND"),
        "ORIG_ORDER_DT_TM", "STATUS_DT_TM", "CURRENT_START_DT_TM", "PROJECTED_STOP_DT_TM",
        "SOFT_STOP_DT_TM", "SUSPEND_EFFECTIVE_DT_TM", "RESUME_EFFECTIVE_DT_TM",
        "DISCONTINUE_EFFECTIVE_DT_TM", "MODIFIED_START_DT_TM", "VALID_DOSE_DT_TM",
        F.col("LAST_UPDATE_PROVIDER_ID").cast("long").alias("LAST_UPDATE_PROVIDER_ID"),
        # S3-A11 BEGIN: retain source update counter for prod-twin provenance parity.
        F.col("UPDT_CNT").cast("long").alias("SOURCE_UPDT_CNT"),
        # S3-A11 END
        F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT"),
    )
    for code, description in [
        ("ACTIVITY_TYPE_CD", "ACTIVITY_TYPE_DESCRIPTION"), ("ORDER_STATUS_CD", "ORDER_STATUS_DESCRIPTION"),
        ("DEPT_STATUS_CD", "DEPT_STATUS_DESCRIPTION"), ("ACTIVE_STATUS_CD", "ACTIVE_STATUS_DESCRIPTION"),
        ("STOP_TYPE_CD", "STOP_TYPE_DESCRIPTION"), ("DISCONTINUE_TYPE_CD", "DISCONTINUE_TYPE_DESCRIPTION"),
        ("MED_ORDER_TYPE_CD", "MED_ORDER_TYPE_DESCRIPTION"),
    ]:
        result = add_decode(result, code, description, decode_lookup)
    for column in ["ORIG_ORDER_DT_TM", "STATUS_DT_TM", "CURRENT_START_DT_TM", "PROJECTED_STOP_DT_TM", "SOFT_STOP_DT_TM", "DISCONTINUE_EFFECTIVE_DT_TM", "VALID_DOSE_DT_TM"]:
        result = add_timestamp_quality(result, column, scheduled_future_ok=column in {"CURRENT_START_DT_TM", "PROJECTED_STOP_DT_TM", "SOFT_STOP_DT_TM", "VALID_DOSE_DT_TM"})
    # S4-C1a BEGIN
    # materialize_stage/merge_target write every returned column, so this extends
    # both the staged write and MERGE update/insert paths for the 12 S4 columns.
    result = add_s4_medication_concepts(result, ["ORDER_ID"])
    # S4-C1a END
    return result


def build_action(affected_orders: DataFrame | None, decode_lookup: DataFrame) -> DataFrame:
    parent = order_parent(affected_orders)
    action = (
        spark.table(SRC_ACTION).alias("source")
        .join(parent.alias("parent"), F.col("source.ORDER_ID") == F.col("parent.ORDER_ID_RAW"), "inner")
        .drop(F.col("parent.ORDER_ID"), F.col("parent.ORDER_ID_RAW"))
    )
    result = action.select(
        F.col("ORDER_ID").cast("long").alias("ORDER_ID"),
        F.col("ACTION_SEQUENCE").cast("long").alias("ACTION_SEQUENCE"),
        F.col("PARENT_PERSON_ID").alias("PERSON_ID"),
        F.col("PARENT_ENCNTR_ID").alias("ENCNTR_ID"),
        F.col("PARENT_ORGANIZATION_ID").alias("ORGANIZATION_ID"),
        F.col("ACTION_TYPE_CD").cast("long").alias("ACTION_TYPE_CD"),
        F.col("ACTION_QUALIFIER_CD").cast("long").alias("ACTION_QUALIFIER_CD"),
        F.col("ORDER_STATUS_CD").cast("long").alias("ORDER_STATUS_CD"),
        F.col("DEPT_STATUS_CD").cast("long").alias("DEPT_STATUS_CD"),
        F.col("STOP_TYPE_CD").cast("long").alias("STOP_TYPE_CD"),
        F.col("ACTION_PERSONNEL_ID").cast("long").alias("ACTION_PERSONNEL_ID"),
        F.col("ORDER_PROVIDER_ID").cast("long").alias("ORDER_PROVIDER_ID"),
        F.col("SUPERVISING_PROVIDER_ID").cast("long").alias("SUPERVISING_PROVIDER_ID"),
        F.col("FREQUENCY_ID").cast("long").alias("FREQUENCY_ID"),
        F.col("PRN_IND").cast("long").alias("PRN_IND"),
        F.col("INACTIVE_FLAG").cast("long").alias("INACTIVE_FLAG"),
        F.col("ACTION_REJECTED_IND").cast("long").alias("ACTION_REJECTED_IND"),
        "CLINICAL_DISPLAY_LINE", "ORDER_DETAIL_DISPLAY_LINE", "SIMPLIFIED_DISPLAY_LINE",
        "ORDER_DT_TM", "ACTION_DT_TM", "EFFECTIVE_DT_TM", "ACTION_INITIATED_DT_TM",
        "PROJECTED_STOP_DT_TM", "NEXT_DOSE_DT_TM", "VALID_DOSE_DT_TM", "CURRENT_START_DT_TM",
        F.lit(True).alias("HISTORICAL_FEED_IND"),
        # S3-A11 BEGIN: child and parent source provenance fill forward.
        F.col("UPDT_CNT").cast("long").alias("SOURCE_UPDT_CNT"),
        F.col("ORDER_SOURCE_ADC_UPDT").alias("ORDER_SOURCE_ADC_UPDT"),
        # S3-A11 END
        F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT"),
    )
    for code, description in [
        ("ACTION_TYPE_CD", "ACTION_TYPE_DESCRIPTION"), ("ACTION_QUALIFIER_CD", "ACTION_QUALIFIER_DESCRIPTION"),
        ("ORDER_STATUS_CD", "ORDER_STATUS_DESCRIPTION"), ("DEPT_STATUS_CD", "DEPT_STATUS_DESCRIPTION"),
        ("STOP_TYPE_CD", "STOP_TYPE_DESCRIPTION"),
    ]:
        result = add_decode(result, code, description, decode_lookup)
    for column in ["ORDER_DT_TM", "ACTION_DT_TM", "EFFECTIVE_DT_TM", "ACTION_INITIATED_DT_TM", "PROJECTED_STOP_DT_TM", "NEXT_DOSE_DT_TM", "VALID_DOSE_DT_TM", "CURRENT_START_DT_TM"]:
        result = add_timestamp_quality(result, column, scheduled_future_ok=column in {"PROJECTED_STOP_DT_TM", "NEXT_DOSE_DT_TM", "VALID_DOSE_DT_TM", "CURRENT_START_DT_TM"})
    return result


def build_ingredient(affected_orders: DataFrame | None, decode_lookup: DataFrame) -> DataFrame:
    parent = order_parent(affected_orders)
    ingredient = (
        spark.table(SRC_INGREDIENT).alias("source")
        .join(parent.alias("parent"), F.col("source.ORDER_ID") == F.col("parent.ORDER_ID_RAW"), "inner")
        .drop(F.col("parent.ORDER_ID"), F.col("parent.ORDER_ID_RAW"))
    )
    ingredient = deduplicate_source_rows(ingredient, ["ORDER_ID", "ACTION_SEQUENCE", "COMP_SEQUENCE"])
    result = ingredient.select(
        F.col("ORDER_ID").cast("long").alias("ORDER_ID"),
        F.col("ACTION_SEQUENCE").cast("long").alias("ACTION_SEQUENCE"),
        F.col("COMP_SEQUENCE").cast("long").alias("COMP_SEQUENCE"),
        F.col("PARENT_PERSON_ID").alias("PERSON_ID"),
        F.col("PARENT_ENCNTR_ID").alias("ENCNTR_ID"),
        F.col("PARENT_ORGANIZATION_ID").alias("ORGANIZATION_ID"),
        F.col("CATALOG_TYPE_CD").cast("long").alias("CATALOG_TYPE_CD"),
        F.col("CATALOG_CD").cast("long").alias("CATALOG_CD"),
        F.col("SYNONYM_ID").cast("long").alias("SYNONYM_ID"),
        "ORDER_MNEMONIC", "ORDER_DETAIL_DISPLAY_LINE", "FREETEXT_DOSE",
        "ORDERED_AS_MNEMONIC", "SUPPLIED_AS_MNEMONIC", "HNA_ORDER_MNEMONIC",
        F.col("STRENGTH").cast("double").alias("STRENGTH"),
        F.col("STRENGTH_UNIT").cast("long").alias("STRENGTH_UNIT_CD"),
        F.col("VOLUME").cast("double").alias("VOLUME"),
        F.col("VOLUME_UNIT").cast("long").alias("VOLUME_UNIT_CD"),
        F.col("DOSE_QUANTITY").cast("double").alias("DOSE_QUANTITY"),
        F.col("DOSE_QUANTITY_UNIT").cast("long").alias("DOSE_QUANTITY_UNIT_CD"),
        F.col("ORDERED_DOSE").cast("double").alias("ORDERED_DOSE"),
        F.col("ORDERED_DOSE_UNIT_CD").cast("long").alias("ORDERED_DOSE_UNIT_CD"),
        F.col("NORMALIZED_RATE").cast("double").alias("NORMALIZED_RATE"),
        F.col("NORMALIZED_RATE_UNIT_CD").cast("long").alias("NORMALIZED_RATE_UNIT_CD"),
        F.col("CONCENTRATION").cast("double").alias("CONCENTRATION"),
        F.col("CONCENTRATION_UNIT_CD").cast("long").alias("CONCENTRATION_UNIT_CD"),
        F.col("INGREDIENT_TYPE_FLAG").cast("long").alias("INGREDIENT_TYPE_FLAG"),
        F.col("CLINICALLY_SIGNIFICANT_FLAG").cast("long").alias("CLINICALLY_SIGNIFICANT_FLAG"),
        F.col("INCLUDE_IN_TOTAL_VOLUME_FLAG").cast("long").alias("INCLUDE_IN_TOTAL_VOLUME_FLAG"),
        # S3-A11 BEGIN: child and parent source provenance fill forward.
        F.col("UPDT_CNT").cast("long").alias("SOURCE_UPDT_CNT"),
        F.col("ORDER_SOURCE_ADC_UPDT").alias("ORDER_SOURCE_ADC_UPDT"),
        # S3-A11 END
        F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT"),
    )
    for code, description in [
        ("CATALOG_TYPE_CD", "CATALOG_TYPE_DESCRIPTION"), ("STRENGTH_UNIT_CD", "STRENGTH_UNIT_DESCRIPTION"),
        ("VOLUME_UNIT_CD", "VOLUME_UNIT_DESCRIPTION"), ("DOSE_QUANTITY_UNIT_CD", "DOSE_QUANTITY_UNIT_DESCRIPTION"),
        ("ORDERED_DOSE_UNIT_CD", "ORDERED_DOSE_UNIT_DESCRIPTION"),
        ("NORMALIZED_RATE_UNIT_CD", "NORMALIZED_RATE_UNIT_DESCRIPTION"),
        ("CONCENTRATION_UNIT_CD", "CONCENTRATION_UNIT_DESCRIPTION"),
    ]:
        result = add_decode(result, code, description, decode_lookup)
    # S4-C1a BEGIN
    # The ingredient increment is already unique at these target keys; retain it
    # as the LEFT side so a missing lookup row yields explicit NULL enrichment.
    result = add_s4_medication_concepts(
        result,
        ["ORDER_ID", "ACTION_SEQUENCE", "COMP_SEQUENCE"],
    )
    # S4-C1a END
    return result


def build_detail(affected_orders: DataFrame | None = None) -> DataFrame:
    parent = order_parent(affected_orders)
    detail = (
        spark.table(SRC_DETAIL).alias("source")
        .join(parent.alias("parent"), F.col("source.ORDER_ID") == F.col("parent.ORDER_ID_RAW"), "inner")
        .drop(F.col("parent.ORDER_ID"), F.col("parent.ORDER_ID_RAW"))
    )
    detail = detail.where(F.col("ACTION_SEQUENCE").cast("long") == F.col("LAST_ACTION_SEQUENCE"))
    detail = deduplicate_source_rows(detail, ["ORDER_ID", "ACTION_SEQUENCE", "DETAIL_SEQUENCE"])
    return detail.select(
        F.col("ORDER_ID").cast("long").alias("ORDER_ID"),
        F.col("ACTION_SEQUENCE").cast("long").alias("ACTION_SEQUENCE"),
        F.col("DETAIL_SEQUENCE").cast("long").alias("DETAIL_SEQUENCE"),
        F.col("PARENT_PERSON_ID").alias("PERSON_ID"),
        F.col("PARENT_ENCNTR_ID").alias("ENCNTR_ID"),
        F.col("PARENT_ORGANIZATION_ID").alias("ORGANIZATION_ID"),
        F.col("OE_FIELD_ID").cast("long").alias("OE_FIELD_ID"),
        "OE_FIELD_MEANING", "OE_FIELD_DISPLAY_VALUE", "OE_FIELD_DISPLAY_VALUE_EXTEND",
        "OE_FIELD_DT_TM_VALUE",
        F.col("OE_FIELD_VALUE").cast("double").alias("OE_FIELD_VALUE"),
        F.col("OE_FIELD_MEANING_ID").cast("long").alias("OE_FIELD_MEANING_ID"),
        F.col("PARENT_ACTION_SEQUENCE").cast("long").alias("PARENT_ACTION_SEQUENCE"),
        "LAST_ACTION_SEQUENCE",
        F.lit("LATEST_ACTION_LONG_FORMAT").alias("DETAIL_HISTORY_CONTRACT"),
        # S3-A11 BEGIN: child and parent source provenance fill forward.
        F.col("UPDT_CNT").cast("long").alias("SOURCE_UPDT_CNT"),
        F.col("ORDER_SOURCE_ADC_UPDT").alias("ORDER_SOURCE_ADC_UPDT"),
        # S3-A11 END
        F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT"),
    )

# COMMAND ----------

TARGET_SOURCES = {
    ORDER: [SRC_ORDER, CODE_VALUE, LOGIC_SOURCE],
    ACTION: [SRC_ACTION, SRC_ORDER, CODE_VALUE, LOGIC_SOURCE],
    INGREDIENT: [SRC_INGREDIENT, SRC_ORDER, CODE_VALUE, LOGIC_SOURCE],
    DETAIL: [SRC_DETAIL, SRC_ORDER, LOGIC_SOURCE],
}
TARGET_KEYS = {
    ORDER: ["ORDER_ID"],
    ACTION: ["ORDER_ID", "ACTION_SEQUENCE"],
    INGREDIENT: ["ORDER_ID", "ACTION_SEQUENCE", "COMP_SEQUENCE"],
    DETAIL: ["ORDER_ID", "ACTION_SEQUENCE", "DETAIL_SEQUENCE"],
}
SOURCE_HEALTH: dict[str, dict] = {}
FULL_MODES = {"FULL", "FULL_LOOKUP_CHANGE"}
RECOVERY_CONFIG = None


def _history_has_run_merge(target: str, run_id: str) -> bool:
    history = spark.sql(f"DESCRIBE HISTORY {qname(target)}")
    return (
        history.where(
            (F.col("operation") == F.lit("MERGE"))
            & (F.col("job.jobRunId").cast("string") == F.lit(str(run_id)))
        )
        .limit(1)
        .count()
        == 1
    )


def _run_overwrite_history(
    target: str,
    run_id: str,
    expected_version: int | None = None,
) -> dict | None:
    history = spark.sql(f"DESCRIBE HISTORY {qname(target)}")
    matching = history.where(
        (F.col("operation") == F.lit("WRITE"))
        & (F.col("job.jobRunId").cast("string") == F.lit(str(run_id)))
        & (
            F.lower(
                F.element_at(F.col("operationParameters"), F.lit("mode"))
            ) == F.lit("overwrite")
        )
    )
    if expected_version is not None:
        matching = matching.where(F.col("version") == F.lit(int(expected_version)))
    row = matching.orderBy(F.col("version").desc()).limit(1).collect()
    if not row:
        return None
    item = row[0].asDict(recursive=True)
    latest_write = (
        history.where(F.col("operation") == F.lit("WRITE"))
        .orderBy(F.col("version").desc())
        .limit(1)
        .collect()[0]
        .asDict(recursive=True)
    )
    assert int(latest_write["version"]) == int(item["version"]), {
        "message": "A later WRITE exists; refusing to finalize an older recovery overwrite",
        "expected_recovery_write": item,
        "latest_write": latest_write,
    }
    metrics = item.get("operationMetrics") or {}
    output_rows = int(
        metrics.get("numOutputRows")
        or metrics.get("numTargetRowsInserted")
        or 0
    )
    assert output_rows > 0, {"target": target, "history": item}
    return {
        "operation": "RECOVERY_FINALIZE_EXISTING_OVERWRITE",
        "version": int(item["version"]),
        "timestamp": str(item["timestamp"]),
        "output_rows": output_rows,
        "job_run_id": str((item.get("job") or {}).get("jobRunId") or ""),
        "task_run_id": str((item.get("job") or {}).get("runId") or ""),
    }


def load_recovery_config() -> dict | None:
    if not RECOVERY_CONTROL_TABLE or not bronze_table_exists(RECOVERY_CONTROL_TABLE):
        return None
    rows = (
        spark.table(RECOVERY_CONTROL_TABLE)
        .where(
            (F.col("pipeline_run_id") == F.lit(RUN_ID))
            & (F.col("status") == F.lit("ACTIVE"))
        )
        .orderBy(F.col("created_at").desc())
        .limit(1)
        .collect()
    )
    if not rows:
        return None
    config = rows[0].asDict(recursive=True)
    selected = set(json.loads(config["selected_targets_json"]))
    expected_successful = set(json.loads(config["expected_successful_targets_json"]))
    assert selected == {DETAIL}, selected
    assert expected_successful == {ORDER, ACTION, INGREDIENT}, expected_successful
    missing_success = sorted(
        target for target in expected_successful
        if not _history_has_run_merge(target, RUN_ID)
    )
    assert not missing_success, {
        "message": "Recovery would skip targets without a successful MERGE from this run",
        "run_id": RUN_ID,
        "missing_success": missing_success,
    }
    details = json.loads(config.get("details_json") or "{}")
    expected_write_version = details.get("expected_detail_write_version")
    existing_detail_overwrite = None
    if expected_write_version is not None:
        existing_detail_overwrite = _run_overwrite_history(
            DETAIL,
            RUN_ID,
            int(expected_write_version),
        )
        assert existing_detail_overwrite is not None, {
            "message": "Expected completed medication-detail overwrite was not found",
            "run_id": RUN_ID,
            "expected_version": expected_write_version,
        }
        expected_output_rows = details.get("expected_detail_output_rows")
        if expected_output_rows is not None:
            assert int(existing_detail_overwrite["output_rows"]) == int(expected_output_rows), {
                "expected_output_rows": int(expected_output_rows),
                "actual": existing_detail_overwrite,
            }
    config["selected_targets"] = selected
    config["expected_successful_targets"] = expected_successful
    config["details"] = details
    config["existing_detail_overwrite"] = existing_detail_overwrite
    return config


def apply_recovery_scope(modes: dict[str, str]) -> dict[str, str]:
    global RECOVERY_CONFIG
    RECOVERY_CONFIG = load_recovery_config()
    if RECOVERY_CONFIG is None:
        return modes
    adjusted = {target: "UNCHANGED_SKIP" for target in TARGET_SOURCES}
    if RECOVERY_CONFIG.get("existing_detail_overwrite") is not None:
        adjusted[DETAIL] = "RECOVERY_FINALIZE"
        print(
            "[MEDICATION_ORDER][RECOVERY] existing detail overwrite verified; "
            "finalizing checkpoints without rebuilding 2.83B rows"
        )
    else:
        adjusted[DETAIL] = "FULL_REPLACE"
        print(
            "[MEDICATION_ORDER][RECOVERY] detail-only full replacement; "
            "order/action/ingredient verified and skipped"
        )
    return adjusted


def mark_recovery_control(status: str, details: dict) -> None:
    if RECOVERY_CONFIG is None:
        return
    payload = sql_escape(json.dumps(details, sort_keys=True, default=str))
    spark.sql(
        f"UPDATE {qname(RECOVERY_CONTROL_TABLE)} "
        f"SET status='{sql_escape(status)}', completed_at=current_timestamp(), "
        f"details_json='{payload}' "
        f"WHERE pipeline_run_id='{sql_escape(RUN_ID)}' AND status='ACTIVE'"
    )


def source_mode(source: str, modes: dict[str, str]) -> str:
    consuming_modes = [mode for target, mode in modes.items() if source in TARGET_SOURCES[target]]
    if any(mode in FULL_MODES or mode in {"BOOTSTRAP", "FULL_REPLACE"} for mode in consuming_modes):
        return "FULL"
    if any(mode == "INCREMENTAL" for mode in consuming_modes):
        return "INCREMENTAL"
    return "UNCHANGED_SKIP"


def health_checkpoint(source: str) -> dict | None:
    candidates = [
        last_checkpoint(target, source)
        for target, sources in TARGET_SOURCES.items()
        if source in sources and last_checkpoint(target, source) is not None
    ]
    with_watermark = [row for row in candidates if row.get("source_watermark") is not None]
    if with_watermark:
        return sorted(with_watermark, key=lambda row: row["source_watermark"])[0]
    return candidates[0] if candidates else None


def validate_features(check_target_uniqueness: bool) -> None:
    for target, keys in TARGET_KEYS.items():
        assert bronze_table_exists(target), f"Missing target {target}"
        properties = spark.sql(f"DESCRIBE DETAIL {qname(target)}").collect()[0]["properties"] or {}
        for property_name in (
            "delta.enableChangeDataFeed",
            "delta.enableRowTracking",
            "delta.enableDeletionVectors",
        ):
            assert str(properties.get(property_name, "false")).lower() == "true", (
                f"{target}: {property_name} not enabled"
            )
        if check_target_uniqueness:
            assert_unique_non_null(spark.table(target), keys, target)


def run_incremental_suite(modes: dict[str, str], metrics: dict[str, dict]) -> dict:
    result = {"level": "INCREMENTAL_SLICE", "targets": {}}
    for target, mode in modes.items():
        if mode != "INCREMENTAL":
            continue
        keys = TARGET_KEYS[target]
        staged = spark.table(f"{target}_stg")
        target_frame = spark.table(target)
        condition = reduce(
            lambda left, right: left & right,
            [F.col(f"target.{key}") == F.col(f"staged.{key}") for key in keys],
        )
        matched = (
            target_frame.alias("target")
            .join(staged.alias("staged"), condition, "inner")
            .where(
                F.col("target.SOURCE_PRESENT_IND")
                & (F.col("target.ROW_HASH") == F.col("staged.ROW_HASH"))
            )
            .count()
        )
        staged_rows = staged.count()
        assert matched == staged_rows, f"{target}: post-merge slice parity {matched} != {staged_rows}"
        expected_tombstones = int(metrics[target]["tombstones_staged"])
        observed_tombstones = target_frame.where(
            (~F.col("SOURCE_PRESENT_IND")) & (F.col("PIPELINE_RUN_ID") == RUN_ID)
        ).count()
        assert observed_tombstones == expected_tombstones, (
            f"{target}: tombstone read-back {observed_tombstones} != {expected_tombstones}"
        )
        result["targets"][target] = {
            "staged_rows": int(staged_rows),
            "row_hash_matches": int(matched),
            "tombstones_verified": int(observed_tombstones),
        }
    if modes[ACTION] == "INCREMENTAL":
        max_action_source = spark.table(f"{ACTION}_stg").agg(
            F.max("SOURCE_ADC_UPDT").alias("watermark")
        ).collect()[0]["watermark"]
        assert max_action_source is None or max_action_source.replace(tzinfo=None) <= _action_ceiling
        result["action_history_ceiling"] = str(max_action_source)
    return result


def run_full_parity_suite(modes: dict[str, str]) -> dict:
    result = {"level": "FULL_PARITY", "targets": {}}
    child_targets = (ACTION, INGREDIENT, DETAIL)
    needs_parent_ids = any(modes[target] in FULL_MODES for target in child_targets)

    parent_ids = None
    if needs_parent_ids:
        parent_ids = (
            spark.table(SRC_ORDER)
            .where(F.col("ACTIVITY_TYPE_CD").cast("long") == PHARMACY_ACTIVITY_TYPE_CD)
            .select(
                F.col("ORDER_ID").cast("long").alias("ORDER_ID"),
                F.col("LAST_ACTION_SEQUENCE").cast("long").alias(
                    "LAST_ACTION_SEQUENCE"
                ),
            )
        )

    if modes[ORDER] in FULL_MODES:
        expected_orders = spark.table(SRC_ORDER).where(
            F.col("ACTIVITY_TYPE_CD").cast("long") == PHARMACY_ACTIVITY_TYPE_CD
        ).count()
        present_orders = spark.table(ORDER).where("SOURCE_PRESENT_IND").count()
        assert present_orders == expected_orders, (
            f"order parity: {present_orders} != {expected_orders}"
        )
        encounter_rate = spark.table(ORDER).where("SOURCE_PRESENT_IND").agg(
            F.avg(F.col("ENCNTR_ID").isNotNull().cast("double")).alias("rate")
        ).collect()[0]["rate"]
        result["targets"][ORDER] = {
            "present_rows": int(present_orders),
            "expected_rows": int(expected_orders),
            "order_encounter_rate": encounter_rate,
        }

    if modes[ACTION] in FULL_MODES:
        expected_action = (
            spark.table(SRC_ACTION)
            .select(F.col("ORDER_ID").cast("long").alias("ORDER_ID"))
            .join(parent_ids.select("ORDER_ID"), "ORDER_ID", "inner")
            .count()
        )
        present_action = spark.table(ACTION).where("SOURCE_PRESENT_IND").count()
        assert present_action == expected_action, (
            f"action parity: {present_action} != {expected_action}"
        )
        max_action_source = spark.table(ACTION).agg(
            F.max("SOURCE_ADC_UPDT").alias("watermark")
        ).collect()[0]["watermark"]
        assert max_action_source.replace(tzinfo=None) <= _action_ceiling
        result["targets"][ACTION] = {
            "present_rows": int(present_action),
            "expected_rows": int(expected_action),
            "action_history_ceiling": str(max_action_source),
        }

    if modes[INGREDIENT] in FULL_MODES:
        expected_ingredient = (
            spark.table(SRC_INGREDIENT)
            .select(
                F.col("ORDER_ID").cast("long").alias("ORDER_ID"),
                F.col("ACTION_SEQUENCE").cast("long").alias("ACTION_SEQUENCE"),
                F.col("COMP_SEQUENCE").cast("long").alias("COMP_SEQUENCE"),
            )
            .join(parent_ids.select("ORDER_ID"), "ORDER_ID", "inner")
            .dropDuplicates(["ORDER_ID", "ACTION_SEQUENCE", "COMP_SEQUENCE"])
            .count()
        )
        present_ingredient = (
            spark.table(INGREDIENT).where("SOURCE_PRESENT_IND").count()
        )
        assert present_ingredient == expected_ingredient, (
            f"ingredient parity: {present_ingredient} != {expected_ingredient}"
        )
        result["targets"][INGREDIENT] = {
            "present_rows": int(present_ingredient),
            "expected_rows": int(expected_ingredient),
        }

    if modes[DETAIL] in FULL_MODES:
        expected_detail = (
            spark.table(SRC_DETAIL)
            .select(
                F.col("ORDER_ID").cast("long").alias("ORDER_ID"),
                F.col("ACTION_SEQUENCE").cast("long").alias("ACTION_SEQUENCE"),
                F.col("DETAIL_SEQUENCE").cast("long").alias("DETAIL_SEQUENCE"),
            )
            .join(parent_ids, "ORDER_ID", "inner")
            .where(F.col("ACTION_SEQUENCE") == F.col("LAST_ACTION_SEQUENCE"))
            .select("ORDER_ID", "ACTION_SEQUENCE", "DETAIL_SEQUENCE")
            .dropDuplicates(["ORDER_ID", "ACTION_SEQUENCE", "DETAIL_SEQUENCE"])
            .count()
        )
        present_detail = spark.table(DETAIL).where("SOURCE_PRESENT_IND").count()
        assert present_detail == expected_detail, (
            f"detail parity: {present_detail} != {expected_detail}"
        )
        result["targets"][DETAIL] = {
            "present_rows": int(present_detail),
            "expected_rows": int(expected_detail),
            "detail_history_contract": "LATEST_ACTION_LONG_FORMAT",
        }

    return result


def apply_output_comments() -> None:
    apply_comments(
        ORDER,
        "S10 medication_order feeder: one decoded Millennium Pharmacy order. S4 concept enrichment is retained and S3-A11 publishes the source update counter.",
        {
            "ORDER_ID": "S10 medication_order key and liquid-clustering key.",
            "ORDER_DOMAIN": "S10 source classification: PHARMACY; Pharmacy Consults excluded.",
            "LAST_CORE_ACTION_SEQUENCE": "S10 ordered state-history assembly input; 87 measured values.",
            "LAST_INGREDIENT_ACTION_SEQUENCE": "S10 ingredient-state assembly input; 88 measured values.",
            "SOFT_STOP_DT_TM": "S10 lifecycle timestamp retained despite sparse population.",
            "MODIFIED_START_DT_TM": "S10 lifecycle timestamp retained despite sparse population.",
            "VALID_DOSE_DT_TM": "S10 planned-dose lifecycle timestamp.",
            "SOURCE_ADC_UPDT": "Journey standard block input: raw order ADC timestamp.",
        },
        "S10 medication_order",
    )
    apply_comments(
        ACTION,
        "S10 medication_order state-history feeder. The source is historical and hard-capped at 2024-09-17; S3-A11 retains parent-order contributor stamps and source counters.",
        {
            "HISTORICAL_FEED_IND": "S10 provenance flag; true while mill_order_action remains frozen.",
            "SOURCE_ADC_UPDT": "S10 action-history source timestamp; must not exceed the declared ceiling.",
        },
        "S10 medication_order ordered status history",
    )
    apply_comments(
        INGREDIENT,
        "S10 medication_order ingredient feeder at ORDER_ID + ACTION_SEQUENCE + COMP_SEQUENCE. Natural-key duplicates are deterministically collapsed; S3-A11 retains parent stamps and counters.",
        {"FREETEXT_DOSE": "S10 dosage input; source text with no re-landed person attributes."},
        "S10 medication_order medication/dosage",
    )
    apply_comments(
        DETAIL,
        "S10 latest-action order-detail feeder in long form. Natural-key duplicates are collapsed; historical detail remains excluded while S3-A11 retains parent stamps/counters.",
        {"DETAIL_HISTORY_CONTRACT": "S10 contract: LATEST_ACTION_LONG_FORMAT; historical detail requires a separate bronze product."},
        "S10 medication_order dosage and route detail",
    )


def run_pipeline() -> dict:
    global RUN_FUTURE_HORIZON, SOURCE_HEALTH
    load_state_cache()
    SOURCE_VERSIONS.clear()
    SOURCE_VERSIONS.update(
        {source: source_version(source) for source in [*SOURCE_SLA.keys(), LOGIC_SOURCE]}
    )
    modes = {target: choose_mode(target, sources) for target, sources in TARGET_SOURCES.items()}
    modes = apply_recovery_scope(modes)
    SOURCE_HEALTH = {
        source: source_health_for_mode(source, source_mode(source, modes), health_checkpoint(source))
        for source in [*SOURCE_SLA.keys(), LOGIC_SOURCE]
    }
    for table, (sla_days, freshness_mode) in SOURCE_SLA.items():
        if RECOVERY_CONFIG is not None and SOURCE_HEALTH[table]["scan"] == "REUSED":
            continue
        stale = SOURCE_HEALTH[table]["source_staleness_days"]
        assert stale is not None, f"{table}: source watermark is NULL"
        if stale > sla_days:
            message = f"{table}: staleness {stale:.2f}d exceeds {sla_days}d SLA ({freshness_mode})"
            if freshness_mode == "LIVE":
                raise AssertionError(message)
            print(f"[WARN] {message}")
    if SOURCE_HEALTH[SRC_ACTION]["scan"] == "FULL":
        assert SOURCE_HEALTH[SRC_ACTION]["watermark"].replace(tzinfo=None) <= _action_ceiling, (
            "mill_order_action advanced beyond the declared historical ceiling; stop and plan a controlled backfill"
        )
    RUN_FUTURE_HORIZON = builtins.max(
        health["watermark"] for health in SOURCE_HEALTH.values() if health["watermark"] is not None
    ) + timedelta(days=2)

    needs_decode_lookup = any(
        modes[target] != "UNCHANGED_SKIP"
        for target in (ORDER, ACTION, INGREDIENT)
    )
    decode_lookup = None
    if needs_decode_lookup:
        decode_lookup = spark.table(CODE_VALUE).select(
            F.col("CODE_VALUE").cast("long").alias("__CODE_VALUE"),
            F.coalesce(F.col("DESCRIPTION"), F.col("DISPLAY")).alias("__CODE_DESCRIPTION"),
        )
        pharmacy_decode = (
            decode_lookup.where(F.col("__CODE_VALUE") == PHARMACY_ACTIVITY_TYPE_CD)
            .select(F.col("__CODE_DESCRIPTION").alias("description")).collect()
        )
        assert len(pharmacy_decode) == 1 and pharmacy_decode[0]["description"] == "Pharmacy", pharmacy_decode

    print(
        f"[MEDICATION_ORDER] target={TARGET_SCHEMA}, run_id={RUN_ID}, logic={PIPELINE_LOGIC_VERSION}, "
        f"bootstrap={BOOTSTRAP_MODE}, future_horizon={RUN_FUTURE_HORIZON}"
    )
    for target, mode in modes.items():
        print(f"[MEDICATION_ORDER] {target}: {mode}")
    for source, health in SOURCE_HEALTH.items():
        print(f"[MEDICATION_ORDER][HEALTH] {source}: {health['scan']}")

    metrics: dict[str, dict] = {}
    checkpoints: dict[str, list[str]] = {}

    if modes[ORDER] != "UNCHANGED_SKIP":
        affected = union_order_ids([changed_rows(SRC_ORDER, ORDER)]) if modes[ORDER] == "INCREMENTAL" else None
        staged = materialize_stage(build_order(affected, decode_lookup), ORDER, TARGET_KEYS[ORDER])
        tombstones = affected.select("ORDER_ID").join(staged.select("ORDER_ID"), "ORDER_ID", "left_anti") if affected is not None else None
        metrics[ORDER] = merge_target(staged, ORDER, TARGET_KEYS[ORDER], modes[ORDER] in FULL_MODES, tombstones)
        checkpoints[ORDER] = TARGET_SOURCES[ORDER]

    if modes[ACTION] != "UNCHANGED_SKIP":
        affected = union_order_ids([changed_rows(SRC_ACTION, ACTION), changed_rows(SRC_ORDER, ACTION)]) if modes[ACTION] == "INCREMENTAL" else None
        staged = materialize_stage(build_action(affected, decode_lookup), ACTION, TARGET_KEYS[ACTION])
        tombstones = existing_tombstones(ACTION, affected, staged, TARGET_KEYS[ACTION])
        metrics[ACTION] = merge_target(staged, ACTION, TARGET_KEYS[ACTION], modes[ACTION] in FULL_MODES, tombstones)
        checkpoints[ACTION] = TARGET_SOURCES[ACTION]

    if modes[INGREDIENT] != "UNCHANGED_SKIP":
        affected = union_order_ids([changed_rows(SRC_INGREDIENT, INGREDIENT), changed_rows(SRC_ORDER, INGREDIENT)]) if modes[INGREDIENT] == "INCREMENTAL" else None
        staged = materialize_stage(build_ingredient(affected, decode_lookup), INGREDIENT, TARGET_KEYS[INGREDIENT])
        tombstones = existing_tombstones(INGREDIENT, affected, staged, TARGET_KEYS[INGREDIENT])
        metrics[INGREDIENT] = merge_target(staged, INGREDIENT, TARGET_KEYS[INGREDIENT], modes[INGREDIENT] in FULL_MODES, tombstones)
        checkpoints[INGREDIENT] = TARGET_SOURCES[INGREDIENT]

    if modes[DETAIL] != "UNCHANGED_SKIP":
        if modes[DETAIL] == "RECOVERY_FINALIZE":
            metrics[DETAIL] = dict(RECOVERY_CONFIG["existing_detail_overwrite"])
        elif modes[DETAIL] == "FULL_REPLACE":
            metrics[DETAIL] = replace_target_full(
                build_detail(None),
                DETAIL,
                TARGET_KEYS[DETAIL],
            )
        else:
            affected = union_order_ids([changed_rows(SRC_DETAIL, DETAIL), changed_rows(SRC_ORDER, DETAIL)]) if modes[DETAIL] == "INCREMENTAL" else None
            staged = materialize_stage(build_detail(affected), DETAIL, TARGET_KEYS[DETAIL])
            tombstones = existing_tombstones(DETAIL, affected, staged, TARGET_KEYS[DETAIL])
            metrics[DETAIL] = merge_target(staged, DETAIL, TARGET_KEYS[DETAIL], modes[DETAIL] in FULL_MODES, tombstones)
        checkpoints[DETAIL] = TARGET_SOURCES[DETAIL]

    if BOOTSTRAP_MODE:
        validation = {"level": "BOOTSTRAP_DEFERRED"}
        print("[MEDICATION_ORDER] bootstrap slice complete; checkpoints and reconciliation deferred")
    else:
        if RECOVERY_CONFIG is not None:
            validate_features(check_target_uniqueness=False)
            validation = {
                "level": (
                    "DETAIL_EXISTING_OVERWRITE_FINALIZED"
                    if modes[DETAIL] == "RECOVERY_FINALIZE"
                    else "DETAIL_ONLY_FULL_REPLACE"
                ),
                "targets": {DETAIL: metrics[DETAIL]},
                "skipped_verified_targets": sorted(
                    RECOVERY_CONFIG["expected_successful_targets"]
                ),
            }
            apply_output_comments()
            commit_checkpoints(checkpoints)
            mark_recovery_control("COMPLETE", validation)
        else:
            ran_full = any(mode in FULL_MODES for mode in modes.values())
            ran_incremental = any(mode == "INCREMENTAL" for mode in modes.values())
            validate_features(check_target_uniqueness=ran_full)
            if all(mode == "UNCHANGED_SKIP" for mode in modes.values()):
                validation = {"level": "SKIPPED_ALL_UNCHANGED"}
            else:
                full_validation = (
                    run_full_parity_suite(modes) if ran_full else None
                )
                incremental_validation = (
                    run_incremental_suite(modes, metrics)
                    if ran_incremental
                    else None
                )
                if full_validation is not None and incremental_validation is not None:
                    validation = {
                        "level": "MIXED_FULL_AND_INCREMENTAL",
                        "full": full_validation,
                        "incremental": incremental_validation,
                    }
                elif full_validation is not None:
                    validation = full_validation
                else:
                    validation = incremental_validation
            if any(mode != "UNCHANGED_SKIP" for mode in modes.values()):
                apply_output_comments()
            commit_checkpoints(checkpoints)

    return {
        "status": "SUCCESS",
        "pipeline": "medication_order_pipeline",
        "pipeline_logic_version": PIPELINE_LOGIC_VERSION,
        "run_id": RUN_ID,
        "target_schema": TARGET_SCHEMA,
        "modes": modes,
        "metrics": metrics,
        "validation": validation,
        "run_future_horizon": str(RUN_FUTURE_HORIZON),
        "source_health": {
            table: {**health, "watermark": str(health["watermark"]) if health["watermark"] is not None else None}
            for table, health in SOURCE_HEALTH.items()
        },
        "finished_at": bronze_utc_now(),
    }


acquire_run_lock()
try:
    SUMMARY = run_pipeline()
except Exception as exc:
    mark_recovery_control(
        "FAILED",
        {"error_type": type(exc).__name__, "error": str(exc)},
    )
    raise
finally:
    cleanup_stages()
    release_run_lock()

print(json.dumps(SUMMARY, indent=2, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(SUMMARY, sort_keys=True, default=str))


