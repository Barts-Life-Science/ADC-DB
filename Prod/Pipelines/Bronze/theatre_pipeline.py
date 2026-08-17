# Databricks notebook source
# MAGIC %md
# MAGIC # Theatre / SurgiNet Bronze Pipeline
# MAGIC
# MAGIC Publishes five source-faithful theatre products without manufacturing theatre sessions:
# MAGIC `map_theatre_case`, `map_theatre_case_procedure`, `map_theatre_case_times`,
# MAGIC `map_theatre_case_attendance`, and `map_theatre_implant_log`.
# MAGIC
# MAGIC Live verification 2026-08-08 found `CURR_CASE_STATUS_CD=0` on every case, so it is never
# MAGIC used for business status. `CASE_STATUS` derives only from cancellation and decoded real
# MAGIC milestones. Free-text attendee and cancellation-requestor names are excluded. Person and
# MAGIC encounter identifiers always come from the surgical-case parent; source exceptions remain
# MAGIC null and are reported rather than fabricated.
# MAGIC
# MAGIC Curation (`2026.08.v2.0`): S7 `procedure`, S14 theatre-feeder contracts, care participation
# MAGIC and the Journey standard block are the named consumers. All `*_RAW` timestamp twins,
# MAGIC source counters and contributor-specific source stamps are cut after deterministic quality
# MAGIC classification. Scheduled area/location/specialty are retained because they agree with the
# MAGIC actual attributes on only 46.0%/44.4%/53.9% of co-populated cases. Source `MODIFIER` is
# MAGIC retained alongside coded arrays because agreement is 90.6%, below the 99% duplicate cut.

# COMMAND ----------

for _name, _default in {
    "target_schema": "8_dev.bronze",
    "allow_production_write": "false",
    "force_full_refresh": "false",
    "full_reconciliation": "false",
    "bootstrap_mode": "false",
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

TARGET_SCHEMA = bronze_value("target_schema", "8_dev.bronze")
ALLOW_PRODUCTION_WRITE = bronze_bool("allow_production_write", False)
FORCE_FULL_REFRESH = bronze_bool("force_full_refresh", False)
FULL_RECONCILIATION = bronze_bool("full_reconciliation", False)
BOOTSTRAP_MODE = bronze_bool("bootstrap_mode", False)
RUN_ID = bronze_run_id()
PIPELINE_LOGIC_VERSION = "2026.08.v2.0"
LOGIC_VERSION_INT = 2026080801
LOGIC_SOURCE = "__PIPELINE_LOGIC__"
RUN_FUTURE_HORIZON = None

assert not TARGET_SCHEMA.lower().startswith("4_prod") or ALLOW_PRODUCTION_WRITE, (
    "Production writes are disabled. Only the approved orchestrator may pass allow_production_write=true."
)

RAW = "4_prod.raw"
CODE_VALUE = "3_lookup.mill.mill_code_value"
IMPLANT_DETAILS_SOURCE = bronze_value("implant_details_source", "4_prod.bronze.map_implant_details")
SRC_CASE = f"{RAW}.mill_surgical_case"
SRC_STATE = f"{RAW}.mill_sn_surg_case_st"
SRC_PROCEDURE = f"{RAW}.mill_surg_case_procedure"
SRC_MODIFIER = f"{RAW}.mill_surg_case_proc_modifier"
SRC_TIMES = f"{RAW}.mill_case_times"
SRC_ATTENDANCE = f"{RAW}.mill_case_attendance"
SRC_IMPLANT = f"{RAW}.mill_sn_implant_log_st"

CASE = f"{TARGET_SCHEMA}.map_theatre_case"
PROCEDURE = f"{TARGET_SCHEMA}.map_theatre_case_procedure"
TIMES = f"{TARGET_SCHEMA}.map_theatre_case_times"
ATTENDANCE = f"{TARGET_SCHEMA}.map_theatre_case_attendance"
IMPLANT = f"{TARGET_SCHEMA}.map_theatre_implant_log"
STATE_TABLE = f"{bronze_control_schema(TARGET_SCHEMA)}.theatre_pipeline_state"
LOCK_KEY = "__RUN_LOCK__"
LOCK_TTL_HOURS = 12

SOURCE_SLA = {
    SRC_CASE: (2, "LIVE"),
    SRC_STATE: (2, "LIVE"),
    SRC_PROCEDURE: (2, "LIVE"),
    SRC_MODIFIER: (2, "LIVE"),
    SRC_TIMES: (2, "LIVE"),
    SRC_ATTENDANCE: (2, "LIVE"),
    SRC_IMPLANT: (2, "LIVE"),
    CODE_VALUE: (365, "REFERENCE"),
    IMPLANT_DETAILS_SOURCE: (365, "REFERENCE"),
}
LOOKUP_SOURCES = {CODE_VALUE, IMPLANT_DETAILS_SOURCE}
EXPECTED_COLUMNS = {
    SRC_CASE: {"SURG_CASE_ID", "PERSON_ID", "ENCNTR_ID", "SCH_EVENT_ID", "SCHED_START_DT_TM", "CANCEL_DT_TM", "ADC_UPDT"},
    SRC_STATE: {"SN_SURG_CASE_ST_ID", "SURG_CASE_ID", "SCH_APPT_ID", "SCH_SLOT_TYPE_ID", "ADC_UPDT"},
    SRC_PROCEDURE: {"SURG_CASE_PROC_ID", "SURG_CASE_ID", "SURG_PROC_CD", "PROC_TEXT", "ORDER_ID", "ADC_UPDT"},
    SRC_MODIFIER: {"SURG_CASE_PROC_MOD_ID", "SURG_CASE_PROC_ID", "MODIFIER_CD", "MODIFIER_SEQ", "ADC_UPDT"},
    SRC_TIMES: {"CASE_TIMES_ID", "SURG_CASE_ID", "TASK_ASSAY_CD", "CASE_TIME_DT_TM", "ADC_UPDT"},
    SRC_ATTENDANCE: {"CASE_ATTENDANCE_ID", "SURG_CASE_ID", "CASE_ATTENDEE_ID", "ROLE_PERF_CD", "IN_DT_TM", "OUT_DT_TM", "ADC_UPDT"},
    SRC_IMPLANT: {"IMPLANT_LOG_ST_ID", "SURG_CASE_ID", "ITEM_ID", "SERIAL_NUMBER", "LOT_NUMBER", "ADC_UPDT"},
    CODE_VALUE: {"CODE_VALUE", "DESCRIPTION", "DISPLAY", "ADC_UPDT"},
    IMPLANT_DETAILS_SOURCE: {"PERSON_ID", "EVENT_ID", "SERIAL_NUMBER", "GS1_SERIAL_NUMBER", "ADC_UPDT"},
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


def union_key_frames(frames: list[DataFrame], key: str) -> DataFrame:
    raw_key = f"{key}_RAW"
    selected = [
        frame.select(F.col(key).alias(raw_key), F.col(key).cast("long").alias(key))
        for frame in frames
    ]
    return (
        reduce(lambda left, right: left.unionByName(right), selected)
        .where(F.col(key).isNotNull())
        .groupBy(key)
        .agg(F.min(raw_key).alias(raw_key))
        .select(raw_key, key)
    )


def scope_by_ids(frame: DataFrame, ids: DataFrame | None, key: str) -> DataFrame:
    if ids is None:
        return frame
    raw_key = f"{key}_RAW"
    return (
        frame.alias("source")
        .join(ids.select(raw_key).alias("scope"), F.col(f"source.{key}") == F.col(f"scope.{raw_key}"), "inner")
        .drop(F.col(f"scope.{raw_key}"))
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


def add_performed_timestamp(df: DataFrame, raw_column: str, output_column: str) -> DataFrame:
    quality = f"{output_column}_QUALITY"
    valid = (
        F.col(raw_column).isNotNull()
        & (F.col(raw_column) >= F.lit("1900-01-01").cast("timestamp"))
        & (F.col(raw_column) <= F.lit(RUN_FUTURE_HORIZON).cast("timestamp"))
    )
    return (
        df.withColumn(
            quality,
            F.when(F.col(raw_column).isNull(), F.lit("MISSING"))
            .when(F.col(raw_column) < F.lit("1900-01-01").cast("timestamp"), F.lit("INVALID_EARLY"))
            .when(F.col(raw_column) > F.lit(RUN_FUTURE_HORIZON).cast("timestamp"), F.lit("INVALID_FUTURE"))
            .otherwise(F.lit("VALID")),
        )
        .withColumn(output_column, F.when(valid, F.col(raw_column)))
        .drop(raw_column)
    )


def add_scheduled_quality(df: DataFrame, column: str) -> DataFrame:
    return df.withColumn(
        f"{column}_QUALITY",
        F.when(F.col(column).isNull(), F.lit("MISSING"))
        .when(F.col(column) < F.lit("1900-01-01").cast("timestamp"), F.lit("INVALID_EARLY"))
        .when(F.col(column) > F.lit("2100-01-01").cast("timestamp"), F.lit("INVALID_FUTURE"))
        .otherwise(F.lit("VALID_OR_SCHEDULED_FUTURE")),
    )


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
            comment = "Journey standard block: greatest contributing raw-source ADC update timestamp."
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
        f"Another theatre_pipeline run holds the lock (run_id={owner['run_id']}, acquired "
        f"{owner['committed_at']}, expires after {LOCK_TTL_HOURS}h). Wait, or delete the "
        f"{LOCK_KEY} row from {STATE_TABLE} to break a dead lock."
    )


def release_run_lock() -> None:
    spark.sql(
        f"DELETE FROM {qname(STATE_TABLE)} WHERE target_table = '{LOCK_KEY}' "
        f"AND run_id = '{sql_escape(RUN_ID)}'"
    )


def cleanup_stages() -> None:
    for target in (CASE, PROCEDURE, TIMES, ATTENDANCE, IMPLANT):
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
    COMMENT 'Theatre bronze two-phase source version and watermark checkpoints.'
    """
)
for _table, _columns in EXPECTED_COLUMNS.items():
    assert bronze_table_exists(_table), f"Missing source {_table}"
    assert_expected_columns(_table, _columns)

_cv_counts = spark.table(CODE_VALUE).agg(F.count("CODE_VALUE").alias("n"), F.countDistinct("CODE_VALUE").alias("d")).collect()[0]
assert _cv_counts["n"] == _cv_counts["d"], "Code-value lookup is not unique"

# COMMAND ----------

def scoped_case_parent(case_ids: DataFrame | None = None) -> DataFrame:
    case = scope_by_ids(spark.table(SRC_CASE), case_ids, "SURG_CASE_ID")
    return case.select(
        F.col("SURG_CASE_ID").cast("long").alias("SURG_CASE_ID"),
        F.when(F.col("PERSON_ID").cast("long") != 0, F.col("PERSON_ID").cast("long")).alias("PERSON_ID"),
        F.when(F.col("ENCNTR_ID").cast("long") != 0, F.col("ENCNTR_ID").cast("long")).alias("ENCNTR_ID"),
        F.col("ORGANIZATION_ID").cast("long").alias("ORGANIZATION_ID"),
        F.col("ADC_UPDT").alias("CASE_SOURCE_ADC_UPDT"),
    )


def build_case(case_ids: DataFrame | None, decode_lookup: DataFrame) -> DataFrame:
    case = scope_by_ids(spark.table(SRC_CASE), case_ids, "SURG_CASE_ID")
    case = case.select(
        F.col("SURG_CASE_ID").alias("SURG_CASE_ID_RAW"),
        F.col("SURG_CASE_ID").cast("long").alias("SURG_CASE_ID"),
        F.when(F.col("PERSON_ID").cast("long") != 0, F.col("PERSON_ID").cast("long")).alias("PERSON_ID"),
        F.when(F.col("ENCNTR_ID").cast("long") != 0, F.col("ENCNTR_ID").cast("long")).alias("ENCNTR_ID"),
        F.col("INST_CD").cast("long").alias("INST_CD"),
        F.col("DEPT_CD").cast("long").alias("DEPT_CD"),
        F.col("SURG_AREA_CD").cast("long").alias("SURG_AREA_CD"),
        F.col("SURGEON_PRSNL_ID").cast("long").alias("SURGEON_PERSONNEL_ID"),
        F.col("ANESTH_PRSNL_ID").cast("long").alias("ANAESTHETIST_PERSONNEL_ID"),
        F.col("SURG_OP_LOC_CD").cast("long").alias("SURG_OP_LOC_CD"),
        F.col("SURG_SPECIALTY_ID").cast("long").alias("SURG_SPECIALTY_ID"),
        F.col("WOUND_CLASS_CD").cast("long").alias("WOUND_CLASS_CD"),
        F.col("ANESTH_TYPE_CD").cast("long").alias("ANESTH_TYPE_CD"),
        F.col("ASA_CLASS_CD").cast("long").alias("ASA_CLASS_CD"),
        F.col("CASE_LEVEL_CD").cast("long").alias("CASE_LEVEL_CD"),
        F.col("OR_SHIFT_CD").cast("long").alias("OR_SHIFT_CD"),
        F.col("PAT_TYPE_CD").cast("long").alias("PAT_TYPE_CD"),
        "SURG_CASE_NBR_FORMATTED",
        F.col("SCH_EVENT_ID").cast("long").alias("SCH_EVENT_ID"),
        F.col("SCHED_SURG_AREA_CD").cast("long").alias("SCHED_SURG_AREA_CD"),
        F.col("SCHED_OP_LOC_CD").cast("long").alias("SCHED_OP_LOC_CD"),
        F.col("SCHED_SURG_SPECIALTY_ID").cast("long").alias("SCHED_SURG_SPECIALTY_ID"),
        F.col("SCHED_DUR").cast("double").alias("SCHEDULED_DURATION_MINUTES"),
        F.col("SURG_DUR_MIN").cast("double").alias("SURGERY_DURATION_MINUTES"),
        F.col("TURNOVER_DUR").cast("double").alias("TURNOVER_DURATION_MINUTES"),
        F.col("SCHED_START_DT_TM").alias("SCHED_START_DT_TM"),
        F.col("SURG_START_DT_TM").alias("SURG_START_DT_TM_RAW"),
        F.col("SURG_STOP_DT_TM").alias("SURG_STOP_DT_TM_RAW"),
        F.col("CHECKIN_DT_TM").alias("CHECKIN_DT_TM_RAW"),
        F.col("CANCEL_DT_TM").alias("CANCEL_DT_TM_RAW"),
        F.col("CANCEL_REASON_CD").cast("long").alias("CANCEL_REASON_CD"),
        F.col("CANCEL_REQ_BY_ID").cast("long").alias("CANCEL_REQUESTED_BY_ID"),
        F.col("ADD_ON_IND").cast("long").alias("ADD_ON_IND"),
        F.col("ACTIVE_IND").cast("long").alias("ACTIVE_IND"),
        F.col("ACTIVE_STATUS_CD").cast("long").alias("ACTIVE_STATUS_CD"),
        F.col("ORGANIZATION_ID").cast("long").alias("ORGANIZATION_ID"),
        F.col("ADC_UPDT").alias("CASE_SOURCE_ADC_UPDT"),
    )
    scoped = case.select("SURG_CASE_ID_RAW", "SURG_CASE_ID")
    state = scope_by_ids(spark.table(SRC_STATE), scoped, "SURG_CASE_ID")
    state = state.select(
        F.col("SURG_CASE_ID").cast("long").alias("SURG_CASE_ID"),
        F.col("SN_SURG_CASE_ST_ID").cast("long").alias("SN_SURG_CASE_ST_ID"),
        F.col("SCH_SLOT_TYPE_ID").cast("long").alias("SCH_SLOT_TYPE_ID"),
        F.trim(F.regexp_replace(F.col("SCH_SLOT_STATE_MEANING"), "\\x00", "")).alias("SCH_SLOT_STATE_MEANING"),
        F.col("SCH_SLOT_OVERRIDE_IND").cast("long").alias("SCH_SLOT_OVERRIDE_IND"),
        F.col("SCH_APPT_ID").cast("long").alias("SCH_APPT_ID"),
        F.col("ACTUAL_SLOT_TYPE_ID").cast("long").alias("ACTUAL_SLOT_TYPE_ID"),
        F.col("ADC_UPDT").alias("STATE_SOURCE_ADC_UPDT"),
    )
    milestone = scope_by_ids(spark.table(SRC_TIMES), scoped, "SURG_CASE_ID")
    milestone = add_decode(
        milestone.withColumn("TASK_ASSAY_CD_LONG", F.col("TASK_ASSAY_CD").cast("long")),
        "TASK_ASSAY_CD_LONG",
        "TASK_ASSAY_DESCRIPTION",
        decode_lookup,
    )
    milestone = (
        milestone.where(
            F.lower(F.coalesce(F.col("TASK_ASSAY_DESCRIPTION"), F.lit(""))).rlike(
                r"patient - in room time|surgery - start time|anaesthesia - in room time"
            )
        )
        .groupBy(F.col("SURG_CASE_ID").cast("long").alias("SURG_CASE_ID"))
        .agg(
            F.min("CASE_TIME_DT_TM").alias("FIRST_PERFORMED_MILESTONE_DT_TM_RAW"),
            F.max("ADC_UPDT").alias("MILESTONE_SOURCE_ADC_UPDT"),
        )
        .withColumn("PERFORMED_MILESTONE_IND", F.lit(True))
    )
    result = case.join(state, "SURG_CASE_ID", "left").join(milestone, "SURG_CASE_ID", "left")
    result = result.withColumn(
        "CASE_STATUS",
        F.when(F.col("CANCEL_DT_TM_RAW").isNotNull(), F.lit("CANCELLED"))
        .when(F.col("PERFORMED_MILESTONE_IND") == True, F.lit("PERFORMED"))
        .otherwise(F.lit("SCHEDULED_ONLY")),
    )
    result = add_scheduled_quality(result, "SCHED_START_DT_TM")
    for raw, clean in [
        ("SURG_START_DT_TM_RAW", "SURG_START_DT_TM"),
        ("SURG_STOP_DT_TM_RAW", "SURG_STOP_DT_TM"),
        ("CHECKIN_DT_TM_RAW", "CHECKIN_DT_TM"),
        ("CANCEL_DT_TM_RAW", "CANCEL_DT_TM"),
        ("FIRST_PERFORMED_MILESTONE_DT_TM_RAW", "FIRST_PERFORMED_MILESTONE_DT_TM"),
    ]:
        result = add_performed_timestamp(result, raw, clean)
    for code, description in [
        ("INST_CD", "INST_DESCRIPTION"), ("DEPT_CD", "DEPT_DESCRIPTION"),
        ("SURG_AREA_CD", "SURG_AREA_DESCRIPTION"), ("SURG_OP_LOC_CD", "SURG_OP_LOCATION_DESCRIPTION"),
        ("WOUND_CLASS_CD", "WOUND_CLASS_DESCRIPTION"), ("ANESTH_TYPE_CD", "ANESTH_TYPE_DESCRIPTION"),
        ("ASA_CLASS_CD", "ASA_CLASS_DESCRIPTION"), ("CASE_LEVEL_CD", "CASE_LEVEL_DESCRIPTION"),
        ("OR_SHIFT_CD", "OR_SHIFT_DESCRIPTION"), ("PAT_TYPE_CD", "PAT_TYPE_DESCRIPTION"),
        ("CANCEL_REASON_CD", "CANCEL_REASON_DESCRIPTION"), ("ACTIVE_STATUS_CD", "ACTIVE_STATUS_DESCRIPTION"),
    ]:
        result = add_decode(result, code, description, decode_lookup)
    return (
        result.withColumn(
            "SOURCE_ADC_UPDT",
            F.greatest("CASE_SOURCE_ADC_UPDT", "STATE_SOURCE_ADC_UPDT", "MILESTONE_SOURCE_ADC_UPDT"),
        )
        .drop(
            "SURG_CASE_ID_RAW",
            "CASE_SOURCE_ADC_UPDT",
            "STATE_SOURCE_ADC_UPDT",
            "MILESTONE_SOURCE_ADC_UPDT",
        )
    )


def build_procedure(proc_ids: DataFrame | None, decode_lookup: DataFrame) -> DataFrame:
    proc = scope_by_ids(spark.table(SRC_PROCEDURE), proc_ids, "SURG_CASE_PROC_ID")
    proc = proc.select(
        F.col("SURG_CASE_PROC_ID").alias("SURG_CASE_PROC_ID_RAW"),
        F.col("SURG_CASE_PROC_ID").cast("long").alias("SURG_CASE_PROC_ID"),
        F.col("SURG_CASE_ID").alias("SURG_CASE_ID_RAW"),
        F.col("SURG_CASE_ID").cast("long").alias("SURG_CASE_ID"),
        F.col("SURG_PROC_CD").cast("long").alias("SURG_PROC_CD"),
        F.col("SYNONYM_ID").cast("long").alias("SYNONYM_ID"),
        F.col("PRIMARY_PROC_IND").cast("long").alias("PRIMARY_PROC_IND"),
        F.col("PRIMARY_SURGEON_ID").cast("long").alias("PRIMARY_SURGEON_ID"),
        F.col("SURG_SPECIALTY_ID").cast("long").alias("SURG_SPECIALTY_ID"),
        F.col("ANESTH_TYPE_CD").cast("long").alias("ANESTH_TYPE_CD"),
        F.col("WOUND_CLASS_CD").cast("long").alias("WOUND_CLASS_CD"),
        "PROC_TEXT", "MODIFIER",
        F.col("ORDER_ID").cast("long").alias("ORDER_ID"),
        F.col("PROC_START_DT_TM").alias("PROC_START_DT_TM_RAW"),
        F.col("PROC_END_DT_TM").alias("PROC_END_DT_TM_RAW"),
        F.col("PROC_DUR_MIN").cast("double").alias("PROCEDURE_DURATION_MINUTES"),
        F.col("SCHED_SURG_PROC_CD").cast("long").alias("SCHED_SURG_PROC_CD"),
        F.col("SCHED_DUR").cast("double").alias("SCHEDULED_DURATION_MINUTES"),
        F.col("ACTIVE_IND").cast("long").alias("ACTIVE_IND"),
        F.col("ACTIVE_STATUS_CD").cast("long").alias("ACTIVE_STATUS_CD"),
        F.col("ADC_UPDT").alias("PROCEDURE_SOURCE_ADC_UPDT"),
    )
    scoped = proc.select("SURG_CASE_PROC_ID_RAW", "SURG_CASE_PROC_ID")
    modifiers = scope_by_ids(spark.table(SRC_MODIFIER), scoped, "SURG_CASE_PROC_ID")
    modifiers = modifiers.select(
        F.col("SURG_CASE_PROC_ID").cast("long").alias("SURG_CASE_PROC_ID"),
        F.col("MODIFIER_SEQ").cast("long").alias("MODIFIER_SEQ"),
        F.coalesce(F.col("MODIFIER_SEQ").cast("long"), F.lit(9223372036854775807)).alias("MODIFIER_SORT_SEQ"),
        F.col("MODIFIER_CD").cast("long").alias("MODIFIER_CD"),
        F.col("ADC_UPDT").alias("MODIFIER_SOURCE_ADC_UPDT"),
    )
    modifiers = add_decode(modifiers, "MODIFIER_CD", "MODIFIER_DESCRIPTION", decode_lookup)
    modifiers = (
        modifiers.groupBy("SURG_CASE_PROC_ID")
        .agg(
            F.sort_array(F.collect_list(F.struct("MODIFIER_SORT_SEQ", "MODIFIER_SEQ", "MODIFIER_CD", "MODIFIER_DESCRIPTION"))).alias("MODIFIER_STRUCTS"),
            F.max("MODIFIER_SOURCE_ADC_UPDT").alias("MODIFIER_SOURCE_ADC_UPDT"),
        )
        .withColumn("MODIFIER_CDS", F.transform("MODIFIER_STRUCTS", lambda x: x["MODIFIER_CD"]))
        .withColumn("MODIFIER_DESCRIPTIONS", F.transform("MODIFIER_STRUCTS", lambda x: x["MODIFIER_DESCRIPTION"]))
        .drop("MODIFIER_STRUCTS")
    )
    parent = scoped_case_parent(proc.select("SURG_CASE_ID_RAW", "SURG_CASE_ID").distinct())
    result = proc.join(parent, "SURG_CASE_ID", "left").join(modifiers, "SURG_CASE_PROC_ID", "left")
    result = add_performed_timestamp(result, "PROC_START_DT_TM_RAW", "PROC_START_DT_TM")
    result = add_performed_timestamp(result, "PROC_END_DT_TM_RAW", "PROC_END_DT_TM")
    for code, description in [
        ("SURG_PROC_CD", "SURG_PROC_DESCRIPTION"), ("SURG_SPECIALTY_ID", "SURG_SPECIALTY_DESCRIPTION"),
        ("ANESTH_TYPE_CD", "ANESTH_TYPE_DESCRIPTION"), ("WOUND_CLASS_CD", "WOUND_CLASS_DESCRIPTION"),
        ("SCHED_SURG_PROC_CD", "SCHEDULED_SURG_PROC_DESCRIPTION"), ("ACTIVE_STATUS_CD", "ACTIVE_STATUS_DESCRIPTION"),
    ]:
        result = add_decode(result, code, description, decode_lookup)
    return (
        result.withColumn(
            "SOURCE_ADC_UPDT",
            F.greatest("PROCEDURE_SOURCE_ADC_UPDT", "MODIFIER_SOURCE_ADC_UPDT", "CASE_SOURCE_ADC_UPDT"),
        )
        .drop(
            "SURG_CASE_PROC_ID_RAW",
            "SURG_CASE_ID_RAW",
            "PROCEDURE_SOURCE_ADC_UPDT",
            "MODIFIER_SOURCE_ADC_UPDT",
            "CASE_SOURCE_ADC_UPDT",
        )
    )


def build_times(time_ids: DataFrame | None, decode_lookup: DataFrame) -> DataFrame:
    frame = scope_by_ids(spark.table(SRC_TIMES), time_ids, "CASE_TIMES_ID")
    frame = frame.select(
        F.col("CASE_TIMES_ID").cast("long").alias("CASE_TIMES_ID"),
        F.col("SURG_CASE_ID").alias("SURG_CASE_ID_RAW"),
        F.col("SURG_CASE_ID").cast("long").alias("SURG_CASE_ID"),
        F.col("TASK_ASSAY_CD").cast("long").alias("TASK_ASSAY_CD"),
        F.col("STAGE_CD").cast("long").alias("STAGE_CD"),
        F.col("CASE_TIME_MEANING").alias("SOURCE_CASE_TIME_MEANING"),
        F.col("CASE_TIME_DT_TM").alias("CASE_TIME_DT_TM_RAW"),
        F.col("ACTIVE_IND").cast("long").alias("ACTIVE_IND"),
        F.col("ACTIVE_STATUS_CD").cast("long").alias("ACTIVE_STATUS_CD"),
        F.col("ADC_UPDT").alias("TIME_SOURCE_ADC_UPDT"),
    )
    parent = scoped_case_parent(frame.select("SURG_CASE_ID_RAW", "SURG_CASE_ID").distinct())
    result = frame.join(parent, "SURG_CASE_ID", "left")
    result = add_performed_timestamp(result, "CASE_TIME_DT_TM_RAW", "CASE_TIME_DT_TM")
    for code, description in [
        ("TASK_ASSAY_CD", "TASK_ASSAY_DESCRIPTION"), ("STAGE_CD", "STAGE_DESCRIPTION"),
        ("ACTIVE_STATUS_CD", "ACTIVE_STATUS_DESCRIPTION"),
    ]:
        result = add_decode(result, code, description, decode_lookup)
    return (
        result.withColumn("SOURCE_ADC_UPDT", F.greatest("TIME_SOURCE_ADC_UPDT", "CASE_SOURCE_ADC_UPDT"))
        .drop("SURG_CASE_ID_RAW", "TIME_SOURCE_ADC_UPDT", "CASE_SOURCE_ADC_UPDT")
    )


def build_attendance(attendance_ids: DataFrame | None, decode_lookup: DataFrame) -> DataFrame:
    frame = scope_by_ids(spark.table(SRC_ATTENDANCE), attendance_ids, "CASE_ATTENDANCE_ID")
    frame = frame.select(
        F.col("CASE_ATTENDANCE_ID").cast("long").alias("CASE_ATTENDANCE_ID"),
        F.col("SURG_CASE_ID").alias("SURG_CASE_ID_RAW"),
        F.col("SURG_CASE_ID").cast("long").alias("SURG_CASE_ID"),
        F.col("SURG_AREA_CD").cast("long").alias("SURG_AREA_CD"),
        F.col("CASE_ATTENDEE_ID").cast("long").alias("CASE_ATTENDEE_ID"),
        F.col("ROLE_PERF_CD").cast("long").alias("ROLE_PERF_CD"),
        F.col("REASON_FOR_RELIEF_CD").cast("long").alias("REASON_FOR_RELIEF_CD"),
        F.col("SIGNING_ATTENDEE_IND").cast("long").alias("SIGNING_ATTENDEE_IND"),
        F.col("IN_DT_TM").alias("IN_DT_TM_RAW"),
        F.col("OUT_DT_TM").alias("OUT_DT_TM_RAW"),
        F.col("ACTIVE_IND").cast("long").alias("ACTIVE_IND"),
        F.col("ACTIVE_STATUS_CD").cast("long").alias("ACTIVE_STATUS_CD"),
        F.col("ADC_UPDT").alias("ATTENDANCE_SOURCE_ADC_UPDT"),
    )
    parent = scoped_case_parent(frame.select("SURG_CASE_ID_RAW", "SURG_CASE_ID").distinct())
    result = frame.join(parent, "SURG_CASE_ID", "left")
    result = add_performed_timestamp(result, "IN_DT_TM_RAW", "IN_DT_TM")
    result = add_performed_timestamp(result, "OUT_DT_TM_RAW", "OUT_DT_TM")
    for code, description in [
        ("SURG_AREA_CD", "SURG_AREA_DESCRIPTION"), ("ROLE_PERF_CD", "ROLE_PERF_DESCRIPTION"),
        ("REASON_FOR_RELIEF_CD", "REASON_FOR_RELIEF_DESCRIPTION"), ("ACTIVE_STATUS_CD", "ACTIVE_STATUS_DESCRIPTION"),
    ]:
        result = add_decode(result, code, description, decode_lookup)
    return (
        result.withColumn(
            "SOURCE_ADC_UPDT", F.greatest("ATTENDANCE_SOURCE_ADC_UPDT", "CASE_SOURCE_ADC_UPDT")
        )
        .drop("SURG_CASE_ID_RAW", "ATTENDANCE_SOURCE_ADC_UPDT", "CASE_SOURCE_ADC_UPDT")
    )


_JUNK_SERIALS = ("", "0", "N/A", "NA", "NONE", "UNKNOWN", "NOTAPPLICABLE")


def _norm_serial(column):
    return F.upper(F.regexp_replace(F.trim(column.cast("string")), r"[^A-Za-z0-9]", ""))


def with_implant_details_link(implant_df: DataFrame, implant_details: DataFrame) -> DataFrame:
    crosswalk = (
        implant_details.select(
            F.col("PERSON_ID").cast("long").alias("XW_PERSON_ID"),
            F.col("EVENT_ID").cast("long").alias("MILL_IMPLANT_EVENT_ID"),
            F.explode(
                F.array_distinct(
                    F.array(_norm_serial(F.col("SERIAL_NUMBER")), _norm_serial(F.col("GS1_SERIAL_NUMBER")))
                )
            ).alias("XW_SERIAL"),
        )
        .where((~F.col("XW_SERIAL").isin(*_JUNK_SERIALS)) & F.col("XW_PERSON_ID").isNotNull())
        .groupBy("XW_PERSON_ID", "XW_SERIAL")
        .agg(
            F.countDistinct("MILL_IMPLANT_EVENT_ID").alias("XW_N"),
            F.max("MILL_IMPLANT_EVENT_ID").alias("MILL_IMPLANT_EVENT_ID"),
        )
    )
    joined = implant_df.alias("implant").join(
        crosswalk.alias("crosswalk"),
        (F.col("implant.PERSON_ID") == F.col("crosswalk.XW_PERSON_ID"))
        & (_norm_serial(F.col("implant.SERIAL_NUMBER")) == F.col("crosswalk.XW_SERIAL")),
        "left",
    )
    return (
        joined.withColumn(
            "IMPLANT_LINK_METHOD",
            F.when(F.col("crosswalk.XW_N") == 1, F.lit("SERIAL_UNIQUE"))
            .when(F.col("crosswalk.XW_N") > 1, F.lit("SERIAL_AMBIGUOUS"))
            .otherwise(F.lit("NONE")),
        )
        .withColumn(
            "MILL_IMPLANT_EVENT_ID",
            F.when(F.col("crosswalk.XW_N") == 1, F.col("crosswalk.MILL_IMPLANT_EVENT_ID")),
        )
        .drop("XW_PERSON_ID", "XW_SERIAL", "XW_N")
    )


def build_implant(
    implant_ids: DataFrame | None,
    decode_lookup: DataFrame,
    implant_details: DataFrame,
) -> DataFrame:
    frame = scope_by_ids(spark.table(SRC_IMPLANT), implant_ids, "IMPLANT_LOG_ST_ID")
    frame = frame.select(
        F.col("IMPLANT_LOG_ST_ID").cast("long").alias("IMPLANT_LOG_ST_ID"),
        F.col("SURG_CASE_ID").alias("SURG_CASE_ID_RAW"),
        F.col("SURG_CASE_ID").cast("long").alias("SURG_CASE_ID"),
        F.col("DOC_TYPE_CD").cast("long").alias("DOC_TYPE_CD"),
        "ITEM_ID", "IMPLANT_SITE", "MANUFACTURER", "MANUF_ECRI_CODE", "MODEL_NUMBER",
        "SERIAL_NUMBER", "LOT_NUMBER", "BATCH_NUMBER", "CATALOG_NUMBER", "OTHER_IDENTIFIER",
        "ECRI_DEVICE_CODE", "IMPLANT_SIZE", "QUANTITY", "EXP_DATE", "FREE_TEXT_ITEM_DESC",
        F.col("IMPLANTED_BY_ID").cast("long").alias("IMPLANTED_BY_ID"),
        F.col("IMPLANT_ACTION_CD").cast("long").alias("IMPLANT_ACTION_CD"),
        F.col("ADC_UPDT").alias("IMPLANT_SOURCE_ADC_UPDT"),
    )
    parent = scoped_case_parent(frame.select("SURG_CASE_ID_RAW", "SURG_CASE_ID").distinct())
    result = frame.join(parent, "SURG_CASE_ID", "left")
    for code, description in [
        ("DOC_TYPE_CD", "DOC_TYPE_DESCRIPTION"), ("IMPLANT_ACTION_CD", "IMPLANT_ACTION_DESCRIPTION"),
    ]:
        result = add_decode(result, code, description, decode_lookup)
    result = with_implant_details_link(result, implant_details)
    return (
        result.withColumn(
            "SOURCE_ADC_UPDT", F.greatest("IMPLANT_SOURCE_ADC_UPDT", "CASE_SOURCE_ADC_UPDT")
        )
        .drop("SURG_CASE_ID_RAW", "IMPLANT_SOURCE_ADC_UPDT", "CASE_SOURCE_ADC_UPDT")
    )

# COMMAND ----------

TARGET_SOURCES = {
    CASE: [SRC_CASE, SRC_STATE, SRC_TIMES, CODE_VALUE, LOGIC_SOURCE],
    PROCEDURE: [SRC_PROCEDURE, SRC_MODIFIER, SRC_CASE, CODE_VALUE, LOGIC_SOURCE],
    TIMES: [SRC_TIMES, SRC_CASE, CODE_VALUE, LOGIC_SOURCE],
    ATTENDANCE: [SRC_ATTENDANCE, SRC_CASE, CODE_VALUE, LOGIC_SOURCE],
    IMPLANT: [SRC_IMPLANT, SRC_CASE, CODE_VALUE, IMPLANT_DETAILS_SOURCE, LOGIC_SOURCE],
}
TARGET_KEYS = {
    CASE: ["SURG_CASE_ID"], PROCEDURE: ["SURG_CASE_PROC_ID"], TIMES: ["CASE_TIMES_ID"],
    ATTENDANCE: ["CASE_ATTENDANCE_ID"], IMPLANT: ["IMPLANT_LOG_ST_ID"],
}
SOURCE_HEALTH: dict[str, dict] = {}
FULL_MODES = {"FULL", "FULL_LOOKUP_CHANGE"}


def source_mode(source: str, modes: dict[str, str]) -> str:
    consuming_modes = [mode for target, mode in modes.items() if source in TARGET_SOURCES[target]]
    if any(mode in FULL_MODES or mode == "BOOTSTRAP" for mode in consuming_modes):
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
    return result


def run_full_parity_suite() -> dict:
    parity = {
        CASE: SOURCE_HEALTH[SRC_CASE]["rows"],
        PROCEDURE: SOURCE_HEALTH[SRC_PROCEDURE]["rows"],
        TIMES: SOURCE_HEALTH[SRC_TIMES]["rows"],
        ATTENDANCE: SOURCE_HEALTH[SRC_ATTENDANCE]["rows"],
        IMPLANT: SOURCE_HEALTH[SRC_IMPLANT]["rows"],
    }
    present_counts = {target: spark.table(target).where("SOURCE_PRESENT_IND").count() for target in parity}
    for target, expected in parity.items():
        assert present_counts[target] == expected, f"{target}: {present_counts[target]} != {expected}"

    coverage = spark.table(CASE).where("SOURCE_PRESENT_IND").agg(
        F.avg(F.col("PERSON_ID").isNotNull().cast("double")).alias("person_rate"),
        F.avg(F.col("ENCNTR_ID").isNotNull().cast("double")).alias("encntr_rate"),
    ).collect()[0]
    assert coverage["person_rate"] >= 0.999
    assert coverage["encntr_rate"] >= 0.998

    orphan_state_rows = (
        spark.table(SRC_STATE).alias("state")
        .join(
            spark.table(SRC_CASE).select("SURG_CASE_ID").alias("case"),
            F.col("state.SURG_CASE_ID") == F.col("case.SURG_CASE_ID"),
            "left_anti",
        )
        .count()
    )
    undecoded_nonzero_milestones = (
        spark.table(SRC_TIMES)
        .select(F.col("TASK_ASSAY_CD").cast("long").alias("code"))
        .where("code <> 0").distinct()
        .join(
            spark.table(CODE_VALUE).select(F.col("CODE_VALUE").cast("long").alias("code")).distinct(),
            "code",
            "left_anti",
        )
        .count()
    )
    assert undecoded_nonzero_milestones == 0, (
        f"Undecoded nonzero theatre milestones: {undecoded_nonzero_milestones}"
    )
    assert "ATTENDEE_FREE_TEXT_NAME" not in spark.table(ATTENDANCE).columns
    assert "CANCEL_REQ_BY_TEXT" not in spark.table(CASE).columns
    assert not [column for target in TARGET_KEYS for column in spark.table(target).columns if column.endswith("_RAW")]

    serial_usable = (
        _norm_serial(F.col("SERIAL_NUMBER")).isNotNull()
        & (~_norm_serial(F.col("SERIAL_NUMBER")).isin(*_JUNK_SERIALS))
    )
    implant_link = (
        spark.table(IMPLANT).where(F.col("SOURCE_PRESENT_IND") & serial_usable)
        .agg(
            F.count(F.lit(1)).alias("serial_rows"),
            F.sum((F.col("IMPLANT_LINK_METHOD") == "SERIAL_UNIQUE").cast("long")).alias("unique_rows"),
        ).collect()[0]
    )
    implant_link_rate = (
        float(implant_link["unique_rows"]) / float(implant_link["serial_rows"])
        if implant_link["serial_rows"] else 1.0
    )
    assert implant_link_rate >= 0.80, f"Implant crosswalk rate drifted to {implant_link_rate:.3%}"

    status_distribution = {
        row["CASE_STATUS"]: row["count"]
        for row in spark.table(CASE).where("SOURCE_PRESENT_IND").groupBy("CASE_STATUS").count().collect()
    }
    return {
        "level": "FULL_PARITY",
        "present_counts": present_counts,
        "case_person_rate": coverage["person_rate"],
        "case_encounter_rate": coverage["encntr_rate"],
        "orphan_state_rows_logged": int(orphan_state_rows),
        "undecoded_nonzero_milestones": int(undecoded_nonzero_milestones),
        "implant_serial_rows": int(implant_link["serial_rows"]),
        "implant_serial_unique_rate": implant_link_rate,
        "case_status_distribution": status_distribution,
    }


def apply_output_comments() -> None:
    apply_comments(
        CASE,
        "S14 theatre case feeder: one real SurgiNet case; no synthetic session/list. Raw timestamp twins, source counters and contributor stamps are cut after quality derivation.",
        {
            "CASE_STATUS": "S14 theatre feeder status: CANCELLED, PERFORMED from decoded milestone, or SCHEDULED_ONLY.",
            "SCH_EVENT_ID": "S14 appointment/request-thread link to the real scheduling request.",
            "SCH_APPT_ID": "S14 appointment slot link from the 1:1 SurgiNet state row.",
            "SCHED_SURG_AREA_CD": "S14 theatre booking intent; retained because it materially differs from actual area.",
            "SOURCE_ADC_UPDT": "Journey standard block input: greatest case/state/milestone source timestamp.",
        },
        "S14 theatre feeder and Journey standard block",
    )
    apply_comments(
        PROCEDURE,
        "S7 procedure theatre feeder: one SurgiNet case procedure. Raw timestamp twins, counters and contributor stamps are cut; source MODIFIER stays because it is not a 99% duplicate of coded arrays.",
        {
            "PROC_TEXT": "S7 procedure text input retained because coded coverage is incomplete.",
            "MODIFIER": "S7 procedure modifier input; sparse source text retained where coded arrays are absent/divergent.",
            "MODIFIER_CDS": "S7 procedure CodeableConcept input: deterministically ordered modifier codes.",
            "ORDER_ID": "S7/S10 request-thread link to medication/clinical order where populated.",
        },
        "S7 procedure",
    )
    apply_comments(
        TIMES,
        "S14 theatre milestone feeder: one SurgiNet milestone. Raw timestamp twin, source counter and contributor stamps are cut.",
        {"CASE_TIME_DT_TM": "S14 theatre performed-milestone time after deterministic quality bounds."},
        "S14 theatre activity",
    )
    apply_comments(
        ATTENDANCE,
        "Journey care-participation feeder: one SurgiNet staff attendance. Free-text names, raw timestamp twins, counters and contributor stamps are cut.",
        {"CASE_ATTENDEE_ID": "Journey care_participation practitioner FK; no practitioner attributes are re-landed."},
        "Journey care_participation",
    )
    apply_comments(
        IMPLANT,
        "S7 procedure implant feeder: one SurgiNet implant record. About 28% overlap Mill implant details; identity is bridged rather than content duplicated. SurgiNet-native content remains for unmatched rows.",
        {
            "MILL_IMPLANT_EVENT_ID": "S7 identity crosswalk to map_implant_details.EVENT_ID when person+serial is unique.",
            "IMPLANT_LINK_METHOD": "S7 crosswalk provenance: SERIAL_UNIQUE, SERIAL_AMBIGUOUS or NONE.",
            "SERIAL_NUMBER": "S7 implant traceability and identity-crosswalk input.",
            "LOT_NUMBER": "S7 implant traceability input retained because most SurgiNet rows have no Mill counterpart.",
        },
        "S7 procedure implant feeder",
    )


def run_pipeline() -> dict:
    global RUN_FUTURE_HORIZON, SOURCE_HEALTH
    load_state_cache()
    SOURCE_VERSIONS.clear()
    SOURCE_VERSIONS.update(
        {source: source_version(source) for source in [*SOURCE_SLA.keys(), LOGIC_SOURCE]}
    )
    modes = {target: choose_mode(target, sources) for target, sources in TARGET_SOURCES.items()}
    SOURCE_HEALTH = {
        source: source_health_for_mode(source, source_mode(source, modes), health_checkpoint(source))
        for source in [*SOURCE_SLA.keys(), LOGIC_SOURCE]
    }
    for table, (sla_days, freshness_mode) in SOURCE_SLA.items():
        stale = SOURCE_HEALTH[table]["source_staleness_days"]
        assert stale is not None, f"{table}: source watermark is NULL"
        if stale > sla_days:
            message = f"{table}: staleness {stale:.2f}d exceeds {sla_days}d SLA ({freshness_mode})"
            if freshness_mode == "LIVE":
                raise AssertionError(message)
            print(f"[WARN] {message}")
    RUN_FUTURE_HORIZON = builtins.max(
        health["watermark"] for health in SOURCE_HEALTH.values() if health["watermark"] is not None
    ) + timedelta(days=2)

    print(
        f"[THEATRE] target={TARGET_SCHEMA}, run_id={RUN_ID}, logic={PIPELINE_LOGIC_VERSION}, "
        f"bootstrap={BOOTSTRAP_MODE}, future_horizon={RUN_FUTURE_HORIZON}"
    )
    for target, mode in modes.items():
        print(f"[THEATRE] {target}: {mode}")
    for source, health in SOURCE_HEALTH.items():
        print(f"[THEATRE][HEALTH] {source}: {health['scan']}")

    decode_lookup = spark.table(CODE_VALUE).select(
        F.col("CODE_VALUE").cast("long").alias("__CODE_VALUE"),
        F.coalesce(F.col("DESCRIPTION"), F.col("DISPLAY")).alias("__CODE_DESCRIPTION"),
    )
    implant_details = spark.table(IMPLANT_DETAILS_SOURCE)
    metrics: dict[str, dict] = {}
    checkpoints: dict[str, list[str]] = {}

    if modes[CASE] != "UNCHANGED_SKIP":
        ids = None
        if modes[CASE] == "INCREMENTAL":
            ids = union_key_frames(
                [changed_rows(SRC_CASE, CASE), changed_rows(SRC_STATE, CASE), changed_rows(SRC_TIMES, CASE)],
                "SURG_CASE_ID",
            )
        staged = materialize_stage(build_case(ids, decode_lookup), CASE, TARGET_KEYS[CASE])
        tombstones = ids.select("SURG_CASE_ID").join(staged.select("SURG_CASE_ID"), "SURG_CASE_ID", "left_anti") if ids is not None else None
        metrics[CASE] = merge_target(staged, CASE, TARGET_KEYS[CASE], modes[CASE] in FULL_MODES, tombstones)
        checkpoints[CASE] = TARGET_SOURCES[CASE]

    if modes[PROCEDURE] != "UNCHANGED_SKIP":
        ids = None
        if modes[PROCEDURE] == "INCREMENTAL":
            changed_case_ids = union_key_frames([changed_rows(SRC_CASE, PROCEDURE)], "SURG_CASE_ID")
            case_proc_ids = scope_by_ids(spark.table(SRC_PROCEDURE), changed_case_ids, "SURG_CASE_ID").select("SURG_CASE_PROC_ID")
            ids = union_key_frames(
                [changed_rows(SRC_PROCEDURE, PROCEDURE), changed_rows(SRC_MODIFIER, PROCEDURE), case_proc_ids],
                "SURG_CASE_PROC_ID",
            )
        staged = materialize_stage(build_procedure(ids, decode_lookup), PROCEDURE, TARGET_KEYS[PROCEDURE])
        tombstones = ids.select("SURG_CASE_PROC_ID").join(staged.select("SURG_CASE_PROC_ID"), "SURG_CASE_PROC_ID", "left_anti") if ids is not None else None
        metrics[PROCEDURE] = merge_target(staged, PROCEDURE, TARGET_KEYS[PROCEDURE], modes[PROCEDURE] in FULL_MODES, tombstones)
        checkpoints[PROCEDURE] = TARGET_SOURCES[PROCEDURE]

    if modes[TIMES] != "UNCHANGED_SKIP":
        ids = None
        if modes[TIMES] == "INCREMENTAL":
            changed_case_ids = union_key_frames([changed_rows(SRC_CASE, TIMES)], "SURG_CASE_ID")
            case_time_ids = scope_by_ids(spark.table(SRC_TIMES), changed_case_ids, "SURG_CASE_ID").select("CASE_TIMES_ID")
            ids = union_key_frames([changed_rows(SRC_TIMES, TIMES), case_time_ids], "CASE_TIMES_ID")
        staged = materialize_stage(build_times(ids, decode_lookup), TIMES, TARGET_KEYS[TIMES])
        tombstones = ids.select("CASE_TIMES_ID").join(staged.select("CASE_TIMES_ID"), "CASE_TIMES_ID", "left_anti") if ids is not None else None
        metrics[TIMES] = merge_target(staged, TIMES, TARGET_KEYS[TIMES], modes[TIMES] in FULL_MODES, tombstones)
        checkpoints[TIMES] = TARGET_SOURCES[TIMES]

    if modes[ATTENDANCE] != "UNCHANGED_SKIP":
        ids = None
        if modes[ATTENDANCE] == "INCREMENTAL":
            changed_case_ids = union_key_frames([changed_rows(SRC_CASE, ATTENDANCE)], "SURG_CASE_ID")
            case_attendance_ids = scope_by_ids(spark.table(SRC_ATTENDANCE), changed_case_ids, "SURG_CASE_ID").select("CASE_ATTENDANCE_ID")
            ids = union_key_frames([changed_rows(SRC_ATTENDANCE, ATTENDANCE), case_attendance_ids], "CASE_ATTENDANCE_ID")
        staged = materialize_stage(build_attendance(ids, decode_lookup), ATTENDANCE, TARGET_KEYS[ATTENDANCE])
        tombstones = ids.select("CASE_ATTENDANCE_ID").join(staged.select("CASE_ATTENDANCE_ID"), "CASE_ATTENDANCE_ID", "left_anti") if ids is not None else None
        metrics[ATTENDANCE] = merge_target(staged, ATTENDANCE, TARGET_KEYS[ATTENDANCE], modes[ATTENDANCE] in FULL_MODES, tombstones)
        checkpoints[ATTENDANCE] = TARGET_SOURCES[ATTENDANCE]

    if modes[IMPLANT] != "UNCHANGED_SKIP":
        ids = None
        if modes[IMPLANT] == "INCREMENTAL":
            changed_case_ids = union_key_frames([changed_rows(SRC_CASE, IMPLANT)], "SURG_CASE_ID")
            case_implant_ids = scope_by_ids(spark.table(SRC_IMPLANT), changed_case_ids, "SURG_CASE_ID").select("IMPLANT_LOG_ST_ID")
            ids = union_key_frames([changed_rows(SRC_IMPLANT, IMPLANT), case_implant_ids], "IMPLANT_LOG_ST_ID")
        staged = materialize_stage(
            build_implant(ids, decode_lookup, implant_details), IMPLANT, TARGET_KEYS[IMPLANT]
        )
        tombstones = ids.select("IMPLANT_LOG_ST_ID").join(staged.select("IMPLANT_LOG_ST_ID"), "IMPLANT_LOG_ST_ID", "left_anti") if ids is not None else None
        metrics[IMPLANT] = merge_target(staged, IMPLANT, TARGET_KEYS[IMPLANT], modes[IMPLANT] in FULL_MODES, tombstones)
        checkpoints[IMPLANT] = TARGET_SOURCES[IMPLANT]

    if BOOTSTRAP_MODE:
        validation = {"level": "BOOTSTRAP_DEFERRED"}
        print("[THEATRE] bootstrap mode: checkpoints and reconciliation deferred")
    else:
        ran_full = any(mode in FULL_MODES for mode in modes.values())
        validate_features(check_target_uniqueness=ran_full)
        if all(mode == "UNCHANGED_SKIP" for mode in modes.values()):
            validation = {"level": "SKIPPED_ALL_UNCHANGED"}
        elif ran_full:
            validation = run_full_parity_suite()
        else:
            validation = run_incremental_suite(modes, metrics)
        if any(mode != "UNCHANGED_SKIP" for mode in modes.values()):
            apply_output_comments()
        commit_checkpoints(checkpoints)

    return {
        "status": "SUCCESS",
        "pipeline": "theatre_pipeline",
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
finally:
    cleanup_stages()
    release_run_lock()

print(json.dumps(SUMMARY, indent=2, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(SUMMARY, sort_keys=True, default=str))

