# Databricks notebook source
# MAGIC %md
# MAGIC # Scheduling Bronze Pipeline
# MAGIC
# MAGIC Builds three activity-plane products in the selected bronze schema:
# MAGIC `map_appointment`, `map_appointment_schedule`, and `map_appointment_resource`.
# MAGIC
# MAGIC Source contract verified 2026-08-08. Decodes use the same source as the production
# MAGIC map estate: `3_lookup.mill.mill_code_value` (unique `CODE_VALUE`). Scheduling timestamps
# MAGIC are booked/requested slots, never observed arrivals. `SCH_EVENT_ID` is a scheduling key.
# MAGIC
# MAGIC Curation notes (`2026.08.v2.0`):
# MAGIC - S14 `appointment` is the named consumer for request, booking-iteration and slot/role facts;
# MAGIC   the Journey standard block consumes identifiers, source timestamps and lifecycle flags.
# MAGIC - Drops `REFERRAL_IDENT` (0/32.1M), `REFER_DT_TM` (399/32.1M; UBRN aliases carry the
# MAGIC   linkage evidence), version/update counters, alias surrogate id and contributor stamps.
# MAGIC - Drops schedule machinery with no silver consumer: `INDIRECT_BOOK_IND` (all NULL),
# MAGIC   `UNCONFIRM_COUNT` (39,305,679/39,306,176 are zero), and the constant-zero override and
# MAGIC   additional-minute fields. Retains resource-list and group-session ids as booking identity.
# MAGIC - Drops constant sentinel resource timestamps (`VIS_*`, `END_EFFECTIVE_DT_TM`) and source
# MAGIC   update counters; retains original/effective starts and original end as booking-history facts.
# MAGIC - Excludes unbooked capacity rows whose role is NUL/whitespace; live profiling found the
# MAGIC   126.1M-row lane is encoded with a NUL character, not an ordinary blank.
# MAGIC - Keeps lifecycle states, cancellations, no-shows, recurrence links, resource assignments,
# MAGIC   external event aliases, and source code values with unfiltered decodes.

# COMMAND ----------

for _name, _default in {
    "target_schema": "8_dev.bronze",
    "allow_production_write": "false",
    "force_full_refresh": "false",
    "full_reconciliation": "false",
    "bootstrap_mode": "false",
    "bootstrap_start_year": "",
    "bootstrap_end_year": "",
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
from datetime import datetime, timezone
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

TARGET_SCHEMA = bronze_value("target_schema", "8_dev.bronze")
ALLOW_PRODUCTION_WRITE = bronze_bool("allow_production_write", False)
FORCE_FULL_REFRESH = bronze_bool("force_full_refresh", False)
FULL_RECONCILIATION = bronze_bool("full_reconciliation", False)
BOOTSTRAP_MODE = bronze_bool("bootstrap_mode", False)
BOOTSTRAP_START_YEAR = bronze_value("bootstrap_start_year", "")
BOOTSTRAP_END_YEAR = bronze_value("bootstrap_end_year", "")
RUN_ID = bronze_run_id()
PIPELINE_LOGIC_VERSION = "2026.08.v2.0"
LOGIC_VERSION_INT = 2026080801
LOGIC_SOURCE = "__PIPELINE_LOGIC__"

assert not TARGET_SCHEMA.lower().startswith("4_prod") or ALLOW_PRODUCTION_WRITE, (
    "Production writes are disabled. Only the approved production orchestrator may pass "
    "allow_production_write=true."
)
if BOOTSTRAP_MODE:
    assert BOOTSTRAP_START_YEAR and BOOTSTRAP_END_YEAR, (
        "bootstrap_mode=true requires bootstrap_start_year and bootstrap_end_year"
    )
    assert int(BOOTSTRAP_START_YEAR) <= int(BOOTSTRAP_END_YEAR)

RAW = "4_prod.raw"
CODE_VALUE = "3_lookup.mill.mill_code_value"
SRC_EVENT = f"{RAW}.mill_sch_event"
SRC_PATIENT = f"{RAW}.mill_sch_event_patient"
SRC_SCHEDULE = f"{RAW}.mill_sch_schedule"
SRC_LOCATION = f"{RAW}.mill_sch_location"
SRC_APPT = f"{RAW}.mill_sch_appt"
SRC_ALIAS = f"{RAW}.mill_sch_event_alias"

APPOINTMENT = f"{TARGET_SCHEMA}.map_appointment"
SCHEDULE = f"{TARGET_SCHEMA}.map_appointment_schedule"
RESOURCE = f"{TARGET_SCHEMA}.map_appointment_resource"
STATE_TABLE = f"{bronze_control_schema(TARGET_SCHEMA)}.scheduling_pipeline_state"
LOCK_KEY = "__RUN_LOCK__"
LOCK_TTL_HOURS = 12

SOURCE_SLA = {
    SRC_EVENT: (2, "LIVE"),
    SRC_PATIENT: (2, "LIVE"),
    SRC_SCHEDULE: (2, "LIVE"),
    SRC_LOCATION: (2, "LIVE"),
    SRC_APPT: (2, "LIVE"),
    SRC_ALIAS: (2, "LIVE"),
    CODE_VALUE: (365, "REFERENCE"),
}
LOOKUP_SOURCES = {CODE_VALUE}

EXPECTED_COLUMNS = {
    SRC_EVENT: {"SCH_EVENT_ID", "APPT_TYPE_CD", "SCH_STATE_CD", "SCH_MEANING", "ENCNTR_ID", "ADC_UPDT"},
    SRC_PATIENT: {"SCH_EVENT_ID", "PERSON_ID", "ENCNTR_ID", "VERSION_DT_TM", "UPDT_CNT", "ADC_UPDT"},
    SRC_SCHEDULE: {"SCHEDULE_ID", "SCH_EVENT_ID", "SCHEDULE_SEQ", "SCH_STATE_CD", "BEG_EFFECTIVE_DT_TM", "ADC_UPDT"},
    SRC_LOCATION: {"SCHEDULE_ID", "LOCATION_TYPE_CD", "LOCATION_CD", "LOCATION_FREETEXT", "SCH_CLINIC_ID", "ADC_UPDT"},
    SRC_APPT: {"SCH_APPT_ID", "SCH_EVENT_ID", "SCHEDULE_ID", "ROLE_MEANING", "BEG_DT_TM", "END_DT_TM", "ADC_UPDT"},
    SRC_ALIAS: {"SCH_EVENT_ID", "ALIAS", "EVENT_ALIAS_TYPE_CD", "ALIAS_POOL_CD", "ADC_UPDT"},
    CODE_VALUE: {"CODE_VALUE", "DISPLAY", "DESCRIPTION", "ADC_UPDT"},
}

HASH_EXCLUDE = {
    "ROW_HASH",
    "ADC_UPDT",
    "SOURCE_PRESENT_IND",
    "PIPELINE_RUN_ID",
    "PIPELINE_PROCESSED_TS",
    "SOURCE_ABSENT_DETECTED_TS",
    "CURATED_STATUS",
    "CURATED_NOTES",
}

def qident(value: str) -> str:
    return "`" + str(value).replace("`", "``") + "`"


def qname(value: str) -> str:
    return ".".join(qident(part) for part in str(value).split("."))


def sql_escape(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "''")


def assert_expected_columns(table: str, expected: set[str]) -> None:
    actual = set(spark.table(table).columns)
    missing = sorted(expected - actual)
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
        F.count(F.lit(1)).alias("rows"),
        F.max("ADC_UPDT").alias("watermark"),
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
    threshold = previous["source_watermark"]
    return frame.where(
        F.col("ADC_UPDT") >= F.lit(threshold).cast("timestamp") - F.expr("INTERVAL 24 HOURS")
    )


def union_keys(frames: list[DataFrame], key: str) -> DataFrame:
    raw_key = f"{key}_RAW"
    usable = [
        frame.select(F.col(key).alias(raw_key), F.col(key).cast("long").alias(key))
        for frame in frames
    ]
    return (
        reduce(lambda left, right: left.unionByName(right), usable)
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
    null_count = df.where(null_condition).limit(1).count()
    duplicate_count = df.groupBy(*keys).count().where(F.col("count") > 1).limit(1).count()
    assert null_count == 0, f"{label}: NULL key detected"
    assert duplicate_count == 0, f"{label}: duplicate key detected"


def ensure_table_features(table: str) -> None:
    spark.sql(
        f"ALTER TABLE {qname(table)} SET TBLPROPERTIES ("
        "'delta.enableChangeDataFeed'='true',"
        "'delta.enableRowTracking'='true',"
        "'delta.enableDeletionVectors'='true')"
    )


STAGING_TABLES: set[str] = set()
CLUSTER_KEYS = {RESOURCE: ["SCH_APPT_ID"]}


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
        staged.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(staging_table)
    )
    STAGING_TABLES.add(staging_table)
    snapshot = spark.table(staging_table)
    assert_unique_non_null(snapshot, keys, f"staged {target}")
    return snapshot


def merge_target(
    staged: DataFrame,
    target: str,
    keys: list[str],
    full_compare: bool,
    tombstone_keys: DataFrame | None = None,
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
        builder = (
            DeltaTable.forName(spark, target)
            .alias("t")
            .merge(staged.alias("s"), condition)
            .whenMatchedUpdate(
                condition="t.ROW_HASH <> s.ROW_HASH OR t.SOURCE_PRESENT_IND = false",
                set=values,
            )
            .whenNotMatchedInsert(values=values)
        )
        if full_compare:
            builder = builder.whenNotMatchedBySourceUpdate(
                condition="t.SOURCE_PRESENT_IND = true",
                set={
                    "SOURCE_PRESENT_IND": "false",
                    "SOURCE_ABSENT_DETECTED_TS": "current_timestamp()",
                    "PIPELINE_RUN_ID": f"'{sql_escape(RUN_ID)}'",
                    "ADC_UPDT": "current_timestamp()",
                },
            )
        builder.execute()
        operation = "MERGE"

    tombstone_count = 0
    if tombstone_keys is not None:
        if bronze_table_exists(target):
            present_keys = spark.table(target).where(F.col("SOURCE_PRESENT_IND")).select(*keys)
            tombstones = tombstone_keys.select(*keys).dropDuplicates(keys).join(present_keys, keys, "inner")
        else:
            tombstones = tombstone_keys.select(*keys).where(F.lit(False))
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


def year_slice(df: DataFrame, column: str) -> DataFrame:
    if not BOOTSTRAP_MODE:
        return df
    return df.where(
        F.year(F.col(column)).between(int(BOOTSTRAP_START_YEAR), int(BOOTSTRAP_END_YEAR))
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
            spark.sql(
                f"ALTER TABLE {qname(table)} ALTER COLUMN {qident(column)} "
                f"COMMENT '{sql_escape(comment)}'"
            )


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
        "target_table string, source_table string, source_version long, "
        "source_watermark timestamp, source_rows long, run_id string",
    ).withColumn("committed_at", F.current_timestamp())
    (
        DeltaTable.forName(spark, STATE_TABLE)
        .alias("t")
        .merge(
            updates.alias("s"),
            "t.target_table = s.target_table AND t.source_table = s.source_table",
        )
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
        f"Another scheduling_pipeline run holds the lock (run_id={owner['run_id']}, acquired "
        f"{owner['committed_at']}, expires after {LOCK_TTL_HOURS}h). Wait, or delete the "
        f"{LOCK_KEY} row from {STATE_TABLE} to break a dead lock."
    )


def release_run_lock() -> None:
    spark.sql(
        f"DELETE FROM {qname(STATE_TABLE)} WHERE target_table = '{LOCK_KEY}' "
        f"AND run_id = '{sql_escape(RUN_ID)}'"
    )


def cleanup_stages() -> None:
    for target in (APPOINTMENT, SCHEDULE, RESOURCE):
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
    COMMENT 'Scheduling bronze two-phase source version and watermark checkpoints.'
    """
)

for _table, _columns in EXPECTED_COLUMNS.items():
    assert bronze_table_exists(_table), f"Missing source {_table}"
    assert_expected_columns(_table, _columns)

assert len(spark.table(CODE_VALUE).select("CODE_VALUE").distinct().take(2)) > 0
_cv_counts = spark.table(CODE_VALUE).agg(
    F.count("CODE_VALUE").alias("n"), F.countDistinct("CODE_VALUE").alias("d")
).collect()[0]
assert _cv_counts["n"] == _cv_counts["d"], "Code-value lookup is not unique on CODE_VALUE"

# COMMAND ----------

CURRENT_PATIENT_SENTINEL = "2100-12-31 00:00:00"


def current_event_patient(event_ids: DataFrame | None = None) -> DataFrame:
    frame = scope_by_ids(spark.table(SRC_PATIENT), event_ids, "SCH_EVENT_ID")
    frame = frame.where(F.col("VERSION_DT_TM") == F.lit(CURRENT_PATIENT_SENTINEL).cast("timestamp"))
    window = Window.partitionBy(F.col("SCH_EVENT_ID").cast("long")).orderBy(
        F.col("UPDT_CNT").desc_nulls_last(),
        F.col("UPDT_DT_TM").desc_nulls_last(),
        F.col("ADC_UPDT").desc_nulls_last(),
    )
    return (
        frame.withColumn("__rn", F.row_number().over(window))
        .where(F.col("__rn") == 1)
        .select(
            F.col("SCH_EVENT_ID").cast("long").alias("SCH_EVENT_ID"),
            F.when(F.col("PERSON_ID").cast("long") != 0, F.col("PERSON_ID").cast("long")).alias("PERSON_ID"),
            F.when(F.col("ENCNTR_ID").cast("long") != 0, F.col("ENCNTR_ID").cast("long")).alias("PATIENT_ENCNTR_ID"),
            F.col("ADC_UPDT").alias("PATIENT_SOURCE_ADC_UPDT"),
        )
    )


def build_appointment(event_ids: DataFrame | None, decode_lookup: DataFrame) -> DataFrame:
    event = scope_by_ids(spark.table(SRC_EVENT), event_ids, "SCH_EVENT_ID")
    event = year_slice(event, "BEG_EFFECTIVE_DT_TM")
    event = event.select(
        F.col("SCH_EVENT_ID").alias("SCH_EVENT_ID_RAW"),
        F.col("SCH_EVENT_ID").cast("long").alias("SCH_EVENT_ID"),
        F.col("APPT_TYPE_CD").cast("long").alias("APPT_TYPE_CD"),
        F.col("APPT_SYNONYM_CD").cast("long").alias("APPT_SYNONYM_CD"),
        F.col("SCH_STATE_CD").cast("long").alias("SCH_STATE_CD"),
        F.col("SCH_MEANING").alias("SOURCE_SCHEDULE_MEANING"),
        F.col("CONTRIBUTOR_SYSTEM_CD").cast("long").alias("CONTRIBUTOR_SYSTEM_CD"),
        F.col("REQ_PRSNL_ID").cast("long").alias("REQUESTED_PERSONNEL_ID"),
        F.col("ACTIVE_IND").cast("long").alias("ACTIVE_IND"),
        F.col("ACTIVE_STATUS_CD").cast("long").alias("ACTIVE_STATUS_CD"),
        "BEG_EFFECTIVE_DT_TM",
        "END_EFFECTIVE_DT_TM",
        F.col("RECUR_TYPE_FLAG").cast("long").alias("RECUR_TYPE_FLAG"),
        F.col("RECUR_PARENT_ID").cast("long").alias("RECUR_PARENT_ID"),
        F.col("EVENT_CLASS_CD").cast("long").alias("EVENT_CLASS_CD"),
        "ORIG_REQ_START_DT_TM",
        "ORIG_REQ_END_DT_TM",
        "FIRST_BKD_ASI_DT_TM",
        F.when(F.col("ENCNTR_ID").cast("long") != 0, F.col("ENCNTR_ID").cast("long")).alias("EVENT_ENCNTR_ID"),
        F.col("ORGANIZATION_ID").cast("long").alias("ORGANIZATION_ID"),
        F.col("ADC_UPDT").alias("EVENT_SOURCE_ADC_UPDT"),
    )
    scoped_ids = event.select("SCH_EVENT_ID_RAW", "SCH_EVENT_ID")
    patient = current_event_patient(scoped_ids)
    alias = scope_by_ids(spark.table(SRC_ALIAS), scoped_ids, "SCH_EVENT_ID")
    alias = alias.select(
        F.col("SCH_EVENT_ID").cast("long").alias("SCH_EVENT_ID"),
        F.col("ALIAS").alias("EXTERNAL_EVENT_ALIAS"),
        F.col("EVENT_ALIAS_TYPE_CD").cast("long").alias("EVENT_ALIAS_TYPE_CD"),
        F.col("EVENT_ALIAS_SUB_TYPE_CD").cast("long").alias("EVENT_ALIAS_SUB_TYPE_CD"),
        F.col("ALIAS_POOL_CD").cast("long").alias("ALIAS_POOL_CD"),
        F.col("ASSIGN_AUTHORITY_SYS_CD").cast("long").alias("ASSIGN_AUTHORITY_SYS_CD"),
        F.col("ADC_UPDT").alias("ALIAS_SOURCE_ADC_UPDT"),
    )
    result = event.join(patient, "SCH_EVENT_ID", "left").join(alias, "SCH_EVENT_ID", "left")
    result = result.withColumn("ENCNTR_ID", F.coalesce("PATIENT_ENCNTR_ID", "EVENT_ENCNTR_ID"))
    result = result.withColumn(
        "ENCNTR_SOURCE",
        F.when(F.col("PATIENT_ENCNTR_ID").isNotNull(), F.lit("EVENT_PATIENT"))
        .when(F.col("EVENT_ENCNTR_ID").isNotNull(), F.lit("SCHEDULE_EVENT"))
        .otherwise(F.lit("MISSING")),
    )
    for code, description in [
        ("APPT_TYPE_CD", "APPT_TYPE_DESCRIPTION"),
        ("APPT_SYNONYM_CD", "APPT_SYNONYM_DESCRIPTION"),
        ("SCH_STATE_CD", "SCH_STATE_DESCRIPTION"),
        ("CONTRIBUTOR_SYSTEM_CD", "CONTRIBUTOR_SYSTEM_DESCRIPTION"),
        ("ACTIVE_STATUS_CD", "ACTIVE_STATUS_DESCRIPTION"),
        ("EVENT_CLASS_CD", "EVENT_CLASS_DESCRIPTION"),
        ("EVENT_ALIAS_TYPE_CD", "EVENT_ALIAS_TYPE_DESCRIPTION"),
        ("EVENT_ALIAS_SUB_TYPE_CD", "EVENT_ALIAS_SUB_TYPE_DESCRIPTION"),
        ("ALIAS_POOL_CD", "ALIAS_POOL_DESCRIPTION"),
    ]:
        result = add_decode(result, code, description, decode_lookup)
    return (
        result.withColumn(
            "SOURCE_ADC_UPDT",
            F.greatest("EVENT_SOURCE_ADC_UPDT", "PATIENT_SOURCE_ADC_UPDT", "ALIAS_SOURCE_ADC_UPDT"),
        )
        .drop(
            "SCH_EVENT_ID_RAW",
            "PATIENT_ENCNTR_ID",
            "EVENT_ENCNTR_ID",
            "EVENT_SOURCE_ADC_UPDT",
            "PATIENT_SOURCE_ADC_UPDT",
            "ALIAS_SOURCE_ADC_UPDT",
        )
    )


def build_schedule(schedule_ids: DataFrame | None, decode_lookup: DataFrame) -> DataFrame:
    schedule = scope_by_ids(spark.table(SRC_SCHEDULE), schedule_ids, "SCHEDULE_ID")
    schedule = year_slice(schedule, "BEG_EFFECTIVE_DT_TM")
    schedule = schedule.select(
        F.col("SCHEDULE_ID").alias("SCHEDULE_ID_RAW"),
        F.col("SCHEDULE_ID").cast("long").alias("SCHEDULE_ID"),
        F.col("SCH_EVENT_ID").cast("long").alias("SCH_EVENT_ID"),
        F.col("SCHEDULE_SEQ").cast("long").alias("SCHEDULE_SEQ"),
        F.col("SCH_STATE_CD").cast("long").alias("SCH_STATE_CD"),
        F.col("STATE_MEANING").alias("SOURCE_STATE_MEANING"),
        "BEG_EFFECTIVE_DT_TM",
        "END_EFFECTIVE_DT_TM",
        F.col("ACTIVE_IND").cast("long").alias("ACTIVE_IND"),
        F.col("ACTIVE_STATUS_CD").cast("long").alias("ACTIVE_STATUS_CD"),
        F.col("RES_LIST_ID").cast("long").alias("RESOURCE_LIST_ID"),
        F.col("GRPSESSION_ID").cast("long").alias("GROUP_SESSION_ID"),
        F.col("ADC_UPDT").alias("SCHEDULE_SOURCE_ADC_UPDT"),
    )
    scoped_ids = schedule.select("SCHEDULE_ID_RAW", "SCHEDULE_ID")
    location = scope_by_ids(spark.table(SRC_LOCATION), scoped_ids, "SCHEDULE_ID")
    location = location.select(
        F.col("SCHEDULE_ID").cast("long").alias("SCHEDULE_ID"),
        F.col("LOCATION_TYPE_CD").cast("long").alias("LOCATION_TYPE_CD"),
        F.col("LOCATION_TYPE_MEANING").alias("SOURCE_LOCATION_TYPE_MEANING"),
        F.col("LOCATION_CD").cast("long").alias("LOCATION_CD"),
        "LOCATION_FREETEXT",
        F.col("SCH_CLINIC_ID").cast("long").alias("SCH_CLINIC_ID"),
        F.col("ADC_UPDT").alias("LOCATION_SOURCE_ADC_UPDT"),
    )
    result = schedule.join(location, "SCHEDULE_ID", "left")
    for code, description in [
        ("SCH_STATE_CD", "SCH_STATE_DESCRIPTION"),
        ("ACTIVE_STATUS_CD", "ACTIVE_STATUS_DESCRIPTION"),
        ("LOCATION_TYPE_CD", "LOCATION_TYPE_DESCRIPTION"),
        ("LOCATION_CD", "LOCATION_DESCRIPTION"),
    ]:
        result = add_decode(result, code, description, decode_lookup)
    return (
        result.withColumn(
            "SOURCE_ADC_UPDT", F.greatest("SCHEDULE_SOURCE_ADC_UPDT", "LOCATION_SOURCE_ADC_UPDT")
        )
        .drop("SCHEDULE_ID_RAW", "SCHEDULE_SOURCE_ADC_UPDT", "LOCATION_SOURCE_ADC_UPDT")
    )


def role_is_published():
    return F.length(F.regexp_replace(F.coalesce(F.col("ROLE_MEANING"), F.lit("")), r"[\x00\s]", "")) > 0


def build_resource(resource_ids: DataFrame | None, decode_lookup: DataFrame) -> DataFrame:
    appt = scope_by_ids(spark.table(SRC_APPT), resource_ids, "SCH_APPT_ID")
    appt = year_slice(appt, "BEG_DT_TM").where(role_is_published())
    result = appt.select(
        F.col("SCH_APPT_ID").cast("long").alias("SCH_APPT_ID"),
        F.col("SCH_EVENT_ID").cast("long").alias("SCH_EVENT_ID"),
        F.col("SCHEDULE_ID").cast("long").alias("SCHEDULE_ID"),
        F.col("SCHEDULE_SEQ").cast("long").alias("SCHEDULE_SEQ"),
        F.when(F.col("PERSON_ID").cast("long") != 0, F.col("PERSON_ID").cast("long")).alias("PERSON_ID"),
        F.when(F.col("ENCNTR_ID").cast("long") != 0, F.col("ENCNTR_ID").cast("long")).alias("ENCNTR_ID"),
        F.col("RESOURCE_CD").cast("long").alias("RESOURCE_CD"),
        F.col("SERVICE_RESOURCE_CD").cast("long").alias("SERVICE_RESOURCE_CD"),
        F.col("ACTIVITY_CD").cast("long").alias("ACTIVITY_CD"),
        "ACTIVITY_MEANING",
        F.col("SLOT_TYPE_ID").cast("long").alias("SLOT_TYPE_ID"),
        F.col("SLOT_STATE_CD").cast("long").alias("SLOT_STATE_CD"),
        "SLOT_STATE_MEANING",
        "SLOT_MNEMONIC",
        F.col("SCH_STATE_CD").cast("long").alias("SCH_STATE_CD"),
        F.col("STATE_MEANING").alias("SOURCE_STATE_MEANING"),
        F.col("SCH_ROLE_CD").cast("long").alias("SCH_ROLE_CD"),
        F.trim(F.regexp_replace(F.col("ROLE_MEANING"), "\\x00", "")).alias("ROLE_MEANING"),
        F.col("PRIMARY_ROLE_IND").cast("long").alias("PRIMARY_ROLE_IND"),
        "BEG_DT_TM",
        "END_DT_TM",
        F.col("DURATION").cast("double").alias("DURATION_MINUTES"),
        "ORIG_BEG_DT_TM",
        "ORIG_END_DT_TM",
        "BEG_EFFECTIVE_DT_TM",
        F.col("APPT_LOCATION_CD").cast("long").alias("APPT_LOCATION_CD"),
        F.col("REFERRING_ORG_ID").cast("long").alias("REFERRING_ORGANIZATION_ID"),
        F.col("ALLOCATED_PRSNL_ID").cast("long").alias("ALLOCATED_PERSONNEL_ID"),
        F.col("ACTIVE_IND").cast("long").alias("ACTIVE_IND"),
        F.col("ACTIVE_STATUS_CD").cast("long").alias("ACTIVE_STATUS_CD"),
        F.col("ORGANIZATION_ID").cast("long").alias("ORGANIZATION_ID"),
        F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT"),
    )
    result = result.withColumn(
        "SLOT_TIME_QUALITY",
        F.when(F.col("BEG_DT_TM").isNull(), F.lit("MISSING_START"))
        .when(F.col("BEG_DT_TM") < F.lit("1900-01-01").cast("timestamp"), F.lit("INVALID_EARLY"))
        .when(F.col("BEG_DT_TM") > F.lit("2100-01-01").cast("timestamp"), F.lit("INVALID_FUTURE"))
        .when(F.col("END_DT_TM") < F.col("BEG_DT_TM"), F.lit("END_BEFORE_START"))
        .otherwise(F.lit("VALID_OR_SCHEDULED_FUTURE")),
    )
    for code, description in [
        ("RESOURCE_CD", "RESOURCE_DESCRIPTION"),
        ("SERVICE_RESOURCE_CD", "SERVICE_RESOURCE_DESCRIPTION"),
        ("ACTIVITY_CD", "ACTIVITY_DESCRIPTION"),
        ("SLOT_STATE_CD", "SLOT_STATE_DESCRIPTION"),
        ("SCH_STATE_CD", "SCH_STATE_DESCRIPTION"),
        ("SCH_ROLE_CD", "SCH_ROLE_DESCRIPTION"),
        ("APPT_LOCATION_CD", "APPT_LOCATION_DESCRIPTION"),
        ("ACTIVE_STATUS_CD", "ACTIVE_STATUS_DESCRIPTION"),
    ]:
        result = add_decode(result, code, description, decode_lookup)
    return result

# COMMAND ----------

TARGET_SOURCES = {
    APPOINTMENT: [SRC_EVENT, SRC_PATIENT, SRC_ALIAS, CODE_VALUE, LOGIC_SOURCE],
    SCHEDULE: [SRC_SCHEDULE, SRC_LOCATION, CODE_VALUE, LOGIC_SOURCE],
    RESOURCE: [SRC_APPT, CODE_VALUE, LOGIC_SOURCE],
}
TARGET_KEYS = {APPOINTMENT: ["SCH_EVENT_ID"], SCHEDULE: ["SCHEDULE_ID"], RESOURCE: ["SCH_APPT_ID"]}
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
        observed_tombstones = (
            target_frame.where(
                (~F.col("SOURCE_PRESENT_IND")) & (F.col("PIPELINE_RUN_ID") == RUN_ID)
            ).count()
        )
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
    appointment_present = spark.table(APPOINTMENT).where("SOURCE_PRESENT_IND").count()
    schedule_present = spark.table(SCHEDULE).where("SOURCE_PRESENT_IND").count()
    resource_present = spark.table(RESOURCE).where("SOURCE_PRESENT_IND").count()
    raw_resource = spark.table(SRC_APPT)
    raw_resource_total = SOURCE_HEALTH[SRC_APPT]["rows"]
    raw_resource_published = raw_resource.where(role_is_published()).count()
    raw_resource_excluded = raw_resource_total - raw_resource_published

    assert appointment_present == SOURCE_HEALTH[SRC_EVENT]["rows"], (
        f"appointment parity: {appointment_present} != {SOURCE_HEALTH[SRC_EVENT]['rows']}"
    )
    assert schedule_present == SOURCE_HEALTH[SRC_SCHEDULE]["rows"], (
        f"schedule parity: {schedule_present} != {SOURCE_HEALTH[SRC_SCHEDULE]['rows']}"
    )
    assert resource_present == raw_resource_published, (
        f"resource parity: {resource_present} != {raw_resource_published}"
    )
    assert resource_present + raw_resource_excluded == raw_resource_total

    appointment_rates = spark.table(APPOINTMENT).where("SOURCE_PRESENT_IND").agg(
        F.avg(F.col("PERSON_ID").isNotNull().cast("double")).alias("person_rate"),
        F.avg(F.col("ENCNTR_ID").isNotNull().cast("double")).alias("encntr_rate"),
    ).collect()[0]
    assert appointment_rates["person_rate"] >= 0.97, appointment_rates
    assert appointment_rates["encntr_rate"] >= 0.90, appointment_rates
    return {
        "level": "FULL_PARITY",
        "appointment_present_rows": int(appointment_present),
        "schedule_present_rows": int(schedule_present),
        "resource_present_rows": int(resource_present),
        "resource_excluded_nul_or_blank_rows": int(raw_resource_excluded),
        "appointment_person_rate": appointment_rates["person_rate"],
        "appointment_encounter_rate": appointment_rates["encntr_rate"],
    }


def apply_output_comments() -> None:
    apply_comments(
        APPOINTMENT,
        "S14 appointment feeder: one Millennium scheduling request. Curation cuts referral-ident/date, version counters, alias surrogate and contributor stamps; UBRN alias evidence remains.",
        {
            "SCH_EVENT_ID": "S14 appointment key; Millennium scheduling id, never a clinical EVENT_ID.",
            "EXTERNAL_EVENT_ALIAS": "S14 referral/request-thread linkage evidence via the UBRN alias lane.",
            "ENCNTR_SOURCE": "S14 appointment provenance: EVENT_PATIENT, SCHEDULE_EVENT or MISSING.",
            "SOURCE_ADC_UPDT": "Journey standard block input: greatest contributing raw-source ADC timestamp.",
            "ADC_UPDT": "Journey standard block input: bronze material-change timestamp.",
            "SOURCE_PRESENT_IND": "Journey standard block lifecycle input; false after reconciliation/filter exit.",
        },
        "S14 appointment",
    )
    apply_comments(
        SCHEDULE,
        "S14 appointment booking-history feeder: one booking/reschedule iteration with its 1:1 location. Curation cuts null/constant and consumer-less booking machinery plus contributor stamps.",
        {
            "SCHEDULE_ID": "S14 appointment ordered booking-iteration key.",
            "SCH_EVENT_ID": "S14 appointment parent request identifier.",
            "RESOURCE_LIST_ID": "S14 booking identity: source resource-list assignment.",
            "GROUP_SESSION_ID": "S14 booking identity: sparse group-session linkage retained where populated.",
            "SOURCE_ADC_UPDT": "Journey standard block input: greatest schedule/location source timestamp.",
        },
        "S14 appointment booking history",
    )
    apply_comments(
        RESOURCE,
        "S14 appointment participation/slot feeder: one nonblank role assignment. Constant sentinel VIS/END_EFFECTIVE timestamps and source counters are cut; NUL/blank capacity rows are excluded.",
        {
            "SCH_APPT_ID": "S14 appointment slot/participation key and liquid-clustering key.",
            "BEG_DT_TM": "S14 booked-slot start; never an observed arrival.",
            "END_DT_TM": "S14 booked-slot end; never an observed departure.",
            "ORIG_BEG_DT_TM": "S14 appointment booking-history input: original slot start.",
            "ORIG_END_DT_TM": "S14 appointment booking-history input: original slot end.",
            "BEG_EFFECTIVE_DT_TM": "S14 appointment booking-history input: effective-from timestamp.",
            "ROLE_MEANING": "S14 participation role; NUL removed and blank-only rows excluded.",
            "SLOT_TIME_QUALITY": "Journey timestamp-quality input; permits legitimate scheduled future slots.",
        },
        "S14 appointment and care participation",
    )


def run_pipeline() -> dict:
    global SOURCE_HEALTH
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
        assert stale is not None, f"{table}: ADC_UPDT watermark is NULL"
        if stale > sla_days:
            message = f"{table}: staleness {stale:.2f}d exceeds {sla_days}d SLA ({freshness_mode})"
            if freshness_mode == "LIVE":
                raise AssertionError(message)
            print(f"[WARN] {message}")

    print(
        f"[SCHEDULING] target={TARGET_SCHEMA}, run_id={RUN_ID}, logic={PIPELINE_LOGIC_VERSION}, "
        f"bootstrap={BOOTSTRAP_MODE}, force_full={FORCE_FULL_REFRESH}, reconciliation={FULL_RECONCILIATION}"
    )
    for target, mode in modes.items():
        print(f"[SCHEDULING] {target}: {mode}")
    for source, health in SOURCE_HEALTH.items():
        print(f"[SCHEDULING][HEALTH] {source}: {health['scan']}")

    decode_lookup = spark.table(CODE_VALUE).select(
        F.col("CODE_VALUE").cast("long").alias("__CODE_VALUE"),
        F.coalesce(F.col("DESCRIPTION"), F.col("DISPLAY")).alias("__CODE_DESCRIPTION"),
    )
    metrics: dict[str, dict] = {}
    checkpoints: dict[str, list[str]] = {}

    if modes[APPOINTMENT] != "UNCHANGED_SKIP":
        event_ids = None
        if modes[APPOINTMENT] == "INCREMENTAL":
            event_ids = union_keys(
                [
                    changed_rows(SRC_EVENT, APPOINTMENT),
                    changed_rows(SRC_PATIENT, APPOINTMENT),
                    changed_rows(SRC_ALIAS, APPOINTMENT),
                ],
                "SCH_EVENT_ID",
            )
        staged = materialize_stage(
            build_appointment(event_ids, decode_lookup), APPOINTMENT, TARGET_KEYS[APPOINTMENT]
        )
        tombstones = (
            event_ids.select("SCH_EVENT_ID").join(staged.select("SCH_EVENT_ID"), "SCH_EVENT_ID", "left_anti")
            if event_ids is not None else None
        )
        metrics[APPOINTMENT] = merge_target(
            staged, APPOINTMENT, TARGET_KEYS[APPOINTMENT], modes[APPOINTMENT] in FULL_MODES, tombstones
        )
        checkpoints[APPOINTMENT] = TARGET_SOURCES[APPOINTMENT]

    if modes[SCHEDULE] != "UNCHANGED_SKIP":
        schedule_ids = None
        if modes[SCHEDULE] == "INCREMENTAL":
            schedule_ids = union_keys(
                [changed_rows(SRC_SCHEDULE, SCHEDULE), changed_rows(SRC_LOCATION, SCHEDULE)],
                "SCHEDULE_ID",
            )
        staged = materialize_stage(
            build_schedule(schedule_ids, decode_lookup), SCHEDULE, TARGET_KEYS[SCHEDULE]
        )
        tombstones = (
            schedule_ids.select("SCHEDULE_ID").join(staged.select("SCHEDULE_ID"), "SCHEDULE_ID", "left_anti")
            if schedule_ids is not None else None
        )
        metrics[SCHEDULE] = merge_target(
            staged, SCHEDULE, TARGET_KEYS[SCHEDULE], modes[SCHEDULE] in FULL_MODES, tombstones
        )
        checkpoints[SCHEDULE] = TARGET_SOURCES[SCHEDULE]

    if modes[RESOURCE] != "UNCHANGED_SKIP":
        resource_ids = None
        if modes[RESOURCE] == "INCREMENTAL":
            resource_ids = union_keys([changed_rows(SRC_APPT, RESOURCE)], "SCH_APPT_ID")
        staged = materialize_stage(
            build_resource(resource_ids, decode_lookup), RESOURCE, TARGET_KEYS[RESOURCE]
        )
        tombstones = (
            resource_ids.select("SCH_APPT_ID").join(staged.select("SCH_APPT_ID"), "SCH_APPT_ID", "left_anti")
            if resource_ids is not None else None
        )
        metrics[RESOURCE] = merge_target(
            staged, RESOURCE, TARGET_KEYS[RESOURCE], modes[RESOURCE] in FULL_MODES, tombstones
        )
        checkpoints[RESOURCE] = TARGET_SOURCES[RESOURCE]

    if BOOTSTRAP_MODE:
        validation = {"level": "BOOTSTRAP_DEFERRED"}
        print("[SCHEDULING] bootstrap slice complete; checkpoints and reconciliation deferred")
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
        "pipeline": "scheduling_pipeline",
        "pipeline_logic_version": PIPELINE_LOGIC_VERSION,
        "run_id": RUN_ID,
        "target_schema": TARGET_SCHEMA,
        "modes": modes,
        "metrics": metrics,
        "validation": validation,
        "source_health": {
            table: {
                **health,
                "watermark": str(health["watermark"]) if health["watermark"] is not None else None,
            }
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

