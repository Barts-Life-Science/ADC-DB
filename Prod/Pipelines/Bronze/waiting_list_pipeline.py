# Databricks notebook source
for _name, _default in {
    "target_schema": "8_dev.bronze_imp_new",
    "allow_production_write": "false",
    "force_full_refresh": "false",
    "full_reconciliation": "false",
    "bootstrap_mode": "false",
    "refresh_decodes": "false",
    "run_snapshots": "false",
    "dev_test_exclude_current_ids_table": "",
    "dev_test_fail_after_validation_before_snapshot": "false",
    "dev_test_snapshot_date_utc_override": "",
}.items():
    try:
        dbutils.widgets.get(_name)
    except Exception:
        dbutils.widgets.text(_name, _default)

# COMMAND ----------

# MAGIC %run ./_bronze_common

# COMMAND ----------

import json
import uuid
from datetime import date, timedelta
from functools import reduce

from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

TARGET_SCHEMA = bronze_value("target_schema", "8_dev.bronze_imp_new")
ALLOW_PRODUCTION_WRITE = bronze_bool("allow_production_write", False)
FORCE_FULL_REFRESH = bronze_bool("force_full_refresh", False)
FULL_RECONCILIATION = bronze_bool("full_reconciliation", False)
BOOTSTRAP_MODE = bronze_bool("bootstrap_mode", False)
REFRESH_DECODES = bronze_bool("refresh_decodes", False)
RUN_SNAPSHOTS = bronze_bool("run_snapshots", False)
DEV_TEST_EXCLUDE_CURRENT_IDS_TABLE = bronze_value(
    "dev_test_exclude_current_ids_table", ""
).strip()
DEV_TEST_FAIL_AFTER_VALIDATION_BEFORE_SNAPSHOT = bronze_bool(
    "dev_test_fail_after_validation_before_snapshot", False
)
DEV_TEST_SNAPSHOT_DATE_UTC_OVERRIDE = bronze_value(
    "dev_test_snapshot_date_utc_override", ""
).strip()

RUN_ID = bronze_run_id()
ATTEMPT_ID = uuid.uuid4().hex
RUN_AS_OF = spark.sql("SELECT current_timestamp() AS ts").first()["ts"]
WRITE_TS = RUN_AS_OF
# Assigned exactly once in run_pipeline after both raw Delta versions/frames are pinned.
SNAPSHOT_CUTOFF_TS_VALUE = None
SNAPSHOT_DATE_UTC = None
SNAPSHOT_SPARK_DAYOFWEEK = None

PIPELINE_LOGIC_VERSION = "2026.08.v2.4"
LOGIC_VERSION_INT = 2026081104
LOGIC_SOURCE = "__PIPELINE_LOGIC__"
DECODE_REFRESH_WEEK_SOURCE = "__DECODE_REFRESH_WEEK__"
USE_PM_WAIT_LIST_STATUS = False
DECODE_DRIFT_FALLBACK = True
SNAPSHOT_WEEKDAY = 2
SNAPSHOT_ENABLED = True
REMOVED_STATUS_CD = 3768701
LOCK_KEY = "__RUN_LOCK__"
LOCK_TTL_HOURS = 12
SOURCE_OVERLAP_HOURS = 24
RAW_FRESHNESS_TOLERANCE_DAYS = 2
SOURCE_WATERMARK_FUTURE_TOLERANCE_DAYS = 2

_ROLLOVER = date(2025, 12, 29).isocalendar()
assert (_ROLLOVER.year, _ROLLOVER.week) == (2026, 1)
assert _ROLLOVER.year * 100 + _ROLLOVER.week == 202601
# Assigned from SNAPSHOT_DATE_UTC only after raw Delta versions/frames are pinned.
DECODE_REFRESH_WEEK_VERSION = None
RUN_FUTURE_HORIZON = RUN_AS_OF + timedelta(days=2)

assert USE_PM_WAIT_LIST_STATUS is False
assert DECODE_DRIFT_FALLBACK is True
assert SNAPSHOT_WEEKDAY == 2
assert not TARGET_SCHEMA.lower().startswith("4_prod") or ALLOW_PRODUCTION_WRITE, (
    "Production writes are disabled. Only an approved production orchestrator may pass "
    "allow_production_write=true."
)
if (
    DEV_TEST_EXCLUDE_CURRENT_IDS_TABLE
    and TARGET_SCHEMA != "8_dev.bronze_imp_incr"
):
    raise RuntimeError(
        "dev_test_exclude_current_ids_table is restricted to target_schema="
        "8_dev.bronze_imp_incr"
    )
if (
    DEV_TEST_FAIL_AFTER_VALIDATION_BEFORE_SNAPSHOT
    and TARGET_SCHEMA != "8_dev.bronze_imp_incr"
):
    raise RuntimeError(
        "dev_test_fail_after_validation_before_snapshot is restricted to "
        "target_schema=8_dev.bronze_imp_incr"
    )
if DEV_TEST_SNAPSHOT_DATE_UTC_OVERRIDE:
    if TARGET_SCHEMA != "8_dev.bronze_imp_incr":
        raise RuntimeError(
            "dev_test_snapshot_date_utc_override is restricted to "
            "target_schema=8_dev.bronze_imp_incr"
        )
    try:
        date.fromisoformat(DEV_TEST_SNAPSHOT_DATE_UTC_OVERRIDE)
    except ValueError as exc:
        raise RuntimeError(
            "dev_test_snapshot_date_utc_override must be YYYY-MM-DD"
        ) from exc

RAW = "4_prod.raw"
SRC_CURRENT = f"{RAW}.mill_pm_wait_list"
SRC_HIST = f"{RAW}.mill_pm_wait_list_hist"
CODE_VALUE = "3_lookup.mill.mill_code_value"
TARGET = f"{TARGET_SCHEMA}.map_waiting_list"
SNAPSHOT_TARGET = f"{TARGET_SCHEMA}.map_waiting_list_snapshot"
STATE_TABLE = f"{bronze_control_schema(TARGET_SCHEMA)}.waiting_list_pipeline_state"
DEV_TEST_AUDIT_TABLE = f"{TARGET_SCHEMA}.waiting_list_task6_run_audit"

ATTEMPT_STAGE_SUFFIX = "".join(
    character if character.isalnum() else "_" for character in ATTEMPT_ID
).strip("_")[:64]
if not ATTEMPT_STAGE_SUFFIX:
    raise RuntimeError("ATTEMPT_ID did not yield a usable stage suffix")

MAIN_STAGE = f"{TARGET}_stg_{ATTEMPT_STAGE_SUFFIX}"
DELETE_KEYS_STAGE = f"{TARGET}_delete_keys_stg_{ATTEMPT_STAGE_SUFFIX}"
PRIMARY_UPDATE_KEYS_STAGE = f"{TARGET}_primary_update_keys_stg_{ATTEMPT_STAGE_SUFFIX}"
RESURRECTION_KEYS_STAGE = f"{TARGET}_resurrection_keys_stg_{ATTEMPT_STAGE_SUFFIX}"
AFFECTED_IDS_STAGE = f"{TARGET}_affected_ids_stg_{ATTEMPT_STAGE_SUFFIX}"
LIVE_CURRENT_IDS_STAGE = f"{TARGET}_live_current_ids_stg_{ATTEMPT_STAGE_SUFFIX}"
ABSENT_IDS_STAGE = f"{TARGET}_absent_ids_stg_{ATTEMPT_STAGE_SUFFIX}"
ABSENCE_EXPECTED_STAGE = f"{TARGET}_absence_expected_stg_{ATTEMPT_STAGE_SUFFIX}"
RECOMPUTE_IDS_STAGE = f"{TARGET}_recompute_ids_stg_{ATTEMPT_STAGE_SUFFIX}"
SNAPSHOT_STAGE = f"{SNAPSHOT_TARGET}_stg_{ATTEMPT_STAGE_SUFFIX}"

REFERENCED_CODE_SETS = [
    3, 48, 220, 14229, 14774, 14775, 14776, 14777, 14778, 18529,
    30380, 30381, 30382, 30383, 30385, 30386, 30392, 30394, 254636, 254637,
]

SOURCES = {
    SRC_CURRENT: {"class": "data"},
    SRC_HIST: {"class": "data"},
    DECODE_REFRESH_WEEK_SOURCE: {"class": "logic"},
    LOGIC_SOURCE: {"class": "logic"},
}
TARGET_SOURCES = [SRC_CURRENT, SRC_HIST, DECODE_REFRESH_WEEK_SOURCE, LOGIC_SOURCE]
DATA_SOURCES = [SRC_CURRENT, SRC_HIST]
SOURCE_SLA = {SRC_CURRENT: 2, SRC_HIST: 2}
FULL_MODES = {"FULL", "BOOTSTRAP", "FULL_LOOKUP_CHANGE"}
TARGET_KEYS = ["PM_WAIT_LIST_ID", "ROW_SOURCE", "SOURCE_VERSION_ID"]
INCREMENTAL_DELETE_POLICY = {
    "ordinary_data_change_mode": "INCREMENTAL",
    "adc_overlap_hours": SOURCE_OVERLAP_HOURS,
    "force_full_on_every_data_change": False,
    "physical_delete_without_surviving_changed_row": (
        "BOUNDED_TO_DURABLE_ISO_WEEK_FULL_BEFORE_SCHEDULED_SNAPSHOT"
    ),
    "cdf_policy": "FUTURE_ENHANCEMENT_AFTER_TASK_6; NOT_ACTIVE",
}
SNAPSHOT_IDEMPOTENCE_POLICY = (
    "ATTEMPT_ID_RUN_LOCK_SERIALIZES_EXISTENCE_CHECK_APPEND_AND_CHECKPOINT; "
    "NO_SECOND_MANIFEST"
)
CLUSTER_KEYS = {
    TARGET: ["PM_WAIT_LIST_ID", "PERSON_ID"],
    SNAPSHOT_TARGET: ["SNAPSHOT_DATE"],
}

COMMON_KEEP = [
    "PM_WAIT_LIST_ID", "ENCNTR_ID", "PERSON_ID", "AUTO_BLOOD_IND", "LOCATION_CD",
    "LOC_FACILITY_CD", "LOC_BUILDING_CD", "LOC_NURSE_UNIT_CD", "LOC_ROOM_CD", "LOC_BED_CD",
    "REASON_FOR_CHANGE_CD", "REASON_FOR_REMOVAL_CD", "RECOMMEND_DT_TM", "REMOVAL_DT_TM",
    "PLANNED_PROCEDURE_CD", "PRE_ADMIT_ATTEND_IND", "PROVISIONAL_ADMIT_DT_TM", "URGENCY_CD",
    "STAND_BY_CD", "STATUS_DT_TM", "STATUS_CD", "ACTIVE_IND", "ACTIVE_STATUS_CD",
    "ACTIVE_STATUS_DT_TM", "BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM", "UPDT_CNT",
    "UPDT_DT_TM", "CHG_DT_TM", "WAIT_LIST_IND", "ADMIT_CATEGORY_CD", "ADMIT_BOOKING_CD",
    "MANAGEMENT_CD", "STATUS_END_DT_TM", "REFERRAL_SOURCE_CD", "SERVICE_TYPE_REQUESTED_CD",
    "REFERRAL_TYPE_CD", "ATTENDANCE_CD", "REFERRAL_DT_TM", "ADMIT_GUARANTEED_DT_TM",
    "ADMIT_DECISION_DT_TM", "LAST_DNA_DT_TM", "ADMIT_TYPE_CD", "ADMIT_OFFER_OUTCOME_CD",
    "PREV_PROV_ADMIT_DT_TM", "ANESTHETIC_CD", "ORIG_REQUEST_RECEIVED_DT_TM", "SUB_STATUS_CD",
    "FROM_ED_IND", "LAST_UTC_TS", "ADC_UPDT",
]
CURRENT_ONLY_KEEP = [
    "SUSPENDED_DAYS", "WAITING_START_DT_TM", "WAITING_END_DT_TM",
    "ADJ_WAITING_START_DT_TM", "REQUESTED_DT_TM", "SCHEDULE_DT_TM", "SCH_EVENT_ID",
]
HIST_ONLY_KEEP = ["HIST_ACTION"]
CURRENT_ONLY_TYPES = {
    "SUSPENDED_DAYS": "double",
    "WAITING_START_DT_TM": "timestamp",
    "WAITING_END_DT_TM": "timestamp",
    "ADJ_WAITING_START_DT_TM": "timestamp",
    "REQUESTED_DT_TM": "timestamp",
    "SCHEDULE_DT_TM": "timestamp",
    "SCH_EVENT_ID": "bigint",
}
HIST_ONLY_TYPES = {"HIST_ACTION": "string"}

DECODE_FIELDS = [
    ("LOCATION", "LOCATION_CD"),
    ("LOC_FACILITY", "LOC_FACILITY_CD"),
    ("LOC_BUILDING", "LOC_BUILDING_CD"),
    ("LOC_NURSE_UNIT", "LOC_NURSE_UNIT_CD"),
    ("LOC_ROOM", "LOC_ROOM_CD"),
    ("LOC_BED", "LOC_BED_CD"),
    ("REASON_FOR_CHANGE", "REASON_FOR_CHANGE_CD"),
    ("REASON_FOR_REMOVAL", "REASON_FOR_REMOVAL_CD"),
    ("PLANNED_PROCEDURE", "PLANNED_PROCEDURE_CD"),
    ("URGENCY", "URGENCY_CD"),
    ("STAND_BY", "STAND_BY_CD"),
    ("STATUS", "STATUS_CD"),
    ("ACTIVE_STATUS", "ACTIVE_STATUS_CD"),
    ("ADMIT_CATEGORY", "ADMIT_CATEGORY_CD"),
    ("ADMIT_BOOKING", "ADMIT_BOOKING_CD"),
    ("MANAGEMENT", "MANAGEMENT_CD"),
    ("REFERRAL_SOURCE", "REFERRAL_SOURCE_CD"),
    ("SERVICE_TYPE_REQUESTED", "SERVICE_TYPE_REQUESTED_CD"),
    ("REFERRAL_TYPE", "REFERRAL_TYPE_CD"),
    ("ATTENDANCE", "ATTENDANCE_CD"),
    ("ADMIT_TYPE", "ADMIT_TYPE_CD"),
    ("ADMIT_OFFER_OUTCOME", "ADMIT_OFFER_OUTCOME_CD"),
    ("ANESTHETIC", "ANESTHETIC_CD"),
    ("SUB_STATUS", "SUB_STATUS_CD"),
]
DECODE_COLUMNS = [f"{base}_DESC" for base, _ in DECODE_FIELDS]

PLANNED_FUTURE_DATE_FIELDS = [
    "PROVISIONAL_ADMIT_DT_TM",
    "ADMIT_GUARANTEED_DT_TM",
    "PREV_PROV_ADMIT_DT_TM",
    "SCHEDULE_DT_TM",
]
YEAR_2100_THRESHOLD_SQL = "TIMESTAMP'2100-12-31 23:59:59'"
QUALITY_SAMPLE_LIMIT = 10

TIMESTAMP_COLUMNS = {
    "RECOMMEND_DT_TM", "REMOVAL_DT_TM", "PROVISIONAL_ADMIT_DT_TM", "STATUS_DT_TM",
    "ACTIVE_STATUS_DT_TM", "BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM", "UPDT_DT_TM",
    "CHG_DT_TM", "STATUS_END_DT_TM", "REFERRAL_DT_TM", "ADMIT_GUARANTEED_DT_TM",
    "ADMIT_DECISION_DT_TM", "LAST_DNA_DT_TM", "PREV_PROV_ADMIT_DT_TM",
    "ORIG_REQUEST_RECEIVED_DT_TM", "LAST_UTC_TS", "ADC_UPDT",
    "WAITING_START_DT_TM", "WAITING_END_DT_TM", "ADJ_WAITING_START_DT_TM",
    "REQUESTED_DT_TM", "SCHEDULE_DT_TM",
}
INTEGRAL_SOURCE_COLUMNS = [
    column for column in COMMON_KEEP
    if column not in TIMESTAMP_COLUMNS and column != "ADC_UPDT"
] + ["SCH_EVENT_ID"]
INTEGRAL_SOURCE_COLUMNS = sorted(set(INTEGRAL_SOURCE_COLUMNS))
CURRENT_CAST_COLUMNS = sorted(set(INTEGRAL_SOURCE_COLUMNS))
HIST_CAST_COLUMNS = sorted(set(column for column in INTEGRAL_SOURCE_COLUMNS if column != "SCH_EVENT_ID")) + [
    "PM_WAIT_LIST_HIST_ID"
]

EXPECTED_SOURCE_COLUMNS = {
    SRC_CURRENT: set(COMMON_KEEP + CURRENT_ONLY_KEEP),
    SRC_HIST: set(COMMON_KEEP + HIST_ONLY_KEEP + ["PM_WAIT_LIST_HIST_ID"]),
    CODE_VALUE: {"CODE_VALUE", "CODE_SET", "DESCRIPTION", "DISPLAY"},
}

BASE_OUTPUT_COLUMNS = [
    column for column in COMMON_KEEP if column != "ADC_UPDT"
] + CURRENT_ONLY_KEEP + HIST_ONLY_KEEP + [
    "ROW_SOURCE", "SOURCE_VERSION_ID", "VERSION_DT_TM", "SOURCE_ADC_UPDT", "IS_CURRENT",
] + DECODE_COLUMNS

EXPECTED_TARGET_SCHEMA = {}
for _column in BASE_OUTPUT_COLUMNS:
    if _column in TIMESTAMP_COLUMNS or _column in {"VERSION_DT_TM", "SOURCE_ADC_UPDT"}:
        EXPECTED_TARGET_SCHEMA[_column] = "timestamp"
    elif _column == "SUSPENDED_DAYS":
        EXPECTED_TARGET_SCHEMA[_column] = "double"
    elif _column in {"HIST_ACTION", "ROW_SOURCE"} or _column in DECODE_COLUMNS:
        EXPECTED_TARGET_SCHEMA[_column] = "string"
    elif _column == "IS_CURRENT":
        EXPECTED_TARGET_SCHEMA[_column] = "boolean"
    else:
        EXPECTED_TARGET_SCHEMA[_column] = "bigint"
EXPECTED_TARGET_SCHEMA.update({
    "ROW_HASH": "bigint",
    "PIPELINE_RUN_ID": "string",
    "PIPELINE_ATTEMPT_ID": "string",
    "PIPELINE_PROCESSED_TS": "timestamp",
    "SOURCE_PRESENT_IND": "boolean",
    "SOURCE_ABSENT_DETECTED_TS": "timestamp",
    "ADC_UPDT": "timestamp",
})

SNAPSHOT_COLUMNS = [
    "PM_WAIT_LIST_ID", "ENCNTR_ID", "PERSON_ID", "SCH_EVENT_ID", "AUTO_BLOOD_IND",
    "LOCATION_CD", "LOC_FACILITY_CD", "LOC_BUILDING_CD", "LOC_NURSE_UNIT_CD", "LOC_ROOM_CD",
    "LOC_BED_CD", "REASON_FOR_CHANGE_CD", "REASON_FOR_REMOVAL_CD", "RECOMMEND_DT_TM",
    "REMOVAL_DT_TM", "PLANNED_PROCEDURE_CD", "PRE_ADMIT_ATTEND_IND",
    "PROVISIONAL_ADMIT_DT_TM", "URGENCY_CD", "STAND_BY_CD", "STATUS_DT_TM", "STATUS_CD",
    "ACTIVE_IND", "ACTIVE_STATUS_CD", "ACTIVE_STATUS_DT_TM", "WAIT_LIST_IND",
    "ADMIT_CATEGORY_CD", "ADMIT_BOOKING_CD", "MANAGEMENT_CD", "STATUS_END_DT_TM",
    "REFERRAL_SOURCE_CD", "SERVICE_TYPE_REQUESTED_CD", "REFERRAL_TYPE_CD", "ATTENDANCE_CD",
    "REFERRAL_DT_TM", "ADMIT_GUARANTEED_DT_TM", "ADMIT_DECISION_DT_TM", "LAST_DNA_DT_TM",
    "ADMIT_TYPE_CD", "ADMIT_OFFER_OUTCOME_CD", "PREV_PROV_ADMIT_DT_TM", "ANESTHETIC_CD",
    "ORIG_REQUEST_RECEIVED_DT_TM", "SUB_STATUS_CD", "FROM_ED_IND", "SUSPENDED_DAYS",
    "WAITING_START_DT_TM", "WAITING_END_DT_TM", "ADJ_WAITING_START_DT_TM",
    "REQUESTED_DT_TM", "SCHEDULE_DT_TM",
]
EXPECTED_SNAPSHOT_SCHEMA = {
    column: EXPECTED_TARGET_SCHEMA[column] for column in SNAPSHOT_COLUMNS + DECODE_COLUMNS
}
EXPECTED_SNAPSHOT_SCHEMA.update({
    "SNAPSHOT_DATE": "date",
    "SNAPSHOT_CUTOFF_TS": "timestamp",
})

HASH_EXCLUDE = {
    "ROW_HASH", "IS_CURRENT", "PIPELINE_RUN_ID", "PIPELINE_ATTEMPT_ID",
    "PIPELINE_PROCESSED_TS", "SOURCE_PRESENT_IND", "SOURCE_ABSENT_DETECTED_TS", "ADC_UPDT",
}
TECHNICAL_SNAPSHOT_BANNED = set(HASH_EXCLUDE) | {
    "VERSION_DT_TM", "SOURCE_ADC_UPDT", "ROW_SOURCE", "SOURCE_VERSION_ID",
}
BANNED_EXACT_COLUMNS = {
    "MRN", "NHS_NUMBER", "LOCAL_PATIENT_ID", "PATIENT_NAME", "FORENAME", "SURNAME",
    "DATE_OF_BIRTH", "DOB", "POSTCODE", "ADDRESS_LINE_1", "ADDRESS_LINE_2", "ADDRESS_LINE_3",
    "ADDRESS_LINE_4", "TELEPHONE_HOME", "TELEPHONE_MOBILE", "REASON_FOR_REMOVAL",
    "OTHER_MED_CONDITION", "COMMISSIONER_REFERENCE", "COMMENTS_RE_DISCHARGE",
    "COMMENT_LONG_TEXT_ID", "ACTIVE_STATUS_PRSNL_ID", "ADMITTING_PRSNL_ID",
}

STATE_CACHE = {}
SOURCE_VERSIONS = {}
SOURCE_HEALTH = {}
PINNED_SOURCE_FRAMES = {}
PINNED_SOURCE_METRICS = {}
SOURCE_SLICE_FRAMES = {}
SOURCE_SLICE_STATUS = {}
ATTEMPT_STAGE_TABLES = set()
VALIDATION_RESULTS = {
    "schema": {"stage": None, "target_preflight": None, "target": None, "snapshot_preflight": None, "snapshot": None},
    "pii": {"stage": None, "target_preflight": None, "target": None, "snapshot_preflight": None, "snapshot": None},
    "features": {},
    "clustering": {},
    "comments": {},
}
CODE_VALUE_VERSION = None
CODE_VALUE_LIVE_VERSION = None


def qident(value: str) -> str:
    tick = chr(96)
    return tick + str(value).replace(tick, tick + tick) + tick


def qname(value: str) -> str:
    return ".".join(qident(part) for part in str(value).split("."))


def sql_escape(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "''")


def fail(message: str) -> None:
    raise RuntimeError(message)


def assert_expected_columns(table: str, expected: set[str]) -> None:
    actual = set(spark.table(table).columns)
    missing = sorted(expected - actual)
    if missing:
        fail(f"{table}: missing expected columns {missing}")


def schema_contract_for(table: str) -> dict[str, str]:
    if table == TARGET:
        return EXPECTED_TARGET_SCHEMA
    if table == SNAPSHOT_TARGET:
        return EXPECTED_SNAPSHOT_SCHEMA
    fail(f"No schema contract for {table}")


def validate_schema_contract(
    df: DataFrame,
    table: str,
    label: str,
    expected_override: dict[str, str] | None = None,
    enforce_order: bool = True,
) -> dict:
    heartbeat_run_lock(f"schema validation {label}")
    expected = expected_override or schema_contract_for(table)
    expected_lower = {name.lower(): (name, data_type) for name, data_type in expected.items()}
    actual_lower = {}
    duplicates = []
    actual_order = []
    for field in df.schema.fields:
        actual_order.append(field.name)
        lowered = field.name.lower()
        if lowered in actual_lower:
            duplicates.append([actual_lower[lowered][0], field.name])
        else:
            actual_lower[lowered] = (field.name, field.dataType.simpleString().lower())
    missing = sorted(expected_lower[key][0] for key in expected_lower.keys() - actual_lower.keys())
    extra = sorted(actual_lower[key][0] for key in actual_lower.keys() - expected_lower.keys())
    case_mismatches = sorted(
        [
            {"expected": expected_lower[key][0], "actual": actual_lower[key][0]}
            for key in expected_lower.keys() & actual_lower.keys()
            if expected_lower[key][0] != actual_lower[key][0]
        ],
        key=lambda item: item["expected"],
    )
    type_mismatches = sorted(
        [
            {
                "column": expected_lower[key][0],
                "expected": expected_lower[key][1],
                "actual": actual_lower[key][1],
            }
            for key in expected_lower.keys() & actual_lower.keys()
            if expected_lower[key][1] != actual_lower[key][1]
        ],
        key=lambda item: item["column"],
    )
    expected_order = list(expected)
    order_matches = actual_order == expected_order
    if duplicates or missing or extra or case_mismatches or type_mismatches or (
        enforce_order and not order_matches
    ):
        fail(
            f"{label}: schema contract failed; duplicates={duplicates}, missing={missing}, "
            f"extra={extra}, case={case_mismatches}, types={type_mismatches}, "
            f"expected_order={expected_order}, actual_order={actual_order}"
        )
    return {
        "status": "PASS",
        "column_count": len(actual_order),
        "ordered_fields_match": order_matches,
        "order_enforced": enforce_order,
    }


def assert_no_banned_columns(df: DataFrame, label: str, snapshot: bool = False) -> dict:
    heartbeat_run_lock(f"PII validation {label}")
    actual = {column.upper() for column in df.columns}
    token_banned = {
        column for column in actual
        if any(token in column for token in (
            "PATIENT_NAME", "FORENAME", "SURNAME", "ADDRESS_", "TELEPHONE_", "FREE_TEXT",
        ))
    }
    banned = set(BANNED_EXACT_COLUMNS) & actual
    if snapshot:
        banned |= TECHNICAL_SNAPSHOT_BANNED & actual
    banned |= token_banned
    if banned:
        fail(f"{label}: banned PII/free-text/technical columns present: {sorted(banned)}")
    return {"status": "PASS", "inspected_column_count": len(actual), "banned_columns": []}


def assert_unique_non_null(df: DataFrame, keys: list[str], label: str) -> None:
    heartbeat_run_lock(f"key validation {label}")
    null_condition = reduce(lambda left, right: left | right, [F.col(key).isNull() for key in keys])
    if df.where(null_condition).limit(1).count():
        fail(f"{label}: NULL key detected for {keys}")
    if df.groupBy(*keys).count().where(F.col("count") > 1).limit(1).count():
        fail(f"{label}: duplicate key detected for {keys}")


def bigint_invalid_condition(df: DataFrame, column: str):
    fields = {field.name: field.dataType.simpleString().lower() for field in df.schema.fields}
    if column not in fields:
        fail(f"Missing BIGINT gate column {column}")
    parsed = F.expr(f"try_cast({qident(column)} AS BIGINT)")
    invalid = F.col(column).isNotNull() & parsed.isNull()
    if fields[column] in {"double", "float"} or fields[column].startswith("decimal"):
        invalid = invalid | (
            F.col(column).isNotNull()
            & parsed.isNotNull()
            & (F.col(column).cast("double") != parsed.cast("double"))
        )
    return invalid


def collect_pinned_source_metrics(
    table: str,
    frame: DataFrame,
    cast_columns: list[str],
    primary_key: str,
    source_version_key: str | None = None,
    scan_scope: str = "FULL_PINNED_TABLE",
    require_watermark: bool = True,
) -> dict:
    aggregates = [
        F.count(F.lit(1)).alias("__ROWS"),
        F.max("ADC_UPDT").alias("__WATERMARK"),
        F.sum(F.col(primary_key).isNull().cast("long")).alias("__PRIMARY_KEY_NULLS"),
    ]
    for column in cast_columns:
        aggregates.append(
            F.sum(bigint_invalid_condition(frame, column).cast("long")).alias(
                f"__INVALID__{column}"
            )
        )
    if source_version_key is not None:
        aggregates.extend([
            F.sum(F.col(source_version_key).isNull().cast("long")).alias("__VERSION_KEY_NULLS"),
            F.sum(
                (
                    F.expr(f"try_cast({qident(source_version_key)} AS BIGINT)") == -1
                ).cast("long")
            ).alias("__VERSION_SENTINEL_COLLISIONS"),
        ])
    heartbeat_run_lock(f"pinned source metrics {table} scope={scan_scope}")
    row = frame.agg(*aggregates).collect()[0]
    invalid_counts = {
        column: int(row[f"__INVALID__{column}"] or 0) for column in cast_columns
    }
    failures = {column: count for column, count in invalid_counts.items() if count}
    primary_nulls = int(row["__PRIMARY_KEY_NULLS"] or 0)
    version_nulls = int(row["__VERSION_KEY_NULLS"] or 0) if source_version_key else 0
    sentinel_collisions = (
        int(row["__VERSION_SENTINEL_COLLISIONS"] or 0) if source_version_key else 0
    )
    if failures or primary_nulls or version_nulls or sentinel_collisions:
        fail(
            f"{table}: pinned source cast/null/range/version gate failed; "
            f"scope={scan_scope}, invalid={failures}, primary_key_nulls={primary_nulls}, "
            f"version_key_nulls={version_nulls}, sentinel_collisions={sentinel_collisions}"
        )
    watermark = row["__WATERMARK"]
    if watermark is None and require_watermark:
        fail(f"{table}: pinned ADC_UPDT watermark is NULL for scope={scan_scope}")
    future_days = (
        (watermark - RUN_AS_OF).total_seconds() / 86400.0
        if watermark is not None else None
    )
    if (
        future_days is not None
        and future_days > SOURCE_WATERMARK_FUTURE_TOLERANCE_DAYS
    ):
        fail(
            f"{table}: pinned source watermark is {future_days:.3f} days after RUN_AS_OF; "
            f"tolerance={SOURCE_WATERMARK_FUTURE_TOLERANCE_DAYS} days"
        )
    scanned_rows = int(row["__ROWS"])
    return {
        "status": "PASS",
        "source_table": table,
        "pinned_delta_version": int(SOURCE_VERSIONS[table]),
        "rows": scanned_rows,
        "scanned_rows": scanned_rows,
        "watermark": watermark,
        "watermark_future_days": future_days,
        "watermark_future_tolerance_days": SOURCE_WATERMARK_FUTURE_TOLERANCE_DAYS,
        "primary_key": primary_key,
        "primary_key_nulls": primary_nulls,
        "source_version_key": source_version_key,
        "source_version_key_nulls": version_nulls,
        "source_version_sentinel_collisions": sentinel_collisions,
        "invalid_bigint_counts": invalid_counts,
        "scan_scope": scan_scope,
        "require_watermark": require_watermark,
        "aggregate_actions": 1,
    }


def source_version(table: str) -> int:
    if table == LOGIC_SOURCE:
        return LOGIC_VERSION_INT
    if table == DECODE_REFRESH_WEEK_SOURCE:
        if DECODE_REFRESH_WEEK_VERSION is None:
            fail(
                f"{DECODE_REFRESH_WEEK_SOURCE}: ISO-week version requested before "
                "post-raw-pin SNAPSHOT_DATE_UTC derivation"
            )
        return int(DECODE_REFRESH_WEEK_VERSION)
    row = spark.sql(f"DESCRIBE HISTORY {qname(table)} LIMIT 1").collect()[0]
    return int(row["version"])


def derive_incremental_cumulative_rows(
    table: str,
    checkpoint: dict | None,
) -> dict:
    if (
        checkpoint is None
        or checkpoint.get("source_version") is None
        or checkpoint.get("source_rows") is None
    ):
        fail(
            f"{table}: changed incremental source requires exact checkpoint "
            "source_version and cumulative source_rows"
        )
    checkpoint_version = int(checkpoint["source_version"])
    checkpoint_rows = int(checkpoint["source_rows"])
    pinned_version = int(SOURCE_VERSIONS[table])
    if checkpoint_version > pinned_version:
        fail(
            f"{table}: checkpoint version {checkpoint_version} is ahead of "
            f"pinned version {pinned_version}"
        )
    if checkpoint_version == pinned_version:
        return {
            "checkpoint_version": checkpoint_version,
            "pinned_version": pinned_version,
            "checkpoint_rows": checkpoint_rows,
            "row_delta": 0,
            "cumulative_rows": checkpoint_rows,
            "data_change_commits": [],
            "metadata_commits": [],
            "relation": "UNCHANGED_PINNED_VERSION",
        }

    non_data_operations = {
        "SET TBLPROPERTIES", "UNSET TBLPROPERTIES", "CHANGE COLUMN",
        "ADD COLUMNS", "DROP COLUMNS", "RENAME COLUMN", "SET COLUMN DEFAULT",
        "OPTIMIZE", "VACUUM START", "VACUUM END",
    }
    history_rows = (
        spark.sql(f"DESCRIBE HISTORY {qname(table)}")
        .where(
            (F.col("version") > F.lit(checkpoint_version))
            & (F.col("version") <= F.lit(pinned_version))
        )
        .orderBy("version")
        .collect()
    )
    if not history_rows:
        fail(
            f"{table}: changed incremental version {checkpoint_version}->{pinned_version} "
            "has no Delta history commits"
        )
    observed_versions = [int(row["version"]) for row in history_rows]
    expected_versions = list(range(checkpoint_version + 1, pinned_version + 1))
    if observed_versions != expected_versions:
        fail(
            f"{table}: incomplete or non-contiguous Delta history between checkpoint "
            f"and pinned version; observed={observed_versions}, expected={expected_versions}"
        )
    row_delta = 0
    data_change_commits = []
    metadata_commits = []
    for history_row in history_rows:
        operation = history_row["operation"]
        version = int(history_row["version"])
        if operation == "MERGE":
            metrics = history_row["operationMetrics"] or {}
            required = {"numTargetRowsInserted", "numTargetRowsDeleted"}
            missing = sorted(required - set(metrics))
            if missing:
                fail(
                    f"{table}: MERGE version {version} lacks row-count metrics "
                    f"{missing}; fail closed"
                )
            inserted = int(metrics["numTargetRowsInserted"] or 0)
            deleted = int(metrics["numTargetRowsDeleted"] or 0)
            row_delta += inserted - deleted
            data_change_commits.append({
                "version": version,
                "operation": operation,
                "inserted_rows": inserted,
                "deleted_rows": deleted,
                "row_delta": inserted - deleted,
            })
        elif operation in non_data_operations:
            metadata_commits.append({
                "version": version,
                "operation": operation,
            })
        else:
            fail(
                f"{table}: unsupported data-changing or unknown Delta operation "
                f"{operation!r} at version {version}; fail closed"
            )
    cumulative_rows = checkpoint_rows + row_delta
    if cumulative_rows < 0:
        fail(
            f"{table}: derived cumulative source rows became negative: "
            f"{checkpoint_rows}+{row_delta}"
        )
    return {
        "checkpoint_version": checkpoint_version,
        "pinned_version": pinned_version,
        "checkpoint_rows": checkpoint_rows,
        "row_delta": row_delta,
        "cumulative_rows": cumulative_rows,
        "data_change_commits": data_change_commits,
        "metadata_commits": metadata_commits,
        "relation": "CHECKPOINT_PLUS_DELTA_HISTORY",
    }


def checkpoint_source_health(
    table: str,
    checkpoint: dict | None,
    scan: str,
    slice_metrics: dict | None = None,
) -> dict:
    if checkpoint is None or checkpoint.get("source_watermark") is None:
        fail(f"{table}: checkpoint watermark is required for {scan}")
    checkpoint_watermark = checkpoint["source_watermark"]
    slice_watermark = slice_metrics.get("watermark") if slice_metrics else None
    watermark = max(
        value for value in (checkpoint_watermark, slice_watermark) if value is not None
    )
    future_days = (watermark - RUN_AS_OF).total_seconds() / 86400.0
    if future_days > SOURCE_WATERMARK_FUTURE_TOLERANCE_DAYS:
        fail(
            f"{table}: effective checkpoint/slice watermark is {future_days:.3f} days "
            f"after RUN_AS_OF; tolerance={SOURCE_WATERMARK_FUTURE_TOLERANCE_DAYS} days"
        )
    return {
        "source_table": table,
        "version": int(SOURCE_VERSIONS[table]),
        "rows": checkpoint.get("source_rows"),
        "watermark": watermark,
        "source_staleness_days": (RUN_AS_OF - watermark).total_seconds() / 86400.0,
        "source_watermark_future_days": future_days,
        "scan": scan,
        "scan_scope": slice_metrics.get("scan_scope") if slice_metrics else "NO_RAW_SCAN",
        "scanned_rows": int(slice_metrics["scanned_rows"]) if slice_metrics else 0,
        "slice_watermark": slice_watermark,
        "checkpoint_watermark": checkpoint_watermark,
        "source_rows_semantics": "EXACT_REUSED_CHECKPOINT_TOTAL",
    }


def full_source_health(table: str) -> dict:
    metrics = PINNED_SOURCE_METRICS.get(table)
    if metrics is None:
        fail(f"{table}: full pinned source metrics were not collected")
    watermark = metrics["watermark"]
    return {
        "source_table": table,
        "version": int(metrics["pinned_delta_version"]),
        "rows": int(metrics["rows"]),
        "watermark": watermark,
        "source_staleness_days": (RUN_AS_OF - watermark).total_seconds() / 86400.0,
        "source_watermark_future_days": metrics["watermark_future_days"],
        "scan": "PINNED_FULL_TABLE_AGGREGATE",
        "scan_scope": metrics["scan_scope"],
        "scanned_rows": int(metrics["scanned_rows"]),
        "slice_watermark": None,
        "checkpoint_watermark": None,
        "source_rows_semantics": "EXACT_PINNED_FULL_TABLE_TOTAL",
    }


def source_health_for_mode(
    table: str,
    mode: str,
    checkpoint: dict | None,
    source_changed: bool,
) -> dict:
    version = SOURCE_VERSIONS[table]
    if table in {LOGIC_SOURCE, DECODE_REFRESH_WEEK_SOURCE}:
        return {
            "source_table": table,
            "version": version,
            "rows": None,
            "watermark": None,
            "source_staleness_days": None,
            "source_watermark_future_days": None,
            "scan": "SYNTHETIC",
            "scan_scope": "NO_DATA_SCAN",
            "scanned_rows": 0,
        }
    if mode in FULL_MODES:
        return full_source_health(table)
    if mode == "INCREMENTAL" and source_changed:
        metrics = PINNED_SOURCE_METRICS.get(table)
        if metrics is None:
            fail(f"{table}: changed incremental source metrics were not collected")
        health = checkpoint_source_health(
            table,
            checkpoint,
            "PINNED_CHANGED_OVERLAP_AGGREGATE",
            slice_metrics=metrics,
        )
        row_count_proof = derive_incremental_cumulative_rows(table, checkpoint)
        health["rows"] = int(row_count_proof["cumulative_rows"])
        health["source_rows_semantics"] = (
            "EXACT_CHECKPOINT_PLUS_DELTA_HISTORY_INSERTS_MINUS_DELETES"
        )
        health["cumulative_row_count_proof"] = row_count_proof
        return health
    if mode == "INCREMENTAL":
        return checkpoint_source_health(
            table,
            checkpoint,
            "REUSED_CHECKPOINT_UNCHANGED_VERSION",
        )
    return checkpoint_source_health(
        table,
        checkpoint,
        "REUSED_CHECKPOINT_UNCHANGED_SKIP",
    )


def load_state_cache() -> None:
    state = spark.table(STATE_TABLE)
    duplicate_state_rows = (
        state.groupBy("target_table", "source_table")
        .count().where(F.col("count") > 1).limit(1).count()
    )
    if duplicate_state_rows:
        fail(
            f"{STATE_TABLE}: duplicate (target_table, source_table) state rows detected"
        )
    STATE_CACHE.clear()
    for row in state.where(F.col("target_table") != LOCK_KEY).collect():
        item = row.asDict(recursive=True)
        STATE_CACHE[(item["target_table"], item["source_table"])] = item


def last_checkpoint(source: str) -> dict | None:
    return STATE_CACHE.get((TARGET, source))


def source_version_changed(source: str) -> bool:
    checkpoint = last_checkpoint(source)
    return (
        checkpoint is None
        or int(checkpoint["source_version"]) != int(SOURCE_VERSIONS[source])
    )


def pinned_changed_sources() -> list[str]:
    return [source for source in TARGET_SOURCES if source_version_changed(source)]


def weekly_reconciliation_status() -> dict:
    checkpoint = last_checkpoint(DECODE_REFRESH_WEEK_SOURCE)
    checkpoint_version = (
        int(checkpoint["source_version"]) if checkpoint is not None else None
    )
    current_version = int(DECODE_REFRESH_WEEK_VERSION)
    if checkpoint_version is not None and checkpoint_version > current_version:
        fail(
            f"{DECODE_REFRESH_WEEK_SOURCE}: stored checkpoint version "
            f"{checkpoint_version} is ahead of current ISO week {current_version}"
        )
    pending = checkpoint_version != current_version
    if checkpoint_version is None:
        reason = "MISSING_WEEK_CHECKPOINT"
    elif pending:
        reason = "CURRENT_ISO_WEEK_NOT_COMMITTED"
    else:
        reason = "CURRENT_ISO_WEEK_ALREADY_COMMITTED"
    return {
        "source": DECODE_REFRESH_WEEK_SOURCE,
        "checkpoint_version": checkpoint_version,
        "current_version": current_version,
        "week_pending": bool(pending),
        "reason": reason,
        "checkpoint_advances_only_after_successful_snapshot_and_commit": True,
    }


def choose_mode(changed_sources: list[str]) -> str:
    if BOOTSTRAP_MODE:
        return "BOOTSTRAP"
    if not bronze_table_exists(TARGET):
        fail(
            f"{TARGET}: target is absent; first publication requires explicit "
            "bootstrap_mode=true; force_full_refresh/full_reconciliation/refresh_decodes "
            "cannot substitute for bootstrap ownership"
        )
    if FORCE_FULL_REFRESH or FULL_RECONCILIATION or REFRESH_DECODES:
        return "FULL"
    if any(last_checkpoint(source) is None for source in TARGET_SOURCES):
        return "FULL"
    changed = set(changed_sources)
    if LOGIC_SOURCE in changed or DECODE_REFRESH_WEEK_SOURCE in changed:
        return "FULL"
    if changed:
        # Daily ADC_UPDT-windowed INCREMENTAL is intentional. Physical deletes with no
        # surviving changed row are bounded to the durable ISO-week FULL before snapshot.
        return "INCREMENTAL"
    return "UNCHANGED_SKIP"


def source_mode(source: str, mode: str) -> str:
    if source in {LOGIC_SOURCE, DECODE_REFRESH_WEEK_SOURCE}:
        return "SYNTHETIC"
    if mode in FULL_MODES:
        return "FULL"
    if mode == "INCREMENTAL":
        return "INCREMENTAL"
    return "UNCHANGED_SKIP"


def changed_rows(table: str) -> DataFrame:
    checkpoint = last_checkpoint(table)
    if checkpoint is None or checkpoint.get("source_watermark") is None:
        fail(f"{table}: missing source watermark for incremental mode")
    lower_bound = checkpoint["source_watermark"] - timedelta(hours=SOURCE_OVERLAP_HOURS)
    return PINNED_SOURCE_FRAMES[table].where(
        F.col("ADC_UPDT") >= F.lit(lower_bound).cast("timestamp")
    )


def establish_source_slices(
    mode: str,
    changed_sources: list[str],
) -> tuple[dict[str, DataFrame], dict[str, dict]]:
    changed = set(changed_sources)
    frames = {}
    status = {}
    for source in DATA_SOURCES:
        checkpoint = last_checkpoint(source)
        checkpoint_version = (
            int(checkpoint["source_version"]) if checkpoint is not None else None
        )
        if mode in FULL_MODES:
            frames[source] = PINNED_SOURCE_FRAMES[source]
            scope = "FULL_PINNED_TABLE"
            lower_bound = None
        elif mode == "INCREMENTAL" and source in changed:
            frames[source] = changed_rows(source)
            scope = "PINNED_CHANGED_OVERLAP_SLICE"
            lower_bound = checkpoint["source_watermark"] - timedelta(
                hours=SOURCE_OVERLAP_HOURS
            )
        else:
            # Schema-preserving empty frame: unchanged incremental/skip sources do not scan raw.
            frames[source] = PINNED_SOURCE_FRAMES[source].limit(0)
            scope = (
                "NO_RAW_SCAN_REUSED_CHECKPOINT_UNCHANGED_VERSION"
                if mode == "INCREMENTAL"
                else "NO_RAW_SCAN_UNCHANGED_SKIP"
            )
            lower_bound = None
        status[source] = {
            "pinned_version": int(SOURCE_VERSIONS[source]),
            "checkpoint_version": checkpoint_version,
            "version_changed": source in changed,
            "scope": scope,
            "overlap_hours": SOURCE_OVERLAP_HOURS if lower_bound is not None else None,
            "lower_bound_utc": str(lower_bound) if lower_bound is not None else None,
        }
    return frames, status


def with_row_hash(df: DataFrame) -> DataFrame:
    columns = sorted(column for column in df.columns if column not in HASH_EXCLUDE)
    payload = F.to_json(
        F.struct(*[F.col(column).alias(column) for column in columns]),
        options={"ignoreNullFields": "false"},
    )
    return df.withColumn("ROW_HASH", F.xxhash64(payload))


REQUIRED_DELTA_FEATURES = (
    "delta.enableChangeDataFeed",
    "delta.enableRowTracking",
    "delta.enableDeletionVectors",
)


TABLE_COMMENTS = {
    TARGET: (
        "Curated Millennium waiting-list current and physical history rows. "
        "Physical key: PM_WAIT_LIST_ID, ROW_SOURCE, SOURCE_VERSION_ID."
    ),
    SNAPSHOT_TARGET: (
        "Immutable UTC-date waiting-list open-state snapshots captured after validated main publication."
    ),
}
COLUMN_COMMENTS = {
    TARGET: {
        "PM_WAIT_LIST_ID": "Millennium waiting-list identifier; business entity component of the physical key.",
        "ROW_SOURCE": "Physical row origin: CURRENT or HIST.",
        "SOURCE_VERSION_ID": "Physical source-version identifier; -1 for CURRENT and PM_WAIT_LIST_HIST_ID for HIST.",
        "IS_CURRENT": "True only for a present CURRENT/-1 row; derived from the row-local physical formula.",
    },
    SNAPSHOT_TARGET: {
        "PM_WAIT_LIST_ID": "Millennium waiting-list identifier; unique within SNAPSHOT_DATE.",
        "SNAPSHOT_DATE": "UTC business date of the immutable snapshot.",
        "SNAPSHOT_CUTOFF_TS": "Single immutable UTC cutoff timestamp shared by every row for SNAPSHOT_DATE.",
    },
}


def required_delta_features(table: str) -> tuple[str, ...]:
    return REQUIRED_DELTA_FEATURES + (("delta.appendOnly",) if table == SNAPSHOT_TARGET else ())


def table_feature_state(table: str) -> dict:
    properties = spark.sql(f"DESCRIBE DETAIL {qname(table)}").collect()[0]["properties"] or {}
    return {
        property_name: str(properties.get(property_name, "false")).lower() == "true"
        for property_name in required_delta_features(table)
    }


def validate_or_repair_table_features(table: str, repair_allowed: bool) -> dict:
    heartbeat_run_lock(f"Delta feature validation {table}")
    before = table_feature_state(table)
    missing = sorted(name for name, enabled in before.items() if not enabled)
    altered = False
    if missing:
        if not repair_allowed:
            fail(f"{table}: required Delta features missing on skipped target: {missing}")
        assignments = ",".join(
            f"'{sql_escape(property_name)}'='true'" for property_name in missing
        )
        spark.sql(f"ALTER TABLE {qname(table)} SET TBLPROPERTIES ({assignments})")
        altered = True
    after = table_feature_state(table)
    remaining = sorted(name for name, enabled in after.items() if not enabled)
    if remaining:
        fail(f"{table}: required Delta features remain disabled: {remaining}")
    return {"status": "PASS", "before": before, "after": after, "altered": altered}


def table_comment_state(table: str) -> dict:
    detail = spark.sql(f"DESCRIBE DETAIL {qname(table)}").collect()[0].asDict(recursive=True)
    frame = spark.table(table)
    return {
        "table": detail.get("description"),
        "columns": {
            field.name: field.metadata.get("comment")
            for field in frame.schema.fields
            if field.name in COLUMN_COMMENTS.get(table, {})
        },
    }


def validate_or_repair_comments(table: str, repair_allowed: bool) -> dict:
    expected_table = TABLE_COMMENTS[table]
    expected_columns = COLUMN_COMMENTS[table]
    before = table_comment_state(table)
    table_mismatch = before["table"] != expected_table
    column_mismatches = sorted(
        column for column, comment in expected_columns.items()
        if before["columns"].get(column) != comment
    )
    altered = False
    if table_mismatch or column_mismatches:
        if not repair_allowed:
            fail(
                f"{table}: comments drift in fail-only mode; "
                f"table_mismatch={table_mismatch}, columns={column_mismatches}"
            )
        if table_mismatch:
            spark.sql(f"COMMENT ON TABLE {qname(table)} IS '{sql_escape(expected_table)}'")
        for column in column_mismatches:
            spark.sql(
                f"ALTER TABLE {qname(table)} ALTER COLUMN {qident(column)} "
                f"COMMENT '{sql_escape(expected_columns[column])}'"
            )
        altered = True
    after = table_comment_state(table)
    remaining_table = after["table"] != expected_table
    remaining_columns = sorted(
        column for column, comment in expected_columns.items()
        if after["columns"].get(column) != comment
    )
    if remaining_table or remaining_columns:
        fail(
            f"{table}: required comments remain incorrect; "
            f"table_mismatch={remaining_table}, columns={remaining_columns}"
        )
    return {
        "status": "PASS",
        "altered": altered,
        "table_comment": expected_table,
        "column_comments": expected_columns,
    }


def latest_delta_history(table: str) -> dict:
    row = spark.sql(f"DESCRIBE HISTORY {qname(table)} LIMIT 1").collect()[0].asDict(recursive=True)
    return {
        "version": int(row["version"]),
        "operation": str(row.get("operation")),
        "operation_metrics": {
            str(key): str(value)
            for key, value in (row.get("operationMetrics") or {}).items()
        },
    }


def operation_metric_int(metrics: dict, key: str) -> int:
    value = metrics.get(key)
    return int(value) if value is not None else 0


def required_drain_merge_metrics(metrics: dict, label: str) -> dict:
    required = [
        "numSourceRows",
        "numOutputRows",
        "numTargetRowsUpdated",
        "numTargetRowsInserted",
        "numTargetRowsDeleted",
    ]
    missing = [key for key in required if key not in metrics]
    nulls = [key for key in required if key in metrics and metrics[key] is None]
    if missing or nulls:
        fail(
            f"{label}: MERGE operation metrics incomplete; missing={missing}, "
            f"null={nulls}; fail closed"
        )
    try:
        return {key: int(metrics[key]) for key in required}
    except (TypeError, ValueError) as exc:
        fail(f"{label}: MERGE operation metrics are not integral: {metrics}; {exc}")


def clustering_columns(table: str) -> tuple[list[str], list[str]]:
    detail = spark.sql(f"DESCRIBE DETAIL {qname(table)}").collect()[0].asDict(recursive=True)
    raw = [str(column) for column in (detail.get("clusteringColumns") or [])]
    normalized = [column.strip().strip(chr(96)).upper() for column in raw]
    return raw, normalized


def ensure_clustering_statistics(table: str, repair_allowed: bool) -> dict:
    heartbeat_run_lock(f"clustering statistics {table}")
    detail = spark.sql(f"DESCRIBE DETAIL {qname(table)}").collect()[0].asDict(recursive=True)
    properties = detail.get("properties") or {}
    frame = spark.table(table)
    fields = {field.name.upper(): field for field in frame.schema.fields}
    supported = {
        "byte", "short", "integer", "long", "float", "double",
        "decimal", "string", "date", "timestamp", "timestamp_ntz",
    }
    explicit = str(properties.get("delta.dataSkippingStatsColumns", "")).strip()
    removed = []
    if explicit:
        before = [column.strip() for column in explicit.split(",") if column.strip()]
        sanitized = []
        seen = set()
        for configured in before:
            normalized = configured.strip().strip(chr(96)).upper()
            field = fields.get(normalized)
            if field is None:
                removed.append({"column": configured, "reason": "MISSING_TARGET_COLUMN"})
            elif field.dataType.typeName() not in supported:
                removed.append({"column": field.name, "reason": f"UNSUPPORTED:{field.dataType.typeName()}"})
            elif field.name.upper() not in seen:
                sanitized.append(field.name)
                seen.add(field.name.upper())
    else:
        indexed = int(properties.get("delta.dataSkippingNumIndexedCols", 32) or 32)
        before = [
            field.name for field in frame.schema.fields[:indexed]
            if field.dataType.typeName() in supported
        ]
        sanitized = list(before)
        seen = {column.upper() for column in sanitized}
    added = []
    for desired in CLUSTER_KEYS[table]:
        field = fields.get(desired.upper())
        if field is None or field.dataType.typeName() not in supported:
            fail(f"{table}: invalid clustering statistics column {desired}")
        if field.name.upper() not in seen:
            sanitized.append(field.name)
            seen.add(field.name.upper())
            added.append(field.name)
    before_normalized = [column.strip().strip(chr(96)).upper() for column in before]
    after_normalized = [column.upper() for column in sanitized]
    needs_repair = bool(removed or added or before_normalized != after_normalized)
    if needs_repair:
        if not repair_allowed:
            fail(
                f"{table}: data-skipping statistics drift in fail-only mode; "
                f"removed={removed}, added={added}, before={before}, after={sanitized}"
            )
        if not sanitized:
            fail(f"{table}: no supported data-skipping statistics columns remain")
        spark.sql(
            f"ALTER TABLE {qname(table)} SET TBLPROPERTIES "
            f"('delta.dataSkippingStatsColumns'='{sql_escape(','.join(sanitized))}')"
        )
        heartbeat_run_lock(f"Delta statistics recompute {table}")
        spark.sql(f"ANALYZE TABLE {qname(table)} COMPUTE DELTA STATISTICS")
    return {
        "status": "PASS",
        "before_columns": before,
        "after_columns": sanitized,
        "removed_columns": removed,
        "added_clustering_columns": added,
        "altered": needs_repair,
    }


def validate_or_repair_clustering(table: str, repair_allowed: bool) -> dict:
    heartbeat_run_lock(f"clustering validation {table}")
    desired = [column.upper() for column in CLUSTER_KEYS[table]]
    before_raw, before = clustering_columns(table)
    stats = ensure_clustering_statistics(table, repair_allowed)
    altered = False
    if before != desired:
        if not repair_allowed:
            fail(f"{table}: clustering {before_raw} normalizes to {before}, expected {desired}")
        spark.sql(
            f"ALTER TABLE {qname(table)} CLUSTER BY "
            f"({', '.join(qident(column) for column in CLUSTER_KEYS[table])})"
        )
        altered = True
    after_raw, after = clustering_columns(table)
    if after != desired:
        fail(f"{table}: clustering {after_raw} normalizes to {after}, expected {desired}")
    return {
        "status": "PASS",
        "desired_columns": CLUSTER_KEYS[table],
        "before_columns": before_raw,
        "after_columns": after_raw,
        "statistics": stats,
        "altered": altered,
    }


def validate_snapshot_cutoffs(frame: DataFrame, label: str) -> dict:
    heartbeat_run_lock(f"snapshot cutoff validation {label}")
    if frame.where(F.col("SNAPSHOT_CUTOFF_TS").isNull()).limit(1).count():
        fail(f"{label}: NULL SNAPSHOT_CUTOFF_TS")
    invalid_dates = (
        frame.groupBy("SNAPSHOT_DATE")
        .agg(F.countDistinct("SNAPSHOT_CUTOFF_TS").alias("cutoff_count"))
        .where(F.col("cutoff_count") != 1)
        .limit(1).count()
    )
    if invalid_dates:
        fail(f"{label}: expected exactly one immutable cutoff per SNAPSHOT_DATE")
    return {"status": "PASS", "one_cutoff_per_date": True}


def validate_existing_preflight(table: str, mode: str, snapshot: bool = False) -> dict:
    exists = bronze_table_exists(table)
    if not exists:
        if mode == "UNCHANGED_SKIP" and not snapshot:
            fail(f"{table}: UNCHANGED_SKIP selected but target is absent")
        return {
            "status": "NOT_PRESENT",
            "target_exists": False,
            "repair_allowed": mode != "UNCHANGED_SKIP",
            "validation_scope": "METADATA_ONLY",
        }
    frame = spark.table(table)
    schema_result = validate_schema_contract(frame, table, f"preflight {table}")
    pii_result = assert_no_banned_columns(frame, f"preflight {table}", snapshot=snapshot)
    repair_allowed = mode != "UNCHANGED_SKIP"
    feature_result = validate_or_repair_table_features(table, repair_allowed)
    clustering_result = validate_or_repair_clustering(table, repair_allowed)
    comments_result = validate_or_repair_comments(table, repair_allowed)
    full_scan_validation = mode in FULL_MODES and not snapshot
    data_validation = {"status": "NOT_SCANNED", "scope": "METADATA_ONLY"}
    if full_scan_validation:
        if table == TARGET:
            assert_unique_non_null(frame, TARGET_KEYS, f"preflight {table}")
            data_validation = {"status": "PASS", "scope": "FULL_TARGET_KEYS"}
        else:
            assert_unique_non_null(
                frame, ["SNAPSHOT_DATE", "PM_WAIT_LIST_ID"], f"preflight {table}"
            )
            cutoff_result = validate_snapshot_cutoffs(frame, f"preflight {table}")
            data_validation = {
                "status": "PASS",
                "scope": "FULL_SNAPSHOT_KEYS_AND_CUTOFFS",
                "cutoffs": cutoff_result,
            }
    if table == TARGET:
        VALIDATION_RESULTS["schema"]["target_preflight"] = schema_result
        VALIDATION_RESULTS["pii"]["target_preflight"] = pii_result
    else:
        VALIDATION_RESULTS["schema"]["snapshot_preflight"] = schema_result
        VALIDATION_RESULTS["pii"]["snapshot_preflight"] = pii_result
    VALIDATION_RESULTS["comments"][f"{'snapshot' if snapshot else 'target'}_preflight"] = comments_result
    return {
        "status": "PASS",
        "target_exists": True,
        "repair_allowed": repair_allowed,
        "validation_scope": ("FULL_DATA_AND_METADATA" if full_scan_validation else "METADATA_ONLY_PINNED_DATE_VALIDATED_AT_SNAPSHOT_BOUNDARY" if snapshot else "METADATA_ONLY"),
        "schema": schema_result,
        "pii": pii_result,
        "features": feature_result,
        "clustering": clustering_result,
        "comments": comments_result,
        "data_validation": data_validation,
    }


def write_attempt_stage(frame: DataFrame, table: str, label: str) -> DataFrame:
    heartbeat_run_lock(f"stage write {label}")
    ATTEMPT_STAGE_TABLES.add(table)
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    heartbeat_run_lock(f"stage read {label}")
    return spark.table(table)


def materialize_main_stage(df: DataFrame) -> DataFrame:
    business_contract = {
        name: data_type for name, data_type in EXPECTED_TARGET_SCHEMA.items()
        if name not in {
            "ROW_HASH", "PIPELINE_RUN_ID", "PIPELINE_ATTEMPT_ID", "PIPELINE_PROCESSED_TS",
            "SOURCE_PRESENT_IND", "SOURCE_ABSENT_DETECTED_TS", "ADC_UPDT",
        }
    }
    validate_schema_contract(
        df, TARGET, "pre-stage unordered map_waiting_list",
        expected_override=business_contract, enforce_order=False,
    )
    assert_no_banned_columns(df, "pre-stage map_waiting_list")
    ordered = df.select(*business_contract.keys())
    validate_schema_contract(
        ordered, TARGET, "pre-stage ordered map_waiting_list",
        expected_override=business_contract, enforce_order=True,
    )
    staged = (
        with_row_hash(ordered)
        .withColumn("PIPELINE_RUN_ID", F.lit(RUN_ID))
        .withColumn("PIPELINE_ATTEMPT_ID", F.lit(ATTEMPT_ID))
        .withColumn("PIPELINE_PROCESSED_TS", F.lit(WRITE_TS).cast("timestamp"))
        .withColumn("SOURCE_PRESENT_IND", F.lit(True))
        .withColumn("SOURCE_ABSENT_DETECTED_TS", F.lit(None).cast("timestamp"))
        .withColumn("ADC_UPDT", F.lit(WRITE_TS).cast("timestamp"))
        .select(*EXPECTED_TARGET_SCHEMA.keys())
    )
    snapshot = write_attempt_stage(staged, MAIN_STAGE, "map_waiting_list")
    VALIDATION_RESULTS["schema"]["stage"] = validate_schema_contract(snapshot, TARGET, "staged map_waiting_list")
    VALIDATION_RESULTS["pii"]["stage"] = assert_no_banned_columns(snapshot, "staged map_waiting_list")
    assert_unique_non_null(snapshot, TARGET_KEYS, "staged map_waiting_list")
    hist_version_duplicates = (
        snapshot.where(F.col("ROW_SOURCE") == "HIST")
        .groupBy("SOURCE_VERSION_ID").count().where(F.col("count") > 1)
        .limit(1).count()
    )
    if hist_version_duplicates:
        fail("staged map_waiting_list: HIST SOURCE_VERSION_ID is not globally unique")
    return snapshot


def pin_decode_lookup() -> tuple[DataFrame, dict]:
    pinned = (
        spark.read.format("delta")
        .option("versionAsOf", int(CODE_VALUE_VERSION))
        .table(CODE_VALUE)
        .where(F.expr("try_cast(CODE_SET AS BIGINT)").isin(REFERENCED_CODE_SETS))
        .select(
            F.expr("try_cast(CODE_VALUE AS BIGINT)").alias("__CODE_VALUE"),
            F.coalesce(F.col("DESCRIPTION"), F.col("DISPLAY")).cast("string").alias("__DESCRIPTION"),
        )
    )
    heartbeat_run_lock("decode lookup uniqueness gate")
    metrics = pinned.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.col("__CODE_VALUE").isNull().cast("long")).alias("cast_null_rows"),
    ).collect()[0]
    duplicate_groups = (
        pinned.where(F.col("__CODE_VALUE").isNotNull())
        .groupBy("__CODE_VALUE").count().where(F.col("count") > 1).count()
    )
    if metrics["cast_null_rows"] or duplicate_groups:
        fail(
            "Scoped mill_code_value failed non-null/unique gate: "
            f"cast_null_rows={metrics['cast_null_rows']}, duplicate_groups={duplicate_groups}"
        )
    code_map = pinned.agg(
        F.map_from_entries(
            F.collect_list(F.struct(F.col("__CODE_VALUE"), F.col("__DESCRIPTION")))
        ).alias("__CODE_MAP")
    )
    return F.broadcast(code_map), {
        "status": "PASS",
        "pinned_version": int(CODE_VALUE_VERSION),
        "live_version": int(CODE_VALUE_LIVE_VERSION),
        "scoped_rows": int(metrics["rows"]),
        "cast_null_rows": int(metrics["cast_null_rows"] or 0),
        "duplicate_groups": int(duplicate_groups),
        "mode_trigger": False,
        "decode_strategy": "ONE_BROADCAST_ONE_ROW_MAP; ONE_CROSS_JOIN; ELEMENT_AT",
        "driver_identifier_collection": False,
    }


def add_decodes(df: DataFrame, lookup_map: DataFrame) -> DataFrame:
    result = df.crossJoin(lookup_map)
    for base, code_column in DECODE_FIELDS:
        result = result.withColumn(
            f"{base}_DESC",
            F.element_at(F.col("__CODE_MAP"), F.col(code_column)),
        )
    return result.drop("__CODE_MAP")


def cast_projection(source: DataFrame, row_source: str) -> DataFrame:
    is_current = row_source == "CURRENT"
    expressions = []
    for column in COMMON_KEEP:
        if column == "ADC_UPDT":
            expressions.append(F.col(column).cast("timestamp").alias("SOURCE_ADC_UPDT"))
        elif column in TIMESTAMP_COLUMNS:
            expressions.append(F.col(column).cast("timestamp").alias(column))
        else:
            expressions.append(F.expr(f"try_cast({qident(column)} AS BIGINT)").alias(column))
    for column in CURRENT_ONLY_KEEP:
        if is_current:
            expressions.append(F.col(column).cast(CURRENT_ONLY_TYPES[column]).alias(column))
        else:
            expressions.append(F.lit(None).cast(CURRENT_ONLY_TYPES[column]).alias(column))
    if is_current:
        expressions.append(F.lit(None).cast("string").alias("HIST_ACTION"))
    else:
        expressions.append(F.col("HIST_ACTION").cast("string").alias("HIST_ACTION"))
    expressions.extend([
        F.lit(row_source).cast("string").alias("ROW_SOURCE"),
        (
            F.lit(-1).cast("bigint")
            if is_current else F.expr("try_cast(PM_WAIT_LIST_HIST_ID AS BIGINT)")
        ).alias("SOURCE_VERSION_ID"),
    ])
    projected = source.select(*expressions)
    return (
        projected
        .withColumn("VERSION_DT_TM", F.coalesce("LAST_UTC_TS", "UPDT_DT_TM"))
        .withColumn("IS_CURRENT", F.lit(is_current))
    )


def build_union(current_source: DataFrame, hist_source: DataFrame, lookup: DataFrame) -> tuple[DataFrame, dict]:
    current = cast_projection(current_source, "CURRENT")
    hist = cast_projection(hist_source, "HIST")
    union = current.unionByName(hist)
    decoded = add_decodes(union, lookup)
    return decoded.select(*BASE_OUTPUT_COLUMNS), {
        "status": "PENDING_MATERIALIZED_STAGE_GATES",
        "source_cast_null_range_version_metrics": {
            source: {
                key: (str(value) if key == "watermark" else value)
                for key, value in metrics.items()
            }
            for source, metrics in PINNED_SOURCE_METRICS.items()
        },
        "pre_stage_key_shuffles_removed": True,
        "planned_procedure_missing_is_quality_only": True,
    }


def removal_future_performed_gate(staged: DataFrame) -> dict:
    heartbeat_run_lock("removal future-performed validation")
    horizon = F.lit(RUN_FUTURE_HORIZON).cast("timestamp")
    metrics = staged.agg(
        F.sum(F.col("REMOVAL_DT_TM").isNotNull().cast("long")).alias("populated_rows"),
        F.sum((F.col("REMOVAL_DT_TM") > horizon).cast("long")).alias("future_performed_rows"),
        F.sum(
            (F.col("REMOVAL_DT_TM") > F.expr(YEAR_2100_THRESHOLD_SQL)).cast("long")
        ).alias("after_2100_rows"),
    ).collect()[0]
    future_rows = int(metrics["future_performed_rows"] or 0)
    samples = []
    if future_rows:
        samples = [
            str(row["REMOVAL_DT_TM"])
            for row in (
                staged.where(F.col("REMOVAL_DT_TM") > horizon)
                .select("REMOVAL_DT_TM").distinct()
                .orderBy("REMOVAL_DT_TM")
                .limit(QUALITY_SAMPLE_LIMIT)
                .collect()
            )
        ]
    return {
        "status": "PASS" if future_rows == 0 else "PASS_WITH_QUALITY_ROWS",
        "hard_failure": False,
        "persisted_quality_column": False,
        "row_hash_relative_field": False,
        "run_as_of_utc": str(RUN_AS_OF),
        "future_horizon_utc": str(RUN_FUTURE_HORIZON),
        "populated_removal_rows": int(metrics["populated_rows"] or 0),
        "future_performed_rows": future_rows,
        "after_2100_rows": int(metrics["after_2100_rows"] or 0),
        "year_2100_threshold_utc": YEAR_2100_THRESHOLD_SQL,
        "sample_limit": QUALITY_SAMPLE_LIMIT,
        "future_performed_timestamp_samples": samples,
        "patient_linked_identifiers_emitted": False,
    }


def planned_future_year_2100_gate(staged: DataFrame) -> dict:
    heartbeat_run_lock("planned future year-2100 validation")
    threshold = F.expr(YEAR_2100_THRESHOLD_SQL)
    aggregates = []
    for field in PLANNED_FUTURE_DATE_FIELDS:
        aggregates.extend([
            F.sum(F.col(field).isNotNull().cast("long")).alias(f"{field}__POPULATED"),
            F.sum((F.col(field) > threshold).cast("long")).alias(f"{field}__AFTER_2100"),
        ])
    row = staged.agg(*aggregates).collect()[0]
    field_metrics = {}
    total_after_2100 = 0
    for field in PLANNED_FUTURE_DATE_FIELDS:
        populated_rows = int(row[f"{field}__POPULATED"] or 0)
        after_2100_rows = int(row[f"{field}__AFTER_2100"] or 0)
        total_after_2100 += after_2100_rows
        samples = []
        if after_2100_rows:
            samples = [
                str(item[field])
                for item in (
                    staged.where(F.col(field) > threshold)
                    .select(field).distinct()
                    .orderBy(field)
                    .limit(QUALITY_SAMPLE_LIMIT)
                    .collect()
                )
            ]
        field_metrics[field] = {
            "populated_rows": populated_rows,
            "after_2100_rows": after_2100_rows,
            "sample_limit": QUALITY_SAMPLE_LIMIT,
            "samples": samples,
        }
    return {
        "status": "PASS" if total_after_2100 == 0 else "PASS_WITH_QUALITY_ROWS",
        "hard_failure": False,
        "persisted_quality_columns": False,
        "row_hash_quality_metrics": False,
        "patient_linked_identifiers_emitted": False,
        "threshold_utc": YEAR_2100_THRESHOLD_SQL,
        "classified_fields": list(PLANNED_FUTURE_DATE_FIELDS),
        "total_after_2100_rows": int(total_after_2100),
        "fields": field_metrics,
    }


def finalize_stage_quality(staged: DataFrame, union_gate: dict) -> dict:
    heartbeat_run_lock("materialized stage quality metrics")
    aggregates = [
        F.count(F.lit(1)).alias("__ROWS"),
        F.sum((F.col("ROW_SOURCE") == "CURRENT").cast("long")).alias("__CURRENT_ROWS"),
        F.sum((F.col("ROW_SOURCE") == "HIST").cast("long")).alias("__HIST_ROWS"),
        F.sum(F.col("VERSION_DT_TM").isNull().cast("long")).alias("__VERSION_DT_NULLS"),
        F.sum(
            (
                (F.col("ROW_SOURCE") == "HIST")
                & (F.col("SOURCE_VERSION_ID") == -1)
            ).cast("long")
        ).alias("__HIST_SENTINEL_COLLISIONS"),
    ]
    for base, code_column in DECODE_FIELDS:
        aggregates.append(
            F.sum(
                F.when(
                    F.col(code_column).isNotNull() & F.col(f"{base}_DESC").isNull(),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias(base)
        )
    row = staged.agg(*aggregates).collect()[0]
    version_nulls = int(row["__VERSION_DT_NULLS"] or 0)
    sentinel_collisions = int(row["__HIST_SENTINEL_COLLISIONS"] or 0)
    if version_nulls or sentinel_collisions:
        fail(
            "Materialized stage version gate failed: "
            f"VERSION_DT_TM_nulls={version_nulls}, HIST_sentinel_collisions={sentinel_collisions}"
        )
    decode_quality = {
        base: {"missing_decode_rows": int(row[base] or 0), "hard_failure": False}
        for base, _ in DECODE_FIELDS
    }
    return {
        **union_gate,
        "status": "PASS",
        "output_rows": int(row["__ROWS"]),
        "row_source_counts": {
            "CURRENT": int(row["__CURRENT_ROWS"] or 0),
            "HIST": int(row["__HIST_ROWS"] or 0),
        },
        "version_datetime_null_rows": version_nulls,
        "hist_sentinel_collision_rows": sentinel_collisions,
        "decode_fanout_policy": "ONE_ROW_MAP_CROSS_JOIN_CANNOT_MULTIPLY_SOURCE_ROWS",
        "decode_quality": decode_quality,
        "removal_future_performed_gate": removal_future_performed_gate(staged),
        "planned_future_year_2100_gate": planned_future_year_2100_gate(staged),
    }


def materialize_merge_plan(staged: DataFrame, full_compare: bool) -> dict:
    if bronze_table_exists(TARGET):
        target_frame = spark.table(TARGET)
        delete_keys = (
            target_frame.where(F.col("SOURCE_PRESENT_IND")).select(*TARGET_KEYS)
            .join(staged.select(*TARGET_KEYS), TARGET_KEYS, "left_anti")
            if full_compare else staged.select(*TARGET_KEYS).limit(0)
        )
        target_alias = target_frame.select(
            *TARGET_KEYS, "ROW_HASH", "SOURCE_PRESENT_IND"
        ).alias("t")
        staged_alias = staged.select(*TARGET_KEYS, "ROW_HASH").alias("s")
        matched_condition = reduce(
            lambda left, right: left & right,
            [F.col(f"t.{key}").eqNullSafe(F.col(f"s.{key}")) for key in TARGET_KEYS],
        )
        primary_update_keys = (
            target_alias.join(staged_alias, matched_condition, "inner")
            .where(
                (~F.col("t.ROW_HASH").eqNullSafe(F.col("s.ROW_HASH")))
                | (~F.col("t.SOURCE_PRESENT_IND"))
            )
            .select(*[F.col(f"s.{key}").alias(key) for key in TARGET_KEYS])
        )
        resurrection_keys = (
            target_frame.where(~F.col("SOURCE_PRESENT_IND")).select(*TARGET_KEYS)
            .join(staged.select(*TARGET_KEYS), TARGET_KEYS, "inner")
        )
    else:
        delete_keys = staged.select(*TARGET_KEYS).limit(0)
        primary_update_keys = staged.select(*TARGET_KEYS).limit(0)
        resurrection_keys = staged.select(*TARGET_KEYS).limit(0)
    delete_snapshot = write_attempt_stage(
        delete_keys.dropDuplicates(TARGET_KEYS), DELETE_KEYS_STAGE, "delete keys"
    )
    primary_update_snapshot = write_attempt_stage(
        primary_update_keys.dropDuplicates(TARGET_KEYS),
        PRIMARY_UPDATE_KEYS_STAGE,
        "primary matched-update keys",
    )
    resurrection_snapshot = write_attempt_stage(
        resurrection_keys.dropDuplicates(TARGET_KEYS), RESURRECTION_KEYS_STAGE, "resurrection keys"
    )
    assert_unique_non_null(delete_snapshot, TARGET_KEYS, "delete-key stage")
    assert_unique_non_null(primary_update_snapshot, TARGET_KEYS, "primary matched-update-key stage")
    assert_unique_non_null(resurrection_snapshot, TARGET_KEYS, "resurrection-key stage")
    return {
        "delete_table": DELETE_KEYS_STAGE,
        "primary_update_table": PRIMARY_UPDATE_KEYS_STAGE,
        "resurrection_table": RESURRECTION_KEYS_STAGE,
        "expected_soft_deletes": int(delete_snapshot.count()),
        "expected_primary_matched_updates": int(primary_update_snapshot.count()),
        "resurrection_candidates": int(resurrection_snapshot.count()),
    }


def prepare_incremental_control(
    current_slice: DataFrame,
    hist_slice: DataFrame,
    staged: DataFrame,
    mode: str,
) -> dict:
    empty_ids = staged.select("PM_WAIT_LIST_ID").limit(0)
    if mode == "INCREMENTAL":
        current_ids = current_slice.select(
            F.expr("try_cast(PM_WAIT_LIST_ID AS BIGINT)").alias("PM_WAIT_LIST_ID")
        )
        hist_ids = hist_slice.select(
            F.expr("try_cast(PM_WAIT_LIST_ID AS BIGINT)").alias("PM_WAIT_LIST_ID")
        )
        affected_ids = current_ids.unionByName(hist_ids).distinct()
    else:
        affected_ids = staged.select("PM_WAIT_LIST_ID").distinct()
    affected_snapshot = write_attempt_stage(affected_ids, AFFECTED_IDS_STAGE, "affected ids")
    assert_unique_non_null(affected_snapshot, ["PM_WAIT_LIST_ID"], "affected-id stage")

    if mode == "INCREMENTAL":
        live_current_ids = (
            PINNED_SOURCE_FRAMES[SRC_CURRENT]
            .select(F.expr("try_cast(PM_WAIT_LIST_ID AS BIGINT)").alias("PM_WAIT_LIST_ID"))
            .join(affected_snapshot, "PM_WAIT_LIST_ID", "left_semi")
            .distinct()
        )
        absent_ids = affected_snapshot.join(live_current_ids, "PM_WAIT_LIST_ID", "left_anti")
    else:
        live_current_ids = empty_ids
        absent_ids = empty_ids

    live_snapshot = write_attempt_stage(
        live_current_ids, LIVE_CURRENT_IDS_STAGE, "affected live current ids"
    )
    absent_snapshot = write_attempt_stage(
        absent_ids, ABSENT_IDS_STAGE, "affected absent current ids"
    )
    assert_unique_non_null(live_snapshot, ["PM_WAIT_LIST_ID"], "live current ID stage")
    assert_unique_non_null(absent_snapshot, ["PM_WAIT_LIST_ID"], "absent current ID stage")
    if mode == "INCREMENTAL":
        if live_snapshot.unionByName(absent_snapshot).count() != affected_snapshot.count():
            fail("Affected current-ID classification did not cover the exact affected-ID set")
        if live_snapshot.join(absent_snapshot, "PM_WAIT_LIST_ID", "inner").limit(1).count():
            fail("Affected current-ID live/absent classification overlaps")

    if mode == "INCREMENTAL" and bronze_table_exists(TARGET):
        absence_expected = (
            spark.table(TARGET)
            .where((F.col("ROW_SOURCE") == "CURRENT") & (F.col("SOURCE_VERSION_ID") == -1))
            .join(absent_snapshot, "PM_WAIT_LIST_ID", "inner")
            .select(
                "PM_WAIT_LIST_ID", "ROW_HASH",
                F.col("SOURCE_ABSENT_DETECTED_TS").alias("PRIOR_ABSENT_TS"),
            )
        )
    else:
        absence_expected = empty_ids.select(
            "PM_WAIT_LIST_ID",
            F.lit(None).cast("bigint").alias("ROW_HASH"),
            F.lit(None).cast("timestamp").alias("PRIOR_ABSENT_TS"),
        )
    absence_snapshot = write_attempt_stage(
        absence_expected, ABSENCE_EXPECTED_STAGE, "incremental absence expected"
    )
    assert_unique_non_null(absence_snapshot, ["PM_WAIT_LIST_ID"], "absence expected stage")
    return {
        "affected_ids": AFFECTED_IDS_STAGE,
        "live_current_ids": LIVE_CURRENT_IDS_STAGE,
        "absent_ids": ABSENT_IDS_STAGE,
        "absence_expected": ABSENCE_EXPECTED_STAGE,
        "affected_id_count": int(affected_snapshot.count()),
        "live_current_id_count": int(live_snapshot.count()),
        "absent_current_id_count": int(absent_snapshot.count()),
        "incremental_absent_target_rows": int(absence_snapshot.count()),
    }


def merge_target(staged: DataFrame, mode: str, merge_plan: dict) -> dict:
    heartbeat_run_lock("main target merge")
    existed = bronze_table_exists(TARGET)
    before_history = latest_delta_history(TARGET) if existed else None
    if not existed:
        (
            staged.write.format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .option("delta.enableRowTracking", "true")
            .option("delta.enableDeletionVectors", "true")
            .saveAsTable(TARGET)
        )
        operation = "CREATE"
        matched_policy = "CREATE_INSERT_ALL"
    else:
        condition = " AND ".join(f"t.{qident(key)} <=> s.{qident(key)}" for key in TARGET_KEYS)
        values = {column: f"s.{qident(column)}" for column in staged.columns}
        builder = DeltaTable.forName(spark, TARGET).alias("t").merge(staged.alias("s"), condition)
        if BOOTSTRAP_MODE:
            builder = builder.whenMatchedUpdate(set=values)
            matched_policy = "BOOTSTRAP_FORCE_RESTAMP_ALL_MATCHED"
        else:
            builder = builder.whenMatchedUpdate(
                condition="NOT (t.ROW_HASH <=> s.ROW_HASH) OR t.SOURCE_PRESENT_IND = false",
                set=values,
            )
            matched_policy = "ROW_HASH_OR_RESURRECTION"
        builder = builder.whenNotMatchedInsert(values=values)
        if mode in FULL_MODES:
            builder = builder.whenNotMatchedBySourceUpdate(
                condition="t.SOURCE_PRESENT_IND = true",
                set={
                    "SOURCE_PRESENT_IND": "false",
                    "IS_CURRENT": "false",
                    "SOURCE_ABSENT_DETECTED_TS": f"coalesce(t.SOURCE_ABSENT_DETECTED_TS, TIMESTAMP '{WRITE_TS}')",
                    "PIPELINE_RUN_ID": f"'{sql_escape(RUN_ID)}'",
                    "PIPELINE_ATTEMPT_ID": f"'{sql_escape(ATTEMPT_ID)}'",
                    "PIPELINE_PROCESSED_TS": f"TIMESTAMP '{WRITE_TS}'",
                    "ADC_UPDT": f"TIMESTAMP '{WRITE_TS}'",
                },
            )
        builder.execute()
        operation = "MERGE"
    after_history = latest_delta_history(TARGET)
    operation_metrics = after_history["operation_metrics"]
    actual_updated = operation_metric_int(operation_metrics, "numTargetRowsUpdated")
    actual_inserted = operation_metric_int(operation_metrics, "numTargetRowsInserted")
    expected_primary_updates = int(merge_plan["expected_primary_matched_updates"])
    expected_soft_deletes = int(merge_plan["expected_soft_deletes"])
    primary_updates_from_metrics = max(actual_updated - expected_soft_deletes, 0)
    if existed and actual_updated != expected_primary_updates + expected_soft_deletes:
        fail(
            f"{TARGET}: primary MERGE update metrics mismatch; actual_updated={actual_updated}, "
            f"expected_primary={expected_primary_updates}, expected_soft_deletes={expected_soft_deletes}"
        )
    unchanged_same_source_full = bool(
        existed
        and mode in FULL_MODES
        and expected_primary_updates == 0
        and all(
            last_checkpoint(source) is not None
            and int(last_checkpoint(source)["source_version"]) == int(SOURCE_VERSIONS[source])
            for source in DATA_SOURCES
        )
    )
    if unchanged_same_source_full and primary_updates_from_metrics != 0:
        fail(
            f"{TARGET}: unchanged same-source FULL unexpectedly updated "
            f"{primary_updates_from_metrics} matched rows"
        )
    return {
        "operation": operation,
        "matched_update_policy": matched_policy,
        "staged_rows": int(staged.count()),
        "expected_primary_matched_updates": expected_primary_updates,
        "expected_soft_deletes": expected_soft_deletes,
        "resurrection_candidates": int(merge_plan["resurrection_candidates"]),
        "primary_merge": {
            "target_version_before": before_history["version"] if before_history else None,
            "target_version_after": after_history["version"],
            "operation": after_history["operation"],
            "operation_metrics": operation_metrics,
            "num_target_rows_updated": actual_updated,
            "num_target_rows_inserted": actual_inserted,
            "matched_update_rows": primary_updates_from_metrics,
            "soft_delete_rows": expected_soft_deletes,
            "unchanged_same_source_full": unchanged_same_source_full,
        },
    }


def apply_incremental_absence(mode: str) -> dict:
    expected = spark.table(ABSENCE_EXPECTED_STAGE)
    if mode != "INCREMENTAL" or expected.limit(1).count() == 0:
        return {"status": "NOT_APPLICABLE", "updated_rows": 0}
    updates = expected.select("PM_WAIT_LIST_ID")
    heartbeat_run_lock("incremental current absence mutation")
    (
        DeltaTable.forName(spark, TARGET)
        .alias("t")
        .merge(
            updates.alias("a"),
            "t.PM_WAIT_LIST_ID = a.PM_WAIT_LIST_ID "
            "AND t.ROW_SOURCE = 'CURRENT' AND t.SOURCE_VERSION_ID = -1",
        )
        .whenMatchedUpdate(set={
            "SOURCE_PRESENT_IND": "false",
            "IS_CURRENT": "false",
            "PIPELINE_RUN_ID": f"'{sql_escape(RUN_ID)}'",
            "PIPELINE_ATTEMPT_ID": f"'{sql_escape(ATTEMPT_ID)}'",
            "PIPELINE_PROCESSED_TS": f"TIMESTAMP '{WRITE_TS}'",
            "ADC_UPDT": f"TIMESTAMP '{WRITE_TS}'",
            "SOURCE_ABSENT_DETECTED_TS": f"coalesce(t.SOURCE_ABSENT_DETECTED_TS, TIMESTAMP '{WRITE_TS}')",
        })
        .execute()
    )
    return {"status": "APPLIED", "updated_rows": int(expected.count())}


def recompute_is_current(staged: DataFrame) -> dict:
    ids = staged.select("PM_WAIT_LIST_ID")
    ids = ids.unionByName(spark.table(AFFECTED_IDS_STAGE))
    ids = ids.unionByName(spark.table(DELETE_KEYS_STAGE).select("PM_WAIT_LIST_ID"))
    ids = ids.unionByName(spark.table(ABSENCE_EXPECTED_STAGE).select("PM_WAIT_LIST_ID"))
    recompute_snapshot = write_attempt_stage(
        ids.distinct(), RECOMPUTE_IDS_STAGE, "IS_CURRENT recompute ids"
    )
    assert_unique_non_null(recompute_snapshot, ["PM_WAIT_LIST_ID"], "IS_CURRENT recompute ids")
    recomputed_id_count = int(recompute_snapshot.count())
    expected_formula = (
        (F.col("ROW_SOURCE") == "CURRENT")
        & (F.col("SOURCE_VERSION_ID") == -1)
        & F.col("SOURCE_PRESENT_IND")
    )
    mismatch_rows = (
        spark.table(TARGET)
        .join(recompute_snapshot, "PM_WAIT_LIST_ID", "inner")
        .where(~F.col("IS_CURRENT").eqNullSafe(expected_formula))
        .count()
    )
    before_history = latest_delta_history(TARGET)
    operation_metrics = {}
    if mismatch_rows:
        heartbeat_run_lock("IS_CURRENT mismatch-only safeguard")
        (
            DeltaTable.forName(spark, TARGET)
            .alias("t")
            .merge(recompute_snapshot.alias("r"), "t.PM_WAIT_LIST_ID = r.PM_WAIT_LIST_ID")
            .whenMatchedUpdate(
                condition=(
                    "NOT (t.IS_CURRENT <=> (t.ROW_SOURCE = 'CURRENT' "
                    "AND t.SOURCE_VERSION_ID = -1 AND t.SOURCE_PRESENT_IND = true))"
                ),
                set={
                    "IS_CURRENT": (
                        "t.ROW_SOURCE = 'CURRENT' AND t.SOURCE_VERSION_ID = -1 "
                        "AND t.SOURCE_PRESENT_IND = true"
                    )
                },
            )
            .execute()
        )
        after_history = latest_delta_history(TARGET)
        operation_metrics = after_history["operation_metrics"]
        matched_updates = operation_metric_int(operation_metrics, "numTargetRowsUpdated")
        if matched_updates != mismatch_rows:
            fail(
                f"{TARGET}: IS_CURRENT safeguard updated {matched_updates} rows, expected {mismatch_rows}"
            )
    else:
        after_history = before_history
        matched_updates = 0
    version_advanced = after_history["version"] != before_history["version"]
    if mismatch_rows == 0 and version_advanced:
        fail(f"{TARGET}: zero-mismatch IS_CURRENT safeguard advanced the Delta version")
    return {
        "status": "PASS",
        "scope": "AFFECTED_PM_WAIT_LIST_IDS_ONLY",
        "recomputed_id_count": recomputed_id_count,
        "formula_mismatch_rows_before": int(mismatch_rows),
        "matched_update_rows": int(matched_updates),
        "target_version_before": before_history["version"],
        "target_version_after": after_history["version"],
        "target_version_advanced": version_advanced,
        "operation": after_history["operation"] if version_advanced else "NO_OPERATION",
        "operation_metrics": operation_metrics,
        "cdf_files_written": operation_metric_int(operation_metrics, "numTargetChangeFilesAdded"),
        "cdf_rows_written": 0 if matched_updates == 0 else None,
    }


def validate_current_formula(frame: DataFrame, label: str) -> dict:
    expected = (
        (F.col("ROW_SOURCE") == "CURRENT")
        & (F.col("SOURCE_VERSION_ID") == -1)
        & F.col("SOURCE_PRESENT_IND")
    )
    mismatch = frame.where(~F.col("IS_CURRENT").eqNullSafe(expected)).limit(1).count()
    if mismatch:
        fail(f"{label}: IS_CURRENT physical formula mismatch")
    invalid_current = frame.where(
        F.col("IS_CURRENT")
        & (
            (F.col("ROW_SOURCE") != "CURRENT")
            | (F.col("SOURCE_VERSION_ID") != -1)
            | (~F.col("SOURCE_PRESENT_IND"))
        )
    ).limit(1).count()
    if invalid_current:
        fail(f"{label}: HIST/absent row marked IS_CURRENT")
    duplicate_current = (
        frame.where(F.col("IS_CURRENT"))
        .groupBy("PM_WAIT_LIST_ID").count().where(F.col("count") > 1).limit(1).count()
    )
    if duplicate_current:
        fail(f"{label}: more than one current row for a waiting-list ID")
    return {"status": "PASS", "formula_mismatches": 0, "duplicate_current_ids": 0}


def validate_after_merge(
    staged: DataFrame,
    mode: str,
    metrics: dict,
    merge_plan: dict,
    raw_counts: dict,
) -> dict:
    heartbeat_run_lock("post-merge validation")
    frame = spark.table(TARGET)
    VALIDATION_RESULTS["schema"]["target"] = validate_schema_contract(frame, TARGET, TARGET)
    VALIDATION_RESULTS["pii"]["target"] = assert_no_banned_columns(frame, TARGET)
    if mode in FULL_MODES:
        validation_frame = frame
        validation_scope = "FULL_TARGET"
    else:
        validation_frame = frame.join(
            spark.table(RECOMPUTE_IDS_STAGE), "PM_WAIT_LIST_ID", "inner"
        )
        validation_scope = "AFFECTED_PM_WAIT_LIST_IDS_ONLY"
    assert_unique_non_null(validation_frame, TARGET_KEYS, f"{TARGET} {validation_scope}")
    current_formula = validate_current_formula(validation_frame, f"{TARGET} {validation_scope}")

    present = validation_frame.where(F.col("SOURCE_PRESENT_IND"))
    condition = reduce(
        lambda left, right: left & right,
        [F.col(f"t.{key}") == F.col(f"s.{key}") for key in TARGET_KEYS],
    )
    hash_matches = (
        present.alias("t").join(staged.alias("s"), condition, "inner")
        .where(F.col("t.ROW_HASH").eqNullSafe(F.col("s.ROW_HASH"))).count()
    )
    staged_rows = staged.count()
    if hash_matches != staged_rows:
        fail(f"{TARGET}: staged row-hash parity {hash_matches} != {staged_rows}")

    expected_delete = spark.table(merge_plan["delete_table"])
    deleted_readback = expected_delete.join(
        validation_frame.where(~F.col("SOURCE_PRESENT_IND")).select(*TARGET_KEYS),
        TARGET_KEYS, "inner",
    ).count()
    still_present = expected_delete.join(
        validation_frame.where(F.col("SOURCE_PRESENT_IND")).select(*TARGET_KEYS),
        TARGET_KEYS, "inner",
    ).count()
    if still_present or deleted_readback != int(metrics["expected_soft_deletes"]):
        fail(
            f"{TARGET}: exact soft-delete validation failed; still_present={still_present}, "
            f"deleted={deleted_readback}, expected={metrics['expected_soft_deletes']}"
        )

    expected_resurrection = spark.table(merge_plan["resurrection_table"])
    resurrected = expected_resurrection.join(
        validation_frame.where(F.col("SOURCE_PRESENT_IND")).select(*TARGET_KEYS),
        TARGET_KEYS, "inner",
    ).count()
    if resurrected != int(metrics["resurrection_candidates"]):
        fail(f"{TARGET}: resurrection validation {resurrected} != {metrics['resurrection_candidates']}")

    absence_expected = spark.table(ABSENCE_EXPECTED_STAGE)
    absence_readback = (
        absence_expected.alias("e")
        .join(
            validation_frame.where(
                (F.col("ROW_SOURCE") == "CURRENT") & (F.col("SOURCE_VERSION_ID") == -1)
            ).alias("t"),
            "PM_WAIT_LIST_ID", "inner",
        )
        .where(
            (~F.col("t.SOURCE_PRESENT_IND"))
            & (~F.col("t.IS_CURRENT"))
            & (F.col("t.PIPELINE_RUN_ID") == RUN_ID)
            & (F.col("t.PIPELINE_ATTEMPT_ID") == ATTEMPT_ID)
            & F.col("t.ADC_UPDT").eqNullSafe(F.lit(WRITE_TS).cast("timestamp"))
            & F.col("t.ROW_HASH").eqNullSafe(F.col("e.ROW_HASH"))
            & F.col("t.SOURCE_ABSENT_DETECTED_TS").eqNullSafe(
                F.coalesce(F.col("e.PRIOR_ABSENT_TS"), F.lit(WRITE_TS).cast("timestamp"))
            )
        ).count()
    )
    expected_absence = absence_expected.count()
    if absence_readback != expected_absence:
        fail(f"{TARGET}: incremental absence validation {absence_readback} != {expected_absence}")

    incremental_affected = None
    if mode == "INCREMENTAL":
        live_ids = spark.table(LIVE_CURRENT_IDS_STAGE)
        absent_ids = spark.table(ABSENT_IDS_STAGE)
        target_current = validation_frame.where(
            (F.col("ROW_SOURCE") == "CURRENT") & (F.col("SOURCE_VERSION_ID") == -1)
        )
        live_good = (
            live_ids.join(target_current, "PM_WAIT_LIST_ID", "inner")
            .where(F.col("SOURCE_PRESENT_IND") & F.col("IS_CURRENT")).count()
        )
        live_expected = live_ids.count()
        absent_bad = (
            absent_ids.join(target_current, "PM_WAIT_LIST_ID", "inner")
            .where(F.col("SOURCE_PRESENT_IND") | F.col("IS_CURRENT")).count()
        )
        if live_good != live_expected or absent_bad:
            fail(
                f"{TARGET}: affected-ID current correctness failed; "
                f"live_good={live_good}/{live_expected}, absent_bad={absent_bad}"
            )
        incremental_affected = {
            "validation_scope": validation_scope,
            "live_current_rows_verified": int(live_good),
            "absent_current_bad_rows": int(absent_bad),
        }

    bootstrap_ownership = {"enforced": bool(BOOTSTRAP_MODE), "foreign_present_rows": 0}
    if BOOTSTRAP_MODE:
        foreign = present.where(
            (~F.col("PIPELINE_RUN_ID").eqNullSafe(F.lit(RUN_ID)))
            | (~F.col("PIPELINE_ATTEMPT_ID").eqNullSafe(F.lit(ATTEMPT_ID)))
        ).count()
        bootstrap_ownership["foreign_present_rows"] = int(foreign)
        if foreign:
            fail(f"{TARGET}: bootstrap ownership failed for {foreign} present rows")

    full_parity = None
    if mode in FULL_MODES:
        expected_present = int(raw_counts["current_rows"]) + int(raw_counts["hist_rows"])
        present_rows = present.count()
        if present_rows != expected_present:
            fail(f"{TARGET}: FULL present parity {present_rows} != {expected_present}")
        source_keys = staged.select(*TARGET_KEYS)
        target_keys = present.select(*TARGET_KEYS)
        source_missing = source_keys.join(target_keys, TARGET_KEYS, "left_anti").limit(1).count()
        target_extra = target_keys.join(source_keys, TARGET_KEYS, "left_anti").limit(1).count()
        if source_missing or target_extra:
            fail(f"{TARGET}: FULL raw physical key mismatch")
        live_current_ids = (
            PINNED_SOURCE_FRAMES[SRC_CURRENT]
            .select(F.expr("try_cast(PM_WAIT_LIST_ID AS BIGINT)").alias("PM_WAIT_LIST_ID"))
            .distinct()
        )
        missing_current = live_current_ids.join(
            frame.where(F.col("IS_CURRENT")).select("PM_WAIT_LIST_ID"),
            "PM_WAIT_LIST_ID", "left_anti",
        ).limit(1).count()
        extra_current = (
            frame.where(F.col("IS_CURRENT")).select("PM_WAIT_LIST_ID")
            .join(live_current_ids, "PM_WAIT_LIST_ID", "left_anti").limit(1).count()
        )
        if missing_current or extra_current:
            fail(f"{TARGET}: source-authoritative current ID parity failed")
        full_parity = {
            "validation_scope": validation_scope,
            "present_rows": int(present_rows),
            "expected_present_rows": int(expected_present),
            "raw_physical_keys_match": True,
            "live_current_ids_match": True,
        }

    return {
        "status": "PASS",
        "validation_scope": validation_scope,
        "staged_rows": int(staged_rows),
        "row_hash_matches": int(hash_matches),
        "soft_deletes_verified": int(deleted_readback),
        "resurrections_verified": int(resurrected),
        "absence_rows_verified": int(absence_readback),
        "incremental_affected": incremental_affected,
        "current_formula": current_formula,
        "bootstrap_ownership": bootstrap_ownership,
        "full_parity": full_parity,
    }


CLASSIFICATION_CONTROL = "4_prod.tmp.trust_classification_control_v3"
CLASSIFICATION_TABLE_LOG = "6_mgmt.logs.trust_classification_table_log_v2"
ACCOUNTING_CATEGORIES = (
    "raw", "staging_residue", "excluded_archive", "abandoned_archive",
)


def normalized_table_match(column, basename: str):
    return F.lower(F.regexp_extract(column, r"([^.]+)$", 1)) == basename.lower()


def accounting_category_frame(
    frame: DataFrame,
    key: str,
    category: str,
) -> DataFrame:
    return frame.select(
        F.expr(f"try_cast({qident(key)} AS BIGINT)").alias("__KEY"),
        F.lit(category).alias("__CATEGORY"),
        (
            F.col(key).isNull() | bigint_invalid_condition(frame, key)
        ).cast("long").alias("__INVALID_KEY"),
    )


def optional_archive_category(
    table: str,
    key: str,
    category: str,
) -> tuple[DataFrame, dict]:
    try:
        version = source_version(table)
    except AnalysisException as exc:
        if exc.getErrorClass() == "TABLE_OR_VIEW_NOT_FOUND":
            return spark.createDataFrame(
                [], "__KEY bigint, __CATEGORY string, __INVALID_KEY long"
            ), {
                "table": table,
                "exists": False,
                "pinned_delta_version": None,
                "missing_error_class": exc.getErrorClass(),
            }
        raise
    frame = (
        spark.read.format("delta")
        .option("versionAsOf", int(version))
        .table(table)
    )
    return accounting_category_frame(frame, key, category), {
        "table": table,
        "exists": True,
        "pinned_delta_version": int(version),
        "missing_error_class": None,
    }


def full_drain_accounting() -> dict:
    heartbeat_run_lock("authoritative raw-drain accounting")
    results = {}
    control = spark.table(CLASSIFICATION_CONTROL)
    table_log = spark.table(CLASSIFICATION_TABLE_LOG)
    source_specs = (
        (SRC_CURRENT, "PM_WAIT_LIST_ID"),
        (SRC_HIST, "PM_WAIT_LIST_HIST_ID"),
    )
    for source, key in source_specs:
        basename = source.split(".")[-1]
        control_rows = control.where(
            normalized_table_match(F.col("table_name"), basename)
        ).collect()
        if len(control_rows) != 1:
            fail(
                f"{basename}: expected exactly one authoritative classification control row, "
                f"found {len(control_rows)}; fail closed"
            )
        control_row = control_rows[0].asDict(recursive=True)
        ledger_run_id = control_row.get("run_id")
        staging_version = control_row.get("staging_version")
        staged_at_watermark = control_row.get("staged_at_watermark")
        control_updated_at = control_row.get("updated_at")
        if (
            not ledger_run_id
            or staging_version is None
            or staged_at_watermark is None
            or control_updated_at is None
        ):
            fail(
                f"{basename}: authoritative control row lacks run_id, staging_version, "
                "staged_at_watermark, or updated_at"
            )

        successful_logs = (
            table_log.where(F.col("run_id") == ledger_run_id)
            .where(normalized_table_match(F.col("table_name"), basename))
            .where(F.upper(F.trim(F.col("status"))) == "SUCCESS")
            .where(F.col("logged_at").isNotNull())
            .where(F.col("logged_at") <= F.lit(control_updated_at).cast("timestamp"))
            .collect()
        )
        if len(successful_logs) != 1:
            fail(
                f"{basename}: ledger run_id={ledger_run_id} requires exactly one matching "
                "SUCCESS table-log row with logged_at <= control.updated_at; "
                f"found {len(successful_logs)}; fail closed"
            )
        log_row = successful_logs[0].asDict(recursive=True)
        term_names = (
            "staged_rows", "promoted_keys", "excluded_keys", "abandoned_keys", "remaining_rows",
        )
        if any(log_row.get(name) is None for name in term_names):
            fail(f"{basename}: authoritative SUCCESS log has NULL accounting terms")
        terms = {name: int(log_row[name]) for name in term_names}
        if any(value < 0 for value in terms.values()):
            fail(f"{basename}: authoritative SUCCESS log has negative accounting terms {terms}")
        log_sum = (
            terms["promoted_keys"]
            + terms["excluded_keys"]
            + terms["abandoned_keys"]
            + terms["remaining_rows"]
        )
        if terms["staged_rows"] != log_sum:
            fail(
                f"{basename}: authoritative log identity failed: staged_rows="
                f"{terms['staged_rows']} != promoted+excluded+abandoned+remaining={log_sum}"
            )

        raw_metrics = PINNED_SOURCE_METRICS[source]
        raw_watermark = raw_metrics["watermark"]
        freshness_signed_days = (
            (raw_watermark - staged_at_watermark).total_seconds() / 86400.0
        )
        freshness_absolute_days = abs(freshness_signed_days)
        accounting_mode = (
            "CUMULATIVE_BOOTSTRAP"
            if int(raw_metrics["rows"]) == terms["promoted_keys"]
            else "INCREMENTAL_BATCH"
        )
        staging_table = f"4_prod.staging.{basename}"
        excluded_category, excluded_meta = optional_archive_category(
            f"4_prod.staging_excluded.{basename}",
            key,
            "excluded_archive",
        )
        abandoned_category, abandoned_meta = optional_archive_category(
            f"4_prod.staging_abandoned.{basename}",
            key,
            "abandoned_archive",
        )
        merge_proof = None

        if accounting_mode == "CUMULATIVE_BOOTSTRAP":
            raw_category = accounting_category_frame(
                PINNED_SOURCE_FRAMES[source], key, "raw"
            )
            staging_frame = (
                spark.read.format("delta")
                .option("versionAsOf", int(staging_version))
                .table(staging_table)
            )
            staging_category = accounting_category_frame(
                staging_frame, key, "staging_residue"
            )
            category_union = (
                raw_category
                .unionByName(staging_category)
                .unionByName(excluded_category)
                .unionByName(abandoned_category)
            )
            per_key = category_union.groupBy("__KEY").agg(
                *[
                    F.sum(
                        F.when(F.col("__CATEGORY") == category, 1).otherwise(0)
                    ).alias(f"__ROWS__{category}")
                    for category in ACCOUNTING_CATEGORIES
                ],
                *[
                    F.sum(
                        F.when(
                            (F.col("__CATEGORY") == category)
                            & (F.col("__INVALID_KEY") != 0),
                            1,
                        ).otherwise(0)
                    ).alias(f"__INVALID__{category}")
                    for category in ACCOUNTING_CATEGORIES
                ],
            )
            positive_category_count = reduce(
                lambda left, right: left + right,
                [
                    F.when(F.col(f"__ROWS__{category}") > 0, 1).otherwise(0)
                    for category in ACCOUNTING_CATEGORIES
                ],
            )
            summary = per_key.agg(
                *[
                    F.sum(F.col(f"__ROWS__{category}")).alias(
                        f"__COUNT__{category}"
                    )
                    for category in ACCOUNTING_CATEGORIES
                ],
                *[
                    F.sum(F.col(f"__INVALID__{category}")).alias(
                        f"__BAD__{category}"
                    )
                    for category in ACCOUNTING_CATEGORIES
                ],
                *[
                    F.sum(
                        F.when(F.col(f"__ROWS__{category}") > 1, 1).otherwise(0)
                    ).alias(f"__DUP_GROUPS__{category}")
                    for category in ACCOUNTING_CATEGORIES
                ],
                *[
                    F.sum(
                        F.when(
                            F.col("__KEY").isNull(),
                            F.col(f"__ROWS__{category}"),
                        ).otherwise(0)
                    ).alias(f"__NULL_ROWS__{category}")
                    for category in ACCOUNTING_CATEGORIES
                ],
                F.sum(
                    F.when(positive_category_count > 1, 1).otherwise(0)
                ).alias("__CROSS_CATEGORY_DUPLICATE_KEYS"),
                F.sum(
                    F.when(F.col("__KEY").isNotNull(), 1).otherwise(0)
                ).alias("__UNION_DISTINCT_KEYS"),
            ).collect()[0]
            actual_counts = {
                category: int(summary[f"__COUNT__{category}"] or 0)
                for category in ACCOUNTING_CATEGORIES
            }
            invalid_key_rows = {
                category: int(summary[f"__BAD__{category}"] or 0)
                for category in ACCOUNTING_CATEGORIES
            }
            within_category_duplicate_groups = {
                category: int(summary[f"__DUP_GROUPS__{category}"] or 0)
                for category in ACCOUNTING_CATEGORIES
            }
            null_key_rows = {
                category: int(summary[f"__NULL_ROWS__{category}"] or 0)
                for category in ACCOUNTING_CATEGORIES
            }
            cross_category_duplicate_keys = int(
                summary["__CROSS_CATEGORY_DUPLICATE_KEYS"] or 0
            )
            union_total = int(summary["__UNION_DISTINCT_KEYS"] or 0)
            archive_presence = {
                "policy": "CUMULATIVE_GROUPED_COUNTS",
                "excluded_archive_rows": actual_counts["excluded_archive"],
                "abandoned_archive_rows": actual_counts["abandoned_archive"],
            }
        else:
            excluded_present = int(excluded_category.limit(1).count())
            abandoned_present = int(abandoned_category.limit(1).count())
            archive_presence = {
                "policy": "BOUNDED_LIMIT_ONE_FAIL_CLOSED",
                "excluded_archive_present": bool(excluded_present),
                "abandoned_archive_present": bool(abandoned_present),
            }
            if (
                terms["excluded_keys"] != 0
                or terms["abandoned_keys"] != 0
                or excluded_present
                or abandoned_present
            ):
                fail(
                    f"{basename}: incremental-ledger archives cannot be batch-scoped; "
                    f"terms={terms}, archive_presence={archive_presence}; fail closed"
                )
            actual_counts = {
                "status": "NOT_SCANNED_INCREMENTAL_BATCH",
                "pinned_raw_rows": int(raw_metrics["rows"]),
                "staging_cumulative_rows": "NOT_SCANNED",
                "excluded_archive_rows": "BOUNDED_EMPTY",
                "abandoned_archive_rows": "BOUNDED_EMPTY",
            }

        if accounting_mode == "CUMULATIVE_BOOTSTRAP":
            expected_actual = {
                "raw": terms["promoted_keys"],
                "staging_residue": terms["remaining_rows"],
                "excluded_archive": terms["excluded_keys"],
                "abandoned_archive": terms["abandoned_keys"],
            }
            mismatches = {
                category: {
                    "actual": actual_counts[category],
                    "authoritative": expected_actual[category],
                }
                for category in ACCOUNTING_CATEGORIES
                if actual_counts[category] != expected_actual[category]
            }
            if (
                mismatches
                or any(invalid_key_rows.values())
                or any(null_key_rows.values())
                or any(within_category_duplicate_groups.values())
                or cross_category_duplicate_keys
            ):
                fail(
                    f"{basename}: cumulative/bootstrap pinned category accounting/key "
                    f"ownership failed closed; count_mismatches={mismatches}, "
                    f"invalid_key_rows={invalid_key_rows}, null_key_rows={null_key_rows}, "
                    f"within_category_duplicate_groups={within_category_duplicate_groups}, "
                    f"cross_category_duplicate_keys={cross_category_duplicate_keys}"
                )
            reconciled_actual_counts = actual_counts
            reconciled_invalid_key_rows = invalid_key_rows
            reconciled_null_key_rows = null_key_rows
            reconciled_duplicate_groups = within_category_duplicate_groups
            reconciled_cross_category_duplicate_keys = cross_category_duplicate_keys
            reconciled_union_total = union_total
            actual_sum = sum(reconciled_actual_counts.values())
            if (
                reconciled_union_total != actual_sum
                or reconciled_union_total != terms["staged_rows"]
            ):
                fail(
                    f"{basename}: cumulative/bootstrap physical key union total "
                    f"{reconciled_union_total} does not equal actual category sum "
                    f"{actual_sum} and authoritative staged_rows {terms['staged_rows']}; "
                    "fail closed"
                )
            if freshness_absolute_days > RAW_FRESHNESS_TOLERANCE_DAYS:
                fail(
                    f"{basename}: cumulative/bootstrap pinned raw/control watermark "
                    f"difference {freshness_absolute_days:.3f}d exceeds "
                    f"{RAW_FRESHNESS_TOLERANCE_DAYS}d; fail closed"
                )
            freshness_policy = "CUMULATIVE_TOLERANCE"
            freshness_tolerance_days = RAW_FRESHNESS_TOLERANCE_DAYS
        else:
            staging_history_rows = (
                spark.sql(f"DESCRIBE HISTORY {qname(staging_table)}")
                .where(F.col("version") == F.lit(int(staging_version)))
                .collect()
            )
            if len(staging_history_rows) != 1:
                fail(
                    f"{basename}: incremental-ledger expected one staging commit at "
                    f"version {staging_version}; found {len(staging_history_rows)}; "
                    "fail closed"
                )
            staging_commit = staging_history_rows[0]
            if staging_commit["operation"] != "MERGE":
                fail(
                    f"{basename}: incremental-ledger staging version "
                    f"{staging_version} operation={staging_commit['operation']} "
                    "is not MERGE; fail closed"
                )
            staging_metrics = required_drain_merge_metrics(
                staging_commit["operationMetrics"] or {},
                f"{basename}: staging version {staging_version}",
            )
            staging_source_rows = staging_metrics["numSourceRows"]
            staging_output_rows = staging_metrics["numOutputRows"]
            staging_updated_rows = staging_metrics["numTargetRowsUpdated"]
            staging_inserted_rows = staging_metrics["numTargetRowsInserted"]
            staging_deleted_rows = staging_metrics["numTargetRowsDeleted"]
            if (
                staging_commit["timestamp"] is None
                or staging_commit["timestamp"] > log_row.get("logged_at")
                or staging_source_rows != terms["remaining_rows"]
                or staging_output_rows != terms["remaining_rows"]
                or staging_updated_rows != terms["remaining_rows"]
                or staging_inserted_rows != 0
                or staging_deleted_rows != 0
            ):
                fail(
                    f"{basename}: incremental-ledger staging MERGE metrics do not "
                    f"prove the logged remainder; version={staging_version}, "
                    f"operation={staging_commit['operation']}, timestamp="
                    f"{staging_commit['timestamp']}, source_rows={staging_source_rows}, "
                    f"output_rows={staging_output_rows}, updated_rows="
                    f"{staging_updated_rows}, inserted_rows={staging_inserted_rows}, "
                    f"deleted_rows={staging_deleted_rows}, remaining_rows="
                    f"{terms['remaining_rows']}; fail closed"
                )
            staging_cdf = spark.sql(
                f"SELECT * FROM table_changes('{sql_escape(staging_table)}', "
                f"{int(staging_version)}, {int(staging_version)}) "
                "WHERE _change_type IN ('insert', 'update_postimage')"
            )
            remaining_batch_keys = staging_cdf.select(
                F.expr(f"try_cast({qident(key)} AS BIGINT)").alias("__KEY"),
                (
                    F.col(key).isNull()
                    | bigint_invalid_condition(staging_cdf, key)
                ).cast("long").alias("__INVALID_KEY"),
            )
            remaining_batch_metrics = remaining_batch_keys.agg(
                F.count(F.lit(1)).alias("rows"),
                F.countDistinct("__KEY").alias("distinct_keys"),
                F.sum(F.col("__KEY").isNull().cast("long")).alias("null_rows"),
                F.sum(F.col("__INVALID_KEY")).alias("invalid_rows"),
            ).collect()[0]
            if (
                int(remaining_batch_metrics["rows"]) != terms["remaining_rows"]
                or int(remaining_batch_metrics["distinct_keys"])
                != terms["remaining_rows"]
                or int(remaining_batch_metrics["null_rows"] or 0) != 0
                or int(remaining_batch_metrics["invalid_rows"] or 0) != 0
            ):
                fail(
                    f"{basename}: incremental-ledger staging CDF remainder key "
                    f"gate failed; metrics={remaining_batch_metrics.asDict()}, "
                    f"remaining_rows={terms['remaining_rows']}; fail closed"
                )
            remaining_batch_category = remaining_batch_keys.select(
                "__KEY",
                F.lit("remaining_batch").alias("__CATEGORY"),
                F.col("__INVALID_KEY"),
            )

            checkpoint = last_checkpoint(source)
            if (
                checkpoint is None
                or checkpoint.get("source_version") is None
                or checkpoint.get("source_rows") is None
            ):
                fail(
                    f"{basename}: incremental-ledger reconciliation requires a prior "
                    "checkpoint version and cumulative source_rows; fail closed"
                )
            checkpoint_version = int(checkpoint["source_version"])
            checkpoint_rows = int(checkpoint["source_rows"])
            pinned_version = int(SOURCE_VERSIONS[source])
            if checkpoint_version > pinned_version:
                fail(
                    f"{basename}: incremental-ledger checkpoint version "
                    f"{checkpoint_version} is ahead of pinned raw version "
                    f"{pinned_version}; fail closed"
                )
            history_version_lower_bound = (
                checkpoint_version
                if checkpoint_version == pinned_version
                else checkpoint_version + 1
            )
            if history_version_lower_bound < 0:
                fail(
                    f"{basename}: invalid bounded history lower version "
                    f"{history_version_lower_bound}; fail closed"
                )
            successful_logged_at = log_row.get("logged_at")
            history_rows = (
                spark.sql(f"DESCRIBE HISTORY {qname(source)}")
                .where(
                    (F.col("version") >= F.lit(history_version_lower_bound))
                    & (F.col("version") <= F.lit(pinned_version))
                )
                .collect()
            )

            matching_merges = []
            for history_row in history_rows:
                if history_row["operation"] != "MERGE":
                    continue
                commit_ts = history_row["timestamp"]
                if (
                    commit_ts is None
                    or commit_ts <= staged_at_watermark
                    or commit_ts > successful_logged_at
                ):
                    continue
                metrics = required_drain_merge_metrics(
                    history_row["operationMetrics"] or {},
                    f"{basename}: raw MERGE version {int(history_row['version'])}",
                )
                source_rows = metrics["numSourceRows"]
                output_rows = metrics["numOutputRows"]
                updated_rows = metrics["numTargetRowsUpdated"]
                inserted_rows = metrics["numTargetRowsInserted"]
                deleted_rows = metrics["numTargetRowsDeleted"]
                if (
                    source_rows == terms["promoted_keys"]
                    and output_rows == terms["promoted_keys"]
                    and updated_rows + inserted_rows == terms["promoted_keys"]
                    and deleted_rows == 0
                ):
                    matching_merges.append({
                        "version": int(history_row["version"]),
                        "timestamp": commit_ts,
                        "num_source_rows": source_rows,
                        "num_output_rows": output_rows,
                        "num_updated_rows": updated_rows,
                        "num_inserted_rows": inserted_rows,
                        "num_deleted_rows": deleted_rows,
                    })
            if len(matching_merges) != 1:
                fail(
                    f"{basename}: incremental-ledger expected exactly one matching raw "
                    f"MERGE after control watermark and at/before SUCCESS log; found "
                    f"{matching_merges}; fail closed"
                )
            matching_merge = matching_merges[0]
            merges_since_checkpoint = [
                int(history_row["version"])
                for history_row in history_rows
                if (
                    history_row["operation"] == "MERGE"
                    and checkpoint_version < int(history_row["version"]) <= pinned_version
                )
            ]
            if matching_merge["version"] != pinned_version:
                fail(
                    f"{basename}: authoritative matching raw MERGE version "
                    f"{matching_merge['version']} != pinned raw version "
                    f"{pinned_version}; fail closed"
                )

            checkpoint_watermark = checkpoint.get("source_watermark")
            if checkpoint_version < pinned_version:
                checkpoint_relation = "ADVANCED_ONE_MERGE"
                if merges_since_checkpoint != [matching_merge["version"]]:
                    fail(
                        f"{basename}: advanced incremental-ledger relation requires "
                        f"exactly one matching MERGE since checkpoint; checkpoint_version="
                        f"{checkpoint_version}, pinned_version={pinned_version}, "
                        f"merges_since_checkpoint={merges_since_checkpoint}, "
                        f"matching_merge={matching_merge}; fail closed"
                    )
                cumulative_row_delta = int(raw_metrics["rows"]) - checkpoint_rows
                if cumulative_row_delta != matching_merge["num_inserted_rows"]:
                    fail(
                        f"{basename}: advanced incremental-ledger cumulative raw row "
                        f"delta {cumulative_row_delta} != MERGE inserted rows "
                        f"{matching_merge['num_inserted_rows']}; fail closed"
                    )
            else:
                checkpoint_relation = "REUSED_PINNED_RAW_VERSION"
                cumulative_row_delta = 0
                if merges_since_checkpoint:
                    fail(
                        f"{basename}: reused pinned checkpoint requires zero MERGEs "
                        f"since checkpoint, found {merges_since_checkpoint}; fail closed"
                    )
                if checkpoint_rows != int(raw_metrics["rows"]):
                    fail(
                        f"{basename}: reused pinned checkpoint source_rows "
                        f"{checkpoint_rows} != pinned raw rows {raw_metrics['rows']}; "
                        "fail closed"
                    )
                if checkpoint_watermark != raw_watermark:
                    fail(
                        f"{basename}: reused pinned checkpoint watermark "
                        f"{checkpoint_watermark} != pinned raw watermark "
                        f"{raw_watermark}; fail closed"
                    )

            cdf = spark.sql(
                f"SELECT * FROM table_changes('{sql_escape(source)}', "
                f"{matching_merge['version']}, {matching_merge['version']}) "
                "WHERE _change_type IN ('insert', 'update_postimage')"
            )
            batch_keys = cdf.select(
                F.expr(f"try_cast({qident(key)} AS BIGINT)").alias("__KEY"),
                (
                    F.col(key).isNull() | bigint_invalid_condition(cdf, key)
                ).cast("long").alias("__INVALID_KEY"),
            )
            batch_key_metrics = batch_keys.agg(
                F.count(F.lit(1)).alias("rows"),
                F.countDistinct("__KEY").alias("distinct_keys"),
                F.sum(F.col("__KEY").isNull().cast("long")).alias("null_rows"),
                F.sum(F.col("__INVALID_KEY")).alias("invalid_rows"),
            ).collect()[0]
            if (
                int(batch_key_metrics["rows"]) != terms["promoted_keys"]
                or int(batch_key_metrics["distinct_keys"]) != terms["promoted_keys"]
                or int(batch_key_metrics["null_rows"] or 0) != 0
                or int(batch_key_metrics["invalid_rows"] or 0) != 0
            ):
                fail(
                    f"{basename}: incremental-ledger CDF promoted-batch key gate failed; "
                    f"metrics={batch_key_metrics.asDict()}, promoted_keys="
                    f"{terms['promoted_keys']}; fail closed"
                )

            batch_category = batch_keys.select(
                "__KEY",
                F.lit("promoted_batch").alias("__CATEGORY"),
                F.col("__INVALID_KEY"),
            )
            batch_categories = (
                "promoted_batch", "remaining_batch",
            )
            batch_union = batch_category.unionByName(
                remaining_batch_category
            )
            batch_per_key = batch_union.groupBy("__KEY").agg(
                *[
                    F.sum(
                        F.when(F.col("__CATEGORY") == category, 1).otherwise(0)
                    ).alias(f"__ROWS__{category}")
                    for category in batch_categories
                ],
                *[
                    F.sum(
                        F.when(
                            (F.col("__CATEGORY") == category)
                            & (F.col("__INVALID_KEY") != 0),
                            1,
                        ).otherwise(0)
                    ).alias(f"__INVALID__{category}")
                    for category in batch_categories
                ],
            )
            batch_positive_category_count = reduce(
                lambda left, right: left + right,
                [
                    F.when(F.col(f"__ROWS__{category}") > 0, 1).otherwise(0)
                    for category in batch_categories
                ],
            )
            batch_summary = batch_per_key.agg(
                *[
                    F.sum(F.col(f"__ROWS__{category}")).alias(
                        f"__COUNT__{category}"
                    )
                    for category in batch_categories
                ],
                *[
                    F.sum(F.col(f"__INVALID__{category}")).alias(
                        f"__BAD__{category}"
                    )
                    for category in batch_categories
                ],
                *[
                    F.sum(
                        F.when(F.col(f"__ROWS__{category}") > 1, 1).otherwise(0)
                    ).alias(f"__DUP_GROUPS__{category}")
                    for category in batch_categories
                ],
                *[
                    F.sum(
                        F.when(
                            F.col("__KEY").isNull(),
                            F.col(f"__ROWS__{category}"),
                        ).otherwise(0)
                    ).alias(f"__NULL_ROWS__{category}")
                    for category in batch_categories
                ],
                F.sum(
                    F.when(batch_positive_category_count > 1, 1).otherwise(0)
                ).alias("__CROSS_CATEGORY_DUPLICATE_KEYS"),
                F.sum(
                    F.when(F.col("__KEY").isNotNull(), 1).otherwise(0)
                ).alias("__UNION_DISTINCT_KEYS"),
            ).collect()[0]
            reconciled_actual_counts = {
                category: int(batch_summary[f"__COUNT__{category}"] or 0)
                for category in batch_categories
            }
            reconciled_invalid_key_rows = {
                category: int(batch_summary[f"__BAD__{category}"] or 0)
                for category in batch_categories
            }
            reconciled_duplicate_groups = {
                category: int(batch_summary[f"__DUP_GROUPS__{category}"] or 0)
                for category in batch_categories
            }
            reconciled_null_key_rows = {
                category: int(batch_summary[f"__NULL_ROWS__{category}"] or 0)
                for category in batch_categories
            }
            reconciled_cross_category_duplicate_keys = int(
                batch_summary["__CROSS_CATEGORY_DUPLICATE_KEYS"] or 0
            )
            reconciled_union_total = int(
                batch_summary["__UNION_DISTINCT_KEYS"] or 0
            )
            expected_actual = {
                "promoted_batch": terms["promoted_keys"],
                "remaining_batch": terms["remaining_rows"],
            }
            mismatches = {
                category: {
                    "actual": reconciled_actual_counts[category],
                    "authoritative": expected_actual[category],
                }
                for category in batch_categories
                if reconciled_actual_counts[category] != expected_actual[category]
            }
            actual_sum = sum(reconciled_actual_counts.values())
            if (
                mismatches
                or any(reconciled_invalid_key_rows.values())
                or any(reconciled_null_key_rows.values())
                or any(reconciled_duplicate_groups.values())
                or reconciled_cross_category_duplicate_keys
                or reconciled_union_total != actual_sum
                or reconciled_union_total != terms["staged_rows"]
            ):
                fail(
                    f"{basename}: incremental-ledger batch/staging key ownership failed "
                    f"closed; count_mismatches={mismatches}, invalid_key_rows="
                    f"{reconciled_invalid_key_rows}, null_key_rows="
                    f"{reconciled_null_key_rows}, duplicate_groups="
                    f"{reconciled_duplicate_groups}, cross_category_duplicate_keys="
                    f"{reconciled_cross_category_duplicate_keys}, union_total="
                    f"{reconciled_union_total}, actual_sum={actual_sum}, staged_rows="
                    f"{terms['staged_rows']}"
                )
            if raw_watermark != staged_at_watermark:
                fail(
                    f"{basename}: incremental-ledger pinned raw max ADC_UPDT "
                    f"{raw_watermark} != control staged_at_watermark "
                    f"{staged_at_watermark}; fail closed"
                )
            freshness_policy = "INCREMENTAL_EXACT_CONTROL_WATERMARK"
            freshness_tolerance_days = 0
            merge_proof = {
                **matching_merge,
                "checkpoint_relation": checkpoint_relation,
                "checkpoint_version": checkpoint_version,
                "checkpoint_rows": checkpoint_rows,
                "history_version_lower_bound": history_version_lower_bound,
                "history_version_upper_bound": pinned_version,
                "checkpoint_watermark": str(checkpoint_watermark),
                "pinned_version": pinned_version,
                "pinned_rows": int(raw_metrics["rows"]),
                "cumulative_row_delta": cumulative_row_delta,
                "cdf_promoted_batch_rows": int(batch_key_metrics["rows"]),
                "cdf_promoted_batch_distinct_keys": int(
                    batch_key_metrics["distinct_keys"]
                ),
                "staging_remainder": {
                    "table": staging_table,
                    "version": int(staging_version),
                    "timestamp": str(staging_commit["timestamp"]),
                    "num_source_rows": staging_source_rows,
                    "num_output_rows": staging_output_rows,
                    "num_updated_rows": staging_updated_rows,
                    "num_inserted_rows": staging_inserted_rows,
                    "num_deleted_rows": staging_deleted_rows,
                    "cdf_remaining_batch_rows": int(
                        remaining_batch_metrics["rows"]
                    ),
                    "cdf_remaining_batch_distinct_keys": int(
                        remaining_batch_metrics["distinct_keys"]
                    ),
                },
                "one_merge_since_checkpoint": (
                    checkpoint_relation == "ADVANCED_ONE_MERGE"
                ),
                "zero_merges_since_checkpoint": (
                    checkpoint_relation == "REUSED_PINNED_RAW_VERSION"
                ),
            }

        results[source] = {
            "status": "PASS",
            "key_name": key,
            "ledger_run_id": ledger_run_id,
            "control": {
                "table_name": control_row.get("table_name"),
                "staging_delta_version": int(staging_version),
                "staged_at_watermark": str(staged_at_watermark),
                "updated_at": str(control_updated_at),
            },
            "table_log": {
                "table_name": log_row.get("table_name"),
                "status": log_row.get("status"),
                "logged_at": str(log_row.get("logged_at")),
                **terms,
                "accounting_sum": int(log_sum),
                "identity_exact": True,
                "logged_before_or_at_control_update": True,
            },
            "pinned_versions": {
                "raw_delta_version": int(SOURCE_VERSIONS[source]),
                "staging_residue_delta_version": int(staging_version),
                "excluded_archive_delta_version": excluded_meta["pinned_delta_version"],
                "abandoned_archive_delta_version": abandoned_meta["pinned_delta_version"],
            },
            "archive_state": {
                "excluded": excluded_meta,
                "abandoned": abandoned_meta,
                "presence_evidence": archive_presence,
            },
            "accounting_mode": accounting_mode,
            "cumulative_actual_counts": actual_counts,
            "reconciled_actual_counts": reconciled_actual_counts,
            "incremental_merge_proof": merge_proof,
            "key_ownership": {
                "invalid_key_rows": reconciled_invalid_key_rows,
                "null_key_rows": reconciled_null_key_rows,
                "within_category_duplicate_groups": reconciled_duplicate_groups,
                "cross_category_duplicate_keys": reconciled_cross_category_duplicate_keys,
                "pairwise_disjoint": True,
                "union_total": reconciled_union_total,
                "union_equals_authoritative_staged_rows": True,
                "grouped_accounting_actions": 1,
            },
            "freshness": {
                "policy": freshness_policy,
                "pinned_raw_watermark": str(raw_watermark),
                "control_staged_at_watermark": str(staged_at_watermark),
                "signed_days": freshness_signed_days,
                "absolute_days": freshness_absolute_days,
                "tolerance_days": freshness_tolerance_days,
                "within_tolerance": True,
            },
            "status_table_included": False,
        }

    authoritative_run_ids = {
        result["ledger_run_id"] for result in results.values()
    }
    if len(authoritative_run_ids) != 1:
        fail(
            "Waiting-list current/HIST control rows do not identify one authoritative "
            f"full-accounting run: {sorted(authoritative_run_ids)}; fail closed"
        )
    authoritative_run_id = next(iter(authoritative_run_ids))
    return {
        "status": "PASS",
        "authoritative_run_id": authoritative_run_id,
        "authoritative_control": CLASSIFICATION_CONTROL,
        "authoritative_table_log": CLASSIFICATION_TABLE_LOG,
        "successful_table_log_rows_per_source": 1,
        "sources": results,
        "status_table_included": False,
    }


def snapshot_date_state(snapshot_date_value, label: str) -> dict:
    if not bronze_table_exists(SNAPSHOT_TARGET):
        return {
            "exists": False,
            "rows": 0,
            "cutoff_count": 0,
            "cutoff_ts": None,
            "duplicate_key_groups": 0,
        }
    date_rows = spark.table(SNAPSHOT_TARGET).where(
        F.col("SNAPSHOT_DATE") == F.lit(snapshot_date_value).cast("date")
    )
    metrics = date_rows.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("SNAPSHOT_CUTOFF_TS").alias("cutoff_count"),
        F.min("SNAPSHOT_CUTOFF_TS").alias("cutoff_ts"),
    ).collect()[0]
    rows = int(metrics["rows"])
    if rows == 0:
        return {
            "exists": False,
            "rows": 0,
            "cutoff_count": 0,
            "cutoff_ts": None,
            "duplicate_key_groups": 0,
        }
    duplicate_groups = (
        date_rows.groupBy("SNAPSHOT_DATE", "PM_WAIT_LIST_ID")
        .count().where(F.col("count") > 1).limit(1).count()
    )
    cutoff_count = int(metrics["cutoff_count"])
    if duplicate_groups or cutoff_count != 1:
        fail(
            f"{label}: existing snapshot date {snapshot_date_value} violates idempotence; "
            f"duplicate_key_groups={duplicate_groups}, cutoff_count={cutoff_count}"
        )
    return {
        "exists": True,
        "rows": rows,
        "cutoff_count": cutoff_count,
        "cutoff_ts": str(metrics["cutoff_ts"]),
        "duplicate_key_groups": int(duplicate_groups),
    }


def determine_snapshot_due(weekly_status: dict) -> dict:
    if (
        SNAPSHOT_CUTOFF_TS_VALUE is None
        or SNAPSHOT_DATE_UTC is None
        or SNAPSHOT_SPARK_DAYOFWEEK is None
    ):
        fail("Snapshot cutoff/date must be captured after raw source pinning before due determination")
    target_absent = not bronze_table_exists(SNAPSHOT_TARGET)
    orchestrator_weekday_match = SNAPSHOT_SPARK_DAYOFWEEK == SNAPSHOT_WEEKDAY
    reasons = []
    if BOOTSTRAP_MODE:
        reasons.append("BOOTSTRAP")
    if target_absent:
        reasons.append("SNAPSHOT_TARGET_ABSENT")
    if RUN_SNAPSHOTS:
        reasons.append("MANUAL_RUN_SNAPSHOTS")
    if weekly_status["week_pending"]:
        reasons.append("WEEKLY_RECONCILIATION_PENDING")
    requested = bool(reasons)
    heartbeat_run_lock("snapshot due ownership and existence check")
    existing_state = snapshot_date_state(
        SNAPSHOT_DATE_UTC, "snapshot due determination"
    )
    due = SNAPSHOT_ENABLED and requested and not existing_state["exists"]
    if not SNAPSHOT_ENABLED:
        status_reason = "DISABLED"
    elif existing_state["exists"]:
        status_reason = "SNAPSHOT_EXISTS_FOR_PINNED_UTC_DATE"
    elif requested:
        status_reason = "REQUESTED:" + ",".join(reasons)
    else:
        status_reason = "NOT_REQUESTED_CURRENT_ISO_WEEK_ALREADY_COMMITTED"
    return {
        "enabled": SNAPSHOT_ENABLED,
        "requested": requested,
        "due": bool(due),
        "status_reason": status_reason,
        "request_reasons": reasons,
        "snapshot_date_utc": str(SNAPSHOT_DATE_UTC),
        "snapshot_cutoff_ts_utc": str(SNAPSHOT_CUTOFF_TS_VALUE),
        "snapshot_spark_dayofweek": SNAPSHOT_SPARK_DAYOFWEEK,
        "scheduled_weekday": SNAPSHOT_WEEKDAY,
        "orchestrator_weekday_match": orchestrator_weekday_match,
        "weekday_is_not_cadence_authority": True,
        "weekly_reconciliation": weekly_status,
        "bootstrap": BOOTSTRAP_MODE,
        "target_absent": target_absent,
        "manual_request": RUN_SNAPSHOTS,
        "pinned_date_state": existing_state,
        "idempotence_policy": SNAPSHOT_IDEMPOTENCE_POLICY,
    }


def snapshot_result_when_not_due(snapshot_due: dict) -> dict:
    state = snapshot_due["pinned_date_state"]
    if state["exists"]:
        return {
            "status": "SNAPSHOT_EXISTS",
            "snapshot_date": snapshot_due["snapshot_date_utc"],
            "snapshot_cutoff_ts": state["cutoff_ts"],
            "rows": int(state["rows"]),
            "status_reason": "SNAPSHOT_EXISTS_FOR_PINNED_UTC_DATE",
            "request_reasons": snapshot_due["request_reasons"],
            "weekly_reconciliation": snapshot_due["weekly_reconciliation"],
            "idempotence_policy": SNAPSHOT_IDEMPOTENCE_POLICY,
            "lock_owner_attempt_id_matches": True,
        }
    return {
        "status": "NOT_DUE",
        "status_reason": snapshot_due["status_reason"],
        "snapshot_date": snapshot_due["snapshot_date_utc"],
        "snapshot_cutoff_ts": snapshot_due["snapshot_cutoff_ts_utc"],
        "weekly_reconciliation": snapshot_due["weekly_reconciliation"],
        "idempotence_policy": SNAPSHOT_IDEMPOTENCE_POLICY,
    }


def take_snapshot(snapshot_due: dict) -> dict:
    snapshot_cutoff_value = SNAPSHOT_CUTOFF_TS_VALUE
    snapshot_date_value = SNAPSHOT_DATE_UTC
    heartbeat_run_lock("snapshot initial ownership and existence check")
    existing_state = snapshot_date_state(
        snapshot_date_value, "snapshot initial existence check"
    )
    if existing_state["exists"]:
        return {
            "status": "SNAPSHOT_EXISTS",
            "snapshot_date": str(snapshot_date_value),
            "snapshot_cutoff_ts": existing_state["cutoff_ts"],
            "rows": int(existing_state["rows"]),
            "status_reason": "SNAPSHOT_EXISTS_FOR_PINNED_UTC_DATE",
            "request_reasons": snapshot_due["request_reasons"],
            "weekly_reconciliation": snapshot_due["weekly_reconciliation"],
            "idempotence_policy": SNAPSHOT_IDEMPOTENCE_POLICY,
            "lock_owner_attempt_id_matches": True,
        }

    snapshot = (
        spark.table(TARGET)
        .where(
            F.col("IS_CURRENT")
            & F.col("SOURCE_PRESENT_IND")
            & (F.col("ACTIVE_IND") == 1)
            & (F.col("WAIT_LIST_IND") == 1)
            & (
                F.col("STATUS_CD").isNull()
                | (F.col("STATUS_CD") != REMOVED_STATUS_CD)
            )
            & (
                F.col("REMOVAL_DT_TM").isNull()
                | (F.col("REMOVAL_DT_TM") > F.lit(snapshot_cutoff_value).cast("timestamp"))
            )
        )
        .select(*SNAPSHOT_COLUMNS, *DECODE_COLUMNS)
        .withColumn("SNAPSHOT_DATE", F.lit(snapshot_date_value).cast("date"))
        .withColumn("SNAPSHOT_CUTOFF_TS", F.lit(snapshot_cutoff_value).cast("timestamp"))
        .select(*EXPECTED_SNAPSHOT_SCHEMA.keys())
    )
    validate_schema_contract(snapshot, SNAPSHOT_TARGET, "pre-stage snapshot")
    assert_no_banned_columns(snapshot, "pre-stage snapshot", snapshot=True)
    snapshot_stage = write_attempt_stage(snapshot, SNAPSHOT_STAGE, "snapshot")
    validate_schema_contract(snapshot_stage, SNAPSHOT_TARGET, "staged snapshot")
    assert_no_banned_columns(snapshot_stage, "staged snapshot", snapshot=True)
    assert_unique_non_null(
        snapshot_stage,
        ["SNAPSHOT_DATE", "PM_WAIT_LIST_ID"],
        "staged snapshot before append",
    )
    staged_rows = snapshot_stage.count()

    heartbeat_run_lock("snapshot pre-append ownership and existence check")
    pre_append_state = snapshot_date_state(
        snapshot_date_value, "snapshot pre-append existence check"
    )
    if pre_append_state["exists"]:
        return {
            "status": "SNAPSHOT_EXISTS",
            "snapshot_date": str(snapshot_date_value),
            "snapshot_cutoff_ts": pre_append_state["cutoff_ts"],
            "rows": int(pre_append_state["rows"]),
            "status_reason": "SNAPSHOT_EXISTS_FOR_PINNED_UTC_DATE",
            "request_reasons": snapshot_due["request_reasons"],
            "weekly_reconciliation": snapshot_due["weekly_reconciliation"],
            "idempotence_policy": SNAPSHOT_IDEMPOTENCE_POLICY,
            "lock_owner_attempt_id_matches": True,
        }

    if not bronze_table_exists(SNAPSHOT_TARGET):
        (
            snapshot_stage.write.format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .option("delta.enableRowTracking", "true")
            .option("delta.enableDeletionVectors", "true")
            .option("delta.appendOnly", "true")
            .saveAsTable(SNAPSHOT_TARGET)
        )
    else:
        snapshot_stage.write.format("delta").mode("append").saveAsTable(SNAPSHOT_TARGET)

    feature_result = validate_or_repair_table_features(SNAPSHOT_TARGET, repair_allowed=True)
    clustering_result = validate_or_repair_clustering(SNAPSHOT_TARGET, repair_allowed=True)
    comments_result = validate_or_repair_comments(SNAPSHOT_TARGET, repair_allowed=True)
    VALIDATION_RESULTS["comments"]["snapshot"] = comments_result
    target_frame = spark.table(SNAPSHOT_TARGET)
    VALIDATION_RESULTS["schema"]["snapshot"] = validate_schema_contract(
        target_frame, SNAPSHOT_TARGET, SNAPSHOT_TARGET
    )
    VALIDATION_RESULTS["pii"]["snapshot"] = assert_no_banned_columns(
        target_frame, SNAPSHOT_TARGET, snapshot=True
    )
    pinned_date_frame = target_frame.where(
        F.col("SNAPSHOT_DATE") == F.lit(snapshot_date_value)
    )
    assert_unique_non_null(
        pinned_date_frame,
        ["SNAPSHOT_DATE", "PM_WAIT_LIST_ID"],
        f"{SNAPSHOT_TARGET} pinned date {snapshot_date_value} after append",
    )
    snapshot_cutoff_contract = validate_snapshot_cutoffs(
        pinned_date_frame,
        f"{SNAPSHOT_TARGET} pinned date {snapshot_date_value}",
    )
    snapshot_cutoff_contract["validation_scope"] = "PINNED_SNAPSHOT_DATE_ONLY"
    snapshot_cutoff_contract["snapshot_date"] = str(snapshot_date_value)
    post_append_state = snapshot_date_state(
        snapshot_date_value, "snapshot post-append validation"
    )
    if not post_append_state["exists"] or post_append_state["rows"] != staged_rows:
        fail(
            f"{SNAPSHOT_TARGET}: snapshot readback failed; "
            f"rows={post_append_state['rows']}/{staged_rows}"
        )
    return {
        "status": "TAKEN",
        "snapshot_date": str(snapshot_date_value),
        "snapshot_cutoff_ts": str(snapshot_cutoff_value),
        "rows": int(post_append_state["rows"]),
        "status_reason": snapshot_due["status_reason"],
        "request_reasons": snapshot_due["request_reasons"],
        "weekly_reconciliation": snapshot_due["weekly_reconciliation"],
        "features": feature_result,
        "clustering": clustering_result,
        "comments": comments_result,
        "cutoff_contract": snapshot_cutoff_contract,
        "idempotence_policy": SNAPSHOT_IDEMPOTENCE_POLICY,
        "lock_owner_attempt_id_matches": True,
        "immutable_first_successful_cutoff": True,
    }


def commit_checkpoints() -> None:
    heartbeat_run_lock("checkpoint commit")
    checkpoint_committed_at = spark.sql("SELECT current_timestamp() AS ts").first()["ts"]
    rows = []
    for source in TARGET_SOURCES:
        health = SOURCE_HEALTH[source]
        rows.append((
            TARGET,
            source,
            int(health["version"]),
            health["watermark"],
            int(health["rows"]) if health["rows"] is not None else None,
            RUN_ID,
            checkpoint_committed_at,
        ))
    updates = spark.createDataFrame(
        rows,
        "target_table string, source_table string, source_version long, source_watermark timestamp, "
        "source_rows long, run_id string, committed_at timestamp",
    )
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
    declared = "', '".join(sql_escape(source) for source in TARGET_SOURCES)
    spark.sql(
        f"DELETE FROM {qname(STATE_TABLE)} WHERE target_table = '{sql_escape(TARGET)}' "
        f"AND source_table NOT IN ('{declared}')"
    )


def acquire_run_lock() -> None:
    lock = spark.createDataFrame(
        [(LOCK_KEY, LOCK_KEY, 0, None, None, ATTEMPT_ID)],
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
    owners = spark.table(STATE_TABLE).where(
        (F.col("target_table") == LOCK_KEY) & (F.col("source_table") == LOCK_KEY)
    ).collect()
    if len(owners) != 1 or owners[0]["run_id"] != ATTEMPT_ID:
        owner = owners[0]["run_id"] if owners else None
        fail(
            "Another waiting_list_pipeline attempt holds the lock "
            f"(attempt_id={owner}, orchestration_run_id={RUN_ID}, TTL={LOCK_TTL_HOURS}h)."
        )


def heartbeat_run_lock(label: str) -> None:
    owners = spark.table(STATE_TABLE).where(
        (F.col("target_table") == LOCK_KEY) & (F.col("source_table") == LOCK_KEY)
    ).collect()
    if len(owners) != 1 or owners[0]["run_id"] != ATTEMPT_ID:
        observed = owners[0]["run_id"] if owners else None
        fail(f"Lost waiting_list_pipeline lock before {label}; observed_owner={observed}")
    spark.sql(
        f"UPDATE {qname(STATE_TABLE)} SET committed_at = current_timestamp() "
        f"WHERE target_table = '{LOCK_KEY}' AND source_table = '{LOCK_KEY}' "
        f"AND run_id = '{sql_escape(ATTEMPT_ID)}'"
    )
    refreshed = spark.table(STATE_TABLE).where(
        (F.col("target_table") == LOCK_KEY)
        & (F.col("source_table") == LOCK_KEY)
        & (F.col("run_id") == ATTEMPT_ID)
    ).count()
    if refreshed != 1:
        fail(f"Lost waiting_list_pipeline lock while heartbeating before {label}")


def release_run_lock() -> None:
    spark.sql(
        f"DELETE FROM {qname(STATE_TABLE)} WHERE target_table = '{LOCK_KEY}' "
        f"AND source_table = '{LOCK_KEY}' AND run_id = '{sql_escape(ATTEMPT_ID)}'"
    )


def cleanup_stages() -> None:
    for table in sorted(ATTEMPT_STAGE_TABLES):
        spark.sql(f"DROP TABLE IF EXISTS {qname(table)}")


def optimize_bootstrap_target() -> dict:
    clustering = validate_or_repair_clustering(TARGET, repair_allowed=False)
    heartbeat_run_lock(f"OPTIMIZE {TARGET}")
    try:
        spark.sql(f"OPTIMIZE {qname(TARGET)} FULL")
        mode = "FULL"
    except Exception as exc:
        message = str(exc).lower()
        if not any(token in message for token in ("syntax", "parse", "unsupported", "not supported")):
            raise
        heartbeat_run_lock(f"safe OPTIMIZE fallback {TARGET}")
        spark.sql(f"OPTIMIZE {qname(TARGET)}")
        mode = "SAFE_FALLBACK"
    return {"mode": mode, "verified_clustering": clustering}


def write_dev_test_audit(event: str, status: str, payload: dict) -> None:
    if TARGET_SCHEMA != "8_dev.bronze_imp_incr":
        return
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qname(DEV_TEST_AUDIT_TABLE)} (
          pipeline STRING NOT NULL,
          run_id STRING NOT NULL,
          attempt_id STRING NOT NULL,
          event STRING NOT NULL,
          status STRING NOT NULL,
          payload_json STRING NOT NULL,
          recorded_at TIMESTAMP NOT NULL
        ) USING DELTA
        COMMENT 'Task 6 dev-only waiting-list pipeline mode, scan, failure-boundary and snapshot evidence.'
        """
    )
    updates = (
        spark.createDataFrame(
            [(
                "waiting_list_pipeline", RUN_ID, ATTEMPT_ID,
                event, status, bronze_json(payload),
            )],
            "pipeline string, run_id string, attempt_id string, "
            "event string, status string, payload_json string",
        )
        .withColumn("recorded_at", F.current_timestamp())
    )
    (
        DeltaTable.forName(spark, DEV_TEST_AUDIT_TABLE)
        .alias("t")
        .merge(
            updates.alias("s"),
            "t.run_id = s.run_id AND t.attempt_id = s.attempt_id "
            "AND t.event = s.event",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_pipeline() -> dict:
    global SOURCE_HEALTH, CODE_VALUE_VERSION, CODE_VALUE_LIVE_VERSION
    global SNAPSHOT_CUTOFF_TS_VALUE, SNAPSHOT_DATE_UTC, SNAPSHOT_SPARK_DAYOFWEEK
    global DECODE_REFRESH_WEEK_VERSION
    load_state_cache()
    SOURCE_VERSIONS.clear()
    PINNED_SOURCE_FRAMES.clear()
    PINNED_SOURCE_METRICS.clear()
    SOURCE_SLICE_FRAMES.clear()
    SOURCE_SLICE_STATUS.clear()

    # Capture data-source Delta versions first, then bind every raw read to those versions.
    for source in DATA_SOURCES:
        SOURCE_VERSIONS[source] = source_version(source)
    PINNED_SOURCE_FRAMES.update({
        source: (
            spark.read.format("delta")
            .option("versionAsOf", int(SOURCE_VERSIONS[source]))
            .table(source)
        )
        for source in DATA_SOURCES
    })

    dev_test_current_filter = {
        "enabled": False,
        "ids_table": None,
        "filtered_id_count": 0,
        "target_schema_guard": "8_dev.bronze_imp_incr",
        "applied_after_raw_version_pin": True,
    }
    if DEV_TEST_EXCLUDE_CURRENT_IDS_TABLE:
        if not bronze_table_exists(DEV_TEST_EXCLUDE_CURRENT_IDS_TABLE):
            fail(
                "Missing dev-test CURRENT exclusion table "
                f"{DEV_TEST_EXCLUDE_CURRENT_IDS_TABLE}"
            )
        filter_ids = spark.table(DEV_TEST_EXCLUDE_CURRENT_IDS_TABLE).select(
            F.expr("try_cast(PM_WAIT_LIST_ID AS BIGINT)").alias("PM_WAIT_LIST_ID")
        )
        filter_metrics = filter_ids.agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("PM_WAIT_LIST_ID").alias("distinct_ids"),
            F.sum(F.col("PM_WAIT_LIST_ID").isNull().cast("long")).alias("null_ids"),
        ).collect()[0]
        filter_count = int(filter_metrics["rows"])
        if (
            filter_count != 100
            or int(filter_metrics["distinct_ids"]) != filter_count
            or int(filter_metrics["null_ids"] or 0) != 0
        ):
            fail(
                "Dev-test CURRENT exclusion requires exactly 100 unique non-null BIGINT IDs; "
                f"metrics={filter_metrics.asDict()}"
            )
        PINNED_SOURCE_FRAMES[SRC_CURRENT] = (
            PINNED_SOURCE_FRAMES[SRC_CURRENT]
            .join(F.broadcast(filter_ids), "PM_WAIT_LIST_ID", "left_anti")
        )
        dev_test_current_filter = {
            "enabled": True,
            "ids_table": DEV_TEST_EXCLUDE_CURRENT_IDS_TABLE,
            "filtered_id_count": filter_count,
            "target_schema_guard": TARGET_SCHEMA,
            "applied_after_raw_version_pin": True,
        }

    # The immutable business cutoff is captured only after both raw source snapshots are pinned.
    SNAPSHOT_CUTOFF_TS_VALUE = spark.sql(
        "SELECT current_timestamp() AS ts"
    ).first()["ts"]
    natural_snapshot_date_utc = SNAPSHOT_CUTOFF_TS_VALUE.date()
    SNAPSHOT_DATE_UTC = (
        date.fromisoformat(DEV_TEST_SNAPSHOT_DATE_UTC_OVERRIDE)
        if DEV_TEST_SNAPSHOT_DATE_UTC_OVERRIDE
        else natural_snapshot_date_utc
    )
    SNAPSHOT_SPARK_DAYOFWEEK = (
        SNAPSHOT_DATE_UTC.isoweekday() % 7
    ) + 1
    snapshot_iso = SNAPSHOT_DATE_UTC.isocalendar()
    DECODE_REFRESH_WEEK_VERSION = snapshot_iso.year * 100 + snapshot_iso.week

    # Synthetic versions and mode resolution are ordered after cutoff-date ISO derivation.
    SOURCE_VERSIONS[DECODE_REFRESH_WEEK_SOURCE] = DECODE_REFRESH_WEEK_VERSION
    SOURCE_VERSIONS[LOGIC_SOURCE] = LOGIC_VERSION_INT
    changed_sources = pinned_changed_sources()
    weekly_status = weekly_reconciliation_status()
    mode = choose_mode(changed_sources)
    source_slices, source_slice_status = establish_source_slices(
        mode, changed_sources
    )
    SOURCE_SLICE_FRAMES.update(source_slices)
    SOURCE_SLICE_STATUS.update(source_slice_status)
    snapshot_due = determine_snapshot_due(weekly_status)

    metric_specs = {
        SRC_CURRENT: (CURRENT_CAST_COLUMNS, "PM_WAIT_LIST_ID", None),
        SRC_HIST: (HIST_CAST_COLUMNS, "PM_WAIT_LIST_ID", "PM_WAIT_LIST_HIST_ID"),
    }
    if mode in FULL_MODES:
        for source in DATA_SOURCES:
            cast_columns, primary_key, source_version_key = metric_specs[source]
            PINNED_SOURCE_METRICS[source] = collect_pinned_source_metrics(
                source,
                SOURCE_SLICE_FRAMES[source],
                cast_columns,
                primary_key,
                source_version_key=source_version_key,
                scan_scope="FULL_PINNED_TABLE",
                require_watermark=True,
            )
    elif mode == "INCREMENTAL":
        for source in DATA_SOURCES:
            if source not in changed_sources:
                continue
            cast_columns, primary_key, source_version_key = metric_specs[source]
            PINNED_SOURCE_METRICS[source] = collect_pinned_source_metrics(
                source,
                SOURCE_SLICE_FRAMES[source],
                cast_columns,
                primary_key,
                source_version_key=source_version_key,
                scan_scope="PINNED_CHANGED_OVERLAP_SLICE",
                require_watermark=False,
            )

    SOURCE_HEALTH = {
        source: source_health_for_mode(
            source,
            source_mode(source, mode),
            last_checkpoint(source),
            source in changed_sources,
        )
        for source in TARGET_SOURCES
    }
    for source in DATA_SOURCES:
        stale = SOURCE_HEALTH[source]["source_staleness_days"]
        if stale is None:
            fail(f"{source}: ADC_UPDT watermark is NULL")
        if stale > SOURCE_SLA[source]:
            fail(f"{source}: staleness {stale:.2f}d exceeds {SOURCE_SLA[source]}d SLA")

    # The authoritative drain gate precedes lookup pinning, target repair, source build,
    # and every attempt-scoped stage write in all full-family modes.
    drain_gate = (
        full_drain_accounting()
        if mode in FULL_MODES else {"status": "NOT_APPLICABLE", "mode": mode}
    )

    CODE_VALUE_LIVE_VERSION = source_version(CODE_VALUE)
    CODE_VALUE_VERSION = CODE_VALUE_LIVE_VERSION
    target_preflight = validate_existing_preflight(TARGET, mode)
    snapshot_preflight = (
        validate_existing_preflight(SNAPSHOT_TARGET, mode, snapshot=True)
        if bronze_table_exists(SNAPSHOT_TARGET) else {"status": "NOT_PRESENT", "target_exists": False}
    )
    decode_snapshot = {
        "table": CODE_VALUE,
        "pinned_delta_version": int(CODE_VALUE_VERSION),
        "live_delta_version": int(CODE_VALUE_LIVE_VERSION),
        "mode_trigger": False,
        "policy": "PINNED_PER_RUN; UNRELATED_LOOKUP_CHURN_NEVER_TRIGGERS_ORDINARY_FULL",
    }
    common = {
        "pipeline": "waiting_list_pipeline",
        "pipeline_logic_version": PIPELINE_LOGIC_VERSION,
        "run_id": RUN_ID,
        "attempt_id": ATTEMPT_ID,
        "run_as_of_utc": str(RUN_AS_OF),
        "target_schema": TARGET_SCHEMA,
        "mode": mode,
        "run_overrides": {
            "force_full_refresh": FORCE_FULL_REFRESH,
            "full_reconciliation": FULL_RECONCILIATION,
            "bootstrap_mode": BOOTSTRAP_MODE,
            "refresh_decodes": REFRESH_DECODES,
            "run_snapshots": RUN_SNAPSHOTS,
        },
        "decode_refresh_week_version": DECODE_REFRESH_WEEK_VERSION,
        "weekly_reconciliation": weekly_status,
        "drain_accounting": drain_gate,
        "decode_snapshot": decode_snapshot,
        "dev_test_current_filter": dev_test_current_filter,
        "pinned_raw_versions": {
            source: int(SOURCE_VERSIONS[source]) for source in DATA_SOURCES
        },
        "snapshot_cutoff_capture": {
            "captured_after_raw_versions_pinned": True,
            "snapshot_cutoff_ts_utc": str(SNAPSHOT_CUTOFF_TS_VALUE),
            "snapshot_date_utc": str(SNAPSHOT_DATE_UTC),
            "natural_snapshot_date_utc": str(natural_snapshot_date_utc),
            "dev_test_snapshot_date_override": (
                DEV_TEST_SNAPSHOT_DATE_UTC_OVERRIDE or None
            ),
        },
        "source_metric_policy": {
            "mode": mode,
            "full_family_scope": "FULL_PINNED_TABLE_CONSOLIDATED_AGGREGATE",
            "incremental_changed_scope": "PINNED_CHANGED_OVERLAP_SLICE_ONLY",
            "incremental_unchanged_scope": "REUSED_CHECKPOINT_NO_RAW_AGGREGATE",
            "weekly_full_whole_source_reconciliation": True,
            "hidden_daily_full_raw_aggregate": False,
            "changed_sources": changed_sources,
            "source_slices": SOURCE_SLICE_STATUS,
        },
        "incremental_delete_policy": INCREMENTAL_DELETE_POLICY,
        "snapshot_idempotence_policy": SNAPSHOT_IDEMPOTENCE_POLICY,
        "snapshot_due": snapshot_due,
        "source_health": {
            source: {
                **health,
                "watermark": str(health["watermark"]) if health["watermark"] is not None else None,
            }
            for source, health in SOURCE_HEALTH.items()
        },
        "publication_atomicity": (
            "SEQUENTIAL_MAIN_THEN_LOCK_SERIALIZED_SNAPSHOT; "
            "SNAPSHOT_ONLY_AFTER_VALIDATED_MAIN; CHECKPOINT_AFTER_SNAPSHOT; "
            "NOT_CROSS_TABLE_ATOMIC"
        ),
    }

    if mode == "UNCHANGED_SKIP":
        skip_current = {
            "status": "NOT_SCANNED",
            "validation_scope": "METADATA_ONLY_UNCHANGED_SKIP",
            "formula_mismatches": None,
            "duplicate_current_ids": None,
        }
        snapshot_result = take_snapshot(snapshot_due) if snapshot_due["due"] else snapshot_result_when_not_due(snapshot_due)
        summary = {
            "status": "SUCCESS",
            **common,
            "metrics": {},
            "validation": {
                "level": "UNCHANGED_SKIP_VALIDATED",
                "target_preflight": target_preflight,
                "snapshot_preflight": snapshot_preflight,
                "current_formula": skip_current,
                "schema_contracts": VALIDATION_RESULTS["schema"],
                "pii_inspection": VALIDATION_RESULTS["pii"],
                "comments": VALIDATION_RESULTS["comments"],
            },
            "gates": {
                "drain_accounting": drain_gate,
                "all_stage_validation_before_merge": True,
                "status_source_absent": True,
                "codeset_fingerprint_absent": True,
                "daily_incremental_preserved": True,
                "cdf_active": False,
                "snapshot_lock_serialized": True,
                "weekly_snapshot_catch_up": True,
                "mode_aware_source_metrics": True,
                "snapshot_cutoff_after_raw_pin": True,
                "mode_scoped_validation": "METADATA_ONLY_UNCHANGED_SKIP",
            },
            "snapshot": snapshot_result,
            "finished_at": bronze_utc_now(),
        }
        write_dev_test_audit("PIPELINE_EXIT", "SUCCESS", summary)
        return summary

    lookup, decode_gate = pin_decode_lookup()
    current_source = SOURCE_SLICE_FRAMES[SRC_CURRENT]
    hist_source = SOURCE_SLICE_FRAMES[SRC_HIST]

    built, union_gate = build_union(current_source, hist_source, lookup)
    staged = materialize_main_stage(built)
    union_gate = finalize_stage_quality(staged, union_gate)
    raw_counts = {
        "current_rows": SOURCE_HEALTH[SRC_CURRENT]["rows"],
        "hist_rows": SOURCE_HEALTH[SRC_HIST]["rows"],
    }
    merge_plan = materialize_merge_plan(staged, full_compare=mode in FULL_MODES)
    incremental_control = prepare_incremental_control(current_source, hist_source, staged, mode)

    required_stages = {
        MAIN_STAGE, DELETE_KEYS_STAGE, PRIMARY_UPDATE_KEYS_STAGE, RESURRECTION_KEYS_STAGE, AFFECTED_IDS_STAGE,
        LIVE_CURRENT_IDS_STAGE, ABSENT_IDS_STAGE, ABSENCE_EXPECTED_STAGE,
    }
    if not required_stages.issubset(ATTEMPT_STAGE_TABLES):
        fail(f"Required stages incomplete before merge: {sorted(required_stages - ATTEMPT_STAGE_TABLES)}")
    heartbeat_run_lock("all stage gates complete before merge")

    metrics = merge_target(staged, mode, merge_plan)
    absence_metrics = apply_incremental_absence(mode)
    current_recompute = recompute_is_current(staged)
    validation = validate_after_merge(staged, mode, metrics, merge_plan, raw_counts)

    features = validate_or_repair_table_features(TARGET, repair_allowed=True)
    clustering = validate_or_repair_clustering(TARGET, repair_allowed=True)
    comments = validate_or_repair_comments(TARGET, repair_allowed=True)
    VALIDATION_RESULTS["features"][TARGET] = features
    VALIDATION_RESULTS["clustering"][TARGET] = clustering
    VALIDATION_RESULTS["comments"]["target"] = comments

    optimized = optimize_bootstrap_target() if mode == "BOOTSTRAP" else None
    post_merge_validated_evidence = {
        "status": "VALIDATED_BEFORE_SNAPSHOT_CHECKPOINT",
        **common,
        "validation_level": (
            "FULL_PARITY" if mode in FULL_MODES else "INCREMENTAL_AFFECTED_KEYS"
        ),
        "metrics": {
            **metrics,
            "incremental_absence": absence_metrics,
            "current_recompute": current_recompute,
        },
        "validation": validation,
        "snapshot": {"status": "NOT_ATTEMPTED"},
        "failure_boundary": "AFTER_MODE_SELECTION_AND_POST_MERGE_VALIDATION_BEFORE_SNAPSHOT_CHECKPOINT",
    }
    write_dev_test_audit(
        "POST_MERGE_VALIDATED", "SUCCESS", post_merge_validated_evidence
    )
    if DEV_TEST_FAIL_AFTER_VALIDATION_BEFORE_SNAPSHOT:
        injected = {
            **post_merge_validated_evidence,
            "status": "INJECTED_FAILURE",
            "dev_test_failpoint": True,
        }
        write_dev_test_audit(
            "INJECTED_FAILURE_AFTER_VALIDATION_BEFORE_SNAPSHOT_CHECKPOINT",
            "INJECTED_FAILURE",
            injected,
        )
        fail(
            "Injected dev-only failure after post-merge validation and before "
            "snapshot append/checkpoint commit"
        )

    snapshot_result = take_snapshot(snapshot_due) if snapshot_due["due"] else snapshot_result_when_not_due(snapshot_due)

    if mode in FULL_MODES and drain_gate["status"] != "PASS":
        fail("FULL-family drain accounting did not pass before checkpoint")
    if BOOTSTRAP_MODE and validation["bootstrap_ownership"]["foreign_present_rows"] != 0:
        fail("Bootstrap ownership gate failed before checkpoint")
    write_dev_test_audit(
        "PRE_CHECKPOINT",
        "SUCCESS",
        {
            **post_merge_validated_evidence,
            "status": "SNAPSHOT_COMPLETE_CHECKPOINT_PENDING",
            "snapshot": snapshot_result,
            "checkpoint_pending": True,
        },
    )
    commit_checkpoints()

    return {
        "status": "SUCCESS",
        **common,
        "metrics": {
            **metrics,
            "incremental_absence": absence_metrics,
            "current_recompute": current_recompute,
        },
        "validation": {
            "level": "FULL_PARITY" if mode in FULL_MODES else "INCREMENTAL_AFFECTED_KEYS",
            "target": validation,
            "target_preflight": target_preflight,
            "snapshot_preflight": snapshot_preflight,
            "features": features,
            "clustering": clustering,
            "comments": comments,
            "bootstrap_optimize": optimized,
            "schema_contracts": VALIDATION_RESULTS["schema"],
            "pii_inspection": VALIDATION_RESULTS["pii"],
        },
        "gates": {
            "union": union_gate,
            "decode_lookup": decode_gate,
            "incremental_control": incremental_control,
            "drain_accounting": drain_gate,
            "all_stage_validation_before_merge": True,
            "status_source_absent": True,
            "codeset_fingerprint_absent": True,
            "checkpoint_after_snapshot": True,
            "daily_incremental_preserved": True,
            "cdf_active": False,
            "snapshot_lock_serialized": True,
            "weekly_snapshot_catch_up": True,
            "mode_aware_source_metrics": True,
            "snapshot_cutoff_after_raw_pin": True,
            "mode_scoped_validation": (
                "FULL_TARGET" if mode in FULL_MODES else "AFFECTED_PM_WAIT_LIST_IDS_ONLY"
            ),
        },
        "snapshot": snapshot_result,
        "finished_at": bronze_utc_now(),
    }

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
    COMMENT 'Waiting-list bronze source-version, cadence, logic and unique-attempt lock state.'
    """
)

for _table, _columns in EXPECTED_SOURCE_COLUMNS.items():
    if not bronze_table_exists(_table):
        fail(f"Missing source {_table}")
    assert_expected_columns(_table, _columns)

acquire_run_lock()
try:
    SUMMARY = run_pipeline()
finally:
    try:
        cleanup_stages()
    finally:
        release_run_lock()

print(json.dumps(SUMMARY, indent=2, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(SUMMARY, sort_keys=True, default=str))






