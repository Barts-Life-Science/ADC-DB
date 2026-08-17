# Databricks notebook source
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

import json
import uuid
from datetime import timedelta
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

TARGET_SCHEMA = bronze_value("target_schema", "8_dev.bronze")
ALLOW_PRODUCTION_WRITE = bronze_bool("allow_production_write", False)
FORCE_FULL_REFRESH = bronze_bool("force_full_refresh", False)
FULL_RECONCILIATION = bronze_bool("full_reconciliation", False)
BOOTSTRAP_MODE = bronze_bool("bootstrap_mode", False)
RUN_ID = bronze_run_id()
ATTEMPT_ID = uuid.uuid4().hex
RUN_AS_OF = spark.sql("SELECT current_timestamp() AS ts").first()["ts"]

PIPELINE_LOGIC_VERSION = "2026.08.v1.5"
LOGIC_VERSION_INT = 2026081102
LOGIC_SOURCE = "__PIPELINE_LOGIC__"

assert not TARGET_SCHEMA.lower().startswith("4_prod") or ALLOW_PRODUCTION_WRITE, (
    "Production writes are disabled. Only an approved production orchestrator may pass "
    "allow_production_write=true."
)

RAW = "4_prod.raw"
SRC_REFERRAL = f"{RAW}.luna_referral_core"
SRC_ERS = f"{RAW}.luna_ers_referral"
SRC_RTT_CORE = f"{RAW}.luna_rtt_core"
SRC_RTT_PERIOD = f"{RAW}.luna_rtt_period"
SRC_RTT_ACTIVITY = f"{RAW}.luna_rtt_activities"
SRC_REFERENCE = f"{RAW}.luna_ref_referencevalue"
SRC_PATIENT = f"{RAW}.luna_patient_core"
SRC_ENCOUNTERS = f"{RAW}.luna_rtt_core_encounters"
SRC_PERSON_ALIAS = f"{RAW}.mill_person_alias"

MAP_REFERRAL = f"{TARGET_SCHEMA}.map_referral"
MAP_RTT_PATHWAY = f"{TARGET_SCHEMA}.map_rtt_pathway"
MAP_RTT_ACTIVITY = f"{TARGET_SCHEMA}.map_rtt_activity"
ATTEMPT_STAGE_SUFFIX = "".join(
    character if character.isalnum() else "_" for character in ATTEMPT_ID
).strip("_")[:64]
if not ATTEMPT_STAGE_SUFFIX:
    raise RuntimeError("ATTEMPT_ID did not yield a usable stage suffix")
PERSON_XWALK_STAGE = f"{TARGET_SCHEMA}.person_xwalk_stg_{ATTEMPT_STAGE_SUFFIX}"
STATE_TABLE = f"{bronze_control_schema(TARGET_SCHEMA)}.referral_rtt_pipeline_state"
DEV_TEST_AUDIT_TABLE = f"{TARGET_SCHEMA}.referral_rtt_task6_run_audit"
LOCK_KEY = "__RUN_LOCK__"
LOCK_TTL_HOURS = 12

LUNA_SOURCE_SYSTEM_OID = 1
MRN_ALIAS_TYPE_CD = 10
NHS_ALIAS_TYPE_CD = 18
MRN_ALIAS_POOL_CD = 6200990
NHS_ALIAS_POOL_CD = 3769756
XWALK_MIN_RESOLVED = 0.966183
XWALK_MIN_MRN_EDGE_RATE = 0.964631
XWALK_MIN_NHS_EDGE_RATE = 0.897790
CARRY_SCH_EVENT_ID = False
FORCE_FULL_ON_CHANGE = True

SOURCE_SLA = {
    SRC_REFERRAL: (8, "LIVE"),
    SRC_ERS: (8, "LIVE"),
    SRC_RTT_CORE: (8, "LIVE"),
    SRC_RTT_PERIOD: (8, "LIVE"),
    SRC_RTT_ACTIVITY: (8, "LIVE"),
    SRC_REFERENCE: (8, "LOOKUP"),
    SRC_PATIENT: (8, "LOOKUP"),
    SRC_ENCOUNTERS: (8, "LOOKUP"),
}
LOOKUP_SOURCES = {SRC_REFERENCE, SRC_PATIENT, SRC_ENCOUNTERS}

EXPECTED_COLUMNS = {
    SRC_REFERRAL: {
        "SourceSystemOID", "ReferralOID", "WaitingListOID", "PathwayOID", "PatientOID", "UBRN",
        "AdminCategory_RVID", "Site_RVID", "TreatmentFunction_RVID", "ServiceTypeRequested_RVID",
        "ReferralReceivedDateTime", "ReferralPriority_RVID", "ReferralSource_RVID",
        "ReferralStatus_RVID", "ReferralStatusChangeReason_RVID", "ReferralStatusChangeDateTime",
        "EncounterType_RVID", "SuspectedCancerSite_RVID", "BusinessUnit", "Division", "SourceOID",
        "OriginalReferralReceivedDateTime", "ReferredBy_OrgID", "ReferringFacility_RVID",
        "BookingType_RVID", "CreatedDateTime", "ModifiedDateTime", "ActiveInd", "ADC_UPDT",
    },
    SRC_ERS: {
        "SourceSystemOID", "ReferralOID", "UBRNReceived", "eReferralPathwayStart",
        "eRSServiceName", "eRSSpecialty", "ADC_UPDT",
    },
    SRC_RTT_CORE: {
        "SourceSystemOID", "PathwayOID", "PatientOID", "ClockStartDateTime", "ClockStopDateTime",
        "BreachDate", "DaysWaited", "DaysWaitedActive", "ReferringFacility_RVID",
        "PathwayType_RVID", "PathwayStartDate", "CurrentRTTStatus_RVID", "CreatedDateTime",
        "ModifiedDateTime", "ActiveInd", "ADC_UPDT",
    },
    SRC_RTT_PERIOD: {
        "SourceSystemOID", "PathwayOID", "PeriodOID", "StartRTTActivityOID", "StartDateTime",
        "StartRTTStatus_RVID", "ClockStartCreatedDateTime", "StopRTTActivityOID", "StopDateTime",
        "StopRTTStatus_RVID", "ClockStopCreatedDateTime", "SeqNoASC", "SeqNoDESC",
        "OPAppt_DNA_Count", "TreatmentFunction_RVID", "Site_RVID", "CreatedDateTime",
        "ModifiedDateTime", "ActiveInd", "ADC_UPDT",
    },
    SRC_RTT_ACTIVITY: {
        "SourceSystemOID", "RTTActivityOID", "PathwayOID", "SourceActivityOID", "ReferralOID",
        "Site_RVID", "TreatmentFunction_RVID", "AppointmentOID", "RTTActivity_RVID",
        "RTTActivityType_RVID", "RTTStatus_RVID", "RTTActivityDateTime",
        "RTTStatusSequenceASC", "RTTStatusSequenceDESC", "RTTActivitySequenceASC",
        "RTTActivitySequenceDESC", "IsIllogical", "BusinessUnit", "Division",
        "CreatedDateTime", "ModifiedDateTime", "ActiveInd", "ADC_UPDT",
    },
    SRC_REFERENCE: {
        "ReferenceValueId", "NationalCode", "LocalCode", "TrustDisplayValue",
        "LUNADisplayValue", "Description", "ADC_UPDT",
    },
    SRC_PATIENT: {
        "SourceSystemOID", "PatientOID", "LocalPatientID", "NHSNumber", "ADC_UPDT",
    },
    SRC_ENCOUNTERS: {"PathwayOID", "EncounterType", "ADC_UPDT"},
    SRC_PERSON_ALIAS: {
        "PERSON_ID", "ACTIVE_IND", "PERSON_ALIAS_TYPE_CD", "ALIAS_POOL_CD", "ALIAS",
        "BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM", "ADC_UPDT",
    },
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

TARGET_SOURCES = {
    MAP_REFERRAL: [SRC_REFERRAL, SRC_ERS, SRC_REFERENCE, SRC_PATIENT, LOGIC_SOURCE],
    MAP_RTT_PATHWAY: [
        SRC_RTT_CORE, SRC_RTT_PERIOD, SRC_REFERENCE, SRC_PATIENT, SRC_ENCOUNTERS, LOGIC_SOURCE
    ],
    MAP_RTT_ACTIVITY: [
        SRC_RTT_ACTIVITY, SRC_RTT_CORE, SRC_REFERENCE, SRC_PATIENT, LOGIC_SOURCE
    ],
}
TARGET_KEYS = {
    MAP_REFERRAL: ["SOURCE_SYSTEM_OID", "REFERRAL_OID"],
    MAP_RTT_PATHWAY: ["SOURCE_SYSTEM_OID", "PATHWAY_OID", "PERIOD_OID"],
    MAP_RTT_ACTIVITY: ["SOURCE_SYSTEM_OID", "RTT_ACTIVITY_OID"],
}
CLUSTER_KEYS = {
    MAP_REFERRAL: ["PERSON_ID"],
    MAP_RTT_PATHWAY: ["PATHWAY_OID"],
    MAP_RTT_ACTIVITY: ["PATHWAY_OID"],
}
FULL_MODES = {"FULL", "FULL_LOOKUP_CHANGE", "BOOTSTRAP"}

REFERRAL_DECODE_BASES = [
    "ADMIN_CATEGORY", "SITE", "TREATMENT_FUNCTION", "SERVICE_TYPE_REQUESTED",
    "REFERRAL_PRIORITY", "REFERRAL_SOURCE", "REFERRAL_STATUS",
    "REFERRAL_STATUS_CHANGE_REASON", "ENCOUNTER_TYPE", "SUSPECTED_CANCER_SITE",
    "REFERRING_FACILITY", "BOOKING_TYPE",
]
PATHWAY_DECODE_BASES = [
    "REFERRING_FACILITY", "PATHWAY_TYPE", "CURRENT_RTT_STATUS", "START_RTT_STATUS",
    "STOP_RTT_STATUS", "TREATMENT_FUNCTION", "SITE",
]
ACTIVITY_DECODE_BASES = [
    "SITE", "TREATMENT_FUNCTION", "RTT_ACTIVITY", "RTT_ACTIVITY_TYPE", "RTT_STATUS",
]

EXPECTED_TARGET_SCHEMAS = {
    MAP_REFERRAL: {
        "SOURCE_SYSTEM_OID": "bigint", "REFERRAL_OID": "bigint",
        "WAITING_LIST_OID": "bigint", "PATHWAY_OID": "bigint", "PATIENT_OID": "bigint",
        "UBRN": "string", "ADMIN_CATEGORY_RVID": "bigint", "SITE_RVID": "bigint",
        "TREATMENT_FUNCTION_RVID": "bigint", "SERVICE_TYPE_REQUESTED_RVID": "bigint",
        "REFERRAL_RECEIVED_DATETIME": "timestamp", "REFERRAL_PRIORITY_RVID": "bigint",
        "REFERRAL_SOURCE_RVID": "bigint", "REFERRAL_STATUS_RVID": "bigint",
        "REFERRAL_STATUS_CHANGE_REASON_RVID": "bigint",
        "REFERRAL_STATUS_CHANGE_DATETIME": "timestamp", "ENCOUNTER_TYPE_RVID": "bigint",
        "SUSPECTED_CANCER_SITE_RVID": "bigint", "BUSINESS_UNIT": "string",
        "DIVISION": "string", "SOURCE_OID": "bigint",
        "ORIGINAL_REFERRAL_RECEIVED_DATETIME": "timestamp", "REFERRED_BY_ORG_ID": "bigint",
        "REFERRING_FACILITY_RVID": "bigint", "BOOKING_TYPE_RVID": "bigint",
        "CREATED_DATETIME": "timestamp", "MODIFIED_DATETIME": "timestamp",
        "ACTIVE_IND": "boolean", "REFERRAL_SOURCE_ADC_UPDT": "timestamp",
        "ERS_UBRN_RECEIVED": "date", "ERS_PATHWAY_START": "date",
        "ERS_SERVICE_NAME": "string", "ERS_SPECIALTY": "string",
        "ERS_SOURCE_ADC_UPDT": "timestamp", "PERSON_ID": "bigint",
        "PERSON_LINK_METHOD": "string", "XWALK_SOURCE_ADC_UPDT": "timestamp",
        "SOURCE_ADC_UPDT": "timestamp",
    },
    MAP_RTT_PATHWAY: {
        "SOURCE_SYSTEM_OID": "bigint", "PATHWAY_OID": "bigint", "PERIOD_OID": "bigint",
        "START_RTT_ACTIVITY_OID": "bigint", "START_DATETIME": "timestamp",
        "START_RTT_STATUS_RVID": "bigint", "CLOCK_START_CREATED_DATETIME": "timestamp",
        "STOP_RTT_ACTIVITY_OID": "bigint", "STOP_DATETIME": "timestamp",
        "STOP_RTT_STATUS_RVID": "bigint", "CLOCK_STOP_CREATED_DATETIME": "timestamp",
        "SEQ_NO_ASC": "bigint", "SEQ_NO_DESC": "bigint", "OP_APPT_DNA_COUNT": "bigint",
        "TREATMENT_FUNCTION_RVID": "bigint", "SITE_RVID": "bigint",
        "PERIOD_CREATED_DATETIME": "timestamp", "PERIOD_MODIFIED_DATETIME": "timestamp",
        "PERIOD_ACTIVE_IND": "boolean", "PERIOD_SOURCE_ADC_UPDT": "timestamp",
        "PATIENT_OID": "bigint", "BREACH_DATE": "timestamp", "DAYS_WAITED": "bigint",
        "DAYS_WAITED_ACTIVE": "bigint", "REFERRING_FACILITY_RVID": "bigint",
        "PATHWAY_TYPE_RVID": "bigint", "PATHWAY_START_DATE": "date",
        "CURRENT_RTT_STATUS_RVID": "bigint", "CORE_CREATED_DATETIME": "timestamp",
        "CORE_MODIFIED_DATETIME": "timestamp", "CORE_ACTIVE_IND": "boolean",
        "CORE_SOURCE_ADC_UPDT": "timestamp", "IS_LATEST_PERIOD": "boolean",
        "CLOCK_DISCREPANT": "boolean", "CORE_CLOCK_START_DT_TM": "timestamp",
        "CORE_CLOCK_STOP_DT_TM": "timestamp", "ENCOUNTER_TYPES": "array<string>",
        "TAG_SOURCE_ADC_UPDT": "timestamp", "PERSON_ID": "bigint",
        "PERSON_LINK_METHOD": "string", "XWALK_SOURCE_ADC_UPDT": "timestamp",
        "SOURCE_ADC_UPDT": "timestamp",
    },
    MAP_RTT_ACTIVITY: {
        "SOURCE_SYSTEM_OID": "bigint", "RTT_ACTIVITY_OID": "bigint",
        "PATHWAY_OID": "bigint", "SOURCE_ACTIVITY_OID": "bigint",
        "REFERRAL_OID": "bigint", "SITE_RVID": "bigint",
        "TREATMENT_FUNCTION_RVID": "bigint", "APPOINTMENT_OID": "bigint",
        "RTT_ACTIVITY_RVID": "bigint", "RTT_ACTIVITY_TYPE_RVID": "bigint",
        "RTT_STATUS_RVID": "bigint", "RTT_STATUS_SEQUENCE_ASC": "bigint",
        "RTT_STATUS_SEQUENCE_DESC": "bigint", "RTT_ACTIVITY_SEQUENCE_ASC": "bigint",
        "RTT_ACTIVITY_SEQUENCE_DESC": "bigint", "IS_ILLOGICAL": "boolean",
        "BUSINESS_UNIT": "string", "DIVISION": "string", "CREATED_DATETIME": "timestamp",
        "MODIFIED_DATETIME": "timestamp", "ACTIVE_IND": "boolean",
        "ACTIVITY_SOURCE_ADC_UPDT": "timestamp", "CORE_SOURCE_ADC_UPDT": "timestamp",
        "PERSON_ID": "bigint", "PERSON_LINK_METHOD": "string",
        "XWALK_SOURCE_ADC_UPDT": "timestamp", "SOURCE_ADC_UPDT": "timestamp",
        "RTT_ACTIVITY_DATETIME_QUALITY": "string",
        "RTT_ACTIVITY_DATETIME": "timestamp",
    },
}
for _target, _bases in {
    MAP_REFERRAL: REFERRAL_DECODE_BASES,
    MAP_RTT_PATHWAY: PATHWAY_DECODE_BASES,
    MAP_RTT_ACTIVITY: ACTIVITY_DECODE_BASES,
}.items():
    for _base in _bases:
        EXPECTED_TARGET_SCHEMAS[_target][f"{_base}_CD"] = "string"
        EXPECTED_TARGET_SCHEMAS[_target][f"{_base}_LOCAL_CD"] = "string"
        EXPECTED_TARGET_SCHEMAS[_target][f"{_base}_DESC"] = "string"
    EXPECTED_TARGET_SCHEMAS[_target].update({
        "ROW_HASH": "bigint",
        "PIPELINE_RUN_ID": "string",
        "SOURCE_PRESENT_IND": "boolean",
        "SOURCE_ABSENT_DETECTED_TS": "timestamp",
        "ADC_UPDT": "timestamp",
    })

STATE_CACHE: dict[tuple[str, str], dict] = {}
SOURCE_VERSIONS: dict[str, int] = {}
PINNED_SOURCE_FRAMES: dict[str, DataFrame] = {}
PINNED_ALIAS_FRAME: DataFrame | None = None
SOURCE_HEALTH: dict[str, dict] = {}
ATTEMPT_STAGE_TABLES: set[str] = set()
VALIDATION_RESULTS = {
    "schema": {"stages": {}, "preexisting_targets": {}, "targets": {}},
    "pii": {"stages": {}, "preexisting_targets": {}, "targets": {}},
    "clustering": {},
    "target_preflight": {},
}
ALIAS_VERSION = None
ALIAS_LIVE_VERSION = None
RUN_FUTURE_HORIZON = None

BANNED_EXACT_COLUMNS = {
    "MRN", "MRN_NORM", "NHS_NUMBER", "NHS_NORM", "LOCAL_PATIENT_ID", "LEGACY_PATIENT_ID",
    "PATIENT_NAME", "FORENAME", "SURNAME", "DATE_OF_BIRTH", "DOB", "POSTCODE",
    "ADDRESS_LINE_1", "ADDRESS_LINE_2", "ADDRESS_LINE_3", "ADDRESS_LINE_4",
    "TELEPHONE_HOME", "TELEPHONE_MOBILE", "REFERRAL_REASON", "REFERRAL_COMMENTS",
    "REFERRED_BY_CARE_PROVIDER_FORENAME", "REFERRED_BY_CARE_PROVIDER_SURNAME",
    "REFERRED_TO_CARE_PROVIDER_FORENAME", "REFERRED_TO_CARE_PROVIDER_SURNAME",
    "LEAD_CLINICIAN_FORENAME", "LEAD_CLINICIAN_SURNAME", "REFERRED_BY_TEAM_NAME",
    "REFERRED_TO_TEAM_NAME", "REFERRED_BY_ORGANISATION",
}


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


def assert_no_banned_columns(df: DataFrame, label: str) -> dict:
    heartbeat_run_lock(f"PII validation {label}")
    actual = {column.upper() for column in df.columns}
    exact = sorted(actual & BANNED_EXACT_COLUMNS)
    token_banned = sorted(
        column for column in actual
        if any(token in column for token in ("PATIENT_NAME", "FORENAME", "SURNAME", "ADDRESS_", "TELEPHONE_"))
    )
    banned = sorted(set(exact + token_banned))
    if banned:
        fail(f"{label}: banned PII/free-text columns present: {banned}")
    return {
        "status": "PASS",
        "inspected_column_count": len(df.columns),
        "banned_columns": banned,
    }


def validate_schema_contract(
    df: DataFrame,
    target: str,
    label: str,
    expected_override: dict[str, str] | None = None,
    enforce_order: bool = True,
) -> dict:
    heartbeat_run_lock(f"schema validation {label}")
    expected = expected_override or EXPECTED_TARGET_SCHEMAS[target]
    expected_by_lower = {name.lower(): (name, data_type) for name, data_type in expected.items()}
    actual_by_lower = {}
    duplicate_names = []
    actual_order = []
    for field in df.schema.fields:
        actual_order.append(field.name)
        lowered = field.name.lower()
        if lowered in actual_by_lower:
            duplicate_names.append([actual_by_lower[lowered][0], field.name])
        else:
            actual_by_lower[lowered] = (field.name, field.dataType.simpleString().lower())
    expected_order = list(expected)
    missing = sorted(expected_by_lower[key][0] for key in expected_by_lower.keys() - actual_by_lower.keys())
    extra = sorted(actual_by_lower[key][0] for key in actual_by_lower.keys() - expected_by_lower.keys())
    case_mismatches = sorted([
        {"expected": expected_by_lower[key][0], "actual": actual_by_lower[key][0]}
        for key in expected_by_lower.keys() & actual_by_lower.keys()
        if expected_by_lower[key][0] != actual_by_lower[key][0]
    ], key=lambda item: item["expected"])
    type_mismatches = sorted([
        {"column": expected_by_lower[key][0], "expected": expected_by_lower[key][1], "actual": actual_by_lower[key][1]}
        for key in expected_by_lower.keys() & actual_by_lower.keys()
        if expected_by_lower[key][1] != actual_by_lower[key][1]
    ], key=lambda item: item["column"])
    order_matches = actual_order == expected_order
    if (
        duplicate_names
        or missing
        or extra
        or case_mismatches
        or type_mismatches
        or (enforce_order and not order_matches)
    ):
        fail(
            f"{label}: schema contract failed; duplicate_case_insensitive={duplicate_names}, "
            f"missing={missing}, extra={extra}, case_mismatches={case_mismatches}, "
            f"type_mismatches={type_mismatches}, expected_order={expected_order}, actual_order={actual_order}"
        )
    return {
        "status": "PASS",
        "expected_column_count": len(expected),
        "actual_column_count": len(df.schema.fields),
        "case_insensitive_duplicates": duplicate_names,
        "missing_columns": missing,
        "extra_columns": extra,
        "case_mismatches": case_mismatches,
        "type_mismatches": type_mismatches,
        "ordered_fields_match": order_matches,
        "order_enforced": enforce_order,
    }


def assert_unique_non_null(df: DataFrame, keys: list[str], label: str) -> None:
    heartbeat_run_lock(f"key validation {label}")
    null_condition = reduce(lambda left, right: left | right, [F.col(key).isNull() for key in keys])
    if df.where(null_condition).limit(1).count():
        fail(f"{label}: NULL key detected for {keys}")
    if df.groupBy(*keys).count().where(F.col("count") > 1).limit(1).count():
        fail(f"{label}: duplicate key detected for {keys}")


def validate_bigint_casts(df: DataFrame, columns: list[str], label: str) -> dict:
    fields = {field.name: field.dataType.simpleString().lower() for field in df.schema.fields}
    aggregates = []
    for column in columns:
        if column not in fields:
            fail(f"{label}: missing cast-gate column {column}")
        parsed = F.expr(f"try_cast({qident(column)} AS BIGINT)")
        invalid = F.col(column).isNotNull() & parsed.isNull()
        if fields[column] in {"double", "float"} or fields[column].startswith("decimal"):
            invalid = invalid | (
                F.col(column).isNotNull() & parsed.isNotNull()
                & (F.col(column).cast("double") != parsed.cast("double"))
            )
        aggregates.append(F.sum(F.when(invalid, F.lit(1)).otherwise(F.lit(0))).alias(column))
    heartbeat_run_lock(f"type gate {label}")
    row = df.agg(*aggregates).collect()[0]
    invalid_counts = {column: int(row[column] or 0) for column in columns}
    failures = {column: count for column, count in invalid_counts.items() if count}
    if failures:
        fail(f"{label}: BIGINT parse/integrality/range failures {failures}")
    return {
        "status": "PASS",
        "source_types": {column: fields[column] for column in columns},
        "invalid_counts": invalid_counts,
    }


def source_version(table: str) -> int:
    if table == LOGIC_SOURCE:
        return LOGIC_VERSION_INT
    return int(spark.sql(f"DESCRIBE HISTORY {qname(table)} LIMIT 1").collect()[0]["version"])


def _staleness_days(watermark) -> float | None:
    if watermark is None:
        return None
    now = RUN_AS_OF.replace(tzinfo=None) if getattr(RUN_AS_OF, "tzinfo", None) else RUN_AS_OF
    raw = watermark.replace(tzinfo=None) if getattr(watermark, "tzinfo", None) else watermark
    return (now - raw).total_seconds() / 86400.0


def pinned_source(table: str) -> DataFrame:
    frame = PINNED_SOURCE_FRAMES.get(table)
    if frame is None:
        fail(f"{table}: tracked source content requested before versionAsOf pin")
    return frame


def source_health(table: str) -> dict:
    row = pinned_source(table).agg(
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
    return source_health(table)


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


def last_checkpoint(target: str, source: str) -> dict | None:
    return STATE_CACHE.get((target, source))


def choose_mode(target: str, sources: list[str]) -> str:
    if BOOTSTRAP_MODE:
        return "BOOTSTRAP"
    if FORCE_FULL_REFRESH or FULL_RECONCILIATION or not bronze_table_exists(target):
        return "FULL"

    changed = []
    for source in sources:
        previous = last_checkpoint(target, source)
        if previous is None:
            return "FULL"
        if int(previous["source_version"]) != int(SOURCE_VERSIONS[source]):
            changed.append(source)

    if LOGIC_SOURCE in changed:
        return "FULL"
    if any(source in LOOKUP_SOURCES for source in changed):
        return "FULL_LOOKUP_CHANGE"
    if changed:
        return "FULL" if FORCE_FULL_ON_CHANGE else "INCREMENTAL"
    return "UNCHANGED_SKIP"


def source_mode(source: str, modes: dict[str, str]) -> str:
    consuming_modes = [mode for target, mode in modes.items() if source in TARGET_SOURCES[target]]
    if any(mode in FULL_MODES for mode in consuming_modes):
        return "FULL"
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


def table_feature_state(table: str) -> dict:
    properties = spark.sql(f"DESCRIBE DETAIL {qname(table)}").collect()[0]["properties"] or {}
    return {
        property_name: str(properties.get(property_name, "false")).lower() == "true"
        for property_name in REQUIRED_DELTA_FEATURES
    }


def validate_or_repair_table_features(table: str, repair_allowed: bool) -> dict:
    heartbeat_run_lock(f"Delta feature validation {table}")
    before = table_feature_state(table)
    missing_before = sorted(name for name, enabled in before.items() if not enabled)
    altered = False
    if missing_before:
        if not repair_allowed:
            fail(f"{table}: required Delta features missing on skipped target: {missing_before}")
        spark.sql(
            f"ALTER TABLE {qname(table)} SET TBLPROPERTIES ("
            "'delta.enableChangeDataFeed'='true',"
            "'delta.enableRowTracking'='true',"
            "'delta.enableDeletionVectors'='true')"
        )
        altered = True
    after = table_feature_state(table)
    missing_after = sorted(name for name, enabled in after.items() if not enabled)
    if missing_after:
        fail(f"{table}: required Delta features remain disabled: {missing_after}")
    return {
        "status": "PASS",
        "repair_allowed": repair_allowed,
        "before": before,
        "after": after,
        "altered": altered,
    }


def ensure_table_features(table: str) -> dict:
    return validate_or_repair_table_features(table, repair_allowed=True)


def clustering_columns(table: str) -> tuple[list[str], list[str]]:
    detail = spark.sql(f"DESCRIBE DETAIL {qname(table)}").collect()[0].asDict(recursive=True)
    raw = [str(column) for column in (detail.get("clusteringColumns") or [])]
    normalized = [column.strip().strip(chr(96)).upper() for column in raw]
    return raw, normalized


def ensure_clustering_statistics(target: str, repair_allowed: bool) -> dict:
    heartbeat_run_lock(f"clustering statistics {target}")
    detail = spark.sql(f"DESCRIBE DETAIL {qname(target)}").collect()[0].asDict(recursive=True)
    properties = detail.get("properties") or {}
    frame = spark.table(target)
    fields_by_upper = {field.name.upper(): field for field in frame.schema.fields}
    stats_supported_types = {
        "byte", "short", "integer", "long", "float", "double",
        "decimal", "string", "date", "timestamp", "timestamp_ntz",
    }

    explicit = str(properties.get("delta.dataSkippingStatsColumns", "")).strip()
    removed_columns = []
    if explicit:
        before = [column.strip() for column in explicit.split(",") if column.strip()]
        sanitized = []
        seen = set()
        for configured in before:
            normalized = configured.strip().strip(chr(96)).upper()
            field = fields_by_upper.get(normalized)
            if field is None:
                removed_columns.append({"column": configured, "reason": "MISSING_TARGET_COLUMN"})
                continue
            if field.dataType.typeName() not in stats_supported_types:
                removed_columns.append({
                    "column": field.name,
                    "reason": f"UNSUPPORTED_TYPE:{field.dataType.typeName()}",
                })
                continue
            if field.name.upper() not in seen:
                sanitized.append(field.name)
                seen.add(field.name.upper())
        source = "EXPLICIT"
    else:
        indexed = int(properties.get("delta.dataSkippingNumIndexedCols", 32) or 32)
        before = [
            field.name for field in frame.schema.fields[:indexed]
            if field.dataType.typeName() in stats_supported_types
        ]
        sanitized = list(before)
        seen = {column.upper() for column in sanitized}
        source = f"DEFAULT_SUPPORTED_FIRST_{indexed}"

    missing = []
    for desired in CLUSTER_KEYS[target]:
        field = fields_by_upper.get(desired.upper())
        if field is None:
            fail(f"{target}: clustering statistics column {desired} is missing from target schema")
        if field.dataType.typeName() not in stats_supported_types:
            fail(
                f"{target}: clustering statistics column {field.name} has unsupported type "
                f"{field.dataType.typeName()}"
            )
        if field.name.upper() not in seen:
            sanitized.append(field.name)
            seen.add(field.name.upper())
            missing.append(field.name)

    before_normalized = [column.strip().strip(chr(96)).upper() for column in before]
    after_normalized = [column.upper() for column in sanitized]
    needs_repair = bool(missing or removed_columns or before_normalized != after_normalized)
    if needs_repair:
        if not repair_allowed:
            fail(
                f"{target}: data-skipping stats require repair but mode forbids writes; "
                f"removed={removed_columns}, missing_clustering_columns={missing}, "
                f"before={before}, after={sanitized}"
            )
        if not sanitized:
            fail(f"{target}: no supported data-skipping statistics columns remain after sanitization")
        stats_value = ",".join(sanitized)
        spark.sql(
            f"ALTER TABLE {qname(target)} SET TBLPROPERTIES "
            f"('delta.dataSkippingStatsColumns' = '{sql_escape(stats_value)}')"
        )
        heartbeat_run_lock(f"Delta statistics recompute {target}")
        spark.sql(f"ANALYZE TABLE {qname(target)} COMPUTE DELTA STATISTICS")

    return {
        "status": "PASS",
        "source": source,
        "repair_allowed": repair_allowed,
        "before_columns": before,
        "after_columns": sanitized,
        "removed_columns": removed_columns,
        "added_clustering_columns": missing,
        "altered": needs_repair,
        "delta_statistics_recomputed": needs_repair,
    }

def validate_or_repair_clustering(target: str, repair_allowed: bool) -> dict:
    heartbeat_run_lock(f"clustering validation {target}")
    desired = [column.upper() for column in CLUSTER_KEYS[target]]
    before_raw, before = clustering_columns(target)
    altered = False
    statistics = ensure_clustering_statistics(target, repair_allowed=repair_allowed)
    if before != desired:
        if not repair_allowed:
            fail(
                f"{target}: skipped target clustering {before_raw} normalizes to {before}, "
                f"expected {desired}; skip mode cannot repair drift"
            )
        cluster_columns = ", ".join(qident(column) for column in CLUSTER_KEYS[target])
        spark.sql(f"ALTER TABLE {qname(target)} CLUSTER BY ({cluster_columns})")
        altered = True
    after_raw, after = clustering_columns(target)
    if after != desired:
        fail(f"{target}: clustering columns {after_raw} normalize to {after}, expected {desired}")
    return {
        "status": "PASS",
        "repair_allowed": repair_allowed,
        "desired_columns": CLUSTER_KEYS[target],
        "before_columns": before_raw,
        "after_columns": after_raw,
        "statistics": statistics,
        "altered": altered,
    }


def ensure_and_validate_clustering(target: str) -> dict:
    result = validate_or_repair_clustering(target, repair_allowed=True)
    VALIDATION_RESULTS["clustering"][target] = result
    return result


def validate_existing_target_preflight(target: str, mode: str) -> dict:
    heartbeat_run_lock(f"target preflight {target}")
    exists = bronze_table_exists(target)
    if not exists:
        if mode == "UNCHANGED_SKIP":
            fail(f"{target}: UNCHANGED_SKIP selected but target does not exist")
        result = {
            "status": "NOT_PRESENT_NON_SKIP",
            "mode": mode,
            "target_exists": False,
            "repair_allowed": True,
            "schema": None,
            "pii": None,
            "delta_features": None,
            "clustering": None,
        }
        VALIDATION_RESULTS["target_preflight"][target] = result
        return result

    frame = spark.table(target)
    schema_result = validate_schema_contract(frame, target, f"preflight {target}")
    pii_result = assert_no_banned_columns(frame, f"preflight {target}")
    VALIDATION_RESULTS["schema"]["preexisting_targets"][target] = schema_result
    VALIDATION_RESULTS["pii"]["preexisting_targets"][target] = pii_result

    repair_allowed = mode != "UNCHANGED_SKIP"
    feature_result = validate_or_repair_table_features(target, repair_allowed=repair_allowed)
    clustering_result = validate_or_repair_clustering(target, repair_allowed=repair_allowed)
    result = {
        "status": "PASS",
        "mode": mode,
        "target_exists": True,
        "repair_allowed": repair_allowed,
        "schema": schema_result,
        "pii": pii_result,
        "delta_features": feature_result,
        "clustering": clustering_result,
    }
    VALIDATION_RESULTS["target_preflight"][target] = result
    return result


def materialize_stage(df: DataFrame, target: str, keys: list[str]) -> DataFrame:
    metadata_columns = {
        "ROW_HASH", "PIPELINE_RUN_ID", "SOURCE_PRESENT_IND",
        "SOURCE_ABSENT_DETECTED_TS", "ADC_UPDT",
    }
    business_contract = {
        name: data_type
        for name, data_type in EXPECTED_TARGET_SCHEMAS[target].items()
        if name not in metadata_columns
    }
    validate_schema_contract(
        df,
        target,
        f"pre-stage unordered {target}",
        business_contract,
        enforce_order=False,
    )
    assert_no_banned_columns(df, f"pre-stage {target}")
    ordered = df.select(*business_contract.keys())
    validate_schema_contract(
        ordered,
        target,
        f"pre-stage ordered {target}",
        business_contract,
        enforce_order=True,
    )
    staging_table = f"{target}_stg_{ATTEMPT_STAGE_SUFFIX}"
    staged = (
        with_row_hash(ordered)
        .withColumn("PIPELINE_RUN_ID", F.lit(RUN_ID))
        .withColumn("SOURCE_PRESENT_IND", F.lit(True))
        .withColumn("SOURCE_ABSENT_DETECTED_TS", F.lit(None).cast("timestamp"))
        .withColumn("ADC_UPDT", F.lit(RUN_AS_OF).cast("timestamp"))
    )
    heartbeat_run_lock(f"stage write {target}")
    ATTEMPT_STAGE_TABLES.add(staging_table)
    (
        staged.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(staging_table)
    )
    heartbeat_run_lock(f"stage validation {target}")
    snapshot = spark.table(staging_table)
    VALIDATION_RESULTS["schema"]["stages"][target] = validate_schema_contract(
        snapshot, target, f"staged {target}"
    )
    VALIDATION_RESULTS["pii"]["stages"][target] = assert_no_banned_columns(
        snapshot, f"staged {target}"
    )
    assert_unique_non_null(snapshot, keys, f"staged {target}")
    return snapshot


def prepare_merge_plan(target: str, staged: DataFrame, keys: list[str], full_compare: bool) -> dict:
    heartbeat_run_lock(f"merge-plan stage {target}")
    if bronze_table_exists(target):
        current = spark.table(target)
        delete_keys = (
            current.where(F.col("SOURCE_PRESENT_IND")).select(*keys)
            .join(staged.select(*keys), keys, "left_anti")
            if full_compare else staged.select(*keys).limit(0)
        )
        resurrection_keys = (
            current.where(~F.col("SOURCE_PRESENT_IND")).select(*keys)
            .join(staged.select(*keys), keys, "inner")
        )
    else:
        delete_keys = staged.select(*keys).limit(0)
        resurrection_keys = staged.select(*keys).limit(0)

    delete_table = f"{target}_delete_keys_stg_{ATTEMPT_STAGE_SUFFIX}"
    resurrection_table = f"{target}_resurrection_keys_stg_{ATTEMPT_STAGE_SUFFIX}"
    for frame, table, label in (
        (delete_keys, delete_table, "delete"),
        (resurrection_keys, resurrection_table, "resurrection"),
    ):
        heartbeat_run_lock(f"{label}-key stage write {target}")
        ATTEMPT_STAGE_TABLES.add(table)
        frame.dropDuplicates(keys).write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(table)
    delete_snapshot = spark.table(delete_table)
    resurrection_snapshot = spark.table(resurrection_table)
    assert_unique_non_null(delete_snapshot, keys, f"{target} delete-key stage")
    assert_unique_non_null(resurrection_snapshot, keys, f"{target} resurrection-key stage")
    return {
        "delete_table": delete_table,
        "resurrection_table": resurrection_table,
        "expected_soft_deletes": int(delete_snapshot.count()),
        "resurrection_candidates": int(resurrection_snapshot.count()),
    }


def merge_target(
    staged: DataFrame,
    target: str,
    keys: list[str],
    full_compare: bool,
    merge_plan: dict,
) -> dict:
    heartbeat_run_lock(f"merge {target}")
    staged_rows = staged.count()
    existed = bronze_table_exists(target)
    if not existed:
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
        builder = (
            DeltaTable.forName(spark, target)
            .alias("t")
            .merge(staged.alias("s"), condition)
        )
        if BOOTSTRAP_MODE:
            # A successful bootstrap attempt must own and restamp every present row,
            # including identical rows left by an interrupted uncheckpointed attempt.
            builder = builder.whenMatchedUpdate(set=values)
            matched_update_policy = "BOOTSTRAP_FORCE_RESTAMP_ALL_MATCHED"
        else:
            builder = builder.whenMatchedUpdate(
                condition="NOT (t.ROW_HASH <=> s.ROW_HASH) OR t.SOURCE_PRESENT_IND = false",
                set=values,
            )
            matched_update_policy = "ROW_HASH_OR_RESURRECTION"
        builder = builder.whenNotMatchedInsert(values=values)
        if full_compare:
            builder = builder.whenNotMatchedBySourceUpdate(
                condition="t.SOURCE_PRESENT_IND = true",
                set={
                    "SOURCE_PRESENT_IND": "false",
                    "SOURCE_ABSENT_DETECTED_TS": f"TIMESTAMP '{RUN_AS_OF}'",
                    "PIPELINE_RUN_ID": f"'{sql_escape(RUN_ID)}'",
                    "ADC_UPDT": f"TIMESTAMP '{RUN_AS_OF}'",
                },
            )
        builder.execute()
        operation = "MERGE"
    if not existed:
        matched_update_policy = "CREATE_INSERT_ALL"
    ensure_table_features(target)
    return {
        "operation": operation,
        "matched_update_policy": matched_update_policy,
        "staged_rows": int(staged_rows),
        "expected_soft_deletes": int(merge_plan["expected_soft_deletes"]),
        "resurrection_candidates": int(merge_plan["resurrection_candidates"]),
    }


def validate_target_after_merge(
    target: str,
    staged: DataFrame,
    metrics: dict,
    merge_plan: dict,
) -> dict:
    heartbeat_run_lock(f"final validation {target}")
    keys = TARGET_KEYS[target]
    target_frame = spark.table(target)
    VALIDATION_RESULTS["schema"]["targets"][target] = validate_schema_contract(
        target_frame, target, target
    )
    VALIDATION_RESULTS["pii"]["targets"][target] = assert_no_banned_columns(target_frame, target)
    assert_unique_non_null(target_frame, keys, target)
    present = target_frame.where(F.col("SOURCE_PRESENT_IND"))
    present_rows = present.count()
    staged_rows = staged.count()
    if present_rows != staged_rows:
        fail(f"{target}: present/stage parity {present_rows} != {staged_rows}")

    bootstrap_ownership = {
        "enforced": bool(BOOTSTRAP_MODE),
        "run_id": RUN_ID if BOOTSTRAP_MODE else None,
        "present_rows": int(present_rows),
        "owned_present_rows": None,
        "foreign_present_rows": None,
    }
    if BOOTSTRAP_MODE:
        foreign_present_rows = present.where(
            ~F.col("PIPELINE_RUN_ID").eqNullSafe(F.lit(RUN_ID))
        ).count()
        owned_present_rows = present_rows - foreign_present_rows
        bootstrap_ownership.update({
            "owned_present_rows": int(owned_present_rows),
            "foreign_present_rows": int(foreign_present_rows),
        })
        if foreign_present_rows:
            fail(
                f"{target}: BOOTSTRAP ownership failed; {foreign_present_rows} present rows "
                f"do not carry PIPELINE_RUN_ID={RUN_ID}"
            )

    condition = reduce(
        lambda left, right: left & right,
        [F.col(f"t.{key}") == F.col(f"s.{key}") for key in keys],
    )
    hash_matches = (
        present.alias("t").join(staged.alias("s"), condition, "inner")
        .where(F.col("t.ROW_HASH").eqNullSafe(F.col("s.ROW_HASH"))).count()
    )
    if hash_matches != staged_rows:
        fail(f"{target}: row-hash parity {hash_matches} != {staged_rows}")

    expected_delete = spark.table(merge_plan["delete_table"])
    expected_resurrection = spark.table(merge_plan["resurrection_table"])
    still_present = expected_delete.join(
        target_frame.where(F.col("SOURCE_PRESENT_IND")).select(*keys), keys, "inner"
    ).count()
    deleted_readback = expected_delete.join(
        target_frame.where(~F.col("SOURCE_PRESENT_IND")).select(*keys), keys, "inner"
    ).count()
    if still_present or deleted_readback != int(metrics["expected_soft_deletes"]):
        fail(
            f"{target}: exact soft-delete key validation failed; still_present={still_present}, "
            f"deleted_readback={deleted_readback}, expected={metrics['expected_soft_deletes']}"
        )
    resurrected_present = expected_resurrection.join(
        target_frame.where(F.col("SOURCE_PRESENT_IND")).select(*keys), keys, "inner"
    ).count()
    if resurrected_present != int(metrics["resurrection_candidates"]):
        fail(
            f"{target}: exact resurrection key validation {resurrected_present} != "
            f"{metrics['resurrection_candidates']}"
        )
    return {
        "present_rows": int(present_rows),
        "staged_rows": int(staged_rows),
        "row_hash_matches": int(hash_matches),
        "soft_deletes_verified": int(deleted_readback),
        "resurrection_candidates": int(metrics["resurrection_candidates"]),
        "resurrection_readback_present": int(resurrected_present),
        "bootstrap_ownership": bootstrap_ownership,
        "retry_validation_basis": "EXACT_EXPECTED_KEY_STAGES",
    }


def commit_checkpoints(target_sources: dict[str, list[str]]) -> None:
    heartbeat_run_lock("checkpoint commit")
    rows = []
    for target, sources in target_sources.items():
        for source in sources:
            health = SOURCE_HEALTH[source]
            rows.append((
                target,
                source,
                int(health["version"]),
                health["watermark"],
                int(health["rows"]) if health["rows"] is not None else None,
                RUN_ID,
            ))
    if not rows:
        return

    checkpoint_committed_at = spark.sql(
        "SELECT current_timestamp() AS ts"
    ).first()["ts"]
    updates = spark.createDataFrame(
        rows,
        "target_table string, source_table string, source_version long, "
        "source_watermark timestamp, source_rows long, run_id string",
    ).withColumn(
        "committed_at", F.lit(checkpoint_committed_at).cast("timestamp")
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
    for target, sources in target_sources.items():
        declared = "', '".join(sql_escape(source) for source in sources)
        spark.sql(
            f"DELETE FROM {qname(STATE_TABLE)} WHERE target_table = '{sql_escape(target)}' "
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
            f"Another referral_rtt_pipeline attempt holds the lock "
            f"(lock_rows={len(owners)}, attempt_id={owner}, "
            f"orchestration_run_id={RUN_ID}, TTL={LOCK_TTL_HOURS}h)."
        )


def heartbeat_run_lock(label: str) -> None:
    owner_rows = spark.table(STATE_TABLE).where(
        (F.col("target_table") == LOCK_KEY) & (F.col("source_table") == LOCK_KEY)
    ).collect()
    if len(owner_rows) != 1 or owner_rows[0]["run_id"] != ATTEMPT_ID:
        observed = owner_rows[0]["run_id"] if owner_rows else None
        fail(
            f"Lost referral_rtt_pipeline lock before {label}; "
            f"attempt_id={ATTEMPT_ID}, observed_owner={observed}"
        )
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
        fail(f"Lost referral_rtt_pipeline lock while heartbeating before {label}")


def release_run_lock() -> None:
    spark.sql(
        f"DELETE FROM {qname(STATE_TABLE)} WHERE target_table = '{LOCK_KEY}' "
        f"AND source_table = '{LOCK_KEY}' AND run_id = '{sql_escape(ATTEMPT_ID)}'"
    )


def cleanup_stages() -> None:
    for table in sorted(ATTEMPT_STAGE_TABLES):
        spark.sql(f"DROP TABLE IF EXISTS {qname(table)}")


def validate_reference_lookup() -> tuple[DataFrame, dict]:
    heartbeat_run_lock("reference lookup validation")
    source = pinned_source(SRC_REFERENCE)
    lookup = source.select(
        F.expr("try_cast(ReferenceValueId AS BIGINT)").alias("__RVID_KEY"),
        F.col("ReferenceValueId").alias("__RVID_RAW"),
        F.col("NationalCode").alias("__NATIONAL_CODE"),
        F.col("LocalCode").alias("__LOCAL_CODE"),
        F.coalesce("TrustDisplayValue", "LUNADisplayValue", "Description").alias("__DESCRIPTION"),
    )
    stats = lookup.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.col("__RVID_RAW").isNotNull().cast("long")).alias("raw_non_null"),
        F.sum(F.col("__RVID_KEY").isNull().cast("long")).alias("casted_null_ids"),
        F.countDistinct("__RVID_KEY").alias("distinct_ids"),
    ).collect()[0]
    duplicate_groups = lookup.groupBy("__RVID_KEY").count().where(F.col("count") > 1).limit(1).count()
    if (
        stats["raw_non_null"] != stats["rows"]
        or stats["casted_null_ids"] != 0
        or stats["rows"] != stats["distinct_ids"]
        or duplicate_groups
    ):
        fail(f"{SRC_REFERENCE}: casted ReferenceValueId must be non-null and globally unique: {stats}")
    return lookup.drop("__RVID_RAW"), {
        "status": "PASS",
        "rows": int(stats["rows"]),
        "raw_non_null": int(stats["raw_non_null"]),
        "casted_null_ids": int(stats["casted_null_ids"]),
        "distinct_ids": int(stats["distinct_ids"]),
        "duplicate_groups": int(duplicate_groups),
        "validated_key": "__RVID_KEY",
    }


def rv_decode(df: DataFrame, rvid_column: str, base: str, lookup: DataFrame) -> DataFrame:
    key = f"__{base}_RVID_KEY"
    national = f"__{base}_NATIONAL"
    local = f"__{base}_LOCAL"
    description = f"__{base}_DESCRIPTION"
    lane = F.broadcast(lookup.select(
        F.col("__RVID_KEY").alias(key),
        F.col("__NATIONAL_CODE").alias(national),
        F.col("__LOCAL_CODE").alias(local),
        F.col("__DESCRIPTION").alias(description),
    ))
    return (
        df.join(lane, F.col(rvid_column).cast("long") == F.col(key), "left")
        .withColumn(f"{base}_CD", F.col(national))
        .withColumn(f"{base}_LOCAL_CD", F.col(local))
        .withColumn(f"{base}_DESC", F.col(description))
        .drop(key, national, local, description)
    )


def nhs_valid(column):
    value = F.regexp_replace(column, r"\s", "")
    digits_ok = value.rlike(r"^[0-9]{10}$")
    weighted = reduce(
        lambda left, right: left + right,
        [F.substring(value, index + 1, 1).cast("int") * (10 - index) for index in range(9)],
    )
    check = 11 - (weighted % 11)
    check = F.when(check == 11, F.lit(0)).otherwise(check)
    return digits_ok & (check != 10) & (check == F.substring(value, 10, 1).cast("int"))


def patient_source_gates() -> dict:
    patient = pinned_source(SRC_PATIENT)
    summary = patient.agg(
        F.count(F.lit(1)).alias("source_rows"),
        F.collect_set(F.when(F.col("SourceSystemOID").isNotNull(), F.col("SourceSystemOID"))).alias("systems"),
        F.sum(F.col("PatientOID").isNull().cast("long")).alias("null_patient_oid"),
        F.sum(F.col("SourceSystemOID").isNull().cast("long")).alias("null_source_rows"),
        F.countDistinct(
            F.when(F.col("SourceSystemOID").isNull(), F.col("PatientOID"))
        ).alias("null_source_distinct"),
    ).collect()[0]
    populated_systems = sorted(int(value) for value in summary["systems"])
    if populated_systems != [LUNA_SOURCE_SYSTEM_OID]:
        fail(f"{SRC_PATIENT}: populated SourceSystemOID set {populated_systems} != {[LUNA_SOURCE_SYSTEM_OID]}")
    if int(summary["null_patient_oid"] or 0):
        fail(f"{SRC_PATIENT}: PatientOID contains NULL")
    if int(summary["null_source_rows"]) != int(summary["null_source_distinct"]):
        fail(f"{SRC_PATIENT}: null-source PatientOID values are not unique")

    overlap_ids = (
        patient.groupBy("PatientOID")
        .agg(
            F.max(F.col("SourceSystemOID").isNull().cast("int")).alias("has_null"),
            F.max(F.col("SourceSystemOID").isNotNull().cast("int")).alias("has_populated"),
        )
        .where((F.col("has_null") == 1) & (F.col("has_populated") == 1))
        .limit(1).count()
    )
    if overlap_ids:
        fail(f"{SRC_PATIENT}: null-source PatientOIDs overlap populated-source PatientOIDs")
    normalized = patient.select(
        F.coalesce(F.col("SourceSystemOID"), F.lit(LUNA_SOURCE_SYSTEM_OID))
        .cast("long").alias("SOURCE_SYSTEM_OID"),
        F.col("PatientOID").cast("long").alias("PATIENT_OID"),
    )
    assert_unique_non_null(normalized, ["SOURCE_SYSTEM_OID", "PATIENT_OID"], "normalized patient core")
    return {
        "status": "PASS",
        "source_rows": int(summary["source_rows"]),
        "populated_source_systems": populated_systems,
        "null_source_rows": int(summary["null_source_rows"]),
        "null_source_distinct_patient_oids": int(summary["null_source_distinct"]),
        "null_source_populated_overlap": int(overlap_ids),
        "normalized_key_unique": True,
    }


def build_person_xwalk() -> tuple[DataFrame, dict]:
    global ALIAS_VERSION
    heartbeat_run_lock("patient source gates")
    source_gate = patient_source_gates()
    patient = pinned_source(SRC_PATIENT).select(
        F.coalesce(F.col("SourceSystemOID"), F.lit(LUNA_SOURCE_SYSTEM_OID)).cast("long").alias("SOURCE_SYSTEM_OID"),
        F.col("PatientOID").cast("long").alias("PATIENT_OID"),
        F.upper(F.trim(F.col("LocalPatientID"))).alias("__MRN_NORM"),
        F.regexp_replace(F.col("NHSNumber"), r"\s", "").alias("__NHS_NORM"),
        F.col("ADC_UPDT").alias("__PATIENT_SOURCE_ADC_UPDT"),
    )
    alias_source = PINNED_ALIAS_FRAME
    if alias_source is None:
        fail("Person alias content requested before alias version pin")
    relevant_alias = alias_source.where(
        ((F.col("PERSON_ALIAS_TYPE_CD") == MRN_ALIAS_TYPE_CD) & (F.col("ALIAS_POOL_CD") == MRN_ALIAS_POOL_CD))
        | ((F.col("PERSON_ALIAS_TYPE_CD") == NHS_ALIAS_TYPE_CD) & (F.col("ALIAS_POOL_CD") == NHS_ALIAS_POOL_CD))
    )
    alias_type_gate = validate_bigint_casts(
        relevant_alias, ["PERSON_ID", "PERSON_ALIAS_TYPE_CD", "ALIAS_POOL_CD"],
        "pinned authority-scoped person aliases",
    )
    as_of = F.lit(RUN_AS_OF).cast("timestamp")
    alias_base = relevant_alias.where(
        (F.col("ACTIVE_IND") == 1)
        & (F.col("BEG_EFFECTIVE_DT_TM") <= as_of)
        & (F.col("END_EFFECTIVE_DT_TM").isNull() | (F.col("END_EFFECTIVE_DT_TM") > as_of))
    )
    alias_mrn = (
        alias_base.where(
            (F.col("PERSON_ALIAS_TYPE_CD") == MRN_ALIAS_TYPE_CD)
            & (F.col("ALIAS_POOL_CD") == MRN_ALIAS_POOL_CD)
        )
        .select(
            F.expr("try_cast(PERSON_ID AS BIGINT)").alias("__PERSON_ID"),
            F.upper(F.trim(F.col("ALIAS"))).alias("__ALIAS_NORM"),
            F.col("ADC_UPDT").alias("__ALIAS_ADC_UPDT"),
        )
        .where(F.col("__ALIAS_NORM").isNotNull() & (F.col("__ALIAS_NORM") != ""))
    )
    alias_nhs = (
        alias_base.where(
            (F.col("PERSON_ALIAS_TYPE_CD") == NHS_ALIAS_TYPE_CD)
            & (F.col("ALIAS_POOL_CD") == NHS_ALIAS_POOL_CD)
        )
        .select(
            F.expr("try_cast(PERSON_ID AS BIGINT)").alias("__PERSON_ID"),
            F.regexp_replace(F.col("ALIAS"), r"\s", "").alias("__ALIAS_NORM"),
            F.col("ADC_UPDT").alias("__ALIAS_ADC_UPDT"),
        )
        .where(
            F.col("__ALIAS_NORM").isNotNull() & (F.col("__ALIAS_NORM") != "")
            & nhs_valid(F.col("__ALIAS_NORM"))
        )
    )
    mrn = (
        patient.where(F.col("__MRN_NORM").isNotNull() & (F.col("__MRN_NORM") != ""))
        .join(alias_mrn, F.col("__MRN_NORM") == F.col("__ALIAS_NORM"), "inner")
        .groupBy("SOURCE_SYSTEM_OID", "PATIENT_OID")
        .agg(
            F.sort_array(F.collect_set("__PERSON_ID")).alias("__MRN_PERSON_IDS"),
            F.max("__ALIAS_ADC_UPDT").alias("__MRN_ALIAS_ADC_UPDT"),
        )
    )
    nhs_patient = patient.where(
        F.col("__NHS_NORM").isNotNull() & (F.col("__NHS_NORM") != "")
        & nhs_valid(F.col("__NHS_NORM"))
    )
    nhs = (
        nhs_patient.join(alias_nhs, F.col("__NHS_NORM") == F.col("__ALIAS_NORM"), "inner")
        .groupBy("SOURCE_SYSTEM_OID", "PATIENT_OID")
        .agg(
            F.sort_array(F.collect_set("__PERSON_ID")).alias("__NHS_PERSON_IDS"),
            F.max("__ALIAS_ADC_UPDT").alias("__NHS_ALIAS_ADC_UPDT"),
        )
    )
    candidates = (
        patient.join(mrn, ["SOURCE_SYSTEM_OID", "PATIENT_OID"], "left")
        .join(nhs, ["SOURCE_SYSTEM_OID", "PATIENT_OID"], "left")
        .withColumn("__MRN_AMBIGUOUS", F.size(F.col("__MRN_PERSON_IDS")) > 1)
        .withColumn("__NHS_AMBIGUOUS", F.size(F.col("__NHS_PERSON_IDS")) > 1)
        .withColumn("__MRN_PID", F.when(F.size(F.col("__MRN_PERSON_IDS")) == 1, F.col("__MRN_PERSON_IDS").getItem(0)))
        .withColumn("__NHS_PID", F.when(F.size(F.col("__NHS_PERSON_IDS")) == 1, F.col("__NHS_PERSON_IDS").getItem(0)))
    )
    ambiguous = F.coalesce(F.col("__MRN_AMBIGUOUS"), F.lit(False)) | F.coalesce(F.col("__NHS_AMBIGUOUS"), F.lit(False))
    both = F.col("__MRN_PID").isNotNull() & F.col("__NHS_PID").isNotNull()
    consensus = both & (F.col("__MRN_PID") == F.col("__NHS_PID"))
    xwalk = candidates.select(
        "SOURCE_SYSTEM_OID", "PATIENT_OID",
        F.when(ambiguous, F.lit(None).cast("long"))
        .when(consensus, F.col("__MRN_PID"))
        .when(both, F.lit(None).cast("long"))
        .otherwise(F.coalesce(F.col("__MRN_PID"), F.col("__NHS_PID"))).alias("PERSON_ID"),
        F.when(ambiguous, F.lit("AMBIGUOUS"))
        .when(consensus, F.lit("MRN_NHS_CONSENSUS"))
        .when(both, F.lit("CONFLICT"))
        .when(F.col("__MRN_PID").isNotNull(), F.lit("MRN_ONLY"))
        .when(F.col("__NHS_PID").isNotNull(), F.lit("NHS_ONLY"))
        .otherwise(F.lit("UNRESOLVED")).alias("PERSON_LINK_METHOD"),
        F.greatest("__PATIENT_SOURCE_ADC_UPDT", "__MRN_ALIAS_ADC_UPDT", "__NHS_ALIAS_ADC_UPDT")
        .alias("XWALK_SOURCE_ADC_UPDT"),
    )
    assert_no_banned_columns(xwalk, "person crosswalk")
    heartbeat_run_lock("person crosswalk stage write")
    ATTEMPT_STAGE_TABLES.add(PERSON_XWALK_STAGE)
    xwalk.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(PERSON_XWALK_STAGE)
    heartbeat_run_lock("person crosswalk stage validation")
    staged = spark.table(PERSON_XWALK_STAGE)
    assert_no_banned_columns(staged, "person_xwalk_stg")
    assert_unique_non_null(staged, ["SOURCE_SYSTEM_OID", "PATIENT_OID"], "person_xwalk_stg")
    method_counts = {
        row["PERSON_LINK_METHOD"]: int(row["count"])
        for row in staged.groupBy("PERSON_LINK_METHOD").count().collect()
    }
    for method in {"MRN_NHS_CONSENSUS", "MRN_ONLY", "NHS_ONLY", "CONFLICT", "AMBIGUOUS", "UNRESOLVED"}:
        method_counts.setdefault(method, 0)
    xwalk_rows = sum(method_counts.values())
    if xwalk_rows != source_gate["source_rows"]:
        fail(f"person crosswalk parity {xwalk_rows} != {source_gate['source_rows']}")
    resolved = (
        method_counts["MRN_NHS_CONSENSUS"]
        + method_counts["MRN_ONLY"]
        + method_counts["NHS_ONLY"]
    )
    resolved_rate = resolved / xwalk_rows
    mrn_edge_rate = (method_counts["MRN_NHS_CONSENSUS"] + method_counts["MRN_ONLY"]) / xwalk_rows
    nhs_edge_rate = (method_counts["MRN_NHS_CONSENSUS"] + method_counts["NHS_ONLY"]) / xwalk_rows
    if resolved_rate < XWALK_MIN_RESOLVED:
        fail(f"crosswalk resolved rate {resolved_rate:.6f} < {XWALK_MIN_RESOLVED:.6f}")
    if mrn_edge_rate < XWALK_MIN_MRN_EDGE_RATE:
        fail(f"crosswalk MRN edge rate {mrn_edge_rate:.6f} < {XWALK_MIN_MRN_EDGE_RATE:.6f}")
    if nhs_edge_rate < XWALK_MIN_NHS_EDGE_RATE:
        fail(f"crosswalk NHS edge rate {nhs_edge_rate:.6f} < {XWALK_MIN_NHS_EDGE_RATE:.6f}")
    return staged, {
        **source_gate, "alias_delta_version": int(ALIAS_VERSION),
        "alias_type_safety": alias_type_gate, "run_as_of_utc": str(RUN_AS_OF),
        "output_rows": int(xwalk_rows), "method_counts": method_counts,
        "resolved_rows": int(resolved), "resolved_rate": float(resolved_rate),
        "mrn_edge_rate": float(mrn_edge_rate), "nhs_edge_rate": float(nhs_edge_rate),
        "zero_fanout": True,
    }


def build_referral(xwalk: DataFrame, lookup: DataFrame) -> tuple[DataFrame, dict]:
    heartbeat_run_lock("referral source gates")
    core_source = pinned_source(SRC_REFERRAL)
    core_systems = sorted(
        int(row["SourceSystemOID"])
        for row in core_source.select("SourceSystemOID").distinct().collect()
        if row["SourceSystemOID"] is not None
    )
    if core_systems != [LUNA_SOURCE_SYSTEM_OID]:
        fail(f"{SRC_REFERRAL}: populated SourceSystemOID set {core_systems} is not {[LUNA_SOURCE_SYSTEM_OID]}")
    core = core_source.select(
        F.col("SourceSystemOID").cast("long").alias("SOURCE_SYSTEM_OID"),
        F.col("ReferralOID").cast("long").alias("REFERRAL_OID"),
        F.col("WaitingListOID").cast("long").alias("WAITING_LIST_OID"),
        F.col("PathwayOID").cast("long").alias("PATHWAY_OID"),
        F.col("PatientOID").cast("long").alias("PATIENT_OID"),
        F.col("UBRN").alias("UBRN"),
        F.col("AdminCategory_RVID").cast("long").alias("ADMIN_CATEGORY_RVID"),
        F.col("Site_RVID").cast("long").alias("SITE_RVID"),
        F.col("TreatmentFunction_RVID").cast("long").alias("TREATMENT_FUNCTION_RVID"),
        F.col("ServiceTypeRequested_RVID").cast("long").alias("SERVICE_TYPE_REQUESTED_RVID"),
        F.col("ReferralReceivedDateTime").alias("REFERRAL_RECEIVED_DATETIME"),
        F.col("ReferralPriority_RVID").cast("long").alias("REFERRAL_PRIORITY_RVID"),
        F.col("ReferralSource_RVID").cast("long").alias("REFERRAL_SOURCE_RVID"),
        F.col("ReferralStatus_RVID").cast("long").alias("REFERRAL_STATUS_RVID"),
        F.col("ReferralStatusChangeReason_RVID").cast("long").alias("REFERRAL_STATUS_CHANGE_REASON_RVID"),
        F.col("ReferralStatusChangeDateTime").alias("REFERRAL_STATUS_CHANGE_DATETIME"),
        F.col("EncounterType_RVID").cast("long").alias("ENCOUNTER_TYPE_RVID"),
        F.col("SuspectedCancerSite_RVID").cast("long").alias("SUSPECTED_CANCER_SITE_RVID"),
        F.col("BusinessUnit").alias("BUSINESS_UNIT"),
        F.col("Division").alias("DIVISION"),
        F.col("SourceOID").cast("long").alias("SOURCE_OID"),
        F.col("OriginalReferralReceivedDateTime").alias("ORIGINAL_REFERRAL_RECEIVED_DATETIME"),
        F.col("ReferredBy_OrgID").cast("long").alias("REFERRED_BY_ORG_ID"),
        F.col("ReferringFacility_RVID").cast("long").alias("REFERRING_FACILITY_RVID"),
        F.col("BookingType_RVID").cast("long").alias("BOOKING_TYPE_RVID"),
        F.col("CreatedDateTime").alias("CREATED_DATETIME"),
        F.col("ModifiedDateTime").alias("MODIFIED_DATETIME"),
        F.col("ActiveInd").cast("boolean").alias("ACTIVE_IND"),
        F.col("ADC_UPDT").alias("REFERRAL_SOURCE_ADC_UPDT"),
    )
    assert_unique_non_null(core, TARGET_KEYS[MAP_REFERRAL], SRC_REFERRAL)
    core_rows = core.count()

    ers_source = pinned_source(SRC_ERS)
    ers_systems = sorted(
        int(row["SourceSystemOID"])
        for row in ers_source.select("SourceSystemOID").distinct().collect()
        if row["SourceSystemOID"] is not None
    )
    if any(system != LUNA_SOURCE_SYSTEM_OID for system in ers_systems):
        fail(f"{SRC_ERS}: populated SourceSystemOID values must be {LUNA_SOURCE_SYSTEM_OID}, got {ers_systems}")
    ers = ers_source.select(
        F.coalesce(F.col("SourceSystemOID"), F.lit(LUNA_SOURCE_SYSTEM_OID))
        .cast("long").alias("SOURCE_SYSTEM_OID"),
        F.col("ReferralOID").cast("long").alias("REFERRAL_OID"),
        F.col("UBRNReceived").alias("ERS_UBRN_RECEIVED"),
        F.col("eReferralPathwayStart").alias("ERS_PATHWAY_START"),
        F.col("eRSServiceName").alias("ERS_SERVICE_NAME"),
        F.col("eRSSpecialty").alias("ERS_SPECIALTY"),
        F.col("ADC_UPDT").alias("ERS_SOURCE_ADC_UPDT"),
    ).alias("ers")
    core_ids = core.select("SOURCE_SYSTEM_OID", "REFERRAL_OID").alias("core_ids")
    contained = ers.join(
        core_ids,
        (F.col("ers.SOURCE_SYSTEM_OID") == F.col("core_ids.SOURCE_SYSTEM_OID"))
        & (F.col("ers.REFERRAL_OID") == F.col("core_ids.REFERRAL_OID")),
        "inner",
    ).select("ers.*")
    contained_rows = contained.count()
    distinct_contained = contained.select("SOURCE_SYSTEM_OID", "REFERRAL_OID").distinct().count()
    max_multiplicity = int(
        contained.groupBy("SOURCE_SYSTEM_OID", "REFERRAL_OID").count()
        .agg(F.max("count").alias("max_n")).collect()[0]["max_n"] or 0
    )
    if contained_rows != distinct_contained or max_multiplicity > 1:
        fail(
            f"eRS composite containment is not <=1:1: rows={contained_rows}, "
            f"distinct={distinct_contained}, max={max_multiplicity}"
        )

    result = (
        core.join(contained, ["SOURCE_SYSTEM_OID", "REFERRAL_OID"], "left")
        .join(xwalk, ["SOURCE_SYSTEM_OID", "PATIENT_OID"], "left")
        .withColumn(
            "SOURCE_ADC_UPDT",
            F.greatest("REFERRAL_SOURCE_ADC_UPDT", "ERS_SOURCE_ADC_UPDT", "XWALK_SOURCE_ADC_UPDT"),
        )
    )
    for rvid, base in [
        ("ADMIN_CATEGORY_RVID", "ADMIN_CATEGORY"), ("SITE_RVID", "SITE"),
        ("TREATMENT_FUNCTION_RVID", "TREATMENT_FUNCTION"),
        ("SERVICE_TYPE_REQUESTED_RVID", "SERVICE_TYPE_REQUESTED"),
        ("REFERRAL_PRIORITY_RVID", "REFERRAL_PRIORITY"),
        ("REFERRAL_SOURCE_RVID", "REFERRAL_SOURCE"),
        ("REFERRAL_STATUS_RVID", "REFERRAL_STATUS"),
        ("REFERRAL_STATUS_CHANGE_REASON_RVID", "REFERRAL_STATUS_CHANGE_REASON"),
        ("ENCOUNTER_TYPE_RVID", "ENCOUNTER_TYPE"),
        ("SUSPECTED_CANCER_SITE_RVID", "SUSPECTED_CANCER_SITE"),
        ("REFERRING_FACILITY_RVID", "REFERRING_FACILITY"),
        ("BOOKING_TYPE_RVID", "BOOKING_TYPE"),
    ]:
        result = rv_decode(result, rvid, base, lookup)
    return result, {
        "status": "SOURCE_GATES_PASS",
        "expected_rows": int(core_rows),
        "referral_source_systems": core_systems,
        "ers_populated_source_systems": ers_systems,
        "ers_normalized_source_system_oid": LUNA_SOURCE_SYSTEM_OID,
        "ers_contained_rows": int(contained_rows),
        "ers_contained_distinct_composite_keys": int(distinct_contained),
        "ers_max_multiplicity": int(max_multiplicity),
        "reference_no_fanout": True,
    }


def build_rtt_pathway(xwalk: DataFrame, lookup: DataFrame) -> tuple[DataFrame, dict]:
    heartbeat_run_lock("RTT pathway source gates")
    core = pinned_source(SRC_RTT_CORE).select(
        F.col("SourceSystemOID").cast("long").alias("SOURCE_SYSTEM_OID"),
        F.col("PathwayOID").cast("long").alias("PATHWAY_OID"),
        F.col("PatientOID").cast("long").alias("PATIENT_OID"),
        F.col("BreachDate").alias("BREACH_DATE"),
        F.col("DaysWaited").cast("long").alias("DAYS_WAITED"),
        F.col("DaysWaitedActive").cast("long").alias("DAYS_WAITED_ACTIVE"),
        F.col("ReferringFacility_RVID").cast("long").alias("REFERRING_FACILITY_RVID"),
        F.col("PathwayType_RVID").cast("long").alias("PATHWAY_TYPE_RVID"),
        F.col("PathwayStartDate").alias("PATHWAY_START_DATE"),
        F.col("CurrentRTTStatus_RVID").cast("long").alias("CURRENT_RTT_STATUS_RVID"),
        F.col("ClockStartDateTime").alias("__CORE_CLOCK_START_DT_TM"),
        F.col("ClockStopDateTime").alias("__CORE_CLOCK_STOP_DT_TM"),
        F.col("CreatedDateTime").alias("CORE_CREATED_DATETIME"),
        F.col("ModifiedDateTime").alias("CORE_MODIFIED_DATETIME"),
        F.col("ActiveInd").cast("boolean").alias("CORE_ACTIVE_IND"),
        F.col("ADC_UPDT").alias("CORE_SOURCE_ADC_UPDT"),
        F.lit(1).alias("__CORE_MATCH"),
    )
    assert_unique_non_null(core, ["SOURCE_SYSTEM_OID", "PATHWAY_OID"], SRC_RTT_CORE)
    core_rows = core.count()

    period_source = pinned_source(SRC_RTT_PERIOD)
    period_type_gate = validate_bigint_casts(
        period_source.select("TreatmentFunction_RVID"),
        ["TreatmentFunction_RVID"],
        "RTT period TreatmentFunction_RVID",
    )
    period = period_source.select(
        F.col("SourceSystemOID").cast("long").alias("SOURCE_SYSTEM_OID"),
        F.col("PathwayOID").cast("long").alias("PATHWAY_OID"),
        F.col("PeriodOID").cast("long").alias("PERIOD_OID"),
        F.col("StartRTTActivityOID").cast("long").alias("START_RTT_ACTIVITY_OID"),
        F.col("StartDateTime").alias("START_DATETIME"),
        F.col("StartRTTStatus_RVID").cast("long").alias("START_RTT_STATUS_RVID"),
        F.col("ClockStartCreatedDateTime").alias("CLOCK_START_CREATED_DATETIME"),
        F.col("StopRTTActivityOID").cast("long").alias("STOP_RTT_ACTIVITY_OID"),
        F.col("StopDateTime").alias("STOP_DATETIME"),
        F.col("StopRTTStatus_RVID").cast("long").alias("STOP_RTT_STATUS_RVID"),
        F.col("ClockStopCreatedDateTime").alias("CLOCK_STOP_CREATED_DATETIME"),
        F.col("SeqNoASC").cast("long").alias("SEQ_NO_ASC"),
        F.col("SeqNoDESC").cast("long").alias("SEQ_NO_DESC"),
        F.col("OPAppt_DNA_Count").cast("long").alias("OP_APPT_DNA_COUNT"),
        F.expr("try_cast(TreatmentFunction_RVID AS BIGINT)").alias("TREATMENT_FUNCTION_RVID"),
        F.col("Site_RVID").cast("long").alias("SITE_RVID"),
        F.col("CreatedDateTime").alias("PERIOD_CREATED_DATETIME"),
        F.col("ModifiedDateTime").alias("PERIOD_MODIFIED_DATETIME"),
        F.col("ActiveInd").cast("boolean").alias("PERIOD_ACTIVE_IND"),
        F.col("ADC_UPDT").alias("PERIOD_SOURCE_ADC_UPDT"),
    )
    assert_unique_non_null(period, ["SOURCE_SYSTEM_OID", "PATHWAY_OID", "PERIOD_OID"], SRC_RTT_PERIOD)
    period_rows = period.count()
    if period.where(F.col("PERIOD_OID") == 0).limit(1).count():
        fail(f"{SRC_RTT_PERIOD}: PeriodOID 0 sentinel collision")
    assert_unique_non_null(
        period.select("SOURCE_SYSTEM_OID", "PATHWAY_OID", "SEQ_NO_DESC"),
        ["SOURCE_SYSTEM_OID", "PATHWAY_OID", "SEQ_NO_DESC"], "RTT period orientation",
    )
    if (
        period.groupBy("SOURCE_SYSTEM_OID", "PATHWAY_OID")
        .agg(F.sum((F.col("SEQ_NO_DESC") == 1).cast("long")).alias("latest_rows"))
        .where(F.col("latest_rows") != 1).limit(1).count()
    ):
        fail(f"{SRC_RTT_PERIOD}: each clocked pathway must have exactly one SeqNoDESC=1 row")

    ambiguous_paths = (
        core.groupBy("PATHWAY_OID").agg(F.countDistinct("SOURCE_SYSTEM_OID").alias("SOURCE_SYSTEM_COUNT"))
        .where(F.col("SOURCE_SYSTEM_COUNT") > 1).select("PATHWAY_OID")
    )
    ambiguous_path_count = ambiguous_paths.count()
    tags = (
        pinned_source(SRC_ENCOUNTERS)
        .select(
            F.col("PathwayOID").cast("long").alias("PATHWAY_OID"),
            F.col("EncounterType").alias("ENCOUNTER_TYPE"),
            F.col("ADC_UPDT").alias("TAG_ROW_ADC_UPDT"),
        )
        .join(ambiguous_paths, "PATHWAY_OID", "left_anti")
        .where(F.col("PATHWAY_OID").isNotNull())
        .groupBy("PATHWAY_OID")
        .agg(
            F.sort_array(F.collect_set("ENCOUNTER_TYPE")).alias("ENCOUNTER_TYPES"),
            F.max("TAG_ROW_ADC_UPDT").alias("TAG_SOURCE_ADC_UPDT"),
        )
    )
    assert_unique_non_null(tags, ["PATHWAY_OID"], "RTT encounter tags")

    joined = period.join(core, ["SOURCE_SYSTEM_OID", "PATHWAY_OID"], "left")
    unmatched_periods = joined.where(F.col("__CORE_MATCH").isNull()).count()
    if unmatched_periods:
        fail(f"{SRC_RTT_PERIOD}: {unmatched_periods} period rows do not resolve to RTT core")
    discrepancy = (
        (F.col("SEQ_NO_DESC") == 1)
        & (
            (~F.col("__CORE_CLOCK_START_DT_TM").eqNullSafe(F.col("START_DATETIME")))
            | (~F.col("__CORE_CLOCK_STOP_DT_TM").eqNullSafe(F.col("STOP_DATETIME")))
        )
    )
    clocked = (
        joined.withColumn("IS_LATEST_PERIOD", F.col("SEQ_NO_DESC") == 1)
        .withColumn("CLOCK_DISCREPANT", discrepancy)
        .withColumn("CORE_CLOCK_START_DT_TM", F.when(discrepancy, F.col("__CORE_CLOCK_START_DT_TM")))
        .withColumn("CORE_CLOCK_STOP_DT_TM", F.when(discrepancy, F.col("__CORE_CLOCK_STOP_DT_TM")))
        .drop("__CORE_CLOCK_START_DT_TM", "__CORE_CLOCK_STOP_DT_TM", "__CORE_MATCH")
    )

    period_keys = period.select("SOURCE_SYSTEM_OID", "PATHWAY_OID").distinct()
    clockless_core = core.join(period_keys, ["SOURCE_SYSTEM_OID", "PATHWAY_OID"], "left_anti")
    clockless_rows = clockless_core.count()
    clockless_evidence = (
        F.col("__CORE_CLOCK_START_DT_TM").isNotNull() | F.col("__CORE_CLOCK_STOP_DT_TM").isNotNull()
    )
    clockless = (
        clockless_core
        .withColumn("PERIOD_OID", F.lit(0).cast("long"))
        .withColumn("START_RTT_ACTIVITY_OID", F.lit(None).cast("long"))
        .withColumn("START_DATETIME", F.lit(None).cast("timestamp"))
        .withColumn("START_RTT_STATUS_RVID", F.lit(None).cast("long"))
        .withColumn("CLOCK_START_CREATED_DATETIME", F.lit(None).cast("timestamp"))
        .withColumn("STOP_RTT_ACTIVITY_OID", F.lit(None).cast("long"))
        .withColumn("STOP_DATETIME", F.lit(None).cast("timestamp"))
        .withColumn("STOP_RTT_STATUS_RVID", F.lit(None).cast("long"))
        .withColumn("CLOCK_STOP_CREATED_DATETIME", F.lit(None).cast("timestamp"))
        .withColumn("SEQ_NO_ASC", F.lit(None).cast("long"))
        .withColumn("SEQ_NO_DESC", F.lit(None).cast("long"))
        .withColumn("OP_APPT_DNA_COUNT", F.lit(None).cast("long"))
        .withColumn("TREATMENT_FUNCTION_RVID", F.lit(None).cast("long"))
        .withColumn("SITE_RVID", F.lit(None).cast("long"))
        .withColumn("PERIOD_CREATED_DATETIME", F.lit(None).cast("timestamp"))
        .withColumn("PERIOD_MODIFIED_DATETIME", F.lit(None).cast("timestamp"))
        .withColumn("PERIOD_ACTIVE_IND", F.lit(None).cast("boolean"))
        .withColumn("PERIOD_SOURCE_ADC_UPDT", F.lit(None).cast("timestamp"))
        .withColumn("IS_LATEST_PERIOD", F.lit(True))
        .withColumn("CLOCK_DISCREPANT", clockless_evidence)
        .withColumn("CORE_CLOCK_START_DT_TM", F.col("__CORE_CLOCK_START_DT_TM"))
        .withColumn("CORE_CLOCK_STOP_DT_TM", F.col("__CORE_CLOCK_STOP_DT_TM"))
        .drop("__CORE_CLOCK_START_DT_TM", "__CORE_CLOCK_STOP_DT_TM", "__CORE_MATCH")
    )
    hybrid = (
        clocked.unionByName(clockless)
        .join(tags, "PATHWAY_OID", "left")
        .join(xwalk, ["SOURCE_SYSTEM_OID", "PATIENT_OID"], "left")
        .withColumn(
            "SOURCE_ADC_UPDT",
            F.greatest(
                "PERIOD_SOURCE_ADC_UPDT", "CORE_SOURCE_ADC_UPDT",
                "TAG_SOURCE_ADC_UPDT", "XWALK_SOURCE_ADC_UPDT",
            ),
        )
    )
    for rvid, base in [
        ("REFERRING_FACILITY_RVID", "REFERRING_FACILITY"),
        ("PATHWAY_TYPE_RVID", "PATHWAY_TYPE"),
        ("CURRENT_RTT_STATUS_RVID", "CURRENT_RTT_STATUS"),
        ("START_RTT_STATUS_RVID", "START_RTT_STATUS"),
        ("STOP_RTT_STATUS_RVID", "STOP_RTT_STATUS"),
        ("TREATMENT_FUNCTION_RVID", "TREATMENT_FUNCTION"),
        ("SITE_RVID", "SITE"),
    ]:
        hybrid = rv_decode(hybrid, rvid, base, lookup)
    return hybrid, {
        "status": "SOURCE_GATES_PASS",
        "expected_rows": int(period_rows + clockless_rows),
        "core_rows": int(core_rows),
        "period_rows": int(period_rows),
        "clockless_rows": int(clockless_rows),
        "ambiguous_pathway_oids_excluded_from_tags": int(ambiguous_path_count),
        "period_type_safety": period_type_gate,
        "exactly_one_latest_per_pathway_source_gate": True,
        "reference_no_fanout": True,
    }


def add_performed_timestamp(df: DataFrame, raw_column: str, output_column: str) -> DataFrame:
    valid = (
        F.col(raw_column).isNotNull()
        & (F.col(raw_column) >= F.lit("1900-01-01").cast("timestamp"))
        & (F.col(raw_column) <= F.lit(RUN_FUTURE_HORIZON).cast("timestamp"))
    )
    return (
        df.withColumn(
            f"{output_column}_QUALITY",
            F.when(F.col(raw_column).isNull(), F.lit("MISSING"))
            .when(F.col(raw_column) < F.lit("1900-01-01").cast("timestamp"), F.lit("INVALID_EARLY"))
            .when(F.col(raw_column) > F.lit(RUN_FUTURE_HORIZON).cast("timestamp"), F.lit("INVALID_FUTURE"))
            .otherwise(F.lit("VALID")),
        )
        .withColumn(output_column, F.when(valid, F.col(raw_column)))
        .drop(raw_column)
    )


def build_rtt_activity(xwalk: DataFrame, lookup: DataFrame) -> tuple[DataFrame, dict]:
    heartbeat_run_lock("RTT activity source gates")
    if CARRY_SCH_EVENT_ID:
        fail("CARRY_SCH_EVENT_ID must remain false until a >=90% appointment mapping is proven")
    core_link = pinned_source(SRC_RTT_CORE).select(
        F.col("SourceSystemOID").cast("long").alias("SOURCE_SYSTEM_OID"),
        F.col("PathwayOID").cast("long").alias("PATHWAY_OID"),
        F.col("PatientOID").cast("long").alias("PATIENT_OID"),
        F.col("ADC_UPDT").alias("CORE_SOURCE_ADC_UPDT"),
    )
    assert_unique_non_null(core_link, ["SOURCE_SYSTEM_OID", "PATHWAY_OID"], "RTT core activity link")
    raw = pinned_source(SRC_RTT_ACTIVITY)
    raw_rows = raw.count()
    activity = raw.select(
        F.col("SourceSystemOID").cast("long").alias("SOURCE_SYSTEM_OID"),
        F.col("RTTActivityOID").cast("long").alias("RTT_ACTIVITY_OID"),
        F.col("PathwayOID").cast("long").alias("PATHWAY_OID"),
        F.col("SourceActivityOID").cast("long").alias("SOURCE_ACTIVITY_OID"),
        F.col("ReferralOID").cast("long").alias("REFERRAL_OID"),
        F.col("Site_RVID").cast("long").alias("SITE_RVID"),
        F.col("TreatmentFunction_RVID").cast("long").alias("TREATMENT_FUNCTION_RVID"),
        F.col("AppointmentOID").cast("long").alias("APPOINTMENT_OID"),
        F.col("RTTActivity_RVID").cast("long").alias("RTT_ACTIVITY_RVID"),
        F.col("RTTActivityType_RVID").cast("long").alias("RTT_ACTIVITY_TYPE_RVID"),
        F.col("RTTStatus_RVID").cast("long").alias("RTT_STATUS_RVID"),
        F.col("RTTActivityDateTime").alias("__RTT_ACTIVITY_DATETIME_RAW"),
        F.col("RTTStatusSequenceASC").cast("long").alias("RTT_STATUS_SEQUENCE_ASC"),
        F.col("RTTStatusSequenceDESC").cast("long").alias("RTT_STATUS_SEQUENCE_DESC"),
        F.col("RTTActivitySequenceASC").cast("long").alias("RTT_ACTIVITY_SEQUENCE_ASC"),
        F.col("RTTActivitySequenceDESC").cast("long").alias("RTT_ACTIVITY_SEQUENCE_DESC"),
        F.col("IsIllogical").cast("boolean").alias("IS_ILLOGICAL"),
        F.col("BusinessUnit").alias("BUSINESS_UNIT"),
        F.col("Division").alias("DIVISION"),
        F.col("CreatedDateTime").alias("CREATED_DATETIME"),
        F.col("ModifiedDateTime").alias("MODIFIED_DATETIME"),
        F.col("ActiveInd").cast("boolean").alias("ACTIVE_IND"),
        F.col("ADC_UPDT").alias("ACTIVITY_SOURCE_ADC_UPDT"),
    )
    assert_unique_non_null(activity, TARGET_KEYS[MAP_RTT_ACTIVITY], SRC_RTT_ACTIVITY)
    unmatched_core = activity.select("SOURCE_SYSTEM_OID", "PATHWAY_OID").join(
        core_link.select("SOURCE_SYSTEM_OID", "PATHWAY_OID"),
        ["SOURCE_SYSTEM_OID", "PATHWAY_OID"], "left_anti",
    ).count()
    if unmatched_core:
        fail(f"RTT activity: {unmatched_core} rows do not resolve to unique RTT core")
    result = (
        activity.join(core_link, ["SOURCE_SYSTEM_OID", "PATHWAY_OID"], "inner")
        .join(xwalk, ["SOURCE_SYSTEM_OID", "PATIENT_OID"], "left")
        .drop("PATIENT_OID")
        .withColumn(
            "SOURCE_ADC_UPDT",
            F.greatest("ACTIVITY_SOURCE_ADC_UPDT", "CORE_SOURCE_ADC_UPDT", "XWALK_SOURCE_ADC_UPDT"),
        )
    )
    result = add_performed_timestamp(result, "__RTT_ACTIVITY_DATETIME_RAW", "RTT_ACTIVITY_DATETIME")
    for rvid, base in [
        ("SITE_RVID", "SITE"), ("TREATMENT_FUNCTION_RVID", "TREATMENT_FUNCTION"),
        ("RTT_ACTIVITY_RVID", "RTT_ACTIVITY"),
        ("RTT_ACTIVITY_TYPE_RVID", "RTT_ACTIVITY_TYPE"),
        ("RTT_STATUS_RVID", "RTT_STATUS"),
    ]:
        result = rv_decode(result, rvid, base, lookup)
    return result, {
        "status": "SOURCE_GATES_PASS",
        "expected_rows": int(raw_rows),
        "unmatched_core_rows": int(unmatched_core),
        "carry_sch_event_id": CARRY_SCH_EVENT_ID,
        "reference_no_fanout": True,
    }


def finalize_stage_business_gates(target: str, staged: DataFrame, gate: dict) -> dict:
    heartbeat_run_lock(f"stage business validation {target}")
    staged_rows = staged.count()
    if staged_rows != int(gate["expected_rows"]):
        fail(f"{target}: staged/source parity {staged_rows} != {gate['expected_rows']}")
    result = {**gate, "status": "PASS", "output_rows": int(staged_rows)}
    if target == MAP_RTT_PATHWAY:
        latest_bad = (
            staged.groupBy("SOURCE_SYSTEM_OID", "PATHWAY_OID")
            .agg(F.sum(F.col("IS_LATEST_PERIOD").cast("long")).alias("latest_rows"))
            .where(F.col("latest_rows") != 1).limit(1).count()
        )
        if latest_bad:
            fail(f"{target}: expected exactly one latest row per composite pathway")
        result["latest_period_discrepant_rows"] = int(
            staged.where((F.col("PERIOD_OID") != 0) & F.col("CLOCK_DISCREPANT")).count()
        )
        result["clockless_core_only_clock_rows"] = int(
            staged.where((F.col("PERIOD_OID") == 0) & F.col("CLOCK_DISCREPANT")).count()
        )
        result["clock_discrepant_rows_total"] = (
            result["latest_period_discrepant_rows"] + result["clockless_core_only_clock_rows"]
        )
        result["exactly_one_latest_per_pathway"] = True
    if target == MAP_RTT_ACTIVITY:
        result["timestamp_quality"] = {
            row["RTT_ACTIVITY_DATETIME_QUALITY"]: int(row["count"])
            for row in staged.groupBy("RTT_ACTIVITY_DATETIME_QUALITY").count().collect()
        }
    return result


def validate_table_features() -> dict:
    return {
        target: validate_or_repair_table_features(target, repair_allowed=False)
        for target in TARGET_KEYS
    }


def apply_output_comments(targets: list[str]) -> None:
    comments = {
        MAP_REFERRAL: "LUNA referral bronze: one source referral with eRS sidecars and ambiguity-safe PERSON_ID; Millennium alias resolution is pinned per non-skip weekly LUNA run and is not a mode trigger.",
        MAP_RTT_PATHWAY: "LUNA RTT hybrid bronze: one period row plus one PeriodOID=0 row for each clockless pathway.",
        MAP_RTT_ACTIVITY: "LUNA RTT activity bronze: one source activity with pathway-derived person linkage.",
    }
    for target in targets:
        spark.sql(f"COMMENT ON TABLE {qname(target)} IS '{sql_escape(comments[target])}'")


def optimize_bootstrap_target(target: str) -> dict:
    clustering = validate_or_repair_clustering(target, repair_allowed=False)
    heartbeat_run_lock(f"OPTIMIZE {target}")
    try:
        spark.sql(f"OPTIMIZE {qname(target)} FULL")
        mode = "FULL"
    except Exception as exc:
        message = str(exc).lower()
        if not any(token in message for token in ("syntax", "parse", "unsupported", "not supported")):
            raise
        heartbeat_run_lock(f"safe OPTIMIZE fallback {target}")
        spark.sql(f"OPTIMIZE {qname(target)}")
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
        COMMENT 'Task 6 dev-only referral/RTT chosen-mode and checkpoint evidence.'
        """
    )
    updates = (
        spark.createDataFrame(
            [(
                "referral_rtt_pipeline", RUN_ID, ATTEMPT_ID,
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
    global SOURCE_HEALTH, ALIAS_VERSION, ALIAS_LIVE_VERSION, RUN_FUTURE_HORIZON, PINNED_ALIAS_FRAME
    load_state_cache()
    SOURCE_VERSIONS.clear()
    SOURCE_VERSIONS.update(
        {source: source_version(source) for source in [*SOURCE_SLA.keys(), LOGIC_SOURCE]}
    )
    PINNED_SOURCE_FRAMES.clear()
    PINNED_SOURCE_FRAMES.update({
        source: (
            spark.read.format("delta")
            .option("versionAsOf", int(SOURCE_VERSIONS[source]))
            .table(source)
        )
        for source in SOURCE_SLA
    })
    chosen_modes = {
        target: choose_mode(target, sources) for target, sources in TARGET_SOURCES.items()
    }
    modes = dict(chosen_modes)
    for target in modes:
        if FORCE_FULL_ON_CHANGE and chosen_modes[target] == "INCREMENTAL":
            fail(f"{target}: choose_mode returned INCREMENTAL while FORCE_FULL_ON_CHANGE is enabled")

    any_non_skip = any(mode != "UNCHANGED_SKIP" for mode in modes.values())
    ALIAS_LIVE_VERSION = source_version(SRC_PERSON_ALIAS)
    ALIAS_VERSION = ALIAS_LIVE_VERSION if any_non_skip else None
    PINNED_ALIAS_FRAME = (
        spark.read.format("delta")
        .option("versionAsOf", int(ALIAS_VERSION))
        .table(SRC_PERSON_ALIAS)
        if ALIAS_VERSION is not None
        else None
    )
    changed_luna_sources = []
    for source in SOURCE_SLA:
        consumers = [target for target, sources in TARGET_SOURCES.items() if source in sources]
        if any(
            last_checkpoint(target, source) is None
            or int(last_checkpoint(target, source)["source_version"]) != int(SOURCE_VERSIONS[source])
            for target in consumers
        ):
            changed_luna_sources.append(source)
    if changed_luna_sources and not any_non_skip:
        fail(
            "LUNA source versions changed but every target selected UNCHANGED_SKIP: "
            f"{changed_luna_sources}"
        )
    luna_refresh_policy = {
        "changed_luna_sources": changed_luna_sources,
        "any_non_skip": any_non_skip,
        "tracked_source_change_implies_non_skip": not changed_luna_sources or any_non_skip,
        "weekly_refresh_observed": bool(changed_luna_sources),
    }

    SOURCE_HEALTH = {
        source: source_health_for_mode(source, source_mode(source, modes), health_checkpoint(source))
        for source in [*SOURCE_SLA.keys(), LOGIC_SOURCE]
    }
    for table, (sla_days, freshness_mode) in SOURCE_SLA.items():
        stale = SOURCE_HEALTH[table]["source_staleness_days"]
        if stale is None:
            fail(f"{table}: ADC_UPDT watermark is NULL")
        if stale > sla_days:
            fail(f"{table}: staleness {stale:.2f}d exceeds {sla_days}d SLA ({freshness_mode})")
    RUN_FUTURE_HORIZON = RUN_AS_OF + timedelta(days=2)

    target_preflight = {
        target: validate_existing_target_preflight(target, modes[target])
        for target in TARGET_KEYS
    }
    alias_snapshot = {
        "table": SRC_PERSON_ALIAS,
        "pinned_delta_version": int(ALIAS_VERSION) if ALIAS_VERSION is not None else None,
        "live_delta_version": int(ALIAS_LIVE_VERSION),
        "mode_source": False,
        "policy": "PIN_ON_EACH_NON_SKIP; NOT_A_CHOOSE_MODE_TRIGGER; DRIFT_BOUNDED_TO_WEEKLY_LUNA_REFRESH",
    }
    print(
        f"[REFERRAL_RTT] target={TARGET_SCHEMA}, run_id={RUN_ID}, attempt_id={ATTEMPT_ID}, "
        f"logic={PIPELINE_LOGIC_VERSION}, run_as_of={RUN_AS_OF}, alias={alias_snapshot}, "
        f"future_horizon={RUN_FUTURE_HORIZON}"
    )
    for target in modes:
        print(f"[REFERRAL_RTT] {target}: chosen={chosen_modes[target]}, effective={modes[target]}")
    for source, health in SOURCE_HEALTH.items():
        print(f"[REFERRAL_RTT][HEALTH] {source}: {health['scan']}")

    common_summary = {
        "pipeline": "referral_rtt_pipeline",
        "pipeline_logic_version": PIPELINE_LOGIC_VERSION,
        "run_id": RUN_ID,
        "attempt_id": ATTEMPT_ID,
        "run_as_of_utc": str(RUN_AS_OF),
        "target_schema": TARGET_SCHEMA,
        "chosen_modes": chosen_modes,
        "modes": modes,
        "alias_snapshot": alias_snapshot,
        "luna_refresh_policy": luna_refresh_policy,
        "publication_atomicity": "SEQUENTIAL_DELTA_TARGET_MERGES; CHECKPOINT_AFTER_ALL_SUCCESS; NOT_ATOMIC",
        "run_future_horizon": str(RUN_FUTURE_HORIZON),
        "source_health": {
            table: {
                **health,
                "watermark": str(health["watermark"]) if health["watermark"] is not None else None,
            }
            for table, health in SOURCE_HEALTH.items()
        },
    }
    if not any_non_skip:
        summary = {
            "status": "SUCCESS",
            **common_summary,
            "metrics": {},
            "validation": {
                "level": "SKIPPED_ALL_UNCHANGED",
                "target_preflight": target_preflight,
                "schema_contracts": VALIDATION_RESULTS["schema"],
                "pii_inspection": VALIDATION_RESULTS["pii"],
            },
            "gate_results": {
                "status": "SKIPPED_ALL_UNCHANGED",
                "target_preflight": target_preflight,
                "luna_refresh_policy": luna_refresh_policy,
            },
            "finished_at": bronze_utc_now(),
        }
        write_dev_test_audit("PIPELINE_EXIT", "SUCCESS", summary)
        return summary

    lookup, reference_gate = validate_reference_lookup()
    xwalk, xwalk_gate = build_person_xwalk()
    build_gates = {}
    staged_targets = {}
    checkpoints = {}

    builders = {
        MAP_REFERRAL: lambda: build_referral(xwalk, lookup),
        MAP_RTT_PATHWAY: lambda: build_rtt_pathway(xwalk, lookup),
        MAP_RTT_ACTIVITY: lambda: build_rtt_activity(xwalk, lookup),
    }
    for target, builder in builders.items():
        if modes[target] == "UNCHANGED_SKIP":
            continue
        built, source_gate = builder()
        staged = materialize_stage(built, target, TARGET_KEYS[target])
        staged_targets[target] = staged
        build_gates[target] = finalize_stage_business_gates(target, staged, source_gate)
        checkpoints[target] = TARGET_SOURCES[target]

    expected_stage_count = sum(mode != "UNCHANGED_SKIP" for mode in modes.values())
    if len(staged_targets) != expected_stage_count:
        fail(f"Required target stages incomplete: {len(staged_targets)} != {expected_stage_count}")

    merge_plans = {
        target: prepare_merge_plan(target, staged, TARGET_KEYS[target], True)
        for target, staged in staged_targets.items()
    }
    metrics = {}
    for target, staged in staged_targets.items():
        metrics[target] = merge_target(
            staged, target, TARGET_KEYS[target], True, merge_plans[target]
        )

    parity = {
        target: validate_target_after_merge(
            target, staged_targets[target], metrics[target], merge_plans[target]
        )
        for target in staged_targets
    }
    feature_validation = validate_table_features()
    clustering_validation = {
        target: ensure_and_validate_clustering(target) for target in staged_targets
    }
    apply_output_comments(list(staged_targets))

    optimized = {}
    for target, mode in modes.items():
        if mode == "BOOTSTRAP" and target in staged_targets:
            optimized[target] = optimize_bootstrap_target(target)

    write_dev_test_audit(
        "PRE_CHECKPOINT",
        "SUCCESS",
        {
            "status": "VALIDATED_CHECKPOINT_PENDING",
            **common_summary,
            "metrics": metrics,
            "validation": {
                "level": "FULL_PARITY_EVERY_NON_SKIP",
                "targets": parity,
                "target_preflight": target_preflight,
            },
            "gate_results": {
                "targets": build_gates,
                "all_required_stages_before_first_merge": True,
                "luna_refresh_policy": luna_refresh_policy,
            },
            "checkpoint_pending": True,
        },
    )
    commit_checkpoints(checkpoints)
    return {
        "status": "SUCCESS",
        **common_summary,
        "metrics": metrics,
        "validation": {
            "level": "FULL_PARITY_EVERY_NON_SKIP",
            "targets": parity,
            "target_preflight": target_preflight,
            "table_features": feature_validation,
            "schema_contracts": VALIDATION_RESULTS["schema"],
            "pii_inspection": VALIDATION_RESULTS["pii"],
            "clustering": clustering_validation,
            "bootstrap_optimize": optimized,
        },
        "gate_results": {
            "reference_lookup": reference_gate,
            "person_crosswalk": xwalk_gate,
            "targets": build_gates,
            "target_preflight": target_preflight,
            "schema_contracts": VALIDATION_RESULTS["schema"],
            "pii_inspection": VALIDATION_RESULTS["pii"],
            "clustering": VALIDATION_RESULTS["clustering"],
            "all_required_stages_before_first_merge": True,
            "checkpoint_commit_count": 1 if checkpoints else 0,
            "luna_refresh_policy": luna_refresh_policy,
        },
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
    COMMENT 'Referral/RTT bronze two-phase source version and watermark checkpoints.'
    """
)

for _table, _columns in EXPECTED_COLUMNS.items():
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

