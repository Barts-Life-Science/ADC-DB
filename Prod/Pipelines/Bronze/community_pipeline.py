# Databricks notebook source
# MAGIC %md
# MAGIC # BH Community (CSDS) Bronze Pipeline
# MAGIC
# MAGIC Builds three source-specific canonical tables:
# MAGIC
# MAGIC - `map_community_patient_link` — one row per Community service registration.
# MAGIC - `map_community_care_contact` — one row per `CDB + CCI + SRI`.
# MAGIC - `map_community_care_activity` — one row per distinct source activity row.
# MAGIC
# MAGIC Direct patient identifiers are used transiently for deterministic linkage and are not
# MAGIC published, except the restricted source `PatientNumber` required for source reconciliation.
# MAGIC Exact SNOMED name/synonym matches are explicitly candidate enrichment, not approved coding.

# COMMAND ----------

# MAGIC %run ./_bronze_common

# COMMAND ----------

import hashlib
import json
from datetime import datetime, timezone
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


def _ensure_text_widget(name: str, default: str) -> None:
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)


_ensure_text_widget("target_schema", "8_dev.bronze")
_ensure_text_widget("enable_snomed_candidate_mapping", "true")
_ensure_text_widget("allow_production_write", "false")

TARGET_SCHEMA = bronze_value("target_schema", "8_dev.bronze")
ENABLE_SNOMED_CANDIDATES = bronze_bool("enable_snomed_candidate_mapping", True)
ALLOW_PRODUCTION_WRITE = bronze_bool("allow_production_write", False)
FORCE_FULL_REFRESH = bronze_bool("force_full_refresh", False)
COMMUNITY_RUN_ID = bronze_run_id()
STARTED_AT = bronze_utc_now()

assert TARGET_SCHEMA != "4_prod.bronze" or ALLOW_PRODUCTION_WRITE, (
    "Production writes are disabled in the interim notebook. "
    "Set allow_production_write=true only after approved promotion."
)

RAW = "4_prod.raw"
SRC_PATIENT = f"{RAW}.bh_community_dbo_csds_lkp_patientid"
SRC_CONTACT = f"{RAW}.bh_community_dbo_csds_cyp201carecontact"
SRC_ACTIVITY = f"{RAW}.bh_community_dbo_csds_cyp202careactivity"
PERSON_ALIAS = f"{RAW}.mill_person_alias"
PERSON_CANONICAL = "4_prod.bronze.map_person"
ORGANIZATION_ALIAS = f"{RAW}.mill_organization_alias"
ORGANIZATION = f"{RAW}.mill_organization"
OMOP_CONCEPT = "3_lookup.omop.concept"
OMOP_CONCEPT_SYNONYM = "3_lookup.omop.concept_synonym"

PATIENT_TABLE = f"{TARGET_SCHEMA}.map_community_patient_link"
CONTACT_TABLE = f"{TARGET_SCHEMA}.map_community_care_contact"
ACTIVITY_TABLE = f"{TARGET_SCHEMA}.map_community_care_activity"
STATE_TABLE = f"{bronze_control_schema(TARGET_SCHEMA)}.community_pipeline_state"
AUDIT_TABLE = f"{bronze_control_schema(TARGET_SCHEMA)}.community_pipeline_audit"

PATIENT_CONTRACT_VERSION = "community_patient_v1"
CONTACT_CONTRACT_VERSION = "community_contact_v1"
ACTIVITY_CONTRACT_VERSION = "community_activity_v1"
CSDS_MAPPING_VERSION = "CSDS_ETOS_1.6.10"
CONSULTATION_TERM_MAPPING_VERSION = "COMMUNITY_CONTACT_TERM_CANDIDATE_V1"
UCUM_MAPPING_VERSION = "COMMUNITY_UCUM_ALIAS_V1"
SNOMED_MAPPING_VERSION = "OMOP_SNOMED_EXACT_CANDIDATE_V1"

MRN_ALIAS_TYPE = 10
NHS_ALIAS_TYPE = 18
SOURCE_SYSTEM = "BH_COMMUNITY"
MAX_SOURCE_SKEW_SECONDS = 300

PHYSICAL_DEPENDENCIES = [
    SRC_PATIENT,
    SRC_CONTACT,
    SRC_ACTIVITY,
    PERSON_ALIAS,
    PERSON_CANONICAL,
    ORGANIZATION_ALIAS,
    ORGANIZATION,
    OMOP_CONCEPT,
    OMOP_CONCEPT_SYNONYM,
]
ROW_COUNT_DEPENDENCIES = {SRC_PATIENT, SRC_CONTACT, SRC_ACTIVITY}

ADMIN_CATEGORY_MAP = {
    "01": "NHS PATIENT",
    "02": "PRIVATE PATIENT",
    "03": "AMENITY PATIENT",
    "04": "CATEGORY II PATIENT",
    "98": "NOT APPLICABLE",
    "99": "NOT KNOWN",
}
CONSULT_TYPE_MAP = {
    "01": "INITIAL CONSULTATION",
    "02": "FOLLOW-UP CONSULTATION",
}
CONTACT_SUBJECT_MAP = {
    "01": "PATIENT",
    "02": "PATIENT PROXY",
}
CONSULTATION_MECHANISM_MAP = {
    "01": "FACE TO FACE",
    "02": "TELEPHONE",
    "04": "TALK TYPE FOR PERSON UNABLE TO SPEAK",
    "05": "EMAIL",
    "09": "TEXT MESSAGE (ASYNCHRONOUS)",
    "10": "INSTANT MESSAGING (SYNCHRONOUS)",
    "11": "VIDEO CONSULTATION",
    "12": "MESSAGE BOARD (ASYNCHRONOUS)",
    "13": "CHAT ROOM (SYNCHRONOUS)",
    "98": "OTHER",
}
ACTIVITY_LOCATION_MAP = {
    "A01": "PATIENT'S HOME",
    "B01": "PRIMARY CARE HEALTH CENTRE",
    "B02": "POLYCLINIC",
    "C01": "GENERAL MEDICAL PRACTITIONER PRACTICE",
    "E03": "DAY HOSPITAL",
    "H01": "DAY CENTRE",
    "L01": "SCHOOL",
    "X01": "OTHER LOCATIONS NOT ELSEWHERE CLASSIFIED",
}
GROUP_THERAPY_MAP = {
    "Y": "GROUP THERAPY",
    "N": "INDIVIDUAL ACTIVITY",
    "Z": "NOT KNOWN",
}
ATTENDANCE_MAP = {
    "2": "CANCELLED BY OR ON BEHALF OF PATIENT",
    "3": "DID NOT ATTEND - NO ADVANCE WARNING",
    "4": "CANCELLED OR POSTPONED BY PROVIDER",
    "5": "ATTENDED ON TIME OR BEFORE PROFESSIONAL READY",
    "6": "ARRIVED LATE BUT SEEN",
    "7": "ARRIVED LATE AND COULD NOT BE SEEN",
}
ACTIVITY_TYPE_MAP = {
    "01": "ADMINISTERING TESTS",
    "02": "ASSESSMENT",
    "03": "CLINICAL INTERVENTION",
    "04": "COUNSELLING, ADVICE, SUPPORT",
    "05": "PATIENT SPECIFIC HEALTH PROMOTION",
    "06": "MULTIDISCIPLINARY TEAM REVIEW",
    "07": "SUPPORTING ANOTHER CLINICIAN",
    "08": "HEALTH VISITOR NEW BIRTH VISIT",
    "09": "HEALTH VISITOR HEALTH REVIEW (6-8 WEEKS)",
    "10": "HEALTH VISITOR HEALTH REVIEW (1 YEAR)",
    "11": "HEALTH VISITOR HEALTH REVIEW (2-2.5 YEARS)",
    "12": "HEALTH VISITOR HANDOVER TO SCHOOL NURSING",
    "97": "OTHER",
}
TEAM_TYPE_MAP = {
    "01": "APPLIANCES SERVICE",
    "02": "ARTS THERAPY SERVICE",
    "03": "CANCER SERVICE",
    "04": "CARDIAC SERVICE",
    "05": "COMMUNITY DENTAL SERVICE",
    "06": "COMMUNITY PAEDIATRICS SERVICE",
    "07": "CONTINENCE SERVICE",
    "09": "COUNSELLING SERVICE",
    "10": "DERMATOLOGY SERVICE",
    "11": "DIABETES SERVICE",
    "12": "DISTRICT NURSING SERVICE",
    "13": "EAR, NOSE AND THROAT SERVICE",
    "14": "END OF LIFE CARE SERVICE",
    "15": "GASTROINTESTINAL SERVICE",
    "16": "HEALTH VISITING SERVICE",
    "17": "HEARING SERVICE",
    "19": "LONG TERM CONDITIONS CASE MANAGEMENT SERVICE",
    "20": "MUSCULOSKELETAL SERVICE",
    "21": "NEUROLOGY SERVICE",
    "23": "OCCUPATIONAL THERAPY SERVICE",
    "24": "ORTHOPTIST SERVICE",
    "25": "PAIN MANAGEMENT SERVICE",
    "26": "PHYSIOTHERAPY SERVICE",
    "27": "PODIATRY SERVICE",
    "29": "REHABILITATION SERVICE",
    "30": "RESPIRATORY SERVICE",
    "31": "RHEUMATOLOGY SERVICE",
    "32": "SCHOOL NURSING SERVICE",
    "33": "SPEECH AND LANGUAGE THERAPY SERVICE",
    "34": "VULNERABLE CHILDREN'S SERVICE",
    "35": "VULNERABLE ADULT'S SERVICE",
    "36": "RESPITE CARE SERVICE",
    "37": "CLINICAL PSYCHOLOGY SERVICE",
    "38": "CHILDREN'S COMMUNITY NURSING SERVICE",
    "39": "DIAGNOSTIC SERVICE",
    "40": "TREATMENT ROOM NURSING SERVICE",
    "41": "HAEMATOLOGY SERVICE",
    "42": "PHLEBOTOMY SERVICE",
    "43": "TISSUE VIABILITY SERVICE",
    "44": "FAMILY SUPPORT SERVICE",
    "45": "INTEGRATED MULTIDISCIPLINARY TEAM",
    "46": "PROSTHETIC SERVICE",
    "47": "SPECIALIST PALLIATIVE CARE SERVICE",
    "48": "ENABLEMENT SERVICE",
    "49": "URGENT CARE SERVICE",
    "50": "WHEELCHAIR SERVICE",
    "51": "CRISIS RESPONSE INTERMEDIATE CARE SERVICE",
    "52": "REABLEMENT INTERMEDIATE CARE SERVICE",
    "53": "HOME-BASED INTERMEDIATE CARE SERVICE",
    "54": "COMMUNITY BED-BASED INTERMEDIATE CARE SERVICE",
    "55": "CHILDREN'S WEIGHT MANAGEMENT SERVICE",
    "56": "ADULT'S WEIGHT MANAGEMENT SERVICE",
    "57": "PUBLIC HEALTH AND LIFESTYLE SERVICE (EXCLUDING WEIGHT MANAGEMENT)",
    "58": "NUTRITION AND DIETETICS SERVICE (EXCLUDING WEIGHT MANAGEMENT)",
}

CONSULTATION_TERM_MECHANISM_MAP = {
    "face to face consultation": "01",
    "home visit note": "01",
    "clinic note": "01",
    "group consultation": "01",
    "gp surgery": "01",
    "nursing home visit note": "01",
    "school visit note": "01",
    "residential home visit note": "01",
    "face to face consultation with relative/carer": "01",
    "first attendance face to face": "01",
    "follow up attendance face to face": "01",
    "joint consultation": "01",
    "telephone consultation": "02",
    "telephone call to a patient": "02",
    "telephone follow-up": "02",
    "telephone encounter": "02",
    "telephone call to relative/carer": "02",
    "telephone call from a patient": "02",
    "telephone call from relative/carer": "02",
    "telephone triage encounter": "02",
    "first telephone consultation": "02",
    "e-mail consultation": "05",
    "e-mail sent to patient": "05",
    "e-mail received from patient": "05",
    "e-mail encounter to carer": "05",
    "email received from carer": "05",
    "e-mail received from carer": "05",
    "consultation via sms text message": "09",
    "consultation via telemedicine web camera": "11",
    "consultation via video conference": "11",
    "group consultation via video conference": "11",
}

UCUM_ALIAS_MAP = {
    "mmHg": "mm[Hg]",
    "mm Hg": "mm[Hg]",
    "beats/min": "/min",
    "/minute": "/min",
    "year": "a",
    "years": "a",
    "litre": "L",
    "month": "mo",
    "months": "mo",
    "Months": "mo",
    "hour": "h",
    "hours": "h",
    "degrees C": "Cel",
    "minute": "min",
    "day": "d",
    "Day": "d",
    "days": "d",
    "week": "wk",
    "weeks": "wk",
    "Kg": "kg",
    "ml/min": "mL/min",
    "ml": "mL",
    "g/l": "g/L",
    "mg/l": "mg/L",
    "IU/L": "[iU]/L",
    "iu/ml": "[iU]/mL",
    "IU/mL": "[iU]/mL",
    "ppm": "[ppm]",
    "ppb": "[ppb]",
}

EXPECTED_COLUMNS = {
    SRC_PATIENT: {
        "PatientNumber", "NHSNumber", "NHSNumberStatus", "RegCDB",
        "RegOrganisationName", "RegOrganisationId", "PersonBirthDate",
        "DateofRegistration", "PracticeCode", "PracticeParentCode",
        "InChildrenService", "archived", "IsConfidential", "IsPatientArchive",
        "CaseloadPatientStatusId", "CaseloadPatientStatusDescription", "eMRN",
        "ADC_UPDT",
    },
    SRC_CONTACT: {
        "CCI", "SRI", "LPI", "PatientNumber", "TeamLocalID", "CContactDate",
        "CContactTime", "OrgCodeComm", "AdminCatCode", "ClinDuration",
        "ConsultType", "CCSubject", "Medium", "LocCode", "SiteCode",
        "GPTherapyInd", "AttendCode", "OfferDate", "AppropDate", "CancelDate",
        "CancelReason", "ReplApptOffDate", "ReplApptDate",
        "ConsultationSourceOriginalTerm", "CDB", "TeamType", "InReport_Period",
        "InReport_Error", "ADC_UPDT",
    },
    SRC_ACTIVITY: {
        "CAI", "CCI", "PatientNumber", "ActivityCode", "CPLID",
        "ContactDuration", "ProcScheme", "ProcCode", "FindingScheme",
        "FindingCode", "ObsScheme", "ObsCode", "ObsValue", "UCUMUnit",
        "LegacyCodeOriginalTerm", "CareActivityDate", "CDB", "ObservationType",
        "CodeCategoryId", "InReport_Period", "ADC_UPDT",
    },
}


def qident(name: str) -> str:
    q = chr(96)
    return q + str(name).replace(q, q + q) + q


def qname(name: str) -> str:
    return ".".join(qident(part) for part in name.split("."))


def sql_escape(value) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "''")


def hash_object(value) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def blank_to_null(column):
    value = F.trim(column.cast("string"))
    return F.when(value == "", F.lit(None).cast("string")).otherwise(value)


def normalized_upper_alnum(column):
    value = F.upper(F.regexp_replace(F.trim(column.cast("string")), r"[^A-Za-z0-9]", ""))
    return F.when(F.length(value) > 0, value)


def normalized_digits(column):
    value = F.regexp_replace(column.cast("string"), r"[^0-9]", "")
    return F.when(F.length(value) > 0, value)


def nhs_checksum_valid(column):
    weighted = reduce(
        lambda total, index: total
        + F.substring(column, index + 1, 1).cast("int") * (10 - index),
        range(9),
        F.lit(0),
    )
    check = 11 - (weighted % 11)
    check = F.when(check == 11, F.lit(0)).otherwise(check)
    return (
        column.rlike(r"^[0-9]{10}$")
        & (check != 10)
        & (check == F.substring(column, 10, 1).cast("int"))
    )


def source_key(*columns):
    return F.sha2(
        F.concat_ws(
            "|",
            F.lit(SOURCE_SYSTEM),
            *[F.coalesce(column.cast("string"), F.lit("<NULL>")) for column in columns],
        ),
        256,
    )


def stable_hash(columns):
    return F.sha2(
        F.concat_ws(
            "\u0001",
            *[
                F.coalesce(F.col(column).cast("string"), F.lit("<NULL>"))
                for column in columns
            ],
        ),
        256,
    )


def mapping_value(column, mapping):
    pairs = []
    for key, value in mapping.items():
        pairs.extend([F.lit(key), F.lit(value)])
    if not pairs:
        return F.lit(None).cast("string")
    return F.create_map(*pairs)[column]


def mapping_status(code_column, description_column):
    return (
        F.when(code_column.isNull(), F.lit("NULL"))
        .when(description_column.isNotNull(), F.lit("MAPPED"))
        .otherwise(F.lit("UNRECOGNISED_OR_LEGACY"))
    )


def date_quality(column):
    return (
        F.when(column.isNull(), F.lit("MISSING"))
        .when(column < F.lit("2000-01-01").cast("date"), F.lit("BEFORE_2000"))
        .when(column > F.current_date(), F.lit("FUTURE"))
        .otherwise(F.lit("PLAUSIBLE"))
    )


def duration_quality(column):
    return (
        F.when(column.isNull(), F.lit("MISSING"))
        .when(column < 0, F.lit("NEGATIVE"))
        .when(column > 9999, F.lit("OVER_CSDS_MAX"))
        .when(column > 1440, F.lit("OVER_24_HOURS"))
        .otherwise(F.lit("PLAUSIBLE"))
    )


def verify_unique_key(df: DataFrame, keys: list[str]) -> int:
    return df.groupBy(*keys).count().where(F.col("count") > 1).count()


def assert_unique_non_null(df: DataFrame, keys: list[str], label: str) -> None:
    null_condition = reduce(
        lambda left, right: left | right,
        [F.col(key).isNull() for key in keys],
    )
    null_count = df.where(null_condition).count()
    duplicate_count = verify_unique_key(df, keys)
    assert null_count == 0, f"{label}: {null_count} rows have NULL keys"
    assert duplicate_count == 0, f"{label}: {duplicate_count} duplicate keys"


def _latest_version(table: str) -> int:
    return int(spark.sql(f"DESCRIBE HISTORY {qname(table)} LIMIT 1").collect()[0]["version"])


def capture_dependency(table: str) -> dict:
    detail = spark.sql(f"DESCRIBE DETAIL {qname(table)}").collect()[0].asDict(recursive=True)
    schema_json = spark.table(table).schema.json()
    fields = {field.name for field in spark.table(table).schema.fields}
    include_count = table in ROW_COUNT_DEPENDENCIES
    row_count = None
    max_adc_value = None
    if include_count:
        aggregates = [F.count(F.lit(1)).alias("row_count")]
        if "ADC_UPDT" in fields:
            aggregates.append(F.max("ADC_UPDT").alias("max_adc"))
        summary = spark.table(table).agg(*aggregates).collect()[0].asDict()
        row_count = int(summary["row_count"])
        max_adc_value = summary.get("max_adc")
    payload = {
        "table": table,
        "id": str(detail.get("id")),
        "location": str(detail.get("location")),
        "last_modified": str(detail.get("lastModified")),
        "version": _latest_version(table),
        "row_count": row_count,
        "max_adc": str(max_adc_value) if max_adc_value is not None else None,
        "schema_hash": hashlib.sha256(schema_json.encode("utf-8")).hexdigest(),
    }
    return {
        **payload,
        "max_adc_value": max_adc_value,
        "fingerprint": hash_object(payload),
    }


def _community_hardening_sql_identifier(table_name: str) -> str:
    return '.'.join('`' + part.replace('`', '``') + '`' for part in table_name.split('.'))

def _community_hardening_snapshot_fallback_allowed(exc: BaseException) -> bool:
    messages = []
    current = exc
    while current is not None and len(messages) < 8:
        messages.append(str(current).lower())
        current = current.__cause__ or current.__context__
    combined = ' '.join(messages)
    return any(marker in combined for marker in (
        'delta_unsupported_time_travel_beyond_deleted_file_retention_duration',
        'time travel beyond delta.deletedfileretentionduration',
        'delta_log_file_not_found',
        'unable to retrieve the delta log files to construct table version',
        'delta log file not found',
    ))

def _community_hardening_read_pinned_snapshot(table_name: str, version: int):
    requested_version = int(version)
    try:
        pinned = (
            spark.read.format('delta')
            .option('versionAsOf', requested_version)
            .table(table_name)
        )
        # Spark Connect defers Delta-log validation until analysis/action.
        _ = pinned.schema
        pinned.limit(1).collect()
        return pinned
    except Exception as exc:
        if not _community_hardening_snapshot_fallback_allowed(exc):
            raise
        row = (
            spark.sql('DESCRIBE HISTORY ' + _community_hardening_sql_identifier(table_name) + ' LIMIT 1')
            .select('version')
            .first()
        )
        latest_version = None if row is None else int(row['version'])
        if latest_version != requested_version:
            raise
        print(
            '[WARN] Explicit time travel is outside retained Delta history for '
            + table_name + ' at current version ' + str(requested_version)
            + '; reading the same current snapshot without versionAsOf.'
        )
        current_snapshot = spark.table(table_name)
        _ = current_snapshot.schema
        return current_snapshot

def read_pinned(metadata: dict) -> DataFrame:
    # Unity Catalog managed-storage paths cannot be read directly on serverless.
    # Pin by table name/version and re-check the full dependency fingerprint before writes.
    return _community_hardening_read_pinned_snapshot(
        metadata["table"],
        int(metadata["version"]),
    )


def ensure_cdf(table: str) -> None:
    spark.sql(
        f"ALTER TABLE {qname(table)} "
        "SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
    )


def cdf_enabled(table: str) -> bool:
    properties = spark.sql(f"DESCRIBE DETAIL {qname(table)}").collect()[0]["properties"] or {}
    return str(properties.get("delta.enableChangeDataFeed", "false")).lower() == "true"


def apply_comments(table: str, table_comment: str, comments: dict[str, str]) -> None:
    spark.sql(
        f"COMMENT ON TABLE {qname(table)} IS '{sql_escape(table_comment)}'"
    )
    columns = set(spark.table(table).columns)
    for column, comment in comments.items():
        if column in columns:
            spark.sql(
                f"ALTER TABLE {qname(table)} ALTER COLUMN {qident(column)} "
                f"COMMENT '{sql_escape(comment)}'"
            )


def create_operational_tables() -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qname(TARGET_SCHEMA)}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qname(STATE_TABLE)} (
          target_table STRING NOT NULL,
          dependency_name STRING NOT NULL,
          dependency_fingerprint STRING NOT NULL,
          contract_version STRING NOT NULL,
          run_id STRING NOT NULL,
          committed_at TIMESTAMP NOT NULL
        ) USING DELTA
        COMMENT 'Community Bronze dependency fingerprints committed after successful verification.'
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qname(AUDIT_TABLE)} (
          run_id STRING NOT NULL,
          target_schema STRING NOT NULL,
          started_at STRING NOT NULL,
          completed_at STRING NOT NULL,
          status STRING NOT NULL,
          force_full_refresh BOOLEAN NOT NULL,
          rebuilt_targets ARRAY<STRING>,
          result_json STRING NOT NULL,
          audit_ts TIMESTAMP NOT NULL
        ) USING DELTA
        COMMENT 'Community Bronze run-level profile, validation, mapping and merge metrics.'
        """
    )


def state_for_target(target: str) -> dict[str, str]:
    rows = (
        spark.table(STATE_TABLE)
        .where(F.col("target_table") == target)
        .select("dependency_name", "dependency_fingerprint")
        .collect()
    )
    return {row["dependency_name"]: row["dependency_fingerprint"] for row in rows}


def needs_run(target: str, fingerprints: dict[str, str]) -> bool:
    if FORCE_FULL_REFRESH or not bronze_table_exists(target):
        return True
    return state_for_target(target) != fingerprints


def record_state(target: str, fingerprints: dict[str, str], contract_version: str) -> None:
    spark.sql(
        f"DELETE FROM {qname(STATE_TABLE)} "
        f"WHERE target_table = '{sql_escape(target)}'"
    )
    rows = [
        (target, name, value, contract_version, COMMUNITY_RUN_ID)
        for name, value in sorted(fingerprints.items())
    ]
    (
        spark.createDataFrame(
            rows,
            "target_table string, dependency_name string, "
            "dependency_fingerprint string, contract_version string, run_id string",
        )
        .withColumn("committed_at", F.current_timestamp())
        .write.mode("append")
        .saveAsTable(STATE_TABLE)
    )


HASH_EXCLUDE = {
    "ROW_HASH",
    "SOURCE_PRESENT_IND",
    "SOURCE_ABSENT_DETECTED_TS",
    "PIPELINE_RUN_ID",
    "PIPELINE_PROCESSED_TS",
}


def with_row_hash(df: DataFrame) -> DataFrame:
    columns = sorted(column for column in df.columns if column not in HASH_EXCLUDE)
    return df.withColumn("ROW_HASH", stable_hash(columns))


def latest_delta_metrics(table: str) -> dict:
    history = spark.sql(f"DESCRIBE HISTORY {qname(table)} LIMIT 1").collect()[0]
    operation_metrics = history["operationMetrics"] or {}
    return {
        "operation": history["operation"],
        **{key: str(value) for key, value in operation_metrics.items()},
    }


def update_snapshot_table(
    df: DataFrame,
    target: str,
    key: str,
) -> dict:
    staged = bronze_project_contract(
        with_row_hash(df),
        target,
    )
    if not bronze_table_exists(target):
        (
            staged.write.format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .saveAsTable(target)
        )
        return {"operation": "CREATE", "rows": str(staged.count())}

    ensure_cdf(target)
    values = {column: f"s.{qident(column)}" for column in staged.columns}
    comparisons = " OR ".join(
        f"NOT (t.{qident(column)} <=> s.{qident(column)})"
        for column in staged.columns
        if column != key
    )
    (
        DeltaTable.forName(spark, target)
        .alias("t")
        .merge(
            staged.alias("s"),
            f"t.{qident(key)} = s.{qident(key)}",
        )
        .whenMatchedUpdate(
            condition=comparisons or "false",
            set=values,
        )
        .whenNotMatchedInsert(values=values)
        .execute()
    )
    return latest_delta_metrics(target)


def append_audit(payload: dict, status: str, rebuilt_targets: list[str]) -> None:
    row = [
        (
            COMMUNITY_RUN_ID,
            TARGET_SCHEMA,
            STARTED_AT,
            bronze_utc_now(),
            status,
            bool(FORCE_FULL_REFRESH),
            rebuilt_targets,
            bronze_json(payload),
        )
    ]
    schema = T.StructType(
        [
            T.StructField("run_id", T.StringType(), False),
            T.StructField("target_schema", T.StringType(), False),
            T.StructField("started_at", T.StringType(), False),
            T.StructField("completed_at", T.StringType(), False),
            T.StructField("status", T.StringType(), False),
            T.StructField("force_full_refresh", T.BooleanType(), False),
            T.StructField("rebuilt_targets", T.ArrayType(T.StringType()), True),
            T.StructField("result_json", T.StringType(), False),
        ]
    )
    (
        spark.createDataFrame(row, schema)
        .withColumn("audit_ts", F.current_timestamp())
        .write.mode("append")
        .saveAsTable(AUDIT_TABLE)
    )


def assert_expected_columns() -> None:
    for table, expected in EXPECTED_COLUMNS.items():
        actual = set(spark.table(table).columns)
        missing = sorted(expected - actual)
        assert not missing, f"{table}: missing required columns {missing}"


def run_preflight(
    metadata: dict[str, dict],
    patient_source: DataFrame,
    contact_source: DataFrame,
    activity_source: DataFrame,
) -> dict:
    assert_expected_columns()

    raw_times = [
        metadata[table]["max_adc_value"]
        for table in (SRC_PATIENT, SRC_CONTACT, SRC_ACTIVITY)
        if metadata[table]["max_adc_value"] is not None
    ]
    assert len(raw_times) == 3, "All three Community raw tables must contain ADC_UPDT"
    skew_seconds = int((max(raw_times) - min(raw_times)).total_seconds())
    assert skew_seconds <= MAX_SOURCE_SKEW_SECONDS, (
        f"Community raw snapshot skew {skew_seconds}s exceeds "
        f"{MAX_SOURCE_SKEW_SECONDS}s"
    )

    patient_key_duplicates = (
        patient_source.groupBy("RegCDB", "PatientNumber")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    contact_key_duplicates = (
        contact_source.groupBy("CDB", "CCI", "SRI")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    assert patient_key_duplicates == 0, (
        f"{SRC_PATIENT}: {patient_key_duplicates} duplicate RegCDB+PatientNumber keys"
    )
    assert contact_key_duplicates == 0, (
        f"{SRC_CONTACT}: {contact_key_duplicates} duplicate CDB+CCI+SRI keys"
    )

    patient_keys = patient_source.select(
        F.col("RegCDB").alias("CDB"),
        F.col("PatientNumber"),
    )
    contacts_without_patient = (
        contact_source.select("CDB", "PatientNumber")
        .distinct()
        .join(patient_keys, ["CDB", "PatientNumber"], "left_anti")
        .count()
    )
    activities_without_patient = (
        activity_source.select("CDB", "PatientNumber")
        .distinct()
        .join(patient_keys, ["CDB", "PatientNumber"], "left_anti")
        .count()
    )
    assert contacts_without_patient == 0, (
        f"{contacts_without_patient} contact patient keys are absent from {SRC_PATIENT}"
    )
    assert activities_without_patient == 0, (
        f"{activities_without_patient} activity patient keys are absent from {SRC_PATIENT}"
    )

    deferred_columns = [
        "ProcScheme",
        "ProcCode",
        "FindingScheme",
        "FindingCode",
        "ObsScheme",
        "ObsCode",
    ]
    deferred_summary = (
        activity_source.agg(
            *[
                F.sum(F.when(F.col(column).isNotNull(), 1).otherwise(0)).alias(column)
                for column in deferred_columns
            ]
        )
        .collect()[0]
        .asDict()
    )
    populated_deferred = {
        column: int(value or 0)
        for column, value in deferred_summary.items()
        if int(value or 0) > 0
    }
    assert not populated_deferred, (
        "Previously empty CSDS terminology columns are now populated and require "
        f"string-safe scheme validation before publication: {populated_deferred}"
    )

    excluded_contact_columns = [
        "CancelDate",
        "CancelReason",
        "ReplApptOffDate",
        "ReplApptDate",
    ]
    excluded_summary = (
        contact_source.agg(
            *[
                F.sum(
                    F.when(
                        F.col(column).isNotNull()
                        & (F.trim(F.col(column).cast("string")) != ""),
                        1,
                    ).otherwise(0)
                ).alias(column)
                for column in excluded_contact_columns
            ]
        )
        .collect()[0]
        .asDict()
    )
    newly_populated_excluded = {
        column: int(value or 0)
        for column, value in excluded_summary.items()
        if int(value or 0) > 0
    }
    assert not newly_populated_excluded, (
        "Excluded cancellation/replacement columns are now populated and must be "
        f"added to the contract: {newly_populated_excluded}"
    )

    in_report_contact = {
        str(row["InReport_Period"]): int(row["count"])
        for row in contact_source.groupBy("InReport_Period").count().collect()
    }
    in_report_activity = {
        str(row["InReport_Period"]): int(row["count"])
        for row in activity_source.groupBy("InReport_Period").count().collect()
    }
    in_report_error = {
        str(row["InReport_Error"]): int(row["count"])
        for row in contact_source.groupBy("InReport_Error").count().collect()
    }

    return {
        "source_rows": {
            table: int(metadata[table]["row_count"])
            for table in (SRC_PATIENT, SRC_CONTACT, SRC_ACTIVITY)
        },
        "source_skew_seconds": skew_seconds,
        "patient_key_duplicates": patient_key_duplicates,
        "contact_key_duplicates": contact_key_duplicates,
        "contacts_without_patient_lookup": contacts_without_patient,
        "activities_without_patient_lookup": activities_without_patient,
        "deferred_column_non_nulls": deferred_summary,
        "excluded_contact_column_non_nulls": excluded_summary,
        "in_report_period_contact": in_report_contact,
        "in_report_period_activity": in_report_activity,
        "in_report_error_contact": in_report_error,
    }


def build_alias_bridge(
    person_alias: DataFrame,
    alias_type: int,
    normalizer,
    key_name: str,
    candidate_name: str,
    person_name: str,
) -> DataFrame:
    now = F.current_timestamp()
    normalized = normalizer(F.col("ALIAS"))
    grouped = (
        person_alias.where(
            (F.col("PERSON_ALIAS_TYPE_CD").cast("long") == alias_type)
            & (F.col("ACTIVE_IND").cast("long") == 1)
            & (
                F.col("BEG_EFFECTIVE_DT_TM").isNull()
                | (F.col("BEG_EFFECTIVE_DT_TM") <= now)
            )
            & (
                F.col("END_EFFECTIVE_DT_TM").isNull()
                | (F.col("END_EFFECTIVE_DT_TM") > now)
            )
        )
        .withColumn(key_name, normalized)
        .where(F.col(key_name).isNotNull())
        .groupBy(key_name)
        .agg(
            F.countDistinct(F.col("PERSON_ID").cast("long")).alias(candidate_name),
            F.min(F.col("PERSON_ID").cast("long")).alias("_minimum_person_id"),
        )
    )
    return grouped.withColumn(
        person_name,
        F.when(F.col(candidate_name) == 1, F.col("_minimum_person_id")),
    ).drop("_minimum_person_id")


def build_patient_stage(
    patient_source: DataFrame,
    person_alias: DataFrame,
    person_canonical: DataFrame,
) -> DataFrame:
    nhs_alias = build_alias_bridge(
        person_alias,
        NHS_ALIAS_TYPE,
        normalized_digits,
        "_nhs_key",
        "nhs_candidate_count",
        "_nhs_person_id",
    )
    mrn_alias = build_alias_bridge(
        person_alias,
        MRN_ALIAS_TYPE,
        normalized_upper_alnum,
        "_mrn_key",
        "mrn_candidate_count",
        "_mrn_person_id",
    )

    source = (
        patient_source.withColumn("_nhs_norm", normalized_digits(F.col("NHSNumber")))
        .withColumn(
            "nhs_checksum_valid_ind",
            nhs_checksum_valid(F.col("_nhs_norm")),
        )
        .withColumn(
            "_nhs_key",
            F.when(
                (F.col("NHSNumberStatus") == "02")
                & F.col("nhs_checksum_valid_ind"),
                F.col("_nhs_norm"),
            ),
        )
        .withColumn("_mrn_key", normalized_upper_alnum(F.col("eMRN")))
    )

    linked = (
        source.join(nhs_alias, "_nhs_key", "left")
        .join(mrn_alias, "_mrn_key", "left")
        .withColumn(
            "nhs_candidate_count",
            F.coalesce(F.col("nhs_candidate_count"), F.lit(0)).cast("long"),
        )
        .withColumn(
            "mrn_candidate_count",
            F.coalesce(F.col("mrn_candidate_count"), F.lit(0)).cast("long"),
        )
        .withColumn(
            "_match_method",
            F.when(
                (F.col("nhs_candidate_count") > 1)
                | (F.col("mrn_candidate_count") > 1),
                F.lit("NONE"),
            )
            .when(
                F.col("_nhs_person_id").isNotNull()
                & F.col("_mrn_person_id").isNotNull()
                & (F.col("_nhs_person_id") == F.col("_mrn_person_id")),
                F.lit("NHS_MRN_AGREE"),
            )
            .when(
                F.col("_nhs_person_id").isNotNull()
                & F.col("_mrn_person_id").isNotNull(),
                F.lit("NONE"),
            )
            .when(F.col("_nhs_person_id").isNotNull(), F.lit("NHS_ONLY"))
            .when(F.col("_mrn_person_id").isNotNull(), F.lit("MRN_ONLY"))
            .otherwise(F.lit("NONE")),
        )
        .withColumn(
            "_pre_status",
            F.when(
                (F.col("nhs_candidate_count") > 1)
                | (F.col("mrn_candidate_count") > 1),
                F.lit("AMBIGUOUS"),
            )
            .when(
                F.col("_nhs_person_id").isNotNull()
                & F.col("_mrn_person_id").isNotNull()
                & (F.col("_nhs_person_id") != F.col("_mrn_person_id")),
                F.lit("CONFLICT"),
            )
            .when(
                F.col("_match_method").isin("NHS_MRN_AGREE", "NHS_ONLY", "MRN_ONLY"),
                F.lit("MATCHED"),
            )
            .otherwise(F.lit("UNMATCHED")),
        )
        .withColumn(
            "_resolved_person_id",
            F.when(
                F.col("_match_method").isin("NHS_MRN_AGREE", "NHS_ONLY"),
                F.col("_nhs_person_id"),
            ).when(
                F.col("_match_method") == "MRN_ONLY",
                F.col("_mrn_person_id"),
            ),
        )
    )

    canonical = person_canonical.select(
        F.col("person_id").cast("long").alias("_canonical_person_id"),
        F.col("birth_date").alias("_canonical_birth_date"),
    )
    result = (
        linked.join(
            canonical,
            F.col("_resolved_person_id") == F.col("_canonical_person_id"),
            "left",
        )
        .withColumn(
            "person_id",
            F.when(
                (F.col("_pre_status") == "MATCHED")
                & F.col("_canonical_person_id").isNotNull(),
                F.col("_canonical_person_id"),
            ).cast("long"),
        )
        .withColumn(
            "person_match_status",
            F.when(
                (F.col("_pre_status") == "MATCHED")
                & F.col("_canonical_person_id").isNull(),
                F.lit("CANONICAL_PERSON_MISSING"),
            ).otherwise(F.col("_pre_status")),
        )
        .withColumn(
            "person_match_method",
            F.when(F.col("person_match_status") == "MATCHED", F.col("_match_method"))
            .otherwise(F.lit("NONE")),
        )
        .withColumn(
            "birth_date_concordance_status",
            F.when(F.col("person_id").isNull(), F.lit("NOT_ASSESSED"))
            .when(
                F.col("PersonBirthDate").isNull()
                | F.col("_canonical_birth_date").isNull(),
                F.lit("NOT_ASSESSED"),
            )
            .when(
                F.col("PersonBirthDate") == F.col("_canonical_birth_date"),
                F.lit("MATCH"),
            )
            .otherwise(F.lit("MISMATCH")),
        )
    )

    return result.select(
        source_key(
            F.col("RegCDB"),
            F.col("PatientNumber"),
        ).alias("community_patient_key"),
        F.col("RegCDB").cast("long").alias("source_database_id"),
        F.col("PatientNumber").cast("long").alias("source_patient_number"),
        F.col("RegOrganisationId").cast("long").alias("source_service_id"),
        blank_to_null(F.col("RegOrganisationName")).alias("source_service_name"),
        F.col("DateofRegistration").alias("community_registration_date"),
        F.col("CaseloadPatientStatusId").cast("long").alias(
            "caseload_patient_status_id"
        ),
        blank_to_null(F.col("CaseloadPatientStatusDescription")).alias(
            "caseload_patient_status_description"
        ),
        (F.coalesce(F.col("InChildrenService"), F.lit(0)) == 1).alias(
            "in_children_service_ind"
        ),
        F.col("archived").cast("long").alias("source_archived_ind"),
        F.col("IsPatientArchive").cast("long").alias("source_patient_archive_ind"),
        (
            (F.coalesce(F.col("archived"), F.lit(0)) == 0)
            & (F.coalesce(F.col("IsPatientArchive"), F.lit(0)) == 0)
        ).alias("source_current_ind"),
        (F.coalesce(F.col("IsConfidential"), F.lit(0)) == 1).alias(
            "source_confidential_ind"
        ),
        blank_to_null(F.col("PracticeCode")).alias("registered_practice_ods_code"),
        blank_to_null(F.col("PracticeParentCode")).alias(
            "registered_practice_parent_ods_code"
        ),
        blank_to_null(F.col("NHSNumberStatus")).alias("nhs_number_status_code"),
        F.col("nhs_checksum_valid_ind").cast("boolean"),
        F.col("nhs_candidate_count"),
        F.col("mrn_candidate_count"),
        F.col("person_id"),
        F.col("person_match_status"),
        F.col("person_match_method"),
        (F.col("_pre_status") == "CONFLICT").alias("identifier_conflict_ind"),
        F.col("birth_date_concordance_status"),
        F.col("ADC_UPDT").alias("source_snapshot_loaded_ts"),
        F.lit(SRC_PATIENT).alias("source_table"),
        F.lit(PATIENT_CONTRACT_VERSION).alias("community_schema_version"),
    )


def build_organization_bridge(
    organization_alias: DataFrame,
    organization: DataFrame,
) -> DataFrame:
    now = F.current_timestamp()
    aliases = (
        organization_alias.where(
            (F.col("ACTIVE_IND").cast("long") == 1)
            & (
                F.col("BEG_EFFECTIVE_DT_TM").isNull()
                | (F.col("BEG_EFFECTIVE_DT_TM") <= now)
            )
            & (
                F.col("END_EFFECTIVE_DT_TM").isNull()
                | (F.col("END_EFFECTIVE_DT_TM") > now)
            )
        )
        .withColumn("_organization_alias_key", normalized_upper_alnum(F.col("ALIAS")))
        .where(F.col("_organization_alias_key").isNotNull())
        .groupBy("_organization_alias_key")
        .agg(
            F.countDistinct(F.col("ORGANIZATION_ID").cast("long")).alias(
                "organization_candidate_count"
            ),
            F.min(F.col("ORGANIZATION_ID").cast("long")).alias(
                "_minimum_organization_id"
            ),
        )
        .withColumn(
            "organization_id",
            F.when(
                F.col("organization_candidate_count") == 1,
                F.col("_minimum_organization_id"),
            ),
        )
        .drop("_minimum_organization_id")
    )
    organizations = organization.select(
        F.col("ORGANIZATION_ID").cast("long").alias("organization_id"),
        blank_to_null(F.col("ORG_NAME")).alias("organization_name"),
    )
    return aliases.join(organizations, "organization_id", "left")


def build_contact_stage(
    contact_source: DataFrame,
    patient_stage: DataFrame,
    organization_alias: DataFrame,
    organization: DataFrame,
) -> DataFrame:
    patient_join = patient_stage.select(
        F.col("source_database_id").alias("CDB"),
        F.col("source_patient_number").alias("PatientNumber"),
        "community_patient_key",
        "person_id",
        "person_match_status",
        "source_service_id",
        "source_service_name",
    )
    variants = contact_source.groupBy("CDB", "CCI").agg(
        F.count(F.lit(1)).cast("long").alias("care_contact_id_variant_count")
    )
    org_bridge = build_organization_bridge(organization_alias, organization)
    commissioner_org = org_bridge.select(
        F.col("_organization_alias_key").alias("_commissioner_alias_key"),
        F.col("organization_candidate_count").alias(
            "commissioner_organization_candidate_count"
        ),
        F.col("organization_id").alias("commissioner_organization_id"),
        F.col("organization_name").alias("commissioner_organization_name"),
    )
    treatment_org = org_bridge.select(
        F.col("_organization_alias_key").alias("_treatment_alias_key"),
        F.col("organization_candidate_count").alias(
            "treatment_organization_candidate_count"
        ),
        F.col("organization_id").alias("treatment_organization_id"),
        F.col("organization_name").alias("treatment_organization_name"),
    )
    term_rows = [
        (term, code) for term, code in CONSULTATION_TERM_MECHANISM_MAP.items()
    ]
    term_map = spark.createDataFrame(
        term_rows,
        "source_consultation_term_key string, _term_mechanism_code string",
    )

    base = (
        contact_source.join(variants, ["CDB", "CCI"], "left")
        .join(patient_join, ["CDB", "PatientNumber"], "left")
        .withColumn(
            "_commissioner_alias_key",
            normalized_upper_alnum(F.col("OrgCodeComm")),
        )
        .join(commissioner_org, "_commissioner_alias_key", "left")
        .withColumn(
            "_treatment_alias_key",
            normalized_upper_alnum(F.col("SiteCode")),
        )
        .join(treatment_org, "_treatment_alias_key", "left")
        .withColumn(
            "source_consultation_term",
            blank_to_null(F.col("ConsultationSourceOriginalTerm")),
        )
        .withColumn(
            "source_consultation_term_key",
            F.lower(F.trim(F.col("source_consultation_term"))),
        )
        .join(F.broadcast(term_map), "source_consultation_term_key", "left")
        .withColumn(
            "submitted_consultation_mechanism_code",
            blank_to_null(F.col("Medium")),
        )
    )

    for source_column, output_prefix, mapping in [
        ("AdminCatCode", "administrative_category", ADMIN_CATEGORY_MAP),
        ("ConsultType", "consultation_type", CONSULT_TYPE_MAP),
        ("CCSubject", "care_contact_subject", CONTACT_SUBJECT_MAP),
        ("LocCode", "activity_location_type", ACTIVITY_LOCATION_MAP),
        ("GPTherapyInd", "group_therapy", GROUP_THERAPY_MAP),
        ("AttendCode", "attendance_status", ATTENDANCE_MAP),
        ("TeamType", "service_team_type", TEAM_TYPE_MAP),
    ]:
        code_name = f"{output_prefix}_code"
        description_name = f"{output_prefix}_description"
        status_name = f"{output_prefix}_mapping_status"
        base = (
            base.withColumn(code_name, blank_to_null(F.col(source_column)))
            .withColumn(
                description_name,
                mapping_value(F.col(code_name), mapping),
            )
            .withColumn(
                status_name,
                mapping_status(F.col(code_name), F.col(description_name)),
            )
        )

    base = (
        base.withColumn(
            "submitted_consultation_mechanism_description",
            mapping_value(
                F.col("submitted_consultation_mechanism_code"),
                CONSULTATION_MECHANISM_MAP,
            ),
        )
        .withColumn(
            "submitted_consultation_mechanism_mapping_status",
            mapping_status(
                F.col("submitted_consultation_mechanism_code"),
                F.col("submitted_consultation_mechanism_description"),
            ),
        )
        .withColumn(
            "normalized_consultation_mechanism_code",
            F.when(
                F.col("submitted_consultation_mechanism_code").isNotNull()
                & (F.col("submitted_consultation_mechanism_code") != "98")
                & F.col("submitted_consultation_mechanism_description").isNotNull(),
                F.col("submitted_consultation_mechanism_code"),
            )
            .when(F.col("_term_mechanism_code").isNotNull(), F.col("_term_mechanism_code"))
            .otherwise(F.col("submitted_consultation_mechanism_code")),
        )
        .withColumn(
            "normalized_consultation_mechanism_description",
            mapping_value(
                F.col("normalized_consultation_mechanism_code"),
                CONSULTATION_MECHANISM_MAP,
            ),
        )
        .withColumn(
            "normalized_consultation_mechanism_method",
            F.when(
                F.col("submitted_consultation_mechanism_code").isNotNull()
                & (F.col("submitted_consultation_mechanism_code") != "98")
                & F.col("submitted_consultation_mechanism_description").isNotNull(),
                F.lit("SUBMITTED_CODE"),
            )
            .when(
                F.col("_term_mechanism_code").isNotNull(),
                F.lit("EXACT_SOURCE_TERM_CANDIDATE"),
            )
            .when(
                F.col("submitted_consultation_mechanism_code").isNotNull(),
                F.lit("SUBMITTED_OTHER_OR_UNMAPPED"),
            )
            .otherwise(F.lit("UNMAPPED")),
        )
        .withColumn(
            "care_contact_datetime_local",
            F.expr(
                "try_cast(concat(cast(CContactDate as string), ' ', "
                "CContactTime) as timestamp_ntz)"
            ),
        )
    )

    return base.select(
        source_key(F.col("CDB"), F.col("CCI"), F.col("SRI")).alias(
            "community_care_contact_key"
        ),
        F.col("CDB").cast("long").alias("source_database_id"),
        blank_to_null(F.col("CCI")).alias("care_contact_id"),
        blank_to_null(F.col("SRI")).alias("service_request_id"),
        F.col("community_patient_key"),
        F.col("person_id"),
        F.col("person_match_status"),
        F.col("source_service_id"),
        F.col("source_service_name"),
        blank_to_null(F.col("TeamLocalID")).alias("source_team_id"),
        F.col("care_contact_id_variant_count"),
        (F.col("care_contact_id_variant_count") > 1).alias(
            "care_contact_id_duplicate_ind"
        ),
        F.col("CContactDate").alias("care_contact_date"),
        blank_to_null(F.col("CContactTime")).alias("care_contact_time_raw"),
        F.col("care_contact_datetime_local"),
        F.when(
            F.col("care_contact_datetime_local").isNull(),
            F.lit("PARSE_FAILED"),
        ).otherwise(F.lit("PARSED")).alias("care_contact_datetime_status"),
        date_quality(F.col("CContactDate")).alias("care_contact_date_quality_status"),
        F.col("ClinDuration").cast("long").alias(
            "clinical_contact_duration_minutes"
        ),
        duration_quality(F.col("ClinDuration")).alias(
            "clinical_contact_duration_quality_status"
        ),
        F.col("OfferDate").alias("earliest_reasonable_offer_date"),
        F.col("AppropDate").alias("earliest_clinically_appropriate_date"),
        blank_to_null(F.col("OrgCodeComm")).alias("commissioner_ods_code"),
        F.col("commissioner_organization_candidate_count"),
        F.col("commissioner_organization_id"),
        F.col("commissioner_organization_name"),
        F.when(
            F.col("commissioner_organization_candidate_count") == 1,
            F.lit("EXACT_UNIQUE_ALIAS"),
        )
        .when(
            F.col("commissioner_organization_candidate_count") > 1,
            F.lit("AMBIGUOUS_ALIAS"),
        )
        .otherwise(F.lit("UNMAPPED"))
        .alias("commissioner_organization_match_status"),
        blank_to_null(F.col("SiteCode")).alias("treatment_site_ods_code"),
        F.col("treatment_organization_candidate_count"),
        F.col("treatment_organization_id"),
        F.col("treatment_organization_name"),
        F.when(
            F.col("treatment_organization_candidate_count") == 1,
            F.lit("EXACT_UNIQUE_ALIAS"),
        )
        .when(
            F.col("treatment_organization_candidate_count") > 1,
            F.lit("AMBIGUOUS_ALIAS"),
        )
        .otherwise(F.lit("UNMAPPED"))
        .alias("treatment_organization_match_status"),
        "administrative_category_code",
        "administrative_category_description",
        "administrative_category_mapping_status",
        "consultation_type_code",
        "consultation_type_description",
        "consultation_type_mapping_status",
        "care_contact_subject_code",
        "care_contact_subject_description",
        "care_contact_subject_mapping_status",
        "submitted_consultation_mechanism_code",
        "submitted_consultation_mechanism_description",
        "submitted_consultation_mechanism_mapping_status",
        "normalized_consultation_mechanism_code",
        "normalized_consultation_mechanism_description",
        "normalized_consultation_mechanism_method",
        "activity_location_type_code",
        "activity_location_type_description",
        "activity_location_type_mapping_status",
        "group_therapy_code",
        "group_therapy_description",
        "group_therapy_mapping_status",
        "attendance_status_code",
        "attendance_status_description",
        "attendance_status_mapping_status",
        "service_team_type_code",
        "service_team_type_description",
        "service_team_type_mapping_status",
        "source_consultation_term",
        F.lit(CSDS_MAPPING_VERSION).alias("csds_mapping_version"),
        F.lit(CONSULTATION_TERM_MAPPING_VERSION).alias(
            "consultation_term_mapping_version"
        ),
        F.col("ADC_UPDT").alias("source_snapshot_loaded_ts"),
        F.lit(SRC_CONTACT).alias("source_table"),
        F.lit(CONTACT_CONTRACT_VERSION).alias("community_schema_version"),
    )


def clean_activity_term(column):
    return F.lower(
        F.trim(
            F.regexp_replace(
                blank_to_null(column),
                r":\s*$",
                "",
            )
        )
    )


def build_snomed_candidate_map(
    activity_distinct: DataFrame,
    omop_concept: DataFrame,
    omop_synonym: DataFrame,
) -> DataFrame:
    term_universe = (
        activity_distinct.select(
            clean_activity_term(F.col("LegacyCodeOriginalTerm")).alias(
                "source_clinical_term_key"
            )
        )
        .where(F.col("source_clinical_term_key").isNotNull())
        .distinct()
    )
    concepts = (
        omop_concept.where(
            (F.col("vocabulary_id") == "SNOMED")
            & F.col("invalid_reason").isNull()
        )
        .select(
            F.col("concept_id").cast("long").alias("_concept_id"),
            F.col("concept_code").alias("_concept_code"),
            F.col("concept_name").alias("_concept_name"),
            F.col("domain_id").alias("_concept_domain"),
            F.col("concept_class_id").alias("_concept_class"),
            F.col("standard_concept").alias("_standard_concept"),
            F.lower(F.trim(F.col("concept_name"))).alias("_concept_name_key"),
            F.when(F.col("standard_concept") == "S", 1).otherwise(0).alias(
                "_is_standard"
            ),
        )
    )

    name_candidates_0 = F.broadcast(term_universe).join(
        concepts,
        F.col("source_clinical_term_key") == F.col("_concept_name_key"),
        "inner",
    )
    name_window = Window.partitionBy("source_clinical_term_key")
    name_candidates = (
        name_candidates_0.withColumn(
            "_max_standard",
            F.max("_is_standard").over(name_window),
        )
        .where(F.col("_is_standard") == F.col("_max_standard"))
        .drop("_max_standard")
    )
    name_group = name_candidates.groupBy("source_clinical_term_key").agg(
        F.countDistinct("_concept_id").cast("long").alias(
            "snomed_candidate_count"
        ),
        F.min("_concept_id").alias("_selected_concept_id"),
        F.min("_concept_code").alias("_selected_concept_code"),
        F.min("_concept_name").alias("_selected_concept_name"),
        F.min("_concept_domain").alias("_selected_concept_domain"),
        F.min("_concept_class").alias("_selected_concept_class"),
        F.min("_standard_concept").alias("_selected_standard_concept"),
    )
    name_terms = name_group.select("source_clinical_term_key")

    synonyms = (
        omop_synonym.join(
            concepts.select(
                "_concept_id",
                "_concept_code",
                "_concept_name",
                "_concept_domain",
                "_concept_class",
                "_standard_concept",
                "_is_standard",
            ),
            omop_synonym["concept_id"].cast("long") == concepts["_concept_id"],
            "inner",
        )
        .select(
            F.lower(F.trim(F.col("concept_synonym_name"))).alias(
                "_synonym_name_key"
            ),
            "_concept_id",
            "_concept_code",
            "_concept_name",
            "_concept_domain",
            "_concept_class",
            "_standard_concept",
            "_is_standard",
        )
    )
    synonym_terms = term_universe.join(
        name_terms,
        "source_clinical_term_key",
        "left_anti",
    )
    synonym_candidates_0 = F.broadcast(synonym_terms).join(
        synonyms,
        F.col("source_clinical_term_key") == F.col("_synonym_name_key"),
        "inner",
    )
    synonym_window = Window.partitionBy("source_clinical_term_key")
    synonym_candidates = (
        synonym_candidates_0.withColumn(
            "_max_standard",
            F.max("_is_standard").over(synonym_window),
        )
        .where(F.col("_is_standard") == F.col("_max_standard"))
        .drop("_max_standard")
    )
    synonym_group = synonym_candidates.groupBy("source_clinical_term_key").agg(
        F.countDistinct("_concept_id").cast("long").alias(
            "snomed_candidate_count"
        ),
        F.min("_concept_id").alias("_selected_concept_id"),
        F.min("_concept_code").alias("_selected_concept_code"),
        F.min("_concept_name").alias("_selected_concept_name"),
        F.min("_concept_domain").alias("_selected_concept_domain"),
        F.min("_concept_class").alias("_selected_concept_class"),
        F.min("_standard_concept").alias("_selected_standard_concept"),
    )

    def finish(grouped: DataFrame, method: str) -> DataFrame:
        return grouped.select(
            "source_clinical_term_key",
            "snomed_candidate_count",
            F.when(
                F.col("snomed_candidate_count") == 1,
                F.col("_selected_concept_id"),
            ).alias("snomed_candidate_concept_id"),
            F.when(
                F.col("snomed_candidate_count") == 1,
                F.col("_selected_concept_code"),
            ).alias("snomed_candidate_code"),
            F.when(
                F.col("snomed_candidate_count") == 1,
                F.col("_selected_concept_name"),
            ).alias("snomed_candidate_name"),
            F.when(
                F.col("snomed_candidate_count") == 1,
                F.col("_selected_concept_domain"),
            ).alias("snomed_candidate_domain"),
            F.when(
                F.col("snomed_candidate_count") == 1,
                F.col("_selected_concept_class"),
            ).alias("snomed_candidate_class"),
            F.when(
                F.col("snomed_candidate_count") == 1,
                F.col("_selected_standard_concept"),
            ).alias("snomed_candidate_standard_concept"),
            F.lit(method).alias("snomed_candidate_method"),
            F.when(
                F.col("snomed_candidate_count") == 1,
                F.lit("CANDIDATE_UNREVIEWED"),
            )
            .otherwise(F.lit("AMBIGUOUS"))
            .alias("snomed_candidate_status"),
        )

    return finish(name_group, "EXACT_NAME").unionByName(
        finish(synonym_group, "EXACT_SYNONYM")
    )


def build_ucum_map(
    activity_distinct: DataFrame,
    omop_concept: DataFrame,
) -> DataFrame:
    unit_universe = (
        activity_distinct.select(
            blank_to_null(F.col("UCUMUnit")).alias("unit_source_value")
        )
        .where(F.col("unit_source_value").isNotNull())
        .distinct()
    )
    ucum_concepts = (
        omop_concept.where(
            (F.col("vocabulary_id") == "UCUM")
            & F.col("invalid_reason").isNull()
        )
        .select(
            F.col("concept_code").alias("_ucum_code"),
            F.col("concept_id").cast("long").alias("_ucum_concept_id"),
            F.col("concept_name").alias("_ucum_concept_name"),
        )
    )
    exact = ucum_concepts.select(
        F.col("_ucum_code").alias("_exact_source_value"),
        F.col("_ucum_code").alias("_exact_code"),
    )
    aliases = spark.createDataFrame(
        [(source, target) for source, target in UCUM_ALIAS_MAP.items()],
        "_alias_source_value string, _alias_target_code string",
    )
    normalized = (
        unit_universe.join(
            exact,
            F.col("unit_source_value") == F.col("_exact_source_value"),
            "left",
        )
        .join(
            F.broadcast(aliases),
            F.col("unit_source_value") == F.col("_alias_source_value"),
            "left",
        )
        .withColumn(
            "_non_unit_context",
            F.col("unit_source_value").rlike(r"^/[0-9]+$")
            | F.lower(F.col("unit_source_value")).contains("grade")
            | F.lower(F.col("unit_source_value")).endswith(" id"),
        )
        .withColumn(
            "normalized_ucum_code",
            F.when(F.col("_exact_code").isNotNull(), F.col("_exact_code"))
            .when(~F.col("_non_unit_context"), F.col("_alias_target_code")),
        )
        .join(
            ucum_concepts,
            F.col("normalized_ucum_code") == F.col("_ucum_code"),
            "left",
        )
    )
    return normalized.select(
        "unit_source_value",
        "normalized_ucum_code",
        F.col("_ucum_concept_id").alias("unit_omop_concept_id"),
        F.col("_ucum_concept_name").alias("unit_omop_concept_name"),
        F.when(F.col("_exact_code").isNotNull(), F.lit("EXACT"))
        .when(F.col("_non_unit_context"), F.lit("NON_UNIT_CONTEXT"))
        .when(F.col("_alias_target_code").isNotNull(), F.lit("APPROVED_ALIAS"))
        .otherwise(F.lit("UNMAPPED"))
        .alias("unit_mapping_status"),
        F.when(F.col("_exact_code").isNotNull(), F.lit("EXACT_UCUM_CODE"))
        .when(F.col("_non_unit_context"), F.lit("SOURCE_CONTEXT_LABEL"))
        .when(F.col("_alias_target_code").isNotNull(), F.lit("ALIAS_DICTIONARY"))
        .otherwise(F.lit("NONE"))
        .alias("unit_mapping_method"),
    )


def build_contact_resolver(contact_source: DataFrame):
    contact_rows = contact_source.select(
        F.col("CDB").cast("long").alias("CDB"),
        blank_to_null(F.col("CCI")).alias("CCI"),
        F.col("CContactDate").alias("_contact_date"),
        source_key(F.col("CDB"), F.col("CCI"), F.col("SRI")).alias(
            "_contact_key"
        ),
    )
    summary = contact_rows.groupBy("CDB", "CCI").agg(
        F.count(F.lit(1)).cast("long").alias("_contact_candidate_count"),
        F.min("_contact_key").alias("_only_contact_key"),
        F.min("_contact_date").alias("_only_contact_date"),
    )
    same_date = contact_rows.groupBy("CDB", "CCI", "_contact_date").agg(
        F.count(F.lit(1)).cast("long").alias("_same_date_candidate_count"),
        F.min("_contact_key").alias("_same_date_contact_key"),
    )
    return summary, same_date


def build_activity_distinct(activity_source: DataFrame) -> DataFrame:
    business_columns = [
        column for column in activity_source.columns if column != "ADC_UPDT"
    ]
    distinct_rows = activity_source.groupBy(*business_columns).agg(
        F.count(F.lit(1)).cast("long").alias("source_exact_duplicate_count"),
        F.max("ADC_UPDT").alias("source_snapshot_loaded_ts"),
    )
    distinct_rows = distinct_rows.withColumn(
        "source_activity_business_hash",
        stable_hash(business_columns),
    )
    variants = distinct_rows.groupBy("CDB", "CAI").agg(
        F.count(F.lit(1)).cast("long").alias("source_activity_id_variant_count")
    )
    return distinct_rows.join(variants, ["CDB", "CAI"], "left")


def build_activity_stage(
    activity_source: DataFrame,
    contact_source: DataFrame,
    patient_stage: DataFrame,
    omop_concept: DataFrame,
    omop_synonym: DataFrame,
) -> DataFrame:
    activity_distinct = build_activity_distinct(activity_source)
    patient_join = patient_stage.select(
        F.col("source_database_id").alias("CDB"),
        F.col("source_patient_number").alias("PatientNumber"),
        "community_patient_key",
        "person_id",
        "person_match_status",
        "source_service_id",
        "source_service_name",
    )
    contact_summary, same_date_summary = build_contact_resolver(contact_source)
    same_date = same_date_summary.select(
        F.col("CDB").alias("_sd_cdb"),
        F.col("CCI").alias("_sd_cci"),
        F.col("_contact_date").alias("_same_date_value"),
        "_same_date_candidate_count",
        "_same_date_contact_key",
    )

    base = (
        activity_distinct.withColumn("CDB", F.col("CDB").cast("long"))
        .withColumn("CCI", blank_to_null(F.col("CCI")))
        .join(patient_join, ["CDB", "PatientNumber"], "left")
        .join(contact_summary, ["CDB", "CCI"], "left")
        .join(
            same_date,
            (F.col("CDB") == F.col("_sd_cdb"))
            & (F.col("CCI") == F.col("_sd_cci"))
            & (F.col("CareActivityDate") == F.col("_same_date_value")),
            "left",
        )
        .drop("_sd_cdb", "_sd_cci")
        .withColumn(
            "contact_match_status",
            F.when(
                F.coalesce(F.col("_contact_candidate_count"), F.lit(0)) == 0,
                F.lit("NO_CONTACT"),
            )
            .when(
                (F.col("_contact_candidate_count") == 1)
                & (F.col("CareActivityDate") == F.col("_only_contact_date")),
                F.lit("UNIQUE_ID_DATE_MATCH"),
            )
            .when(
                F.col("_contact_candidate_count") == 1,
                F.lit("UNIQUE_ID_DATE_MISMATCH"),
            )
            .when(
                (F.col("_contact_candidate_count") > 1)
                & (F.col("_same_date_candidate_count") == 1),
                F.lit("DUPLICATE_ID_RESOLVED_BY_DATE"),
            )
            .when(
                (F.col("_contact_candidate_count") > 1)
                & (F.col("_same_date_candidate_count") > 1),
                F.lit("AMBIGUOUS_SAME_DATE"),
            )
            .otherwise(F.lit("DUPLICATE_ID_NO_DATE_MATCH")),
        )
        .withColumn(
            "community_care_contact_key",
            F.when(
                F.col("_contact_candidate_count") == 1,
                F.col("_only_contact_key"),
            ).when(
                (F.col("_contact_candidate_count") > 1)
                & (F.col("_same_date_candidate_count") == 1),
                F.col("_same_date_contact_key"),
            ),
        )
        .withColumn(
            "source_clinical_term",
            blank_to_null(F.col("LegacyCodeOriginalTerm")),
        )
        .withColumn(
            "source_clinical_term_key",
            clean_activity_term(F.col("LegacyCodeOriginalTerm")),
        )
        .withColumn(
            "unit_source_value",
            blank_to_null(F.col("UCUMUnit")),
        )
    )

    if ENABLE_SNOMED_CANDIDATES:
        snomed_map = build_snomed_candidate_map(
            activity_distinct,
            omop_concept,
            omop_synonym,
        )
        base = (
            base.join(snomed_map, "source_clinical_term_key", "left")
            .withColumn(
                "snomed_candidate_status",
                F.coalesce(
                    F.col("snomed_candidate_status"),
                    F.when(
                        F.col("source_clinical_term_key").isNull(),
                        F.lit("NO_SOURCE_TERM"),
                    ).otherwise(F.lit("UNMAPPED")),
                ),
            )
            .withColumn(
                "snomed_candidate_count",
                F.coalesce(F.col("snomed_candidate_count"), F.lit(0)).cast(
                    "long"
                ),
            )
        )
    else:
        base = (
            base.withColumn("snomed_candidate_count", F.lit(0).cast("long"))
            .withColumn(
                "snomed_candidate_concept_id", F.lit(None).cast("long")
            )
            .withColumn("snomed_candidate_code", F.lit(None).cast("string"))
            .withColumn("snomed_candidate_name", F.lit(None).cast("string"))
            .withColumn("snomed_candidate_domain", F.lit(None).cast("string"))
            .withColumn("snomed_candidate_class", F.lit(None).cast("string"))
            .withColumn(
                "snomed_candidate_standard_concept", F.lit(None).cast("string")
            )
            .withColumn("snomed_candidate_method", F.lit("DISABLED"))
            .withColumn("snomed_candidate_status", F.lit("DISABLED"))
        )

    ucum_map = build_ucum_map(activity_distinct, omop_concept)
    base = base.join(ucum_map, "unit_source_value", "left").withColumn(
        "unit_mapping_status",
        F.coalesce(
            F.col("unit_mapping_status"),
            F.when(
                F.col("unit_source_value").isNull(),
                F.lit("NO_SOURCE_UNIT"),
            ).otherwise(F.lit("UNMAPPED")),
        ),
    )

    activity_code = blank_to_null(F.col("ActivityCode"))
    activity_description = mapping_value(activity_code, ACTIVITY_TYPE_MAP)
    observation_raw = blank_to_null(F.col("ObsValue"))

    return base.select(
        source_key(
            F.col("CDB"),
            F.col("CAI"),
            blank_to_null(F.col("CPLID")),
            F.col("source_activity_business_hash"),
        ).alias("community_care_activity_key"),
        F.col("CDB").cast("long").alias("source_database_id"),
        blank_to_null(F.col("CAI")).alias("care_activity_id"),
        F.col("source_activity_business_hash"),
        F.col("source_exact_duplicate_count"),
        F.col("source_activity_id_variant_count"),
        (F.col("source_activity_id_variant_count") > 1).alias(
            "source_activity_id_duplicate_ind"
        ),
        blank_to_null(F.col("CCI")).alias("care_contact_id"),
        F.col("community_care_contact_key"),
        F.col("contact_match_status"),
        F.coalesce(F.col("_contact_candidate_count"), F.lit(0)).cast("long").alias(
            "contact_candidate_count"
        ),
        F.coalesce(F.col("_same_date_candidate_count"), F.lit(0)).cast(
            "long"
        ).alias("same_date_contact_candidate_count"),
        F.col("community_patient_key"),
        F.col("person_id"),
        F.col("person_match_status"),
        F.col("source_service_id"),
        F.col("source_service_name"),
        F.col("CareActivityDate").alias("care_activity_date"),
        date_quality(F.col("CareActivityDate")).alias(
            "care_activity_date_quality_status"
        ),
        activity_code.alias("community_care_activity_type_code"),
        activity_description.alias("community_care_activity_type_description"),
        mapping_status(activity_code, activity_description).alias(
            "community_care_activity_type_mapping_status"
        ),
        blank_to_null(F.col("CPLID")).alias(
            "source_care_professional_local_id"
        ),
        F.col("ContactDuration").cast("long").alias(
            "clinical_contact_duration_minutes"
        ),
        duration_quality(F.col("ContactDuration")).alias(
            "clinical_contact_duration_quality_status"
        ),
        F.col("source_clinical_term"),
        F.col("source_clinical_term_key"),
        F.col("ObservationType").cast("long").alias(
            "source_observation_type_id"
        ),
        F.col("CodeCategoryId").cast("long").alias("source_code_category_id"),
        observation_raw.alias("observation_value_raw"),
        F.expr("try_cast(ObsValue as double)").alias("observation_value_numeric"),
        F.when(observation_raw.isNull(), F.lit("MISSING"))
        .when(F.expr("try_cast(ObsValue as double)").isNotNull(), F.lit("NUMERIC"))
        .otherwise(F.lit("NON_NUMERIC"))
        .alias("observation_value_parse_status"),
        F.col("unit_source_value"),
        F.col("normalized_ucum_code"),
        F.col("unit_omop_concept_id"),
        F.col("unit_omop_concept_name"),
        F.col("unit_mapping_status"),
        F.col("unit_mapping_method"),
        F.col("snomed_candidate_count"),
        F.col("snomed_candidate_concept_id"),
        F.col("snomed_candidate_code"),
        F.col("snomed_candidate_name"),
        F.col("snomed_candidate_domain"),
        F.col("snomed_candidate_class"),
        F.col("snomed_candidate_standard_concept"),
        F.col("snomed_candidate_method"),
        F.col("snomed_candidate_status"),
        F.lit(CSDS_MAPPING_VERSION).alias("csds_mapping_version"),
        F.lit(UCUM_MAPPING_VERSION).alias("ucum_mapping_version"),
        F.lit(SNOMED_MAPPING_VERSION).alias("snomed_mapping_version"),
        F.col("source_snapshot_loaded_ts"),
        F.lit(SRC_ACTIVITY).alias("source_table"),
        F.lit(ACTIVITY_CONTRACT_VERSION).alias("community_schema_version"),
    )


def validate_patient_stage(
    patient_stage: DataFrame,
    expected_rows: int,
) -> dict:
    rows = patient_stage.count()
    assert rows == expected_rows, (
        f"Patient stage rows {rows} != source rows {expected_rows}"
    )
    assert_unique_non_null(
        patient_stage,
        ["community_patient_key"],
        "map_community_patient_link",
    )
    status_rows = (
        patient_stage.groupBy("person_match_status")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.when(F.col("person_id").isNotNull(), 1).otherwise(0)).alias(
                "linked"
            ),
        )
        .collect()
    )
    status = {
        row["person_match_status"]: {
            "rows": int(row["rows"]),
            "linked": int(row["linked"] or 0),
        }
        for row in status_rows
    }
    linked_rows = sum(item["linked"] for item in status.values())
    link_rate = linked_rows / max(rows, 1)
    assert link_rate >= 0.85, f"Patient linkage rate too low: {link_rate:.4f}"
    dob_status = {
        row["birth_date_concordance_status"]: int(row["count"])
        for row in patient_stage.groupBy("birth_date_concordance_status")
        .count()
        .collect()
    }
    return {
        "rows": rows,
        "linked_rows": linked_rows,
        "link_rate": link_rate,
        "person_match_status": status,
        "birth_date_concordance_status": dob_status,
    }


def validate_contact_stage(
    contact_stage: DataFrame,
    expected_rows: int,
) -> dict:
    rows = contact_stage.count()
    assert rows == expected_rows, (
        f"Contact stage rows {rows} != source rows {expected_rows}"
    )
    assert_unique_non_null(
        contact_stage,
        ["community_care_contact_key"],
        "map_community_care_contact",
    )
    summary = (
        contact_stage.agg(
            F.sum(
                F.when(
                    F.col("care_contact_datetime_status") != "PARSED",
                    1,
                ).otherwise(0)
            ).alias("datetime_failures"),
            F.sum(
                F.when(F.col("care_contact_id_duplicate_ind"), 1).otherwise(0)
            ).alias("rows_with_repeated_cci"),
            F.countDistinct(
                F.when(
                    F.col("care_contact_id_duplicate_ind"),
                    F.concat_ws(
                        "|",
                        F.col("source_database_id"),
                        F.col("care_contact_id"),
                    ),
                )
            ).alias("repeated_cci_groups"),
            F.sum(
                F.when(F.col("person_id").isNotNull(), 1).otherwise(0)
            ).alias("linked_rows"),
        )
        .collect()[0]
        .asDict()
    )
    assert int(summary["datetime_failures"] or 0) == 0, (
        f"Contact timestamp parse failures: {summary['datetime_failures']}"
    )
    return {
        "rows": rows,
        **{key: int(value or 0) for key, value in summary.items()},
    }


def validate_activity_stage(
    activity_stage: DataFrame,
    raw_rows: int,
) -> dict:
    rows = activity_stage.count()
    assert rows <= raw_rows, "Activity exact deduplication increased row count"
    assert_unique_non_null(
        activity_stage,
        ["community_care_activity_key"],
        "map_community_care_activity",
    )
    exact_duplicates_collapsed = int(raw_rows - rows)
    statuses = {
        row["contact_match_status"]: int(row["count"])
        for row in activity_stage.groupBy("contact_match_status").count().collect()
    }
    assert sum(statuses.values()) == rows, "Activity contact statuses do not reconcile"

    mapping_summary = (
        activity_stage.agg(
            F.sum(
                F.when(F.col("source_clinical_term").isNotNull(), 1).otherwise(0)
            ).alias("termed_rows"),
            F.sum(
                F.when(
                    F.col("snomed_candidate_concept_id").isNotNull(),
                    1,
                ).otherwise(0)
            ).alias("snomed_candidate_rows"),
            F.sum(
                F.when(
                    F.col("snomed_candidate_status") == "AMBIGUOUS",
                    1,
                ).otherwise(0)
            ).alias("snomed_ambiguous_rows"),
            F.sum(
                F.when(F.col("unit_source_value").isNotNull(), 1).otherwise(0)
            ).alias("unit_rows"),
            F.sum(
                F.when(
                    F.col("unit_omop_concept_id").isNotNull(),
                    1,
                ).otherwise(0)
            ).alias("unit_concept_rows"),
            F.sum(
                F.when(
                    F.col("observation_value_parse_status") == "NON_NUMERIC",
                    1,
                ).otherwise(0)
            ).alias("non_numeric_observation_rows"),
        )
        .collect()[0]
        .asDict()
    )
    mapping_summary = {
        key: int(value or 0) for key, value in mapping_summary.items()
    }
    termed = mapping_summary["termed_rows"]
    snomed_rate = (
        mapping_summary["snomed_candidate_rows"] / termed if termed else 0.0
    )
    if ENABLE_SNOMED_CANDIDATES and not (0.40 <= snomed_rate <= 0.60):
        print(
            "[COMMUNITY][WARN] SNOMED candidate coverage outside 40-60% band: "
            f"{snomed_rate:.4f}"
        )
    invalid_accepted = activity_stage.where(
        F.col("snomed_candidate_concept_id").isNotNull()
        & (F.col("snomed_candidate_count") != 1)
    ).count()
    assert invalid_accepted == 0, (
        f"{invalid_accepted} SNOMED candidates accepted from non-unique matches"
    )
    null_unit_status = activity_stage.where(
        F.col("unit_source_value").isNotNull()
        & F.col("unit_mapping_status").isNull()
    ).count()
    assert null_unit_status == 0, (
        f"{null_unit_status} populated units have no mapping status"
    )
    return {
        "rows": rows,
        "raw_rows": raw_rows,
        "exact_duplicates_collapsed": exact_duplicates_collapsed,
        "contact_match_status": statuses,
        "snomed_candidate_rate": snomed_rate,
        **mapping_summary,
    }


PATIENT_COMMENTS = {
    "community_patient_key": "SHA-256 source registration key from BH_COMMUNITY, RegCDB and PatientNumber.",
    "source_database_id": "Community database/service instance identifier.",
    "source_patient_number": "Restricted local patient number; unique only within source_database_id.",
    "person_id": "Canonical Millennium PERSON_ID from strict NHS/MRN alias linkage.",
    "person_match_status": "MATCHED, UNMATCHED, AMBIGUOUS, CONFLICT or CANONICAL_PERSON_MISSING.",
    "person_match_method": "NHS_MRN_AGREE, NHS_ONLY, MRN_ONLY or NONE.",
    "birth_date_concordance_status": "QA-only comparison; source DOB is not published.",
    "registered_practice_ods_code": "Current registered-practice ODS code from the Community snapshot.",
    "source_snapshot_loaded_ts": "Raw source ADC_UPDT snapshot timestamp.",
    "ROW_HASH": "SHA-256 of pipeline-managed business fields.",
    "SOURCE_PRESENT_IND": "False when the registration disappeared from the latest source snapshot.",
}
CONTACT_COMMENTS = {
    "community_care_contact_key": "SHA-256 key from BH_COMMUNITY, CDB, CCI and SRI.",
    "care_contact_id": "Source CYP201 Care Contact Identifier; not unique by itself.",
    "service_request_id": "Source CYP201 Service Request Identifier.",
    "care_contact_datetime_local": "Source local wall-clock contact timestamp (TIMESTAMP_NTZ).",
    "care_contact_id_variant_count": "Rows sharing source_database_id and care_contact_id.",
    "normalized_consultation_mechanism_method": "SUBMITTED_CODE, EXACT_SOURCE_TERM_CANDIDATE, SUBMITTED_OTHER_OR_UNMAPPED or UNMAPPED.",
    "commissioner_ods_code": "Submitted commissioner ODS code.",
    "treatment_site_ods_code": "Submitted treatment organisation-site ODS code.",
    "source_snapshot_loaded_ts": "Raw source ADC_UPDT snapshot timestamp.",
    "ROW_HASH": "SHA-256 of pipeline-managed business fields.",
    "SOURCE_PRESENT_IND": "False when the contact disappeared from the latest source snapshot.",
}
ACTIVITY_COMMENTS = {
    "community_care_activity_key": "Content-addressed SHA-256 key retaining distinct variants of repeated source activity identifiers.",
    "source_activity_business_hash": "Hash of the complete source activity payload excluding ADC_UPDT.",
    "source_exact_duplicate_count": "Number of byte-equivalent source rows collapsed into this row.",
    "source_activity_id_variant_count": "Distinct rows sharing source_database_id and care_activity_id.",
    "contact_match_status": "UNIQUE_ID_DATE_MATCH, UNIQUE_ID_DATE_MISMATCH, DUPLICATE_ID_RESOLVED_BY_DATE, AMBIGUOUS_SAME_DATE, DUPLICATE_ID_NO_DATE_MATCH or NO_CONTACT.",
    "source_care_professional_local_id": "CSDS Care Professional Local Identifier; not a Millennium personnel ID.",
    "snomed_candidate_concept_id": "Unreviewed deterministic exact-name/synonym candidate; not an approved resolved mapping.",
    "snomed_candidate_status": "CANDIDATE_UNREVIEWED, AMBIGUOUS, UNMAPPED, NO_SOURCE_TERM or DISABLED.",
    "normalized_ucum_code": "Exact or approved-alias normalized UCUM code.",
    "source_snapshot_loaded_ts": "Raw source ADC_UPDT snapshot timestamp.",
    "ROW_HASH": "SHA-256 of pipeline-managed business fields.",
    "SOURCE_PRESENT_IND": "False when the activity variant disappeared from the latest source snapshot.",
}


def target_fingerprints(metadata: dict[str, dict]) -> dict[str, dict[str, str]]:
    physical = {
        table: metadata[table]["fingerprint"] for table in metadata
    }
    csds_map_fingerprint = hash_object(
        {
            "version": CSDS_MAPPING_VERSION,
            "admin": ADMIN_CATEGORY_MAP,
            "consult_type": CONSULT_TYPE_MAP,
            "contact_subject": CONTACT_SUBJECT_MAP,
            "mechanism": CONSULTATION_MECHANISM_MAP,
            "location": ACTIVITY_LOCATION_MAP,
            "group": GROUP_THERAPY_MAP,
            "attendance": ATTENDANCE_MAP,
            "activity": ACTIVITY_TYPE_MAP,
            "team": TEAM_TYPE_MAP,
        }
    )
    patient = {
        SRC_PATIENT: physical[SRC_PATIENT],
        PERSON_ALIAS: physical[PERSON_ALIAS],
        PERSON_CANONICAL: physical[PERSON_CANONICAL],
        "__CONTRACT__": hash_object(PATIENT_CONTRACT_VERSION),
    }
    contact = {
        SRC_PATIENT: physical[SRC_PATIENT],
        SRC_CONTACT: physical[SRC_CONTACT],
        PERSON_ALIAS: physical[PERSON_ALIAS],
        PERSON_CANONICAL: physical[PERSON_CANONICAL],
        ORGANIZATION_ALIAS: physical[ORGANIZATION_ALIAS],
        ORGANIZATION: physical[ORGANIZATION],
        "__CONTRACT__": hash_object(CONTACT_CONTRACT_VERSION),
        "__CSDS_MAP__": csds_map_fingerprint,
        "__CONSULTATION_TERM_MAP__": hash_object(
            {
                "version": CONSULTATION_TERM_MAPPING_VERSION,
                "map": CONSULTATION_TERM_MECHANISM_MAP,
            }
        ),
    }
    activity = {
        SRC_PATIENT: physical[SRC_PATIENT],
        SRC_CONTACT: physical[SRC_CONTACT],
        SRC_ACTIVITY: physical[SRC_ACTIVITY],
        PERSON_ALIAS: physical[PERSON_ALIAS],
        PERSON_CANONICAL: physical[PERSON_CANONICAL],
        OMOP_CONCEPT: physical[OMOP_CONCEPT],
        OMOP_CONCEPT_SYNONYM: physical[OMOP_CONCEPT_SYNONYM],
        "__CONTRACT__": hash_object(ACTIVITY_CONTRACT_VERSION),
        "__CSDS_MAP__": csds_map_fingerprint,
        "__UCUM_ALIAS_MAP__": hash_object(
            {"version": UCUM_MAPPING_VERSION, "map": UCUM_ALIAS_MAP}
        ),
        "__SNOMED_MODE__": hash_object(
            {
                "version": SNOMED_MAPPING_VERSION,
                "enabled": ENABLE_SNOMED_CANDIDATES,
            }
        ),
    }
    return {
        PATIENT_TABLE: patient,
        CONTACT_TABLE: contact,
        ACTIVITY_TABLE: activity,
    }


def assert_dependencies_unchanged(
    initial_metadata: dict[str, dict],
    dependencies: set[str],
) -> None:
    changed = {}
    for table in sorted(dependencies):
        current = capture_dependency(table)
        if current["fingerprint"] != initial_metadata[table]["fingerprint"]:
            changed[table] = {
                "before": initial_metadata[table]["fingerprint"],
                "after": current["fingerprint"],
            }
    assert not changed, (
        "Source/dependency changed during staging; targets were not modified: "
        f"{changed}"
    )


def verify_target(
    target: str,
    key: str,
    expected_present_rows: int | None,
) -> dict:
    assert bronze_table_exists(target), f"Missing target {target}"
    assert cdf_enabled(target), f"CDF is not enabled on {target}"
    target_rows = spark.table(target).count()
    if expected_present_rows is not None:
        assert target_rows >= expected_present_rows, (
            f"{target}: retained rows {target_rows} "
            f"< current source rows {expected_present_rows}"
        )
    duplicate_keys = verify_unique_key(spark.table(target), [key])
    assert duplicate_keys == 0, f"{target}: {duplicate_keys} duplicate target keys"
    bronze_assert_primary_contract(target)
    return {
        "target_rows": target_rows,
        "current_source_rows": expected_present_rows,
        "duplicate_keys": duplicate_keys,
        "cdf_enabled": True,
    }


def _write_target_comments(target: str) -> None:
    if target == PATIENT_TABLE:
        apply_comments(
            target,
            "BH Community patient/service registration bridge. Direct identifiers and duplicate demographics are excluded.",
            PATIENT_COMMENTS,
        )
    elif target == CONTACT_TABLE:
        apply_comments(
            target,
            "BH Community CSDS CYP201 care contacts at CDB+CCI+SRI grain.",
            CONTACT_COMMENTS,
        )
    elif target == ACTIVITY_TABLE:
        apply_comments(
            target,
            "BH Community CSDS CYP202 distinct activity rows with contact linkage, candidate SNOMED enrichment and UCUM mapping.",
            ACTIVITY_COMMENTS,
        )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute

# COMMAND ----------

result_payload = None
rebuilt_targets: list[str] = []

try:
    missing_dependencies = [
        table for table in PHYSICAL_DEPENDENCIES if not bronze_table_exists(table)
    ]
    assert not missing_dependencies, (
        f"Missing Community dependencies: {missing_dependencies}"
    )

    create_operational_tables()
    dependency_metadata = {
        table: capture_dependency(table) for table in PHYSICAL_DEPENDENCIES
    }
    pinned = {
        table: read_pinned(dependency_metadata[table])
        for table in PHYSICAL_DEPENDENCIES
    }

    preflight = run_preflight(
        dependency_metadata,
        pinned[SRC_PATIENT],
        pinned[SRC_CONTACT],
        pinned[SRC_ACTIVITY],
    )
    fingerprints = target_fingerprints(dependency_metadata)
    rebuild = {
        target: needs_run(target, target_fingerprints)
        for target, target_fingerprints in fingerprints.items()
    }
    print(
        "[COMMUNITY] "
        f"target_schema={TARGET_SCHEMA}, run_id={COMMUNITY_RUN_ID}, "
        f"force_full_refresh={FORCE_FULL_REFRESH}, "
        f"snomed_candidates={ENABLE_SNOMED_CANDIDATES}, rebuild={rebuild}"
    )

    patient_stage = None
    contact_stage = None
    activity_stage = None
    validation = {}

    if any(rebuild.values()):
        patient_stage = build_patient_stage(
            pinned[SRC_PATIENT],
            pinned[PERSON_ALIAS],
            pinned[PERSON_CANONICAL],
        )
        validation["patient"] = validate_patient_stage(
            patient_stage,
            int(dependency_metadata[SRC_PATIENT]["row_count"]),
        )

    if rebuild[CONTACT_TABLE]:
        contact_stage = build_contact_stage(
            pinned[SRC_CONTACT],
            patient_stage,
            pinned[ORGANIZATION_ALIAS],
            pinned[ORGANIZATION],
        )
        validation["contact"] = validate_contact_stage(
            contact_stage,
            int(dependency_metadata[SRC_CONTACT]["row_count"]),
        )

    if rebuild[ACTIVITY_TABLE]:
        activity_stage = build_activity_stage(
            pinned[SRC_ACTIVITY],
            pinned[SRC_CONTACT],
            patient_stage,
            pinned[OMOP_CONCEPT],
            pinned[OMOP_CONCEPT_SYNONYM],
        )
        validation["activity"] = validate_activity_stage(
            activity_stage,
            int(dependency_metadata[SRC_ACTIVITY]["row_count"]),
        )

    dependencies_to_recheck = {
        dependency
        for target, should_rebuild in rebuild.items()
        if should_rebuild
        for dependency in fingerprints[target]
        if not dependency.startswith("__")
    }
    if dependencies_to_recheck:
        assert_dependencies_unchanged(
            dependency_metadata,
            dependencies_to_recheck,
        )

    merge_metrics = {}
    if rebuild[PATIENT_TABLE]:
        merge_metrics["patient"] = update_snapshot_table(
            patient_stage,
            PATIENT_TABLE,
            "community_patient_key",
        )
        rebuilt_targets.append(PATIENT_TABLE)
        _write_target_comments(PATIENT_TABLE)
    else:
        merge_metrics["patient"] = {"operation": "SKIP_UNCHANGED"}

    if rebuild[CONTACT_TABLE]:
        merge_metrics["contact"] = update_snapshot_table(
            contact_stage,
            CONTACT_TABLE,
            "community_care_contact_key",
        )
        rebuilt_targets.append(CONTACT_TABLE)
        _write_target_comments(CONTACT_TABLE)
    else:
        merge_metrics["contact"] = {"operation": "SKIP_UNCHANGED"}

    if rebuild[ACTIVITY_TABLE]:
        merge_metrics["activity"] = update_snapshot_table(
            activity_stage,
            ACTIVITY_TABLE,
            "community_care_activity_key",
        )
        rebuilt_targets.append(ACTIVITY_TABLE)
        _write_target_comments(ACTIVITY_TABLE)
    else:
        merge_metrics["activity"] = {"operation": "SKIP_UNCHANGED"}

    target_verification = {
        "patient": verify_target(
            PATIENT_TABLE,
            "community_patient_key",
            validation.get("patient", {}).get("rows"),
        ),
        "contact": verify_target(
            CONTACT_TABLE,
            "community_care_contact_key",
            validation.get("contact", {}).get("rows"),
        ),
        "activity": verify_target(
            ACTIVITY_TABLE,
            "community_care_activity_key",
            validation.get("activity", {}).get("rows"),
        ),
    }

    for target in rebuilt_targets:
        contract_version = {
            PATIENT_TABLE: PATIENT_CONTRACT_VERSION,
            CONTACT_TABLE: CONTACT_CONTRACT_VERSION,
            ACTIVITY_TABLE: ACTIVITY_CONTRACT_VERSION,
        }[target]
        record_state(target, fingerprints[target], contract_version)

    state_verified = {}
    for target, expected in fingerprints.items():
        actual = state_for_target(target)
        state_verified[target] = actual == expected
        assert state_verified[target], (
            f"Committed dependency state does not match current fingerprints for {target}"
        )

    result_payload = {
        "pipeline": "community_pipeline",
        "status": "SUCCESS",
        "run_id": COMMUNITY_RUN_ID,
        "target_schema": TARGET_SCHEMA,
        "started_at": STARTED_AT,
        "completed_at": bronze_utc_now(),
        "force_full_refresh": FORCE_FULL_REFRESH,
        "enable_snomed_candidate_mapping": ENABLE_SNOMED_CANDIDATES,
        "rebuild": rebuild,
        "rebuilt_targets": rebuilt_targets,
        "preflight": preflight,
        "validation": validation,
        "merge_metrics": merge_metrics,
        "target_verification": target_verification,
        "state_verified": state_verified,
        "dependency_fingerprints": {
            table: metadata["fingerprint"]
            for table, metadata in dependency_metadata.items()
        },
    }
    append_audit(result_payload, "SUCCESS", rebuilt_targets)
    print(bronze_json(result_payload))

except Exception as community_error:
    failure_payload = {
        "pipeline": "community_pipeline",
        "status": "FAILED",
        "run_id": COMMUNITY_RUN_ID,
        "target_schema": TARGET_SCHEMA,
        "started_at": STARTED_AT,
        "completed_at": bronze_utc_now(),
        "force_full_refresh": FORCE_FULL_REFRESH,
        "rebuilt_targets": rebuilt_targets,
        "error_type": type(community_error).__name__,
        "error": str(community_error),
    }
    try:
        if bronze_table_exists(AUDIT_TABLE):
            append_audit(failure_payload, "FAILED", rebuilt_targets)
    except Exception as audit_error:
        print(
            "[COMMUNITY][WARN] Failed to write failure audit: "
            f"{type(audit_error).__name__}: {audit_error}"
        )
    print(bronze_json(failure_payload))
    raise

dbutils.notebook.exit(bronze_json(result_payload))

