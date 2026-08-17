# Databricks notebook source
# MAGIC %md
# MAGIC # PowerForms Canonical Bronze Pipeline
# MAGIC
# MAGIC Publishes a lossless PowerForms foundation:
# MAGIC
# MAGIC - `map_powerform_assessment_item`: one row per `DOC_RESPONSE_KEY`.
# MAGIC - `map_powerform_assessment`: one row per represented `DCP_FORMS_ACTIVITY_ID`.
# MAGIC - `powerform_item_map`: one governed question + optional answer mapping rule per row.
# MAGIC
# MAGIC The repaired v3.1 `mill_form_activity` table supplies PowerForm context. Canonical event
# MAGIC tables supply result provenance and existing mappings. Form-specific pivots, scores and
# MAGIC clinical interpretation remain Silver responsibilities.
# MAGIC
# MAGIC Safe default: `run_mode=define_only`. A full build is fixed-version, resumable and
# MAGIC event-sharded. Incremental processing uses Delta CDF for the source, event lanes and
# MAGIC mapping table; OMOP vocabulary changes use fixed-version snapshot diff because the
# MAGIC vocabulary refresh is CREATE OR REPLACE.

# COMMAND ----------

# MAGIC %run ./_bronze_common

# COMMAND ----------

from datetime import datetime, timezone
from functools import reduce
import json
import re
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def _widget_text(name: str, default: str) -> None:
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)


for _widget_name, _widget_default in {
    "write_environment": "prod",
    "target_schema": "4_prod.bronze",
    "source_powerform_table": "4_prod.bronze.mill_form_activity",
    "source_nomen_lane": "4_prod.bronze.map_nomen_events",
    "source_coded_lane": "4_prod.bronze.map_coded_events",
    "source_numeric_lane": "4_prod.bronze.map_numeric_events",
    "source_text_lane": "4_prod.bronze.map_text_events",
    "source_date_lane": "4_prod.bronze.map_date_events",
    "source_concept_table": "3_lookup.omop.concept",
    "run_mode": "define_only",
    "mapping_version": "2026.07.v1",
    "force_full_refresh": "false",
    "create_cutover_backups": "false",
    "run_post_deployment_checks": "false",
    "full_build_shards": "128",
    "max_shards_per_run": "1",
    "max_incremental_candidates": "5000000",
    "min_lane_coverage": "0.98",
    "max_lane_schedule_skew_hours": "36",
    "max_context_conflict_count": "1000",
    "max_context_conflict_rate": "0.001",
    "max_upstream_age_hours": "36",
    "allow_stale_upstream": "false",
    "allow_unmaintained_coded_lane": "false",
    "override_running_lock": "false",
}.items():
    _widget_text(_widget_name, _widget_default)


WRITE_ENVIRONMENT = bronze_value("write_environment", "prod").strip().lower()
if WRITE_ENVIRONMENT not in {"prod", "dev"}:
    raise ValueError(
        "write_environment must be exactly 'prod' or 'dev'; "
        f"received {WRITE_ENVIRONMENT!r}"
    )
TARGET_SCHEMA = {
    "prod": "4_prod.bronze",
    "dev": "8_dev.bronze",
}[WRITE_ENVIRONMENT]
_legacy_target_schema = bronze_value("target_schema", TARGET_SCHEMA)
if _legacy_target_schema != TARGET_SCHEMA:
    print(
        f"[INFO] Ignoring target_schema={_legacy_target_schema}; "
        f"write_environment={WRITE_ENVIRONMENT} selects {TARGET_SCHEMA}"
    )

SRC_POWERFORM = bronze_value(
    "source_powerform_table", "4_prod.bronze.mill_form_activity"
)
SRC_NOMEN = bronze_value(
    "source_nomen_lane", "4_prod.bronze.map_nomen_events"
)
SRC_CODED = bronze_value(
    "source_coded_lane", "4_prod.bronze.map_coded_events"
)
SRC_NUMERIC = bronze_value(
    "source_numeric_lane", "4_prod.bronze.map_numeric_events"
)
SRC_TEXT = bronze_value(
    "source_text_lane", "4_prod.bronze.map_text_events"
)
SRC_DATE = bronze_value(
    "source_date_lane", "4_prod.bronze.map_date_events"
)
SRC_CONCEPT = bronze_value("source_concept_table", "3_lookup.omop.concept")

RUN_MODE = bronze_value("run_mode", "define_only").lower()
MAPPING_VERSION = bronze_value("mapping_version", "2026.07.v1")
FORCE_FULL_REFRESH = bronze_bool("force_full_refresh", False)
CREATE_BACKUPS = bronze_bool("create_cutover_backups", True)
RUN_POST_CHECKS = bronze_bool("run_post_deployment_checks", False)
FULL_BUILD_SHARDS = int(bronze_value("full_build_shards", "128"))
MAX_SHARDS_PER_RUN = int(bronze_value("max_shards_per_run", "1"))
MAX_INCREMENTAL_CANDIDATES = int(
    bronze_value("max_incremental_candidates", "5000000")
)
MIN_LANE_COVERAGE = float(bronze_value("min_lane_coverage", "0.98"))
MAX_LANE_SKEW_HOURS = int(bronze_value("max_lane_schedule_skew_hours", "36"))
MAX_CONTEXT_CONFLICT_COUNT = int(
    bronze_value("max_context_conflict_count", "1000")
)
MAX_CONTEXT_CONFLICT_RATE = float(
    bronze_value("max_context_conflict_rate", "0.001")
)
MAX_UPSTREAM_AGE_HOURS = int(bronze_value("max_upstream_age_hours", "36"))
ALLOW_STALE_UPSTREAM = bronze_bool("allow_stale_upstream", False)
ALLOW_UNMAINTAINED_CODED = bronze_bool("allow_unmaintained_coded_lane", False)
OVERRIDE_RUNNING_LOCK = bronze_bool("override_running_lock", False)

assert RUN_MODE in {
    "define_only",
    "full_rebuild",
    "incremental",
    "recode",
    "validate",
}, f"Unsupported run_mode={RUN_MODE}"
assert FULL_BUILD_SHARDS > 0
assert MAX_SHARDS_PER_RUN > 0
assert MAX_INCREMENTAL_CANDIDATES > 0

PIPELINE_NAME = "powerform_canonical_v1"
POWERFORM_RUN_ID = bronze_run_id()
STARTED_AT = bronze_utc_now()

TGT_ITEM = f"{TARGET_SCHEMA}.map_powerform_assessment_item"
TGT_ASSESSMENT = f"{TARGET_SCHEMA}.map_powerform_assessment"
TGT_MAP = f"{bronze_lookup_schema(TARGET_SCHEMA)}.powerform_item_map"
STATE_TABLE = f"{bronze_control_schema(TARGET_SCHEMA)}.powerform_pipeline_state"
RUNS_TABLE = f"{bronze_control_schema(TARGET_SCHEMA)}.powerform_pipeline_runs"
CANDIDATE_TABLE = f"{bronze_control_schema(TARGET_SCHEMA)}.powerform_pipeline_candidates"
ITEM_CURRENT_VIEW = f"{TARGET_SCHEMA}.map_powerform_assessment_item_current"
ASSESSMENT_CURRENT_VIEW = f"{TARGET_SCHEMA}.map_powerform_assessment_current"

DEFAULT_SOURCES = {
    "powerform": "4_prod.bronze.mill_form_activity",
    "nomen": "4_prod.bronze.map_nomen_events",
    "coded": "4_prod.bronze.map_coded_events",
    "numeric": "4_prod.bronze.map_numeric_events",
    "text": "4_prod.bronze.map_text_events",
    "date": "4_prod.bronze.map_date_events",
    "concept": "3_lookup.omop.concept",
}
ACTUAL_SOURCES = {
    "powerform": SRC_POWERFORM,
    "nomen": SRC_NOMEN,
    "coded": SRC_CODED,
    "numeric": SRC_NUMERIC,
    "text": SRC_TEXT,
    "date": SRC_DATE,
    "concept": SRC_CONCEPT,
}

_source_overrides = {
    key: value
    for key, value in ACTUAL_SOURCES.items()
    if value != DEFAULT_SOURCES[key]
}
if _source_overrides and TARGET_SCHEMA in {"8_dev.bronze", "4_prod.bronze"}:
    raise ValueError(
        "Source overrides are validation-only and require an isolated target_schema; "
        f"overrides={_source_overrides}"
    )


def qident(name: str) -> str:
    q = chr(96)
    return q + str(name).replace(q, q + q) + q


def qname(name: str) -> str:
    return ".".join(qident(part) for part in str(name).split("."))


def sql_string(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "''")


def safe_token(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", str(value))[:80]


def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def source_version(table_name: str) -> int:
    row = spark.sql(f"DESCRIBE HISTORY {qname(table_name)} LIMIT 1").first()
    if row is None:
        raise RuntimeError(f"No Delta history returned for {table_name}")
    return int(row["version"])


def version_timestamp(table_name: str, version: Optional[int] = None):
    history = spark.sql(f"DESCRIBE HISTORY {qname(table_name)}")
    if version is not None:
        history = history.where(F.col("version") == int(version))
    row = history.select("timestamp").first()
    return None if row is None else row["timestamp"]


def _powerform_hardening_sql_identifier(table_name: str) -> str:
    return '.'.join('`' + part.replace('`', '``') + '`' for part in table_name.split('.'))

def _powerform_hardening_snapshot_fallback_allowed(exc: BaseException) -> bool:
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

def _powerform_hardening_read_pinned_snapshot(table_name: str, version: int):
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
        if not _powerform_hardening_snapshot_fallback_allowed(exc):
            raise
        row = (
            spark.sql('DESCRIBE HISTORY ' + _powerform_hardening_sql_identifier(table_name) + ' LIMIT 1')
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

def _powerform_hardening_validate_cdf(changes):
    # Keep lazy Spark Connect failures inside the caller's recovery block.
    _ = changes.schema
    changes.limit(1).collect()
    return changes

def read_snapshot(table_name: str, version: Optional[int] = None) -> DataFrame:
    if version is None:
        return spark.table(table_name)
    return _powerform_hardening_read_pinned_snapshot(
        table_name,
        int(version),
    )


def table_properties(table_name: str) -> dict:
    if not table_exists(table_name):
        return {}
    row = spark.sql(f"DESCRIBE DETAIL {qname(table_name)}").first()
    return dict((row["properties"] or {}) if row is not None else {})


def cdf_enabled(table_name: str) -> bool:
    return (
        str(table_properties(table_name).get("delta.enableChangeDataFeed", "false"))
        .lower()
        == "true"
    )


def read_cdf(
    table_name: str, starting_version: int, ending_version: int
) -> Optional[DataFrame]:
    if int(starting_version) > int(ending_version):
        return None
    try:
        changes = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", int(starting_version))
            .option("endingVersion", int(ending_version))
            .table(table_name)
        )
        return _powerform_hardening_validate_cdf(changes)
    except Exception as exc:
        raise RuntimeError(
            f"CDF unavailable for {table_name} versions "
            f"{starting_version}-{ending_version}; refusing timestamp fallback"
        ) from exc


def is_empty(df: Optional[DataFrame]) -> bool:
    return df is None or df.limit(1).count() == 0


def union_distinct(
    frames: Iterable[Optional[DataFrame]], columns: Sequence[str]
) -> Optional[DataFrame]:
    present = [frame.select(*columns) for frame in frames if frame is not None]
    if not present:
        return None
    return reduce(lambda left, right: left.unionByName(right), present).dropDuplicates(
        list(columns)
    )


def optional_value(df: DataFrame, name: str, data_type: str):
    """Return a typed column expression without requiring the field to exist."""
    if name in df.columns:
        return F.col(name).cast(data_type)
    return F.lit(None).cast(data_type)


def optional_col(
    df: DataFrame, name: str, data_type: str, alias: Optional[str] = None
):
    output = alias or name
    return optional_value(df, name, data_type).alias(output)


def empty_long_array():
    return F.expr("CAST(array() AS ARRAY<BIGINT>)")


def clean_response_text(column):
    cleaned = F.trim(
        F.regexp_replace(column.cast("string"), r"[\\x00\\u00A0\\t\\n\\r]", " ")
    )
    return F.when(cleaned == "", F.lit(None).cast("string")).otherwise(cleaned)


def normalized_answer_text(column):
    cleaned = clean_response_text(column)
    collapsed = F.regexp_replace(cleaned, r"\\s+", " ")
    uppered = F.upper(F.trim(collapsed))
    return F.when(uppered == "", F.lit(None).cast("string")).otherwise(uppered)


def stable_hash(df: DataFrame, excluded: Sequence[str] = ()):
    excluded_set = set(excluded)
    columns = [
        F.col(name)
        for name in sorted(df.columns)
        if name not in excluded_set
    ]
    return F.sha2(F.to_json(F.struct(*columns)), 256)


def assert_unique_non_null(
    df: DataFrame, keys: Sequence[str], label: str
) -> Dict[str, int]:
    null_condition = reduce(
        lambda left, right: left | right,
        [F.col(key).isNull() for key in keys],
    )
    metrics = (
        df.agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct(F.struct(*[F.col(key) for key in keys])).alias(
                "distinct_keys"
            ),
            F.sum(F.when(null_condition, 1).otherwise(0)).alias("null_keys"),
        )
        .first()
        .asDict()
    )
    result = {key: int(value or 0) for key, value in metrics.items()}
    if result["null_keys"]:
        raise ValueError(f"{label}: {result['null_keys']} rows have null keys {keys}")
    if result["rows"] != result["distinct_keys"]:
        raise ValueError(
            f"{label}: {result['rows']} rows but "
            f"{result['distinct_keys']} distinct keys {keys}"
        )
    return result


def align_to_schema(df: DataFrame, schema: T.StructType) -> DataFrame:
    expressions = []
    for field in schema.fields:
        if field.name in df.columns:
            expressions.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            expressions.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.select(*expressions)


def latest_operation_metrics(table_name: str) -> dict:
    history = spark.sql(f"DESCRIBE HISTORY {qname(table_name)} LIMIT 20").collect()
    row = next((candidate for candidate in history if candidate["operation"] == "MERGE"), None)
    if row is None and history:
        row = history[0]
    if row is None:
        return {}
    metrics = dict(row["operationMetrics"] or {})
    return {"operation": row["operation"], **metrics}


def ensure_table_properties(table_name: str, properties: dict) -> None:
    if not table_exists(table_name):
        return
    current = table_properties(table_name)
    changed = {
        key: value
        for key, value in properties.items()
        if str(current.get(key)) != str(value)
    }
    if not changed:
        return
    clause = ", ".join(
        f"'{sql_string(key)}' = '{sql_string(value)}'"
        for key, value in changed.items()
    )
    spark.sql(f"ALTER TABLE {qname(table_name)} SET TBLPROPERTIES ({clause})")


def set_table_comment(table_name: str, comment: str) -> None:
    if not table_exists(table_name):
        return
    try:
        current = spark.catalog.getTable(table_name).description
    except Exception:
        current = None
    if current != comment:
        spark.sql(
            f"COMMENT ON TABLE {qname(table_name)} IS '{sql_string(comment)}'"
        )


def set_column_comments(table_name: str, comments: dict) -> int:
    if not table_exists(table_name):
        return 0
    schema = spark.table(table_name).schema
    existing = {
        field.name: field.metadata.get("comment")
        for field in schema.fields
    }
    changed = 0
    for column_name, comment in comments.items():
        if column_name not in existing or existing[column_name] == comment:
            continue
        spark.sql(
            f"ALTER TABLE {qname(table_name)} ALTER COLUMN "
            f"{qident(column_name)} COMMENT '{sql_string(comment)}'"
        )
        changed += 1
    return changed


def is_serverless_compute() -> bool:
    values = []
    for key in (
        "spark.databricks.cluster.profile",
        "spark.databricks.clusterUsageTags.clusterName",
        "spark.databricks.isServerless",
    ):
        try:
            values.append(str(spark.conf.get(key)))
        except Exception:
            pass
    return any(
        "serverless" in value.lower() or value.lower() == "true" for value in values
    )


SOURCE_COLUMN_TYPES: List[Tuple[str, str]] = [
    ("DOC_RESPONSE_KEY", "string"),
    ("ACTIVE_IND", "int"),
    ("ENCNTR_ID", "string"),
    ("PERSON_ID", "string"),
    ("DOC_INPUT_ID", "string"),
    ("ELEMENT_EVENT_ID", "string"),
    ("FORM_EVENT_ID", "string"),
    ("SECTION_EVENT_ID", "string"),
    ("GRID_EVENT_ID", "string"),
    ("ROW_EVENT_ID", "string"),
    ("FORM_STATUS_CD", "string"),
    ("STATUS", "string"),
    ("DOCUMENTATION_DT_TM", "timestamp"),
    ("FIRST_DOCUMENTED_DT_TM", "timestamp"),
    ("LAST_DOCUMENTED_DT_TM", "timestamp"),
    ("PERFORMED_PRSNL_ID", "string"),
    ("PERFORMED_DT_TM", "timestamp"),
    ("RESPONSE_VALUE_TXT", "string"),
    ("NUMERIC_RESPONSE_NBR", "double"),
    ("RESPONSE_DT_TM", "timestamp"),
    ("STRING_RESPONSE_TXT", "string"),
    ("RESPONSE_NOMEN_ID", "string"),
    ("RESPONSE_CODE_VALUE_CD", "string"),
    ("FORM_DESC_TXT", "string"),
    ("SECTION_DESC_TXT", "string"),
    ("ELEMENT_LABEL_TXT", "string"),
    ("GRID_NAME_TXT", "string"),
    ("GRID_COLUMN_DESC_TXT", "string"),
    ("ELEMENT_DESC_TXT", "string"),
    ("ELEMENT_MNEMONIC_TXT", "string"),
    ("GRID_COLUMN_MNEMONIC_TXT", "string"),
    ("GRID_ROW_DESC_TXT", "string"),
    ("GRID_ROW_MNEMONIC_TXT", "string"),
    ("ADC_UPDT", "timestamp"),
    ("DCP_FORMS_ACTIVITY_ID", "long"),
    ("PIPELINE_RUN_ID", "string"),
    ("PIPELINE_PROCESSED_DT_TM", "timestamp"),
    ("ELEMENT_EVENT_CD", "long"),
    ("TASK_ASSAY_ID", "long"),
    ("RESPONSE_SEQUENCE_NBR", "long"),
    ("DCP_SECTION_INSTANCE_ID", "long"),
    ("ORGANIZATION_ID", "long"),
    ("TRUST", "string"),
    ("ENCNTR_ID_LONG", "long"),
    ("PERSON_ID_LONG", "long"),
    ("ELEMENT_EVENT_ID_LONG", "long"),
    ("PERFORMED_PRSNL_ID_LONG", "long"),
    ("FORM_STATUS_CD_LONG", "long"),
    ("RESPONSE_NOMENCLATURE_ID", "long"),
    ("RESPONSE_CODE_VALUE_ID", "long"),
    ("RESPONSE_HAS_NOMENCLATURE_IND", "int"),
    ("RESPONSE_HAS_CODE_VALUE_IND", "int"),
    ("SOURCE_ACTIVITY_ADC_UPDT", "timestamp"),
    ("SOURCE_EVENT_ADC_UPDT", "timestamp"),
    ("SOURCE_LABEL_ADC_UPDT", "timestamp"),
    ("FORM_REF_ID", "long"),
    ("FORM_INSTANCE_ID", "long"),
    ("SECTION_REF_ID", "long"),
    ("DCP_INPUT_REF_ID", "long"),
    ("GRID_NAME_CD", "long"),
    ("GRID_EVENT_CD", "long"),
    ("INPUT_TYPE_FLG", "int"),
    ("GRID_NAME_CURRENT_FALLBACK_IND", "int"),
]
SOURCE_REQUIRED_COLUMNS = {name for name, _ in SOURCE_COLUMN_TYPES}

LANE_KEY_CONTRACTS = {
    SRC_NOMEN: ["EVENT_ID", "SEQUENCE_NBR"],
    SRC_CODED: ["EVENT_ID", "SEQUENCE_NBR"],
    SRC_NUMERIC: ["EVENT_ID"],
    SRC_TEXT: ["EVENT_ID"],
    SRC_DATE: ["EVENT_ID"],
}


def ensure_schema_and_control_tables() -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qname(TARGET_SCHEMA)}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qname(TGT_MAP)} (
          MAPPING_RULE_ID STRING NOT NULL,
          FORM_REF_ID BIGINT,
          DCP_INPUT_REF_ID BIGINT,
          SECTION_REF_ID BIGINT,
          TASK_ASSAY_ID BIGINT,
          ELEMENT_EVENT_CD BIGINT,
          GRID_EVENT_CD BIGINT,
          INPUT_TYPE_FLG INT,
          ANSWER_KIND STRING NOT NULL,
          ANSWER_NOMENCLATURE_ID BIGINT,
          ANSWER_CODE_VALUE_ID BIGINT,
          ANSWER_TEXT_NORMALIZED STRING,
          QUESTION_CONCEPT_ID BIGINT,
          VALUE_CONCEPT_ID BIGINT,
          UNIT_CONCEPT_ID BIGINT,
          TARGET_OMOP_TABLE STRING,
          TARGET_OMOP_FIELD STRING,
          SUGGESTED_QUESTION_CONCEPT_ID BIGINT,
          SUGGESTED_VALUE_CONCEPT_ID BIGINT,
          SUGGESTED_UNIT_CONCEPT_ID BIGINT,
          SUGGESTION_SOURCE STRING,
          FORM_DESC_EXAMPLE STRING,
          SECTION_DESC_EXAMPLE STRING,
          QUESTION_LABEL_EXAMPLE STRING,
          ANSWER_DISPLAY_EXAMPLE STRING,
          DISTINCT_FORM_REF_COUNT BIGINT,
          EXAMPLE_ROW_COUNT BIGINT,
          FIRST_SEEN_DT_TM TIMESTAMP,
          LAST_SEEN_DT_TM TIMESTAMP,
          SOURCE_PRESENT_IND BOOLEAN NOT NULL,
          MAPPING_METHOD STRING NOT NULL,
          CURATED_STATUS STRING NOT NULL,
          CURATED_BY STRING,
          CURATED_AT TIMESTAMP,
          CURATED_NOTES STRING,
          MAPPING_VERSION STRING NOT NULL,
          VALID_FROM_DT DATE,
          VALID_TO_DT DATE,
          PIPELINE_STATS_HASH STRING,
          PIPELINE_STATS_UPDT_DT_TM TIMESTAMP,
          MAP_ROW_UPDT_DT_TM TIMESTAMP
        ) USING DELTA
        TBLPROPERTIES (
          'delta.enableChangeDataFeed' = 'true',
          'delta.deletedFileRetentionDuration' = 'interval 30 days',
          'delta.logRetentionDuration' = 'interval 30 days'
        )
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qname(STATE_TABLE)} (
          PIPELINE_NAME STRING NOT NULL,
          SOURCE_TABLE STRING NOT NULL,
          LAST_PROCESSED_VERSION BIGINT NOT NULL,
          LAST_COMMIT_TIMESTAMP TIMESTAMP,
          LAST_RUN_ID STRING,
          ITEM_TARGET_VERSION BIGINT,
          ASSESSMENT_TARGET_VERSION BIGINT,
          UPDATED_AT TIMESTAMP NOT NULL
        ) USING DELTA
        COMMENT 'Downstream PowerForm canonical checkpoints. Distinct from upstream powerforms_pipeline_* producer control tables.'
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qname(RUNS_TABLE)} (
          RUN_ID STRING NOT NULL,
          PIPELINE_NAME STRING NOT NULL,
          RUN_TYPE STRING NOT NULL,
          STATUS STRING NOT NULL,
          STARTED_AT TIMESTAMP NOT NULL,
          COMPLETED_AT TIMESTAMP,
          SOURCE_VERSIONS_JSON STRING,
          CHANGED_RANGES_JSON STRING,
          CANDIDATE_COUNTS_JSON STRING,
          ITEM_METRICS_JSON STRING,
          ASSESSMENT_METRICS_JSON STRING,
          COVERAGE_JSON STRING,
          MAPPING_JSON STRING,
          TARGET_VERSIONS_BEFORE_JSON STRING,
          TARGET_VERSIONS_AFTER_JSON STRING,
          MESSAGE STRING,
          ERROR_CLASS STRING,
          ERROR_MESSAGE STRING
        ) USING DELTA
        COMMENT 'Append/update audit log for the downstream PowerForm canonical builder; not the upstream powerforms_pipeline_runs table.'
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qname(CANDIDATE_TABLE)} (
          RUN_ID STRING NOT NULL,
          DOC_RESPONSE_KEY STRING NOT NULL,
          DCP_FORMS_ACTIVITY_ID BIGINT,
          ELEMENT_EVENT_ID_LONG BIGINT,
          REASON STRING NOT NULL,
          SHARD_ID INT,
          STATUS STRING NOT NULL,
          CREATED_AT TIMESTAMP NOT NULL,
          PROCESSED_AT TIMESTAMP
        ) USING DELTA
        COMMENT 'Resumable work for the downstream PowerForm canonical builder; distinct from upstream powerforms_pipeline_candidates.'
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """
    )
    for table_name in (TGT_MAP, STATE_TABLE, RUNS_TABLE, CANDIDATE_TABLE):
        ensure_table_properties(
            table_name,
            {
                "delta.enableChangeDataFeed": "true",
                "delta.deletedFileRetentionDuration": "interval 30 days",
                "delta.logRetentionDuration": "interval 30 days",
            },
        )
    set_table_comment(
        TGT_MAP,
        "Combined PowerForm question and optional answer mapping rules. "
        "Only APPROVED current rows with valid standard OMOP concepts are applied. "
        "Question and answer are intentionally governed in one row.",
    )


def coded_maintenance_report() -> dict:
    report = {
        "table_exists": table_exists(SRC_CODED),
        "comment_present": False,
        "checkpoint_table": None,
        "checkpoint_rows": 0,
    }
    if not report["table_exists"]:
        return report
    try:
        report["comment_present"] = bool(
            (spark.catalog.getTable(SRC_CODED).description or "").strip()
        )
    except Exception:
        pass
    checkpoint = (
        f"{bronze_control_schema(TARGET_SCHEMA)}."
        "map_coded_events_checkpoints"
    )
    report["checkpoint_table"] = checkpoint
    if table_exists(checkpoint):
        report["checkpoint_rows"] = spark.table(checkpoint).limit(1).count()
    return report


def preflight_report(strict: bool = False) -> dict:
    required_tables = [
        SRC_POWERFORM,
        SRC_NOMEN,
        SRC_CODED,
        SRC_NUMERIC,
        SRC_TEXT,
        SRC_DATE,
        SRC_CONCEPT,
        TGT_MAP,
    ]
    missing_tables = [name for name in required_tables if not table_exists(name)]
    blockers: List[str] = []
    warnings: List[str] = []
    if missing_tables:
        blockers.append(f"missing tables: {missing_tables}")

    source_missing: List[str] = []
    source_additive: List[str] = []
    source_type_mismatches: List[dict] = []
    if table_exists(SRC_POWERFORM):
        source_schema = spark.table(SRC_POWERFORM).schema
        actual = set(source_schema.fieldNames())
        source_missing = sorted(SOURCE_REQUIRED_COLUMNS - actual)
        source_additive = sorted(actual - SOURCE_REQUIRED_COLUMNS)
        if source_missing:
            blockers.append(
                "upstream v3.1 contract not complete; missing source columns: "
                + ", ".join(source_missing)
            )
        expected_types = dict(SOURCE_COLUMN_TYPES)
        aliases = {
            "bigint": "long",
            "integer": "int",
            "tinyint": "int",
            "smallint": "int",
            "float": "double",
        }
        for field in source_schema.fields:
            if field.name not in expected_types:
                continue
            actual_type = aliases.get(field.dataType.simpleString(), field.dataType.simpleString())
            expected_type = expected_types[field.name]
            if actual_type != expected_type:
                source_type_mismatches.append(
                    {
                        "column": field.name,
                        "expected": expected_type,
                        "actual": actual_type,
                    }
                )
        if source_type_mismatches:
            blockers.append(
                f"upstream v3.1 type mismatches: {source_type_mismatches}"
            )

    cdf_report = {}
    retention_report = {}
    for table_name in [
        SRC_POWERFORM,
        SRC_NOMEN,
        SRC_CODED,
        SRC_NUMERIC,
        SRC_TEXT,
        SRC_DATE,
        TGT_MAP,
    ]:
        if table_exists(table_name):
            cdf_report[table_name] = cdf_enabled(table_name)
            if not cdf_report[table_name]:
                blockers.append(f"CDF is not enabled on {table_name}")
            properties = table_properties(table_name)
            retention = str(
                properties.get("delta.deletedFileRetentionDuration", "")
            )
            match = re.search(r"(\d+)\s*days?", retention, flags=re.IGNORECASE)
            days = int(match.group(1)) if match else None
            retention_report[table_name] = {
                "deleted_file_retention": retention or None,
                "parsed_days": days,
            }
            if days is None:
                blockers.append(
                    f"{table_name} does not declare deleted-file retention of at least 30 days"
                )
            elif days < 30:
                blockers.append(
                    f"{table_name} deleted-file retention is {days} days; minimum is 30"
                )

    lane_contract = {}
    for table_name, keys in LANE_KEY_CONTRACTS.items():
        if not table_exists(table_name):
            continue
        columns = set(spark.table(table_name).columns)
        missing = sorted(set(keys) - columns)
        lane_contract[table_name] = {"keys": keys, "missing_key_columns": missing}
        if missing:
            blockers.append(f"{table_name} missing lane key columns {missing}")

    coded_report = coded_maintenance_report()
    if coded_report["table_exists"] and (
        not coded_report["comment_present"]
        or coded_report["checkpoint_rows"] == 0
    ):
        message = (
            "coded canonical lane exists but lacks demonstrated scheduled maintenance "
            f"or documentation: {coded_report}"
        )
        if ALLOW_UNMAINTAINED_CODED or RUN_MODE == "define_only":
            warnings.append(message)
        else:
            blockers.append(message)

    source_versions = {}
    source_commit_timestamps = {}
    for table_name in required_tables:
        if table_exists(table_name):
            try:
                source_versions[table_name] = source_version(table_name)
                source_commit_timestamps[table_name] = version_timestamp(table_name)
            except Exception as exc:
                blockers.append(f"cannot read Delta version for {table_name}: {exc}")

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    freshness_hours = {}
    for table_name in [SRC_POWERFORM, SRC_NOMEN, SRC_CODED, SRC_NUMERIC, SRC_TEXT, SRC_DATE]:
        timestamp = source_commit_timestamps.get(table_name)
        if timestamp is None:
            continue
        if getattr(timestamp, "tzinfo", None) is not None:
            timestamp = timestamp.replace(tzinfo=None)
        age = max(0.0, (now - timestamp).total_seconds() / 3600.0)
        freshness_hours[table_name] = age
        if age > MAX_UPSTREAM_AGE_HOURS and not ALLOW_STALE_UPSTREAM:
            blockers.append(
                f"{table_name} latest Delta commit is {age:.1f}h old; "
                f"maximum is {MAX_UPSTREAM_AGE_HOURS}h"
            )
    lane_ages = [
        freshness_hours[name]
        for name in [SRC_NOMEN, SRC_CODED, SRC_NUMERIC, SRC_TEXT, SRC_DATE]
        if name in freshness_hours
    ]
    lane_schedule_skew_hours = (
        max(lane_ages) - min(lane_ages) if len(lane_ages) > 1 else 0.0
    )
    if lane_schedule_skew_hours > MAX_LANE_SKEW_HOURS and not ALLOW_STALE_UPSTREAM:
        blockers.append(
            f"canonical lane commit skew is {lane_schedule_skew_hours:.1f}h; "
            f"maximum is {MAX_LANE_SKEW_HOURS}h"
        )

    report = {
        "missing_tables": missing_tables,
        "source_missing_columns": source_missing,
        "source_additive_columns": source_additive,
        "source_type_mismatches": source_type_mismatches,
        "cdf": cdf_report,
        "retention": retention_report,
        "lane_contracts": lane_contract,
        "lane_grain_validation": "asserted on every affected narrow lane projection and full-build stage",
        "coded_maintenance": coded_report,
        "versions": source_versions,
        "commit_freshness_hours": freshness_hours,
        "lane_schedule_skew_hours": lane_schedule_skew_hours,
        "warnings": warnings,
        "blockers": blockers,
    }
    if strict and blockers:
        raise RuntimeError("PowerForm preflight failed: " + " | ".join(blockers))
    return report


ensure_schema_and_control_tables()
PREFLIGHT = preflight_report(strict=False)
print(bronze_json({"pipeline": PIPELINE_NAME, "mode": RUN_MODE, "preflight": PREFLIGHT}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run control and fixed-version snapshots

# COMMAND ----------


def current_versions() -> Dict[str, int]:
    return {
        table_name: source_version(table_name)
        for table_name in [
            SRC_POWERFORM,
            SRC_NOMEN,
            SRC_CODED,
            SRC_NUMERIC,
            SRC_TEXT,
            SRC_DATE,
            SRC_CONCEPT,
            TGT_MAP,
        ]
    }


def state_versions() -> Dict[str, int]:
    rows = (
        spark.table(STATE_TABLE)
        .where(F.col("PIPELINE_NAME") == PIPELINE_NAME)
        .select("SOURCE_TABLE", "LAST_PROCESSED_VERSION")
        .collect()
    )
    return {
        row["SOURCE_TABLE"]: int(row["LAST_PROCESSED_VERSION"])
        for row in rows
    }


def table_version_or_none(table_name: str) -> Optional[int]:
    return source_version(table_name) if table_exists(table_name) else None


def target_versions() -> dict:
    return {
        "item": table_version_or_none(TGT_ITEM),
        "assessment": table_version_or_none(TGT_ASSESSMENT),
        "map": table_version_or_none(TGT_MAP),
    }


def begin_run(run_id: str, run_type: str, versions: Dict[str, int]) -> None:
    existing = (
        spark.table(RUNS_TABLE)
        .where(F.col("RUN_ID") == run_id)
        .select("STATUS", "SOURCE_VERSIONS_JSON")
        .first()
    )
    if existing is not None:
        if existing["STATUS"] == "running":
            return
        resumable_full = run_type == "full_rebuild" and existing["STATUS"] == "partial"
        if not resumable_full and not OVERRIDE_RUNNING_LOCK:
            raise RuntimeError(
                f"run_id={run_id} already exists with status={existing['STATUS']}; "
                "use a new pipeline_run_id or explicitly override"
            )

    running = (
        spark.table(RUNS_TABLE)
        .where(
            (F.col("PIPELINE_NAME") == PIPELINE_NAME)
            & (F.col("STATUS") == "running")
            & (F.col("RUN_ID") != run_id)
        )
        .orderBy(F.col("STARTED_AT").desc())
        .first()
    )
    if running is not None and not OVERRIDE_RUNNING_LOCK:
        raise RuntimeError(
            f"another PowerForm canonical run is active: {running['RUN_ID']}"
        )

    row = spark.createDataFrame(
        [
            (
                run_id,
                PIPELINE_NAME,
                run_type,
                "running",
                datetime.now(timezone.utc).replace(tzinfo=None),
                json.dumps(versions, sort_keys=True),
                json.dumps(target_versions(), sort_keys=True),
            )
        ],
        """
        RUN_ID string, PIPELINE_NAME string, RUN_TYPE string, STATUS string,
        STARTED_AT timestamp, SOURCE_VERSIONS_JSON string,
        TARGET_VERSIONS_BEFORE_JSON string
        """,
    )
    if existing is None:
        (
            row.withColumn("COMPLETED_AT", F.lit(None).cast("timestamp"))
            .withColumn("CHANGED_RANGES_JSON", F.lit(None).cast("string"))
            .withColumn("CANDIDATE_COUNTS_JSON", F.lit(None).cast("string"))
            .withColumn("ITEM_METRICS_JSON", F.lit(None).cast("string"))
            .withColumn("ASSESSMENT_METRICS_JSON", F.lit(None).cast("string"))
            .withColumn("COVERAGE_JSON", F.lit(None).cast("string"))
            .withColumn("MAPPING_JSON", F.lit(None).cast("string"))
            .withColumn("TARGET_VERSIONS_AFTER_JSON", F.lit(None).cast("string"))
            .withColumn("MESSAGE", F.lit(None).cast("string"))
            .withColumn("ERROR_CLASS", F.lit(None).cast("string"))
            .withColumn("ERROR_MESSAGE", F.lit(None).cast("string"))
            .select(*spark.table(RUNS_TABLE).columns)
            .write.mode("append")
            .saveAsTable(RUNS_TABLE)
        )
    else:
        DeltaTable.forName(spark, RUNS_TABLE).update(
            condition=F.col("RUN_ID") == run_id,
            set={
                "STATUS": F.lit("running"),
                "STARTED_AT": F.current_timestamp(),
                "COMPLETED_AT": F.lit(None).cast("timestamp"),
                "SOURCE_VERSIONS_JSON": F.lit(json.dumps(versions, sort_keys=True)),
                "ERROR_CLASS": F.lit(None).cast("string"),
                "ERROR_MESSAGE": F.lit(None).cast("string"),
            },
        )


def update_run(run_id: str, status: str, **values) -> None:
    assignments = {
        "STATUS": F.lit(status),
        "MESSAGE": F.lit(values.get("message")),
    }
    if status in {"success", "noop", "partial", "failed"}:
        assignments["COMPLETED_AT"] = F.current_timestamp()
    json_fields = {
        "changed_ranges": "CHANGED_RANGES_JSON",
        "candidate_counts": "CANDIDATE_COUNTS_JSON",
        "item_metrics": "ITEM_METRICS_JSON",
        "assessment_metrics": "ASSESSMENT_METRICS_JSON",
        "coverage": "COVERAGE_JSON",
        "mapping": "MAPPING_JSON",
        "target_versions_after": "TARGET_VERSIONS_AFTER_JSON",
    }
    for input_name, column_name in json_fields.items():
        if input_name in values:
            assignments[column_name] = F.lit(
                json.dumps(values[input_name], default=str, sort_keys=True)
            )
    if "error" in values and values["error"] is not None:
        assignments["ERROR_CLASS"] = F.lit(type(values["error"]).__name__)
        assignments["ERROR_MESSAGE"] = F.lit(str(values["error"]))
    DeltaTable.forName(spark, RUNS_TABLE).update(
        condition=F.col("RUN_ID") == run_id,
        set=assignments,
    )


def powerform_run_progress(run_id: str, phase: str, **details) -> None:
    """Print and persist a lightweight heartbeat without making progress logging fatal."""
    payload = {
        "phase": phase,
        "at": bronze_utc_now(),
        **details,
    }
    message = bronze_json(payload)
    print(f"[POWERFORM] {message}")
    try:
        update_run(run_id, "running", message=message)
    except Exception as exc:
        print(f"[WARN] Could not persist PowerForm progress heartbeat: {exc}")


def write_state(versions: Dict[str, int], run_id: str) -> None:
    item_version = table_version_or_none(TGT_ITEM)
    assessment_version = table_version_or_none(TGT_ASSESSMENT)
    rows = [
        (
            PIPELINE_NAME,
            table_name,
            int(version),
            version_timestamp(table_name, int(version)),
            run_id,
            item_version,
            assessment_version,
        )
        for table_name, version in versions.items()
    ]
    updates = (
        spark.createDataFrame(
            rows,
            """
            PIPELINE_NAME string, SOURCE_TABLE string,
            LAST_PROCESSED_VERSION long, LAST_COMMIT_TIMESTAMP timestamp,
            LAST_RUN_ID string, ITEM_TARGET_VERSION long,
            ASSESSMENT_TARGET_VERSION long
            """,
        )
        .withColumn("UPDATED_AT", F.current_timestamp())
    )
    (
        DeltaTable.forName(spark, STATE_TABLE)
        .alias("t")
        .merge(
            updates.alias("s"),
            "t.PIPELINE_NAME = s.PIPELINE_NAME "
            "AND t.SOURCE_TABLE = s.SOURCE_TABLE",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def write_candidate_rows(rows: DataFrame) -> None:
    expected = spark.table(CANDIDATE_TABLE).schema
    staged = align_to_schema(rows, expected)
    (
        DeltaTable.forName(spark, CANDIDATE_TABLE)
        .alias("t")
        .merge(
            staged.alias("s"),
            "t.RUN_ID = s.RUN_ID AND "
            "t.DOC_RESPONSE_KEY = s.DOC_RESPONSE_KEY AND "
            "t.REASON = s.REASON",
        )
        .whenMatchedUpdate(
            set={
                "DCP_FORMS_ACTIVITY_ID": "s.DCP_FORMS_ACTIVITY_ID",
                "ELEMENT_EVENT_ID_LONG": "s.ELEMENT_EVENT_ID_LONG",
                "SHARD_ID": "s.SHARD_ID",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def candidate_scope(run_id: str) -> DataFrame:
    return (
        spark.table(CANDIDATE_TABLE)
        .where(
            (F.col("RUN_ID") == run_id)
            & F.col("STATUS").isin("pending", "processing")
        )
        .groupBy("DOC_RESPONSE_KEY")
        .agg(
            F.max("DCP_FORMS_ACTIVITY_ID").alias("DCP_FORMS_ACTIVITY_ID"),
            F.max("ELEMENT_EVENT_ID_LONG").alias("ELEMENT_EVENT_ID_LONG"),
            F.concat_ws(",", F.sort_array(F.collect_set("REASON"))).alias(
                "TRIGGER_SOURCES"
            ),
            F.min("SHARD_ID").alias("SHARD_ID"),
        )
    )


def mark_candidates(run_id: str, status: str) -> None:
    assignments = {"STATUS": F.lit(status)}
    if status == "completed":
        assignments["PROCESSED_AT"] = F.current_timestamp()
    DeltaTable.forName(spark, CANDIDATE_TABLE).update(
        condition=(F.col("RUN_ID") == run_id)
        & F.col("STATUS").isin("pending", "processing"),
        set=assignments,
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Source projection and response classification

# COMMAND ----------


def source_projection(source: DataFrame) -> DataFrame:
    expressions = []
    available = set(source.columns)
    for name, data_type in SOURCE_COLUMN_TYPES:
        if name not in available:
            raise ValueError(f"{SRC_POWERFORM} missing required column {name}")
        output = {
            "PIPELINE_RUN_ID": "SOURCE_POWERFORM_RUN_ID",
            "PIPELINE_PROCESSED_DT_TM": "SOURCE_POWERFORM_PROCESSED_DT_TM",
        }.get(name, name)
        expressions.append(F.col(name).cast(data_type).alias(output))
    return source.select(*expressions)


def classify_responses(source: DataFrame) -> DataFrame:
    base = source_projection(source)
    suffix = F.regexp_extract(F.col("DOC_RESPONSE_KEY"), r"~([^~]+)$", 1)
    suffix_long = F.expr(
        "try_cast(regexp_extract(DOC_RESPONSE_KEY, '~([^~]+)$', 1) as bigint)"
    )
    event_prefix = F.expr(
        "try_cast(regexp_extract(DOC_RESPONSE_KEY, '^([^~]+)', 1) as bigint)"
    )
    response_text = clean_response_text(
        F.coalesce(F.col("STRING_RESPONSE_TXT"), F.col("RESPONSE_VALUE_TXT"))
    )
    both_numeric_text = (
        F.col("NUMERIC_RESPONSE_NBR").isNotNull() & response_text.isNotNull()
    )
    is_coded = F.col("RESPONSE_SEQUENCE_NBR").isNotNull()
    coded_suffix_valid = is_coded & (
        suffix_long == F.col("RESPONSE_SEQUENCE_NBR")
    )
    is_date = (suffix == "-2") & F.col("RESPONSE_DT_TM").isNotNull()
    is_string_variant = suffix == "-1"
    is_numeric = is_string_variant & F.col("NUMERIC_RESPONSE_NBR").isNotNull()
    is_text = (
        is_string_variant
        & F.col("NUMERIC_RESPONSE_NBR").isNull()
        & response_text.isNotNull()
    )
    is_empty_string = (
        is_string_variant
        & F.col("NUMERIC_RESPONSE_NBR").isNull()
        & response_text.isNull()
    )
    is_empty_else = (
        (suffix == "-3")
        & F.col("RESPONSE_SEQUENCE_NBR").isNull()
        & F.col("RESPONSE_DT_TM").isNull()
        & F.col("NUMERIC_RESPONSE_NBR").isNull()
        & response_text.isNull()
    )

    kind = (
        F.when(
            is_coded & F.col("RESPONSE_NOMENCLATURE_ID").isNotNull(),
            F.lit("CODED_NOMENCLATURE"),
        )
        .when(
            is_coded & F.col("RESPONSE_CODE_VALUE_ID").isNotNull(),
            F.lit("CODED_CODE_VALUE"),
        )
        .when(is_coded, F.lit("CODED_OTHER"))
        .when(is_date, F.lit("DATE"))
        .when(is_numeric, F.lit("NUMERIC"))
        .when(is_text, F.lit("TEXT"))
        .when(is_empty_string | is_empty_else, F.lit("EMPTY"))
        .otherwise(F.lit("INVALID"))
    )
    classification_valid = (
        F.when(is_coded, coded_suffix_valid)
        .when(is_date, F.lit(True))
        .when(is_string_variant, F.lit(True))
        .when(is_empty_else, F.lit(True))
        .otherwise(F.lit(False))
    )
    lane = (
        F.when(kind == "CODED_NOMENCLATURE", F.lit("NOMEN"))
        .when(kind.isin("CODED_CODE_VALUE", "CODED_OTHER"), F.lit("CODED"))
        .when(kind == "NUMERIC", F.lit("NUMERIC"))
        .when(kind == "TEXT", F.lit("TEXT"))
        .when(kind == "DATE", F.lit("DATE"))
        .otherwise(F.lit("NONE"))
    )
    return (
        base.withColumn("RESPONSE_VARIANT_SUFFIX", suffix)
        .withColumn(
            "RESPONSE_HAS_BOTH_CODE_TYPES_IND",
            F.col("RESPONSE_NOMENCLATURE_ID").isNotNull()
            & F.col("RESPONSE_CODE_VALUE_ID").isNotNull(),
        )
        .withColumn(
            "RESPONSE_HAS_BOTH_NUMERIC_AND_TEXT_IND", both_numeric_text
        )
        .withColumn("RESPONSE_TEXT_TXT", response_text)
        .withColumn("RESPONSE_KIND", kind)
        .withColumn("CANONICAL_LANE", lane)
        .withColumn(
            "RESPONSE_CLASSIFICATION_VALID_IND",
            classification_valid
            & (event_prefix == F.col("ELEMENT_EVENT_ID_LONG"))
            & (
                (
                    F.col("RESPONSE_HAS_NOMENCLATURE_IND")
                    == F.col("RESPONSE_NOMENCLATURE_ID").isNotNull().cast("int")
                )
                & (
                    F.col("RESPONSE_HAS_CODE_VALUE_IND")
                    == F.col("RESPONSE_CODE_VALUE_ID").isNotNull().cast("int")
                )
            ),
        )
    )


LANE_OUTPUT_TYPES = {
    "_L_EVENT_ID": "long",
    "_L_SEQUENCE_NBR": "long",
    "_L_NOMENCLATURE_ID": "long",
    "_L_RESULT_CD": "long",
    "_L_NUMERIC_RESULT": "double",
    "_L_TEXT_SHORT": "string",
    "_L_TEXT_RESULT": "string",
    "_L_DATE_RESULT": "timestamp",
    "_L_SOURCE_ROW_KEY": "string",
    "_L_ROW_HASH": "string",
    "_L_PIPELINE_RUN_ID": "string",
    "_L_SOURCE_CURRENT_IND": "boolean",
    "_L_SOURCE_DELETED_IND": "boolean",
    "_L_Q_CONCEPT_RAW": "long",
    "_L_VALUE_CONCEPT_RAW": "long",
    "_L_UNIT_CONCEPT_RAW": "long",
    "_L_Q_RULE_ID": "string",
    "_L_VALUE_RULE_ID": "string",
    "_L_UNIT_RULE_ID": "string",
    "_L_Q_VALID_RAW": "boolean",
    "_L_VALUE_VALID_RAW": "boolean",
    "_L_UNIT_VALID_RAW": "boolean",
    "_L_Q_AMBIGUOUS": "boolean",
    "_L_VALUE_AMBIGUOUS": "boolean",
    "_L_UNIT_AMBIGUOUS": "boolean",
    "_L_Q_CANDIDATES": "array<long>",
    "_L_VALUE_CANDIDATES": "array<long>",
    "_L_UNIT_CANDIDATES": "array<long>",
}


def _lane_base(
    table_name: str,
    version: int,
    event_ids: Optional[DataFrame] = None,
    broadcast_ids: bool = False,
) -> DataFrame:
    lane = read_snapshot(table_name, version)
    if event_ids is not None:
        ids = (
            event_ids.select(F.col("EVENT_ID").cast("long").alias("EVENT_ID"))
            .where(F.col("EVENT_ID").isNotNull())
            .distinct()
        )
        if broadcast_ids:
            ids = F.broadcast(ids)
        lane = lane.join(ids, "EVENT_ID", "left_semi")
    return lane


def nomen_lane_projection(
    version: int,
    event_ids: Optional[DataFrame] = None,
    broadcast_ids: bool = False,
) -> DataFrame:
    df = _lane_base(SRC_NOMEN, version, event_ids, broadcast_ids)
    q_amb = (
        F.coalesce(F.col("OMOP_MANUAL_AMBIGUOUS_IND"), F.lit(False))
        if "OMOP_MANUAL_AMBIGUOUS_IND" in df.columns
        else F.lit(False)
    )
    v_amb = (
        F.coalesce(F.col("OMOP_MANUAL_VALUE_AMBIGUOUS_IND"), F.lit(False))
        if "OMOP_MANUAL_VALUE_AMBIGUOUS_IND" in df.columns
        else F.lit(False)
    )
    manual_value = (
        F.col("OMOP_MANUAL_VALUE_CONCEPT_ID").cast("long")
        if "OMOP_MANUAL_VALUE_CONCEPT_ID" in df.columns
        else F.lit(None).cast("long")
    )
    manual_value_rule = (
        F.col("OMOP_MANUAL_VALUE_RULE_KEY").cast("string")
        if "OMOP_MANUAL_VALUE_RULE_KEY" in df.columns
        else F.lit(None).cast("string")
    )
    intrinsic_value = (
        F.col("OMOP_CONCEPT_ID").cast("long")
        if "OMOP_CONCEPT_ID" in df.columns
        else F.lit(None).cast("long")
    )
    intrinsic_valid = (
        (F.col("OMOP_MATCH_NUMBER") == 1)
        & (F.col("OMOP_STANDARD_CONCEPT") == "S")
        if {"OMOP_MATCH_NUMBER", "OMOP_STANDARD_CONCEPT"}.issubset(df.columns)
        else F.lit(False)
    )
    value_raw = (
        F.when(~v_amb & manual_value.isNotNull(), manual_value)
        .when(intrinsic_valid, intrinsic_value)
        .otherwise(F.lit(None).cast("long"))
    )
    value_rule = (
        F.when(manual_value.isNotNull(), manual_value_rule)
        .otherwise(
            F.when(
                intrinsic_valid,
                F.concat(F.lit("NOMENCLATURE:"), F.col("NOMENCLATURE_ID")),
            )
        )
        .cast("string")
    )
    source_deleted = (
        F.coalesce(F.col("SOURCE_DELETED_IND"), F.lit(False))
        if "SOURCE_DELETED_IND" in df.columns
        else F.lit(False)
    )
    return df.select(
        F.col("EVENT_ID").cast("long").alias("_L_EVENT_ID"),
        F.col("SEQUENCE_NBR").cast("long").alias("_L_SEQUENCE_NBR"),
        F.col("NOMENCLATURE_ID").cast("long").alias("_L_NOMENCLATURE_ID"),
        F.lit(None).cast("long").alias("_L_RESULT_CD"),
        F.lit(None).cast("double").alias("_L_NUMERIC_RESULT"),
        F.lit(None).cast("string").alias("_L_TEXT_SHORT"),
        F.lit(None).cast("string").alias("_L_TEXT_RESULT"),
        F.lit(None).cast("timestamp").alias("_L_DATE_RESULT"),
        optional_col(df, "SOURCE_ROW_KEY", "string", "_L_SOURCE_ROW_KEY"),
        optional_col(df, "ROW_HASH", "string", "_L_ROW_HASH"),
        optional_col(df, "PIPELINE_RUN_ID", "string", "_L_PIPELINE_RUN_ID"),
        (~source_deleted).cast("boolean").alias("_L_SOURCE_CURRENT_IND"),
        source_deleted.cast("boolean").alias("_L_SOURCE_DELETED_IND"),
        optional_col(
            df, "OMOP_MANUAL_CONCEPT_ID", "long", "_L_Q_CONCEPT_RAW"
        ),
        value_raw.alias("_L_VALUE_CONCEPT_RAW"),
        F.lit(None).cast("long").alias("_L_UNIT_CONCEPT_RAW"),
        optional_col(df, "OMOP_MANUAL_RULE_KEY", "string", "_L_Q_RULE_ID"),
        value_rule.alias("_L_VALUE_RULE_ID"),
        F.lit(None).cast("string").alias("_L_UNIT_RULE_ID"),
        (~q_amb).cast("boolean").alias("_L_Q_VALID_RAW"),
        ((~v_amb) & value_raw.isNotNull()).cast("boolean").alias(
            "_L_VALUE_VALID_RAW"
        ),
        F.lit(False).alias("_L_UNIT_VALID_RAW"),
        q_amb.cast("boolean").alias("_L_Q_AMBIGUOUS"),
        v_amb.cast("boolean").alias("_L_VALUE_AMBIGUOUS"),
        F.lit(False).alias("_L_UNIT_AMBIGUOUS"),
        (
            F.col("OMOP_MANUAL_CANDIDATE_CONCEPT_IDS").cast("array<long>")
            if "OMOP_MANUAL_CANDIDATE_CONCEPT_IDS" in df.columns
            else empty_long_array()
        ).alias("_L_Q_CANDIDATES"),
        (
            F.col("OMOP_MANUAL_VALUE_CANDIDATE_CONCEPT_IDS").cast("array<long>")
            if "OMOP_MANUAL_VALUE_CANDIDATE_CONCEPT_IDS" in df.columns
            else F.when(intrinsic_value.isNotNull(), F.array(intrinsic_value)).otherwise(
                empty_long_array()
            )
        ).alias("_L_VALUE_CANDIDATES"),
        empty_long_array().alias("_L_UNIT_CANDIDATES"),
    )


def coded_lane_projection(
    version: int,
    event_ids: Optional[DataFrame] = None,
    broadcast_ids: bool = False,
) -> DataFrame:
    df = _lane_base(SRC_CODED, version, event_ids, broadcast_ids)
    q_amb = (
        F.coalesce(F.col("OMOP_MAPPING_AMBIGUOUS_IND"), F.lit(False))
        if "OMOP_MAPPING_AMBIGUOUS_IND" in df.columns
        else F.lit(False)
    )
    v_amb = (
        F.coalesce(F.col("OMOP_VALUE_MAPPING_AMBIGUOUS_IND"), F.lit(False))
        if "OMOP_VALUE_MAPPING_AMBIGUOUS_IND" in df.columns
        else F.lit(False)
    )
    current = (
        F.coalesce(F.col("CR_CURRENT_AT_LOAD_IND"), F.lit(True))
        if "CR_CURRENT_AT_LOAD_IND" in df.columns
        else F.lit(True)
    )
    return df.select(
        F.col("EVENT_ID").cast("long").alias("_L_EVENT_ID"),
        F.col("SEQUENCE_NBR").cast("long").alias("_L_SEQUENCE_NBR"),
        optional_col(df, "NOMENCLATURE_ID", "long", "_L_NOMENCLATURE_ID"),
        optional_col(df, "RESULT_CD", "long", "_L_RESULT_CD"),
        F.lit(None).cast("double").alias("_L_NUMERIC_RESULT"),
        F.lit(None).cast("string").alias("_L_TEXT_SHORT"),
        F.lit(None).cast("string").alias("_L_TEXT_RESULT"),
        F.lit(None).cast("timestamp").alias("_L_DATE_RESULT"),
        optional_col(df, "SOURCE_ROW_KEY", "string", "_L_SOURCE_ROW_KEY"),
        optional_col(df, "ROW_HASH", "string", "_L_ROW_HASH"),
        optional_col(df, "PIPELINE_RUN_ID", "string", "_L_PIPELINE_RUN_ID"),
        current.cast("boolean").alias("_L_SOURCE_CURRENT_IND"),
        F.lit(False).alias("_L_SOURCE_DELETED_IND"),
        optional_col(df, "OMOP_MANUAL_CONCEPT", "long", "_L_Q_CONCEPT_RAW"),
        optional_col(
            df, "OMOP_MANUAL_VALUE_CONCEPT", "long", "_L_VALUE_CONCEPT_RAW"
        ),
        F.lit(None).cast("long").alias("_L_UNIT_CONCEPT_RAW"),
        optional_col(df, "OMOP_MAPPING_RULE_ID", "string", "_L_Q_RULE_ID"),
        optional_col(
            df, "OMOP_MAPPING_RULE_ID", "string", "_L_VALUE_RULE_ID"
        ),
        F.lit(None).cast("string").alias("_L_UNIT_RULE_ID"),
        (~q_amb).cast("boolean").alias("_L_Q_VALID_RAW"),
        (~v_amb).cast("boolean").alias("_L_VALUE_VALID_RAW"),
        F.lit(False).alias("_L_UNIT_VALID_RAW"),
        q_amb.cast("boolean").alias("_L_Q_AMBIGUOUS"),
        v_amb.cast("boolean").alias("_L_VALUE_AMBIGUOUS"),
        F.lit(False).alias("_L_UNIT_AMBIGUOUS"),
        (
            F.col("OMOP_MAPPING_CANDIDATE_CONCEPT_IDS").cast("array<long>")
            if "OMOP_MAPPING_CANDIDATE_CONCEPT_IDS" in df.columns
            else empty_long_array()
        ).alias("_L_Q_CANDIDATES"),
        (
            F.col("OMOP_VALUE_MAPPING_CANDIDATE_CONCEPT_IDS").cast("array<long>")
            if "OMOP_VALUE_MAPPING_CANDIDATE_CONCEPT_IDS" in df.columns
            else empty_long_array()
        ).alias("_L_VALUE_CANDIDATES"),
        empty_long_array().alias("_L_UNIT_CANDIDATES"),
    )


def numeric_lane_projection(
    version: int,
    event_ids: Optional[DataFrame] = None,
    broadcast_ids: bool = False,
) -> DataFrame:
    df = _lane_base(SRC_NUMERIC, version, event_ids, broadcast_ids)
    q_concept = optional_value(df, "OMOP_MANUAL_CONCEPT_ID", "long")
    unit_concept = F.coalesce(
        optional_value(df, "OMOP_MANUAL_UNIT_CONCEPT_ID", "long"),
        optional_value(df, "OMOP_MANUAL_UNITS", "long"),
    )
    q_amb = F.coalesce(
        optional_value(df, "OMOP_MAPPING_AMBIGUOUS_IND", "boolean"),
        F.lit(False),
    )
    u_amb = F.coalesce(
        optional_value(df, "OMOP_UNIT_MAPPING_AMBIGUOUS_IND", "boolean"),
        F.lit(False),
    )
    deleted = F.coalesce(
        optional_value(df, "SOURCE_DELETED_IND", "boolean"), F.lit(False)
    )
    current = F.coalesce(
        optional_value(df, "SOURCE_CURRENT_IND", "boolean"), ~deleted
    )
    q_valid = (
        F.coalesce(
            optional_value(df, "OMOP_MAPPING_VALID_IND", "boolean"),
            q_concept.isNotNull(),
        )
        & ~q_amb
    )
    unit_valid = (
        F.coalesce(
            optional_value(df, "OMOP_UNIT_MAPPING_VALID_IND", "boolean"),
            unit_concept.isNotNull(),
        )
        & ~u_amb
    )
    q_candidates = F.coalesce(
        optional_value(df, "OMOP_MAPPING_CANDIDATE_CONCEPT_IDS", "array<long>"),
        F.when(q_concept.isNotNull(), F.array(q_concept)).otherwise(
            empty_long_array()
        ),
    )
    unit_candidates = F.coalesce(
        optional_value(
            df, "OMOP_UNIT_MAPPING_CANDIDATE_CONCEPT_IDS", "array<long>"
        ),
        F.when(unit_concept.isNotNull(), F.array(unit_concept)).otherwise(
            empty_long_array()
        ),
    )
    return df.select(
        F.col("EVENT_ID").cast("long").alias("_L_EVENT_ID"),
        F.lit(None).cast("long").alias("_L_SEQUENCE_NBR"),
        F.lit(None).cast("long").alias("_L_NOMENCLATURE_ID"),
        F.lit(None).cast("long").alias("_L_RESULT_CD"),
        F.col("NUMERIC_RESULT").cast("double").alias("_L_NUMERIC_RESULT"),
        optional_col(df, "STRING_RESULT_TEXT", "string", "_L_TEXT_SHORT"),
        F.lit(None).cast("string").alias("_L_TEXT_RESULT"),
        F.lit(None).cast("timestamp").alias("_L_DATE_RESULT"),
        F.lit(None).cast("string").alias("_L_SOURCE_ROW_KEY"),
        optional_col(df, "ROW_HASH", "string", "_L_ROW_HASH"),
        optional_col(df, "PIPELINE_RUN_ID", "string", "_L_PIPELINE_RUN_ID"),
        current.cast("boolean").alias("_L_SOURCE_CURRENT_IND"),
        deleted.cast("boolean").alias("_L_SOURCE_DELETED_IND"),
        q_concept.alias("_L_Q_CONCEPT_RAW"),
        F.lit(None).cast("long").alias("_L_VALUE_CONCEPT_RAW"),
        unit_concept.alias("_L_UNIT_CONCEPT_RAW"),
        optional_col(df, "OMOP_MAPPING_RULE_ID", "string", "_L_Q_RULE_ID"),
        F.lit(None).cast("string").alias("_L_VALUE_RULE_ID"),
        optional_col(
            df, "OMOP_UNIT_MAPPING_RULE_ID", "string", "_L_UNIT_RULE_ID"
        ),
        q_valid.cast("boolean").alias("_L_Q_VALID_RAW"),
        F.lit(False).alias("_L_VALUE_VALID_RAW"),
        unit_valid.cast("boolean").alias("_L_UNIT_VALID_RAW"),
        q_amb.cast("boolean").alias("_L_Q_AMBIGUOUS"),
        F.lit(False).alias("_L_VALUE_AMBIGUOUS"),
        u_amb.cast("boolean").alias("_L_UNIT_AMBIGUOUS"),
        q_candidates.alias("_L_Q_CANDIDATES"),
        empty_long_array().alias("_L_VALUE_CANDIDATES"),
        unit_candidates.alias("_L_UNIT_CANDIDATES"),
    )

def text_lane_projection(
    version: int,
    event_ids: Optional[DataFrame] = None,
    broadcast_ids: bool = False,
) -> DataFrame:
    df = _lane_base(SRC_TEXT, version, event_ids, broadcast_ids)
    q_concept = optional_value(df, "OMOP_MANUAL_CONCEPT_ID", "long")
    value_concept = optional_value(df, "OMOP_MANUAL_VALUE_CONCEPT_ID", "long")
    unit_concept = F.coalesce(
        optional_value(df, "OMOP_MANUAL_UNIT_CONCEPT_ID", "long"),
        optional_value(df, "OMOP_MANUAL_UNITS", "long"),
    )
    q_amb = F.coalesce(
        optional_value(df, "OMOP_MAPPING_AMBIGUOUS_IND", "boolean"),
        F.lit(False),
    )
    v_amb = F.coalesce(
        optional_value(df, "OMOP_VALUE_MAPPING_AMBIGUOUS_IND", "boolean"),
        F.lit(False),
    )
    u_amb = F.coalesce(
        optional_value(df, "OMOP_UNIT_MAPPING_AMBIGUOUS_IND", "boolean"),
        F.lit(False),
    )
    deleted = F.coalesce(
        optional_value(df, "SOURCE_DELETED_IND", "boolean"), F.lit(False)
    )
    current = F.coalesce(
        optional_value(df, "SOURCE_CURRENT_IND", "boolean"), ~deleted
    )
    q_valid = (
        F.coalesce(
            optional_value(df, "OMOP_MAPPING_VALID_IND", "boolean"),
            q_concept.isNotNull(),
        )
        & ~q_amb
    )
    value_valid = (
        F.coalesce(
            optional_value(df, "OMOP_VALUE_MAPPING_VALID_IND", "boolean"),
            value_concept.isNotNull(),
        )
        & ~v_amb
    )
    unit_valid = (
        F.coalesce(
            optional_value(df, "OMOP_UNIT_MAPPING_VALID_IND", "boolean"),
            unit_concept.isNotNull(),
        )
        & ~u_amb
    )
    q_candidates = F.coalesce(
        optional_value(df, "OMOP_MAPPING_CANDIDATE_CONCEPT_IDS", "array<long>"),
        F.when(q_concept.isNotNull(), F.array(q_concept)).otherwise(
            empty_long_array()
        ),
    )
    value_candidates = F.coalesce(
        optional_value(
            df, "OMOP_VALUE_MAPPING_CANDIDATE_CONCEPT_IDS", "array<long>"
        ),
        F.when(value_concept.isNotNull(), F.array(value_concept)).otherwise(
            empty_long_array()
        ),
    )
    unit_candidates = F.coalesce(
        optional_value(
            df, "OMOP_UNIT_MAPPING_CANDIDATE_CONCEPT_IDS", "array<long>"
        ),
        F.when(unit_concept.isNotNull(), F.array(unit_concept)).otherwise(
            empty_long_array()
        ),
    )
    return df.select(
        F.col("EVENT_ID").cast("long").alias("_L_EVENT_ID"),
        F.lit(None).cast("long").alias("_L_SEQUENCE_NBR"),
        F.lit(None).cast("long").alias("_L_NOMENCLATURE_ID"),
        F.lit(None).cast("long").alias("_L_RESULT_CD"),
        F.lit(None).cast("double").alias("_L_NUMERIC_RESULT"),
        optional_col(df, "TEXT_RESULT_SHORT", "string", "_L_TEXT_SHORT"),
        F.col("TEXT_RESULT").cast("string").alias("_L_TEXT_RESULT"),
        optional_col(df, "RESULT_DT_TM", "timestamp", "_L_DATE_RESULT"),
        F.lit(None).cast("string").alias("_L_SOURCE_ROW_KEY"),
        optional_col(df, "ROW_HASH", "string", "_L_ROW_HASH"),
        optional_col(df, "PIPELINE_RUN_ID", "string", "_L_PIPELINE_RUN_ID"),
        current.cast("boolean").alias("_L_SOURCE_CURRENT_IND"),
        deleted.cast("boolean").alias("_L_SOURCE_DELETED_IND"),
        q_concept.alias("_L_Q_CONCEPT_RAW"),
        value_concept.alias("_L_VALUE_CONCEPT_RAW"),
        unit_concept.alias("_L_UNIT_CONCEPT_RAW"),
        optional_col(df, "OMOP_MAPPING_RULE_ID", "string", "_L_Q_RULE_ID"),
        optional_col(
            df, "OMOP_VALUE_MAPPING_RULE_ID", "string", "_L_VALUE_RULE_ID"
        ),
        optional_col(
            df, "OMOP_UNIT_MAPPING_RULE_ID", "string", "_L_UNIT_RULE_ID"
        ),
        q_valid.cast("boolean").alias("_L_Q_VALID_RAW"),
        value_valid.cast("boolean").alias("_L_VALUE_VALID_RAW"),
        unit_valid.cast("boolean").alias("_L_UNIT_VALID_RAW"),
        q_amb.cast("boolean").alias("_L_Q_AMBIGUOUS"),
        v_amb.cast("boolean").alias("_L_VALUE_AMBIGUOUS"),
        u_amb.cast("boolean").alias("_L_UNIT_AMBIGUOUS"),
        q_candidates.alias("_L_Q_CANDIDATES"),
        value_candidates.alias("_L_VALUE_CANDIDATES"),
        unit_candidates.alias("_L_UNIT_CANDIDATES"),
    )

def date_lane_projection(
    version: int,
    event_ids: Optional[DataFrame] = None,
    broadcast_ids: bool = False,
) -> DataFrame:
    df = _lane_base(SRC_DATE, version, event_ids, broadcast_ids)
    deleted = F.coalesce(
        optional_value(df, "SOURCE_DELETED_IND", "boolean"), F.lit(False)
    )
    current = F.coalesce(
        optional_value(df, "SOURCE_CURRENT_IND", "boolean"), ~deleted
    )
    return df.select(
        F.col("EVENT_ID").cast("long").alias("_L_EVENT_ID"),
        F.lit(None).cast("long").alias("_L_SEQUENCE_NBR"),
        F.lit(None).cast("long").alias("_L_NOMENCLATURE_ID"),
        F.lit(None).cast("long").alias("_L_RESULT_CD"),
        F.lit(None).cast("double").alias("_L_NUMERIC_RESULT"),
        F.lit(None).cast("string").alias("_L_TEXT_SHORT"),
        F.lit(None).cast("string").alias("_L_TEXT_RESULT"),
        F.col("RESULT_DT_TM").cast("timestamp").alias("_L_DATE_RESULT"),
        F.lit(None).cast("string").alias("_L_SOURCE_ROW_KEY"),
        optional_col(df, "ROW_HASH", "string", "_L_ROW_HASH"),
        optional_col(df, "PIPELINE_RUN_ID", "string", "_L_PIPELINE_RUN_ID"),
        current.cast("boolean").alias("_L_SOURCE_CURRENT_IND"),
        deleted.cast("boolean").alias("_L_SOURCE_DELETED_IND"),
        F.lit(None).cast("long").alias("_L_Q_CONCEPT_RAW"),
        F.lit(None).cast("long").alias("_L_VALUE_CONCEPT_RAW"),
        F.lit(None).cast("long").alias("_L_UNIT_CONCEPT_RAW"),
        F.lit(None).cast("string").alias("_L_Q_RULE_ID"),
        F.lit(None).cast("string").alias("_L_VALUE_RULE_ID"),
        F.lit(None).cast("string").alias("_L_UNIT_RULE_ID"),
        F.lit(False).alias("_L_Q_VALID_RAW"),
        F.lit(False).alias("_L_VALUE_VALID_RAW"),
        F.lit(False).alias("_L_UNIT_VALID_RAW"),
        F.lit(False).alias("_L_Q_AMBIGUOUS"),
        F.lit(False).alias("_L_VALUE_AMBIGUOUS"),
        F.lit(False).alias("_L_UNIT_AMBIGUOUS"),
        empty_long_array().alias("_L_Q_CANDIDATES"),
        empty_long_array().alias("_L_VALUE_CANDIDATES"),
        empty_long_array().alias("_L_UNIT_CANDIDATES"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Canonical lane joins and concept validation

# COMMAND ----------


def lane_projection(
    lane_name: str,
    version: int,
    event_ids: Optional[DataFrame] = None,
    broadcast_ids: bool = False,
) -> DataFrame:
    builders = {
        "NOMEN": nomen_lane_projection,
        "CODED": coded_lane_projection,
        "NUMERIC": numeric_lane_projection,
        "TEXT": text_lane_projection,
        "DATE": date_lane_projection,
    }
    return builders[lane_name](version, event_ids, broadcast_ids)


def concept_dimension(version: int) -> DataFrame:
    concept = read_snapshot(SRC_CONCEPT, version)
    columns = {name.lower(): name for name in concept.columns}

    def source_name(name: str) -> str:
        if name.lower() not in columns:
            raise ValueError(f"{SRC_CONCEPT} missing required column {name}")
        return columns[name.lower()]

    valid_from = F.col(source_name("valid_start_date")).cast("date")
    valid_to = F.col(source_name("valid_end_date")).cast("date")
    return concept.select(
        F.col(source_name("concept_id")).cast("long").alias("CONCEPT_ID"),
        F.col(source_name("standard_concept")).cast("string").alias(
            "STANDARD_CONCEPT"
        ),
        F.col(source_name("invalid_reason")).cast("string").alias(
            "INVALID_REASON"
        ),
        valid_from.alias("VALID_START_DATE"),
        valid_to.alias("VALID_END_DATE"),
        optional_col(
            concept, columns.get("domain_id", "__missing_domain_id"), "string", "DOMAIN_ID"
        ),
        (
            (F.col(source_name("standard_concept")) == "S")
            & F.col(source_name("invalid_reason")).isNull()
            & (valid_from <= F.current_date())
            & (valid_to >= F.current_date())
        ).alias("CONCEPT_VALID_STANDARD_IND"),
    ).dropDuplicates(["CONCEPT_ID"])


def _empty_lane_columns(source: DataFrame) -> DataFrame:
    result = source
    for name, data_type in LANE_OUTPUT_TYPES.items():
        result = result.withColumn(name, F.lit(None).cast(data_type))
    return result


def _join_lane_branch(
    source_rows: DataFrame,
    lane_name: str,
    lane: DataFrame,
) -> DataFrame:
    keys = ["_L_EVENT_ID"]
    condition = F.col("s.ELEMENT_EVENT_ID_LONG") == F.col("l._L_EVENT_ID")
    if lane_name in {"NOMEN", "CODED"}:
        keys.append("_L_SEQUENCE_NBR")
        condition = condition & (
            F.col("s.RESPONSE_SEQUENCE_NBR") == F.col("l._L_SEQUENCE_NBR")
        )
    assert_unique_non_null(lane, keys, f"{lane_name} lane projection")
    return (
        source_rows.alias("s")
        .join(lane.alias("l"), condition, "left")
        .select("s.*", "l.*")
    )


def enrich_with_canonical_lanes(
    classified: DataFrame,
    versions: Dict[str, int],
    staged_lanes: Optional[Dict[str, DataFrame]] = None,
) -> DataFrame:
    table_for_lane = {
        "NOMEN": SRC_NOMEN,
        "CODED": SRC_CODED,
        "NUMERIC": SRC_NUMERIC,
        "TEXT": SRC_TEXT,
        "DATE": SRC_DATE,
    }
    branches: List[DataFrame] = []
    source_columns = classified.columns
    for lane_name in ["NOMEN", "CODED", "NUMERIC", "TEXT", "DATE"]:
        source_rows = classified.where(F.col("CANONICAL_LANE") == lane_name)
        if staged_lanes and lane_name in staged_lanes:
            lane = staged_lanes[lane_name]
        else:
            event_ids = source_rows.select(
                F.col("ELEMENT_EVENT_ID_LONG").alias("EVENT_ID")
            )
            lane = lane_projection(
                lane_name,
                versions[table_for_lane[lane_name]],
                event_ids,
                broadcast_ids=True,
            )

        joined_branch = _join_lane_branch(source_rows, lane_name, lane)
        if lane_name != "TEXT":
            branches.append(joined_branch)
            continue

        # Some PowerForm string variants are parsed as numeric by the canonical
        # event pipeline. Prefer a real TEXT row when present; otherwise fall
        # back to NUMERIC provenance without changing RESPONSE_KIND.
        branches.append(joined_branch.where(F.col("_L_EVENT_ID").isNotNull()))
        text_not_found = joined_branch.where(
            F.col("_L_EVENT_ID").isNull()
        ).select(*source_columns)
        if staged_lanes and "NUMERIC" in staged_lanes:
            numeric_fallback_lane = staged_lanes["NUMERIC"]
        else:
            fallback_event_ids = text_not_found.select(
                F.col("ELEMENT_EVENT_ID_LONG").alias("EVENT_ID")
            )
            numeric_fallback_lane = lane_projection(
                "NUMERIC",
                versions[SRC_NUMERIC],
                fallback_event_ids,
                broadcast_ids=True,
            )
        fallback_joined = _join_lane_branch(
            text_not_found,
            "NUMERIC",
            numeric_fallback_lane,
        ).withColumn(
            "CANONICAL_LANE",
            F.when(F.col("_L_EVENT_ID").isNotNull(), F.lit("NUMERIC")).otherwise(
                F.lit("TEXT")
            ),
        )
        branches.append(fallback_joined)

    branches.append(
        _empty_lane_columns(
            classified.where(F.col("CANONICAL_LANE") == "NONE")
        )
    )
    joined = reduce(lambda left, right: left.unionByName(right), branches)

    numeric_tolerance = F.greatest(
        F.lit(1.0e-12),
        F.abs(F.col("NUMERIC_RESPONSE_NBR")) * F.lit(1.0e-9),
    )
    concordance = (
        F.when(F.col("CANONICAL_LANE") == "NONE", F.lit(None).cast("boolean"))
        .when(F.col("_L_EVENT_ID").isNull(), F.lit(None).cast("boolean"))
        .when(
            F.col("RESPONSE_KIND") == "CODED_NOMENCLATURE",
            F.col("RESPONSE_NOMENCLATURE_ID") == F.col("_L_NOMENCLATURE_ID"),
        )
        .when(
            F.col("RESPONSE_KIND") == "CODED_CODE_VALUE",
            F.col("RESPONSE_CODE_VALUE_ID") == F.col("_L_RESULT_CD"),
        )
        .when(
            F.col("RESPONSE_KIND") == "CODED_OTHER",
            F.lit(None).cast("boolean"),
        )
        .when(
            F.col("RESPONSE_KIND") == "NUMERIC",
            F.col("_L_NUMERIC_RESULT").isNotNull()
            & (
                F.abs(
                    F.col("NUMERIC_RESPONSE_NBR") - F.col("_L_NUMERIC_RESULT")
                )
                <= numeric_tolerance
            ),
        )
        .when(
            (F.col("RESPONSE_KIND") == "TEXT")
            & (F.col("CANONICAL_LANE") == "NUMERIC"),
            F.lit(None).cast("boolean"),
        )
        .when(
            F.col("RESPONSE_KIND") == "TEXT",
            clean_response_text(F.col("RESPONSE_TEXT_TXT"))
            == clean_response_text(F.col("_L_TEXT_SHORT")),
        )
        .when(
            F.col("RESPONSE_KIND") == "DATE",
            F.col("RESPONSE_DT_TM") == F.col("_L_DATE_RESULT"),
        )
        .otherwise(F.lit(None).cast("boolean"))
    )
    any_ambiguity = (
        F.coalesce(F.col("_L_Q_AMBIGUOUS"), F.lit(False))
        | F.coalesce(F.col("_L_VALUE_AMBIGUOUS"), F.lit(False))
        | F.coalesce(F.col("_L_UNIT_AMBIGUOUS"), F.lit(False))
    )
    match_status = (
        F.when(F.col("CANONICAL_LANE") == "NONE", F.lit("NOT_APPLICABLE"))
        .when(F.col("_L_EVENT_ID").isNull(), F.lit("NOT_FOUND"))
        .when(
            F.coalesce(F.col("_L_SOURCE_DELETED_IND"), F.lit(False))
            | ~F.coalesce(F.col("_L_SOURCE_CURRENT_IND"), F.lit(True)),
            F.lit("SOURCE_DELETED"),
        )
        .when(any_ambiguity, F.lit("AMBIGUOUS"))
        .when(concordance == F.lit(False), F.lit("VALUE_MISMATCH"))
        .otherwise(F.lit("MATCHED"))
    )
    enriched = (
        joined.withColumn("CANONICAL_VALUE_CONCORDANCE_IND", concordance)
        .withColumn("CANONICAL_MATCH_STATUS", match_status)
        .withColumn("CANONICAL_SOURCE_ROW_KEY", F.col("_L_SOURCE_ROW_KEY"))
        .withColumn("CANONICAL_ROW_HASH", F.col("_L_ROW_HASH").cast("string"))
        .withColumn("CANONICAL_PIPELINE_RUN_ID", F.col("_L_PIPELINE_RUN_ID"))
        .withColumn(
            "CANONICAL_SOURCE_CURRENT_IND", F.col("_L_SOURCE_CURRENT_IND")
        )
        .withColumn(
            "CANONICAL_SOURCE_DELETED_IND", F.col("_L_SOURCE_DELETED_IND")
        )
        .withColumn("CANONICAL_NUMERIC_RESULT", F.col("_L_NUMERIC_RESULT"))
        .withColumn("CANONICAL_TEXT_RESULT", F.col("_L_TEXT_RESULT"))
        .withColumn("CANONICAL_DATE_RESULT", F.col("_L_DATE_RESULT"))
        .withColumn(
            "CANONICAL_QUESTION_MAPPING_RULE_ID", F.col("_L_Q_RULE_ID")
        )
        .withColumn(
            "CANONICAL_VALUE_MAPPING_RULE_ID", F.col("_L_VALUE_RULE_ID")
        )
        .withColumn(
            "CANONICAL_UNIT_MAPPING_RULE_ID", F.col("_L_UNIT_RULE_ID")
        )
        .withColumn(
            "CANONICAL_QUESTION_MAPPING_AMBIGUOUS_IND",
            F.coalesce(F.col("_L_Q_AMBIGUOUS"), F.lit(False)),
        )
        .withColumn(
            "CANONICAL_VALUE_MAPPING_AMBIGUOUS_IND",
            F.coalesce(F.col("_L_VALUE_AMBIGUOUS"), F.lit(False)),
        )
        .withColumn(
            "CANONICAL_UNIT_MAPPING_AMBIGUOUS_IND",
            F.coalesce(F.col("_L_UNIT_AMBIGUOUS"), F.lit(False)),
        )
        .withColumn(
            "CANONICAL_QUESTION_CANDIDATE_CONCEPT_IDS",
            F.coalesce(F.col("_L_Q_CANDIDATES"), empty_long_array()),
        )
        .withColumn(
            "CANONICAL_VALUE_CANDIDATE_CONCEPT_IDS",
            F.coalesce(F.col("_L_VALUE_CANDIDATES"), empty_long_array()),
        )
        .withColumn(
            "CANONICAL_UNIT_CANDIDATE_CONCEPT_IDS",
            F.coalesce(F.col("_L_UNIT_CANDIDATES"), empty_long_array()),
        )
    )

    concepts = concept_dimension(versions[SRC_CONCEPT])
    for label, raw_column, raw_valid_column in [
        ("QUESTION", "_L_Q_CONCEPT_RAW", "_L_Q_VALID_RAW"),
        ("VALUE", "_L_VALUE_CONCEPT_RAW", "_L_VALUE_VALID_RAW"),
        ("UNIT", "_L_UNIT_CONCEPT_RAW", "_L_UNIT_VALID_RAW"),
    ]:
        alias = f"concept_{label.lower()}"
        enriched = (
            enriched.alias("base")
            .join(
                F.broadcast(concepts).alias(alias),
                F.col(f"base.{raw_column}") == F.col(f"{alias}.CONCEPT_ID"),
                "left",
            )
            .select(
                "base.*",
                F.coalesce(
                    F.col(f"{alias}.CONCEPT_VALID_STANDARD_IND"), F.lit(False)
                ).alias(f"_L_{label}_CONCEPT_VALID"),
            )
        )
        valid = (
            F.coalesce(F.col(raw_valid_column), F.lit(False))
            & F.col(f"_L_{label}_CONCEPT_VALID")
            & ~F.coalesce(
                F.col(f"_L_{'Q' if label == 'QUESTION' else label}_AMBIGUOUS"),
                F.lit(False),
            )
        )
        enriched = (
            enriched.withColumn(
                f"CANONICAL_{label}_MAPPING_VALID_IND", valid
            )
            .withColumn(
                f"CANONICAL_{label}_CONCEPT_ID",
                F.when(valid, F.col(raw_column)).otherwise(
                    F.lit(None).cast("long")
                ),
            )
        )
    return enriched


# COMMAND ----------

# MAGIC %md
# MAGIC ## Governed PowerForm mappings

# COMMAND ----------


MAP_SELECTOR_COLUMNS = [
    "FORM_REF_ID",
    "DCP_INPUT_REF_ID",
    "SECTION_REF_ID",
    "TASK_ASSAY_ID",
    "ELEMENT_EVENT_CD",
    "GRID_EVENT_CD",
    "INPUT_TYPE_FLG",
]
MAP_CURATOR_COLUMNS = [
    *MAP_SELECTOR_COLUMNS,
    "ANSWER_KIND",
    "ANSWER_NOMENCLATURE_ID",
    "ANSWER_CODE_VALUE_ID",
    "ANSWER_TEXT_NORMALIZED",
    "QUESTION_CONCEPT_ID",
    "VALUE_CONCEPT_ID",
    "UNIT_CONCEPT_ID",
    "TARGET_OMOP_TABLE",
    "TARGET_OMOP_FIELD",
    "SOURCE_PRESENT_IND",
    "MAPPING_METHOD",
    "CURATED_STATUS",
    "CURATED_BY",
    "CURATED_AT",
    "CURATED_NOTES",
    "MAPPING_VERSION",
    "VALID_FROM_DT",
    "VALID_TO_DT",
]


def mapping_rule_id_expr(prefix: str = ""):
    def c(name: str):
        return F.col(f"{prefix}{name}")

    fields = [
        *MAP_SELECTOR_COLUMNS,
        "ANSWER_KIND",
        "ANSWER_NOMENCLATURE_ID",
        "ANSWER_CODE_VALUE_ID",
        "ANSWER_TEXT_NORMALIZED",
    ]
    encoded = [
        F.concat(
            F.lit(f"{name}="),
            F.coalesce(c(name).cast("string"), F.lit("<NULL>")),
        )
        for name in fields
    ]
    return F.sha2(F.concat_ws(chr(31), *encoded), 256)


def map_validation_metrics(
    map_snapshot: DataFrame, concept_version: int
) -> dict:
    concepts = concept_dimension(concept_version).select(
        "CONCEPT_ID", "CONCEPT_VALID_STANDARD_IND"
    )
    approved = map_snapshot.where(
        F.col("SOURCE_PRESENT_IND")
        & (F.col("CURATED_STATUS") == "APPROVED")
        & (
            F.col("VALID_FROM_DT").isNull()
            | (F.col("VALID_FROM_DT") <= F.current_date())
        )
        & (
            F.col("VALID_TO_DT").isNull()
            | (F.col("VALID_TO_DT") >= F.current_date())
        )
    )
    invalid_concept_condition = F.lit(False)
    checked = approved
    for label, column_name in [
        ("q", "QUESTION_CONCEPT_ID"),
        ("v", "VALUE_CONCEPT_ID"),
        ("u", "UNIT_CONCEPT_ID"),
    ]:
        alias = f"cv_{label}"
        checked = (
            checked.alias("m")
            .join(
                F.broadcast(concepts).alias(alias),
                F.col(f"m.{column_name}") == F.col(f"{alias}.CONCEPT_ID"),
                "left",
            )
            .select(
                "m.*",
                F.col(f"{alias}.CONCEPT_VALID_STANDARD_IND").alias(
                    f"_{label}_valid"
                ),
            )
        )
        invalid_concept_condition = invalid_concept_condition | (
            F.col(column_name).isNotNull()
            & ~F.coalesce(F.col(f"_{label}_valid"), F.lit(False))
        )

    selector_present = (
        F.col("DCP_INPUT_REF_ID").isNotNull()
        | (
            F.col("FORM_REF_ID").isNotNull()
            & F.col("TASK_ASSAY_ID").isNotNull()
            & F.col("ELEMENT_EVENT_CD").isNotNull()
        )
        | (
            F.col("TASK_ASSAY_ID").isNotNull()
            & F.col("ELEMENT_EVENT_CD").isNotNull()
        )
    )
    answer_valid = (
        ((F.col("ANSWER_KIND") == "ANY")
         & F.col("ANSWER_NOMENCLATURE_ID").isNull()
         & F.col("ANSWER_CODE_VALUE_ID").isNull()
         & F.col("ANSWER_TEXT_NORMALIZED").isNull())
        | ((F.col("ANSWER_KIND") == "NOMENCLATURE")
           & F.col("ANSWER_NOMENCLATURE_ID").isNotNull()
           & F.col("ANSWER_CODE_VALUE_ID").isNull()
           & F.col("ANSWER_TEXT_NORMALIZED").isNull())
        | ((F.col("ANSWER_KIND") == "CODE_VALUE")
           & F.col("ANSWER_NOMENCLATURE_ID").isNull()
           & F.col("ANSWER_CODE_VALUE_ID").isNotNull()
           & F.col("ANSWER_TEXT_NORMALIZED").isNull())
        | ((F.col("ANSWER_KIND") == "TEXT_EXACT")
           & F.col("ANSWER_NOMENCLATURE_ID").isNull()
           & F.col("ANSWER_CODE_VALUE_ID").isNull()
           & F.col("ANSWER_TEXT_NORMALIZED").isNotNull())
    )
    metrics = (
        checked.agg(
            F.count(F.lit(1)).alias("approved_rows"),
            F.sum(F.when(~F.coalesce(selector_present, F.lit(False)), 1).otherwise(0)).alias(
                "missing_selector_rows"
            ),
            F.sum(F.when(~F.coalesce(answer_valid, F.lit(False)), 1).otherwise(0)).alias(
                "invalid_answer_selector_rows"
            ),
            F.sum(F.when(invalid_concept_condition, 1).otherwise(0)).alias(
                "invalid_concept_rows"
            ),
            F.sum(
                F.when(
                    F.col("QUESTION_CONCEPT_ID").isNull(),
                    1,
                ).otherwise(0)
            ).alias("approved_rule_without_question_rows"),
        )
        .first()
        .asDict()
    )
    duplicate_ids = (
        map_snapshot.groupBy("MAPPING_RULE_ID")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    bad_status = map_snapshot.where(
        ~F.coalesce(
            F.col("CURATED_STATUS").isin(
                "PROPOSED", "APPROVED", "REJECTED", "RETIRED"
            ),
            F.lit(False),
        )
        | ~F.coalesce(
            F.col("ANSWER_KIND").isin(
                "ANY", "NOMENCLATURE", "CODE_VALUE", "TEXT_EXACT"
            ),
            F.lit(False),
        )
    ).count()
    rule_id_mismatches = map_snapshot.where(
        ~F.col("MAPPING_RULE_ID").eqNullSafe(mapping_rule_id_expr())
    ).count()
    text_normalization_mismatches = map_snapshot.where(
        (F.col("ANSWER_KIND") == "TEXT_EXACT")
        & ~F.col("ANSWER_TEXT_NORMALIZED").eqNullSafe(
            normalized_answer_text(F.col("ANSWER_TEXT_NORMALIZED"))
        )
    ).count()
    result = {key: int(value or 0) for key, value in metrics.items()}
    result.update(
        {
            "duplicate_rule_ids": int(duplicate_ids),
            "invalid_enum_rows": int(bad_status),
            "mapping_rule_id_mismatch_rows": int(rule_id_mismatches),
            "text_normalization_mismatch_rows": int(
                text_normalization_mismatches
            ),
        }
    )
    return result


def approved_mapping_rules(
    map_version: int, concept_version: int
) -> Tuple[DataFrame, dict]:
    snapshot = read_snapshot(TGT_MAP, map_version)
    metrics = map_validation_metrics(snapshot, concept_version)
    failures = {
        key: value
        for key, value in metrics.items()
        if key != "approved_rows" and value
    }
    if failures:
        raise RuntimeError(f"powerform_item_map validation failed: {failures}")
    approved = snapshot.where(
        F.col("SOURCE_PRESENT_IND")
        & (F.col("CURATED_STATUS") == "APPROVED")
        & (
            F.col("VALID_FROM_DT").isNull()
            | (F.col("VALID_FROM_DT") <= F.current_date())
        )
        & (
            F.col("VALID_TO_DT").isNull()
            | (F.col("VALID_TO_DT") >= F.current_date())
        )
    )
    answer_key = (
        F.when(
            F.col("ANSWER_KIND") == "NOMENCLATURE",
            F.concat(
                F.lit("NOMENCLATURE|"),
                F.col("ANSWER_NOMENCLATURE_ID").cast("string"),
            ),
        )
        .when(
            F.col("ANSWER_KIND") == "CODE_VALUE",
            F.concat(
                F.lit("CODE_VALUE|"),
                F.col("ANSWER_CODE_VALUE_ID").cast("string"),
            ),
        )
        .when(
            F.col("ANSWER_KIND") == "TEXT_EXACT",
            F.concat(F.lit("TEXT_EXACT|"), F.col("ANSWER_TEXT_NORMALIZED")),
        )
    )
    specificity = reduce(
        lambda left, right: left + right,
        [F.col(name).isNotNull().cast("int") for name in MAP_SELECTOR_COLUMNS],
    ) + (F.col("ANSWER_KIND") != "ANY").cast("int")
    return approved.select(
        F.col("MAPPING_RULE_ID").alias("_PF_RULE_ID"),
        *[
            F.col(name).alias(f"_M_{name}")
            for name in MAP_SELECTOR_COLUMNS
        ],
        F.col("ANSWER_KIND").alias("_M_ANSWER_KIND"),
        answer_key.alias("_M_ANSWER_KEY"),
        F.col("QUESTION_CONCEPT_ID").cast("long").alias("_PF_Q_CONCEPT"),
        F.col("VALUE_CONCEPT_ID").cast("long").alias("_PF_V_CONCEPT"),
        F.col("UNIT_CONCEPT_ID").cast("long").alias("_PF_U_CONCEPT"),
        F.col("MAPPING_VERSION").alias("_PF_MAPPING_VERSION"),
        specificity.alias("_PF_SPECIFICITY"),
    ), metrics


def _source_answer_key():
    return (
        F.when(
            F.col("RESPONSE_KIND") == "CODED_NOMENCLATURE",
            F.concat(
                F.lit("NOMENCLATURE|"),
                F.col("RESPONSE_NOMENCLATURE_ID").cast("string"),
            ),
        )
        .when(
            F.col("RESPONSE_KIND") == "CODED_CODE_VALUE",
            F.concat(
                F.lit("CODE_VALUE|"),
                F.col("RESPONSE_CODE_VALUE_ID").cast("string"),
            ),
        )
        .when(
            F.col("RESPONSE_KIND") == "TEXT",
            F.concat(
                F.lit("TEXT_EXACT|"),
                normalized_answer_text(
                    F.coalesce(
                        F.col("STRING_RESPONSE_TXT"), F.col("RESPONSE_VALUE_TXT")
                    )
                ),
            ),
        )
    )


def _mapping_branch(
    items: DataFrame,
    rules: DataFrame,
    scope: str,
    exact_answer: bool,
    precedence: int,
) -> DataFrame:
    source = items.alias("s")
    mapped = rules.where(
        F.col("_M_ANSWER_KIND") != "ANY"
        if exact_answer
        else F.col("_M_ANSWER_KIND") == "ANY"
    ).alias("m")
    if scope == "INPUT":
        mapped = mapped.where(F.col("_M_DCP_INPUT_REF_ID").isNotNull()).alias("m")
        join_condition = F.col("s.DCP_INPUT_REF_ID") == F.col(
            "m._M_DCP_INPUT_REF_ID"
        )
        mandatory = {"DCP_INPUT_REF_ID"}
    elif scope == "FORM":
        mapped = mapped.where(
            F.col("_M_DCP_INPUT_REF_ID").isNull()
            & F.col("_M_FORM_REF_ID").isNotNull()
            & F.col("_M_TASK_ASSAY_ID").isNotNull()
            & F.col("_M_ELEMENT_EVENT_CD").isNotNull()
        ).alias("m")
        join_condition = (
            (F.col("s.FORM_REF_ID") == F.col("m._M_FORM_REF_ID"))
            & (F.col("s.TASK_ASSAY_ID") == F.col("m._M_TASK_ASSAY_ID"))
            & (
                F.col("s.ELEMENT_EVENT_CD")
                == F.col("m._M_ELEMENT_EVENT_CD")
            )
        )
        mandatory = {"FORM_REF_ID", "TASK_ASSAY_ID", "ELEMENT_EVENT_CD"}
    else:
        mapped = mapped.where(
            F.col("_M_DCP_INPUT_REF_ID").isNull()
            & F.col("_M_FORM_REF_ID").isNull()
            & F.col("_M_TASK_ASSAY_ID").isNotNull()
            & F.col("_M_ELEMENT_EVENT_CD").isNotNull()
        ).alias("m")
        join_condition = (
            (F.col("s.TASK_ASSAY_ID") == F.col("m._M_TASK_ASSAY_ID"))
            & (
                F.col("s.ELEMENT_EVENT_CD")
                == F.col("m._M_ELEMENT_EVENT_CD")
            )
        )
        mandatory = {"TASK_ASSAY_ID", "ELEMENT_EVENT_CD"}
    if exact_answer:
        join_condition = join_condition & (
            F.col("s._PF_SOURCE_ANSWER_KEY") == F.col("m._M_ANSWER_KEY")
        )

    joined = source.join(F.broadcast(mapped), join_condition, "inner")
    optional_conditions = [
        F.col(f"m._M_{name}").isNull()
        | (F.col(f"s.{name}") == F.col(f"m._M_{name}"))
        for name in MAP_SELECTOR_COLUMNS
        if name not in mandatory
    ]
    if optional_conditions:
        joined = joined.where(
            reduce(lambda left, right: left & right, optional_conditions)
        )
    return joined.select(
        F.col("s.DOC_RESPONSE_KEY"),
        F.col("m._PF_RULE_ID"),
        F.col("m._PF_Q_CONCEPT"),
        F.col("m._PF_V_CONCEPT"),
        F.col("m._PF_U_CONCEPT"),
        F.col("m._PF_MAPPING_VERSION"),
        F.col("m._PF_SPECIFICITY"),
        F.lit(precedence).alias("_PF_PRECEDENCE"),
        F.lit(f"{scope}_{'EXACT' if exact_answer else 'ANY'}").alias(
            "_PF_MATCH_LEVEL"
        ),
    )


def apply_contextual_mappings(
    enriched: DataFrame, versions: Dict[str, int]
) -> Tuple[DataFrame, dict]:
    rules, map_metrics = approved_mapping_rules(
        versions[TGT_MAP], versions[SRC_CONCEPT]
    )
    items = enriched.withColumn("_PF_SOURCE_ANSWER_KEY", _source_answer_key())
    branches = [
        _mapping_branch(items, rules, "INPUT", True, 1),
        _mapping_branch(items, rules, "FORM", True, 2),
        _mapping_branch(items, rules, "GLOBAL", True, 3),
        _mapping_branch(items, rules, "INPUT", False, 4),
        _mapping_branch(items, rules, "FORM", False, 5),
        _mapping_branch(items, rules, "GLOBAL", False, 6),
    ]
    matches = reduce(lambda left, right: left.unionByName(right), branches)
    precedence_window = Window.partitionBy("DOC_RESPONSE_KEY")
    finalists = (
        matches.withColumn(
            "_MIN_PRECEDENCE", F.min("_PF_PRECEDENCE").over(precedence_window)
        )
        .where(F.col("_PF_PRECEDENCE") == F.col("_MIN_PRECEDENCE"))
        .withColumn(
            "_MAX_SPECIFICITY", F.max("_PF_SPECIFICITY").over(precedence_window)
        )
        .where(F.col("_PF_SPECIFICITY") == F.col("_MAX_SPECIFICITY"))
    )
    signature = F.concat_ws(
        "|",
        F.coalesce(F.col("_PF_Q_CONCEPT").cast("string"), F.lit("<NULL>")),
        F.coalesce(F.col("_PF_V_CONCEPT").cast("string"), F.lit("<NULL>")),
        F.coalesce(F.col("_PF_U_CONCEPT").cast("string"), F.lit("<NULL>")),
    )
    winners = finalists.groupBy("DOC_RESPONSE_KEY").agg(
        F.sort_array(F.collect_set("_PF_RULE_ID")).alias("_PF_RULE_IDS"),
        F.collect_set(signature).alias("_PF_CONCEPT_SIGNATURES"),
        F.first("_PF_Q_CONCEPT", ignorenulls=False).alias("_PF_Q_WIN"),
        F.first("_PF_V_CONCEPT", ignorenulls=False).alias("_PF_V_WIN"),
        F.first("_PF_U_CONCEPT", ignorenulls=False).alias("_PF_U_WIN"),
        F.first("_PF_MAPPING_VERSION", ignorenulls=False).alias(
            "_PF_VERSION_WIN"
        ),
        F.first("_PF_MATCH_LEVEL", ignorenulls=False).alias("_PF_LEVEL_WIN"),
        F.max("_PF_SPECIFICITY").alias("_PF_SPECIFICITY_WIN"),
    )
    winners = (
        winners.withColumn(
            "POWERFORM_MAPPING_AMBIGUOUS_IND",
            F.size("_PF_CONCEPT_SIGNATURES") > 1,
        )
        .withColumn("POWERFORM_MATCHED_RULE_COUNT", F.size("_PF_RULE_IDS"))
        .withColumn(
            "POWERFORM_MAPPING_RULE_ID", F.element_at("_PF_RULE_IDS", 1)
        )
    )
    output = items.join(winners, "DOC_RESPONSE_KEY", "left")
    output = (
        output.withColumn(
            "POWERFORM_MAPPING_AMBIGUOUS_IND",
            F.coalesce(F.col("POWERFORM_MAPPING_AMBIGUOUS_IND"), F.lit(False)),
        )
        .withColumn("POWERFORM_MAPPING_MATCH_LEVEL", F.col("_PF_LEVEL_WIN"))
        .withColumn("POWERFORM_MAPPING_SPECIFICITY", F.col("_PF_SPECIFICITY_WIN"))
        .withColumn("POWERFORM_MAPPING_VERSION", F.col("_PF_VERSION_WIN"))
        .withColumn(
            "POWERFORM_QUESTION_CONCEPT_ID",
            F.when(
                ~F.col("POWERFORM_MAPPING_AMBIGUOUS_IND"), F.col("_PF_Q_WIN")
            ).cast("long"),
        )
        .withColumn(
            "POWERFORM_VALUE_CONCEPT_ID",
            F.when(
                ~F.col("POWERFORM_MAPPING_AMBIGUOUS_IND"), F.col("_PF_V_WIN")
            ).cast("long"),
        )
        .withColumn(
            "POWERFORM_UNIT_CONCEPT_ID",
            F.when(
                ~F.col("POWERFORM_MAPPING_AMBIGUOUS_IND"), F.col("_PF_U_WIN")
            ).cast("long"),
        )
    )
    for label in ["QUESTION", "VALUE", "UNIT"]:
        pf = F.col(f"POWERFORM_{label}_CONCEPT_ID")
        canonical = F.col(f"CANONICAL_{label}_CONCEPT_ID")
        output = (
            output.withColumn(
                f"{label}_CONCEPT_ID", F.coalesce(pf, canonical).cast("long")
            )
            .withColumn(
                f"{label}_MAPPING_SOURCE",
                F.when(pf.isNotNull(), F.lit("POWERFORM"))
                .when(canonical.isNotNull(), F.lit("CANONICAL"))
                .otherwise(F.lit("UNMAPPED")),
            )
            .withColumn(
                f"{label}_MAPPING_CONFLICT_IND",
                pf.isNotNull() & canonical.isNotNull() & (pf != canonical),
            )
        )
    ambiguity_count = output.where(
        F.col("POWERFORM_MAPPING_AMBIGUOUS_IND")
    ).count()
    map_metrics["item_mapping_ambiguity_rows"] = int(ambiguity_count)
    return output, map_metrics


# COMMAND ----------

# MAGIC %md
# MAGIC ## Explicit target contracts and Delta writers

# COMMAND ----------


SOURCE_TARGET_COLUMN_TYPES = [
    (
        {
            "PIPELINE_RUN_ID": "SOURCE_POWERFORM_RUN_ID",
            "PIPELINE_PROCESSED_DT_TM": "SOURCE_POWERFORM_PROCESSED_DT_TM",
        }.get(name, name),
        data_type,
    )
    for name, data_type in SOURCE_COLUMN_TYPES
]

ITEM_EXTRA_COLUMN_TYPES: List[Tuple[str, str]] = [
    ("RESPONSE_VARIANT_SUFFIX", "string"),
    ("RESPONSE_HAS_BOTH_CODE_TYPES_IND", "boolean"),
    ("RESPONSE_HAS_BOTH_NUMERIC_AND_TEXT_IND", "boolean"),
    ("RESPONSE_TEXT_TXT", "string"),
    ("RESPONSE_KIND", "string"),
    ("RESPONSE_CLASSIFICATION_VALID_IND", "boolean"),
    ("CANONICAL_LANE", "string"),
    ("CANONICAL_MATCH_STATUS", "string"),
    ("CANONICAL_SOURCE_ROW_KEY", "string"),
    ("CANONICAL_ROW_HASH", "string"),
    ("CANONICAL_PIPELINE_RUN_ID", "string"),
    ("CANONICAL_SOURCE_CURRENT_IND", "boolean"),
    ("CANONICAL_SOURCE_DELETED_IND", "boolean"),
    ("CANONICAL_VALUE_CONCORDANCE_IND", "boolean"),
    ("CANONICAL_NUMERIC_RESULT", "double"),
    ("CANONICAL_TEXT_RESULT", "string"),
    ("CANONICAL_DATE_RESULT", "timestamp"),
    ("CANONICAL_QUESTION_CONCEPT_ID", "long"),
    ("CANONICAL_VALUE_CONCEPT_ID", "long"),
    ("CANONICAL_UNIT_CONCEPT_ID", "long"),
    ("CANONICAL_QUESTION_MAPPING_RULE_ID", "string"),
    ("CANONICAL_VALUE_MAPPING_RULE_ID", "string"),
    ("CANONICAL_UNIT_MAPPING_RULE_ID", "string"),
    ("CANONICAL_QUESTION_MAPPING_VALID_IND", "boolean"),
    ("CANONICAL_VALUE_MAPPING_VALID_IND", "boolean"),
    ("CANONICAL_UNIT_MAPPING_VALID_IND", "boolean"),
    ("CANONICAL_QUESTION_MAPPING_AMBIGUOUS_IND", "boolean"),
    ("CANONICAL_VALUE_MAPPING_AMBIGUOUS_IND", "boolean"),
    ("CANONICAL_UNIT_MAPPING_AMBIGUOUS_IND", "boolean"),
    ("CANONICAL_QUESTION_CANDIDATE_CONCEPT_IDS", "array<long>"),
    ("CANONICAL_VALUE_CANDIDATE_CONCEPT_IDS", "array<long>"),
    ("CANONICAL_UNIT_CANDIDATE_CONCEPT_IDS", "array<long>"),
    ("POWERFORM_MAPPING_RULE_ID", "string"),
    ("POWERFORM_MAPPING_MATCH_LEVEL", "string"),
    ("POWERFORM_MAPPING_SPECIFICITY", "int"),
    ("POWERFORM_MAPPING_VERSION", "string"),
    ("POWERFORM_QUESTION_CONCEPT_ID", "long"),
    ("POWERFORM_VALUE_CONCEPT_ID", "long"),
    ("POWERFORM_UNIT_CONCEPT_ID", "long"),
    ("POWERFORM_MAPPING_AMBIGUOUS_IND", "boolean"),
    ("POWERFORM_MATCHED_RULE_COUNT", "int"),
    ("QUESTION_CONCEPT_ID", "long"),
    ("VALUE_CONCEPT_ID", "long"),
    ("UNIT_CONCEPT_ID", "long"),
    ("QUESTION_MAPPING_SOURCE", "string"),
    ("VALUE_MAPPING_SOURCE", "string"),
    ("UNIT_MAPPING_SOURCE", "string"),
    ("QUESTION_MAPPING_CONFLICT_IND", "boolean"),
    ("VALUE_MAPPING_CONFLICT_IND", "boolean"),
    ("UNIT_MAPPING_CONFLICT_IND", "boolean"),
    ("SOURCE_PRESENT_IND", "boolean"),
    ("TRIGGER_SOURCES", "string"),
    ("ROW_HASH", "string"),
    ("PIPELINE_RUN_ID", "string"),
    ("PIPELINE_UPDT_DT_TM", "timestamp"),
]

ASSESSMENT_COLUMN_TYPES: List[Tuple[str, str]] = [
    ("DCP_FORMS_ACTIVITY_ID", "long"),
    ("PERSON_ID_LONG", "long"),
    ("ENCNTR_ID_LONG", "long"),
    ("ORGANIZATION_ID", "long"),
    ("TRUST", "string"),
    ("FORM_REF_ID", "long"),
    ("FORM_INSTANCE_ID", "long"),
    ("FORM_EVENT_ID", "string"),
    ("FORM_STATUS_CD_LONG", "long"),
    ("FORM_STATUS_CD", "string"),
    ("FORM_DESC_TXT", "string"),
    ("DOCUMENTATION_DT_TM", "timestamp"),
    ("FIRST_DOCUMENTED_DT_TM", "timestamp"),
    ("LAST_DOCUMENTED_DT_TM", "timestamp"),
    ("ASSESSMENT_ACTIVE_IND", "boolean"),
    ("RESPONSE_ROW_COUNT", "long"),
    ("ACTIVE_RESPONSE_ROW_COUNT", "long"),
    ("CODED_NOMENCLATURE_ROW_COUNT", "long"),
    ("CODED_CODE_VALUE_ROW_COUNT", "long"),
    ("CODED_OTHER_ROW_COUNT", "long"),
    ("NUMERIC_ROW_COUNT", "long"),
    ("TEXT_ROW_COUNT", "long"),
    ("DATE_ROW_COUNT", "long"),
    ("EMPTY_ROW_COUNT", "long"),
    ("INVALID_ROW_COUNT", "long"),
    ("CANONICAL_MATCHED_ROW_COUNT", "long"),
    ("CANONICAL_UNMATCHED_ROW_COUNT", "long"),
    ("QUESTION_MAPPED_ROW_COUNT", "long"),
    ("VALUE_MAPPED_ROW_COUNT", "long"),
    ("MAPPING_CONFLICT_ROW_COUNT", "long"),
    ("FIRST_RESPONSE_DT_TM", "timestamp"),
    ("LAST_RESPONSE_DT_TM", "timestamp"),
    ("CONTEXT_CONFLICT_IND", "boolean"),
    ("CONTEXT_CONFLICT_FIELDS", "array<string>"),
    ("CONTEXT_QUARANTINED_IND", "boolean"),
    ("ADC_UPDT", "timestamp"),
    ("SOURCE_PRESENT_IND", "boolean"),
    ("ROW_HASH", "string"),
    ("PIPELINE_RUN_ID", "string"),
    ("PIPELINE_UPDT_DT_TM", "timestamp"),
]


def schema_from_columns(columns: Sequence[Tuple[str, str]]) -> T.StructType:
    primitives = {
        "string": T.StringType(),
        "int": T.IntegerType(),
        "long": T.LongType(),
        "double": T.DoubleType(),
        "boolean": T.BooleanType(),
        "timestamp": T.TimestampType(),
        "date": T.DateType(),
        "array<long>": T.ArrayType(T.LongType()),
        "array<string>": T.ArrayType(T.StringType()),
    }
    return T.StructType(
        [T.StructField(name, primitives[data_type], True) for name, data_type in columns]
    )


ITEM_SCHEMA = schema_from_columns(SOURCE_TARGET_COLUMN_TYPES + ITEM_EXTRA_COLUMN_TYPES)
ASSESSMENT_SCHEMA = schema_from_columns(ASSESSMENT_COLUMN_TYPES)


def ensure_delta_table(
    table_name: str, schema: T.StructType, comment: str
) -> None:
    if not table_exists(table_name):
        (
            spark.createDataFrame([], schema)
            .write.format("delta")
            .mode("error")
            .saveAsTable(table_name)
        )
    ensure_table_properties(
        table_name,
        {
            "delta.enableChangeDataFeed": "true",
            "delta.deletedFileRetentionDuration": "interval 30 days",
            "delta.logRetentionDuration": "interval 30 days",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
            "delta.targetFileSize": "268435456",
        },
    )
    try:
        ensure_table_properties(table_name, {"delta.enableRowTracking": "true"})
    except Exception as exc:
        print(f"Row tracking not enabled for {table_name}: {exc}")
    set_table_comment(table_name, comment)


def ensure_user_tables() -> None:
    ensure_delta_table(
        TGT_ITEM,
        ITEM_SCHEMA,
        "One row per PowerForm DOC_RESPONSE_KEY, including inactive, empty, "
        "unmatched and physical-source tombstone rows. Raw source values are retained "
        "beside canonical-event links and governed mapping provenance. Text fields can "
        "contain clinical free text or identifiers. Silver owns pivots and scores.",
    )
    ensure_delta_table(
        TGT_ASSESSMENT,
        ASSESSMENT_SCHEMA,
        "One row per DCP_FORMS_ACTIVITY_ID represented by at least one PowerForm "
        "response row. This is not a census of started forms with no emitted response. "
        "Conflicting identifiers are quarantined and nulled; Silver owns clinical "
        "completion, selection and scoring logic.",
    )
    set_column_comments(
        TGT_ITEM,
        {
            "DOC_RESPONSE_KEY": "Stable upstream response-variant key and table grain.",
            "RESPONSE_KIND": "Deterministic typed classification; EMPTY rows are retained.",
            "CANONICAL_MATCH_STATUS": "Lane match result; source values are never overwritten by canonical values.",
            "QUESTION_CONCEPT_ID": "Resolved valid standard concept: approved PowerForm rule, then canonical mapping.",
            "SOURCE_PRESENT_IND": "False means the source key physically disappeared; reappearance reactivates it.",
            "ADC_UPDT": "Informational source update timestamp; never an incremental checkpoint.",
        },
    )
    set_column_comments(
        TGT_ASSESSMENT,
        {
            "DCP_FORMS_ACTIVITY_ID": "Represented assessment key and table grain.",
            "CONTEXT_QUARANTINED_IND": "True when stable header identifiers conflict across response rows.",
            "SOURCE_PRESENT_IND": "False when no physical source item remains for the represented activity.",
            "ADC_UPDT": "Greatest informational item source update; never a checkpoint.",
        },
    )


def classification_metrics(classified: DataFrame) -> dict:
    rows = classified.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("DOC_RESPONSE_KEY").alias("distinct_keys"),
        F.sum(F.when(F.col("DOC_RESPONSE_KEY").isNull(), 1).otherwise(0)).alias(
            "null_keys"
        ),
        F.sum(
            F.when(~F.col("RESPONSE_CLASSIFICATION_VALID_IND"), 1).otherwise(0)
        ).alias("classification_invalid_rows"),
        F.sum(
            F.when(
                (F.col("ACTIVE_IND") == 1)
                & ~F.col("RESPONSE_CLASSIFICATION_VALID_IND"),
                1,
            ).otherwise(0)
        ).alias("active_classification_invalid_rows"),
        F.sum(F.when(F.col("RESPONSE_KIND") == "INVALID", 1).otherwise(0)).alias(
            "invalid_kind_rows"
        ),
        F.sum(
            F.when(
                (F.col("ACTIVE_IND") == 1)
                & (
                    F.col("DCP_FORMS_ACTIVITY_ID").isNull()
                    | F.col("ELEMENT_EVENT_ID_LONG").isNull()
                ),
                1,
            ).otherwise(0)
        ).alias("active_missing_stable_context_rows"),
        F.sum(
            F.when(F.col("DCP_FORMS_ACTIVITY_ID").isNull(), 1).otherwise(0)
        ).alias("missing_activity_rows"),
        F.sum(
            F.when(
                (F.col("RESPONSE_NOMENCLATURE_ID") == 0)
                | (F.col("RESPONSE_CODE_VALUE_ID") == 0),
                1,
            ).otherwise(0)
        ).alias("typed_zero_sentinel_rows"),
        F.sum(
            F.when(
                (F.col("RESPONSE_VARIANT_SUFFIX") == "-1")
                & F.col("NUMERIC_RESPONSE_NBR").isNull()
                & F.col("RESPONSE_TEXT_TXT").isNull(),
                1,
            ).otherwise(0)
        ).alias("empty_minus_one_rows"),
        F.sum(
            F.when(F.col("RESPONSE_HAS_BOTH_NUMERIC_AND_TEXT_IND"), 1).otherwise(0)
        ).alias("both_numeric_text_rows"),
    ).first().asDict()
    result = {key: int(value or 0) for key, value in rows.items()}
    hard_failures = {
        key: result[key]
        for key in [
            "null_keys",
            "active_missing_stable_context_rows",
            "typed_zero_sentinel_rows",
        ]
        if result[key]
    }
    if result["rows"] != result["distinct_keys"]:
        hard_failures["duplicate_key_rows"] = (
            result["rows"] - result["distinct_keys"]
        )
    production_target = TARGET_SCHEMA.startswith("4_prod.")
    if production_target and result["active_classification_invalid_rows"]:
        hard_failures["active_classification_invalid_rows"] = result[
            "active_classification_invalid_rows"
        ]
    if hard_failures:
        raise RuntimeError(f"source classification validation failed: {hard_failures}")
    return result


ITEM_HASH_EXCLUDED = {
    "ROW_HASH",
    "PIPELINE_RUN_ID",
    "PIPELINE_UPDT_DT_TM",
    "TRIGGER_SOURCES",
    "SOURCE_POWERFORM_RUN_ID",
    "SOURCE_POWERFORM_PROCESSED_DT_TM",
}


def prepare_item_rows(
    mapped: DataFrame, trigger_sources: Optional[DataFrame] = None
) -> DataFrame:
    result = mapped
    if trigger_sources is not None:
        result = (
            result.alias("i")
            .join(
                trigger_sources.select("DOC_RESPONSE_KEY", "TRIGGER_SOURCES").alias(
                    "c"
                ),
                "DOC_RESPONSE_KEY",
                "left",
            )
            .select("i.*", F.col("c.TRIGGER_SOURCES"))
        )
    elif "TRIGGER_SOURCES" not in result.columns:
        result = result.withColumn("TRIGGER_SOURCES", F.lit(RUN_MODE.upper()))
    result = (
        result.withColumn("SOURCE_PRESENT_IND", F.lit(True))
        .withColumn("PIPELINE_RUN_ID", F.lit(POWERFORM_RUN_ID))
        .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
    )
    result = align_to_schema(result, ITEM_SCHEMA)
    return result.withColumn(
        "ROW_HASH", stable_hash(result, ITEM_HASH_EXCLUDED)
    ).select(*[field.name for field in ITEM_SCHEMA.fields])


def merge_item_rows(rows: DataFrame) -> dict:
    assert_unique_non_null(rows, ["DOC_RESPONSE_KEY"], "item merge batch")
    assignments = {name: f"s.{qident(name)}" for name in rows.columns}
    (
        DeltaTable.forName(spark, TGT_ITEM)
        .alias("t")
        .merge(rows.alias("s"), "t.DOC_RESPONSE_KEY = s.DOC_RESPONSE_KEY")
        .whenMatchedUpdate(
            condition="NOT (t.ROW_HASH <=> s.ROW_HASH) OR NOT t.SOURCE_PRESENT_IND",
            set=assignments,
        )
        .whenNotMatchedInsert(values=assignments)
        .execute()
    )
    return latest_operation_metrics(TGT_ITEM)


def tombstone_item_scope(
    key_scope: DataFrame, source_snapshot: DataFrame, trigger: str
) -> dict:
    source_keys = source_snapshot.select("DOC_RESPONSE_KEY").dropDuplicates()
    missing = (
        spark.table(TGT_ITEM)
        .where(F.col("SOURCE_PRESENT_IND"))
        .join(key_scope.select("DOC_RESPONSE_KEY").distinct(), "DOC_RESPONSE_KEY", "left_semi")
        .join(source_keys, "DOC_RESPONSE_KEY", "left_anti")
    )
    if is_empty(missing):
        return {"tombstoned": 0}
    tombstones = (
        missing.withColumn("SOURCE_PRESENT_IND", F.lit(False))
        .withColumn("ACTIVE_IND", F.lit(0).cast("int"))
        .withColumn("TRIGGER_SOURCES", F.lit(trigger))
        .withColumn("PIPELINE_RUN_ID", F.lit(POWERFORM_RUN_ID))
        .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
    )
    tombstones = tombstones.withColumn(
        "ROW_HASH", stable_hash(tombstones, ITEM_HASH_EXCLUDED)
    )
    metrics = merge_item_rows(align_to_schema(tombstones, ITEM_SCHEMA))
    metrics["tombstoned"] = int(missing.count())
    return metrics


# COMMAND ----------

# MAGIC %md
# MAGIC ## Mapping candidate statistics

# COMMAND ----------


def generated_mapping_candidates(items: DataFrame) -> DataFrame:
    eligible = items.where(
        F.col("SOURCE_PRESENT_IND")
        & F.col("TASK_ASSAY_ID").isNotNull()
        & F.col("ELEMENT_EVENT_CD").isNotNull()
    )
    common = [
        F.lit(None).cast("long").alias("FORM_REF_ID"),
        F.lit(None).cast("long").alias("DCP_INPUT_REF_ID"),
        F.lit(None).cast("long").alias("SECTION_REF_ID"),
        F.col("TASK_ASSAY_ID").cast("long").alias("TASK_ASSAY_ID"),
        F.col("ELEMENT_EVENT_CD").cast("long").alias("ELEMENT_EVENT_CD"),
        F.col("GRID_EVENT_CD").cast("long").alias("GRID_EVENT_CD"),
        F.col("INPUT_TYPE_FLG").cast("int").alias("INPUT_TYPE_FLG"),
    ]
    any_rows = eligible.select(
        *common,
        F.lit("ANY").alias("ANSWER_KIND"),
        F.lit(None).cast("long").alias("ANSWER_NOMENCLATURE_ID"),
        F.lit(None).cast("long").alias("ANSWER_CODE_VALUE_ID"),
        F.lit(None).cast("string").alias("ANSWER_TEXT_NORMALIZED"),
        F.col("FORM_REF_ID").alias("_OBS_FORM_REF_ID"),
        "FORM_DESC_TXT",
        "SECTION_DESC_TXT",
        "ELEMENT_LABEL_TXT",
        "RESPONSE_VALUE_TXT",
        "DOCUMENTATION_DT_TM",
        "ADC_UPDT",
        "CANONICAL_QUESTION_CONCEPT_ID",
        F.lit(None).cast("long").alias("CANONICAL_VALUE_CONCEPT_ID"),
        "CANONICAL_UNIT_CONCEPT_ID",
    )
    nomen_rows = eligible.where(
        F.col("RESPONSE_KIND") == "CODED_NOMENCLATURE"
    ).select(
        *common,
        F.lit("NOMENCLATURE").alias("ANSWER_KIND"),
        F.col("RESPONSE_NOMENCLATURE_ID").cast("long").alias(
            "ANSWER_NOMENCLATURE_ID"
        ),
        F.lit(None).cast("long").alias("ANSWER_CODE_VALUE_ID"),
        F.lit(None).cast("string").alias("ANSWER_TEXT_NORMALIZED"),
        F.col("FORM_REF_ID").alias("_OBS_FORM_REF_ID"),
        "FORM_DESC_TXT",
        "SECTION_DESC_TXT",
        "ELEMENT_LABEL_TXT",
        "RESPONSE_VALUE_TXT",
        "DOCUMENTATION_DT_TM",
        "ADC_UPDT",
        "CANONICAL_QUESTION_CONCEPT_ID",
        "CANONICAL_VALUE_CONCEPT_ID",
        "CANONICAL_UNIT_CONCEPT_ID",
    )
    code_rows = eligible.where(
        F.col("RESPONSE_KIND") == "CODED_CODE_VALUE"
    ).select(
        *common,
        F.lit("CODE_VALUE").alias("ANSWER_KIND"),
        F.lit(None).cast("long").alias("ANSWER_NOMENCLATURE_ID"),
        F.col("RESPONSE_CODE_VALUE_ID").cast("long").alias(
            "ANSWER_CODE_VALUE_ID"
        ),
        F.lit(None).cast("string").alias("ANSWER_TEXT_NORMALIZED"),
        F.col("FORM_REF_ID").alias("_OBS_FORM_REF_ID"),
        "FORM_DESC_TXT",
        "SECTION_DESC_TXT",
        "ELEMENT_LABEL_TXT",
        "RESPONSE_VALUE_TXT",
        "DOCUMENTATION_DT_TM",
        "ADC_UPDT",
        "CANONICAL_QUESTION_CONCEPT_ID",
        "CANONICAL_VALUE_CONCEPT_ID",
        "CANONICAL_UNIT_CONCEPT_ID",
    )
    observations = any_rows.unionByName(nomen_rows).unionByName(code_rows)
    selector_columns = [
        *MAP_SELECTOR_COLUMNS,
        "ANSWER_KIND",
        "ANSWER_NOMENCLATURE_ID",
        "ANSWER_CODE_VALUE_ID",
        "ANSWER_TEXT_NORMALIZED",
    ]
    seen_at = F.coalesce(F.col("DOCUMENTATION_DT_TM"), F.col("ADC_UPDT"))
    grouped = observations.groupBy(*selector_columns).agg(
        F.first("FORM_DESC_TXT", ignorenulls=True).alias("FORM_DESC_EXAMPLE"),
        F.first("SECTION_DESC_TXT", ignorenulls=True).alias(
            "SECTION_DESC_EXAMPLE"
        ),
        F.first("ELEMENT_LABEL_TXT", ignorenulls=True).alias(
            "QUESTION_LABEL_EXAMPLE"
        ),
        F.first("RESPONSE_VALUE_TXT", ignorenulls=True).alias(
            "ANSWER_DISPLAY_EXAMPLE"
        ),
        F.countDistinct("_OBS_FORM_REF_ID").cast("long").alias(
            "DISTINCT_FORM_REF_COUNT"
        ),
        F.count(F.lit(1)).cast("long").alias("EXAMPLE_ROW_COUNT"),
        F.min(seen_at).alias("FIRST_SEEN_DT_TM"),
        F.max(seen_at).alias("LAST_SEEN_DT_TM"),
        F.countDistinct("CANONICAL_QUESTION_CONCEPT_ID").alias("_Q_COUNT"),
        F.max("CANONICAL_QUESTION_CONCEPT_ID").alias("_Q_SUGGESTION"),
        F.countDistinct("CANONICAL_VALUE_CONCEPT_ID").alias("_V_COUNT"),
        F.max("CANONICAL_VALUE_CONCEPT_ID").alias("_V_SUGGESTION"),
        F.countDistinct("CANONICAL_UNIT_CONCEPT_ID").alias("_U_COUNT"),
        F.max("CANONICAL_UNIT_CONCEPT_ID").alias("_U_SUGGESTION"),
    )
    result = (
        grouped.withColumn("MAPPING_RULE_ID", mapping_rule_id_expr())
        .withColumn("QUESTION_CONCEPT_ID", F.lit(None).cast("long"))
        .withColumn("VALUE_CONCEPT_ID", F.lit(None).cast("long"))
        .withColumn("UNIT_CONCEPT_ID", F.lit(None).cast("long"))
        .withColumn("TARGET_OMOP_TABLE", F.lit(None).cast("string"))
        .withColumn("TARGET_OMOP_FIELD", F.lit(None).cast("string"))
        .withColumn(
            "SUGGESTED_QUESTION_CONCEPT_ID",
            F.when(F.col("_Q_COUNT") == 1, F.col("_Q_SUGGESTION")),
        )
        .withColumn(
            "SUGGESTED_VALUE_CONCEPT_ID",
            F.when(F.col("_V_COUNT") == 1, F.col("_V_SUGGESTION")),
        )
        .withColumn(
            "SUGGESTED_UNIT_CONCEPT_ID",
            F.when(F.col("_U_COUNT") == 1, F.col("_U_SUGGESTION")),
        )
        .withColumn("SUGGESTION_SOURCE", F.lit("CANONICAL_EVENT_LANE"))
        .withColumn("SOURCE_PRESENT_IND", F.lit(True))
        .withColumn("MAPPING_METHOD", F.lit("AUTO_CANDIDATE"))
        .withColumn("CURATED_STATUS", F.lit("PROPOSED"))
        .withColumn("CURATED_BY", F.lit(None).cast("string"))
        .withColumn("CURATED_AT", F.lit(None).cast("timestamp"))
        .withColumn("CURATED_NOTES", F.lit(None).cast("string"))
        .withColumn("MAPPING_VERSION", F.lit(MAPPING_VERSION))
        .withColumn("VALID_FROM_DT", F.lit(None).cast("date"))
        .withColumn("VALID_TO_DT", F.lit(None).cast("date"))
        .withColumn("MAP_ROW_UPDT_DT_TM", F.lit(None).cast("timestamp"))
    )
    map_schema = spark.table(TGT_MAP).schema
    aligned = align_to_schema(result, map_schema)
    return (
        aligned.withColumn(
            "PIPELINE_STATS_HASH",
            stable_hash(
                aligned,
                [
                    "PIPELINE_STATS_HASH",
                    "PIPELINE_STATS_UPDT_DT_TM",
                    "MAP_ROW_UPDT_DT_TM",
                    *MAP_CURATOR_COLUMNS,
                ],
            ),
        )
        .withColumn("PIPELINE_STATS_UPDT_DT_TM", F.current_timestamp())
        .select(*map_schema.fieldNames())
    )


def merge_mapping_candidates(candidates: DataFrame, exact_full: bool) -> dict:
    assert_unique_non_null(candidates, ["MAPPING_RULE_ID"], "mapping candidates")
    stats_columns = [
        "SUGGESTED_QUESTION_CONCEPT_ID",
        "SUGGESTED_VALUE_CONCEPT_ID",
        "SUGGESTED_UNIT_CONCEPT_ID",
        "SUGGESTION_SOURCE",
        "FORM_DESC_EXAMPLE",
        "SECTION_DESC_EXAMPLE",
        "QUESTION_LABEL_EXAMPLE",
        "ANSWER_DISPLAY_EXAMPLE",
        "DISTINCT_FORM_REF_COUNT",
        "EXAMPLE_ROW_COUNT",
        "FIRST_SEEN_DT_TM",
        "LAST_SEEN_DT_TM",
        "SOURCE_PRESENT_IND",
        "PIPELINE_STATS_HASH",
        "PIPELINE_STATS_UPDT_DT_TM",
    ]
    update_set = {name: f"s.{qident(name)}" for name in stats_columns}
    if not exact_full:
        update_set["DISTINCT_FORM_REF_COUNT"] = "greatest(t.DISTINCT_FORM_REF_COUNT, s.DISTINCT_FORM_REF_COUNT)"
        update_set["EXAMPLE_ROW_COUNT"] = "t.EXAMPLE_ROW_COUNT"
        update_set["FIRST_SEEN_DT_TM"] = "least(t.FIRST_SEEN_DT_TM, s.FIRST_SEEN_DT_TM)"
        update_set["LAST_SEEN_DT_TM"] = "greatest(t.LAST_SEEN_DT_TM, s.LAST_SEEN_DT_TM)"
    builder = (
        DeltaTable.forName(spark, TGT_MAP)
        .alias("t")
        .merge(candidates.alias("s"), "t.MAPPING_RULE_ID = s.MAPPING_RULE_ID")
        .whenMatchedUpdate(
            condition="NOT (t.PIPELINE_STATS_HASH <=> s.PIPELINE_STATS_HASH) OR NOT t.SOURCE_PRESENT_IND",
            set=update_set,
        )
        .whenNotMatchedInsertAll()
    )
    if exact_full:
        builder = builder.whenNotMatchedBySourceUpdate(
            condition="t.MAPPING_METHOD = 'AUTO_CANDIDATE' AND t.CURATED_STATUS = 'PROPOSED'",
            set={
                "SOURCE_PRESENT_IND": "false",
                "PIPELINE_STATS_UPDT_DT_TM": "current_timestamp()",
            },
        )
    builder.execute()
    return latest_operation_metrics(TGT_MAP)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Assessment header builder

# COMMAND ----------


def build_assessment_rows(
    activity_scope: Optional[DataFrame] = None,
) -> Tuple[DataFrame, dict]:
    items = spark.table(TGT_ITEM).where(
        F.col("SOURCE_PRESENT_IND") & F.col("DCP_FORMS_ACTIVITY_ID").isNotNull()
    )
    if activity_scope is not None:
        items = items.join(
            activity_scope.select("DCP_FORMS_ACTIVITY_ID").distinct(),
            "DCP_FORMS_ACTIVITY_ID",
            "left_semi",
        )
    order_time = F.coalesce(
        F.col("LAST_DOCUMENTED_DT_TM"),
        F.col("DOCUMENTATION_DT_TM"),
        F.col("ADC_UPDT"),
    )
    response_time = F.coalesce(
        F.col("PERFORMED_DT_TM"), F.col("DOCUMENTATION_DT_TM")
    )
    conflict_specs = [
        ("PERSON_ID_LONG", "person"),
        ("ENCNTR_ID_LONG", "encounter"),
        ("ORGANIZATION_ID", "organization"),
        ("FORM_REF_ID", "form_ref"),
        ("FORM_INSTANCE_ID", "form_instance"),
        ("FORM_EVENT_ID", "form_event"),
    ]
    aggregations = []
    for column_name, label in conflict_specs:
        aggregations.extend(
            [
                F.countDistinct(column_name).alias(f"_{label}_cardinality"),
                F.first(column_name, ignorenulls=True).alias(f"_{label}_value"),
            ]
        )
    for kind in [
        "CODED_NOMENCLATURE",
        "CODED_CODE_VALUE",
        "CODED_OTHER",
        "NUMERIC",
        "TEXT",
        "DATE",
        "EMPTY",
        "INVALID",
    ]:
        aggregations.append(
            F.sum(F.when(F.col("RESPONSE_KIND") == kind, 1).otherwise(0)).cast(
                "long"
            ).alias(f"{kind}_ROW_COUNT")
        )
    grouped = items.groupBy("DCP_FORMS_ACTIVITY_ID").agg(
        *aggregations,
        F.max_by("TRUST", order_time).alias("TRUST"),
        F.max_by("FORM_STATUS_CD_LONG", order_time).alias("FORM_STATUS_CD_LONG"),
        F.max_by("FORM_STATUS_CD", order_time).alias("FORM_STATUS_CD"),
        F.max_by("FORM_DESC_TXT", order_time).alias("FORM_DESC_TXT"),
        F.max("DOCUMENTATION_DT_TM").alias("DOCUMENTATION_DT_TM"),
        F.min("FIRST_DOCUMENTED_DT_TM").alias("FIRST_DOCUMENTED_DT_TM"),
        F.max("LAST_DOCUMENTED_DT_TM").alias("LAST_DOCUMENTED_DT_TM"),
        (F.max(F.when(F.col("ACTIVE_IND") == 1, 1).otherwise(0)) == 1).alias(
            "ASSESSMENT_ACTIVE_IND"
        ),
        F.count(F.lit(1)).cast("long").alias("RESPONSE_ROW_COUNT"),
        F.sum(F.when(F.col("ACTIVE_IND") == 1, 1).otherwise(0)).cast("long").alias(
            "ACTIVE_RESPONSE_ROW_COUNT"
        ),
        F.sum(
            F.when(F.col("CANONICAL_MATCH_STATUS") == "MATCHED", 1).otherwise(0)
        ).cast("long").alias("CANONICAL_MATCHED_ROW_COUNT"),
        F.sum(
            F.when(
                (F.col("CANONICAL_LANE") != "NONE")
                & (F.col("CANONICAL_MATCH_STATUS") != "MATCHED"),
                1,
            ).otherwise(0)
        ).cast("long").alias("CANONICAL_UNMATCHED_ROW_COUNT"),
        F.sum(F.when(F.col("QUESTION_CONCEPT_ID").isNotNull(), 1).otherwise(0)).cast(
            "long"
        ).alias("QUESTION_MAPPED_ROW_COUNT"),
        F.sum(F.when(F.col("VALUE_CONCEPT_ID").isNotNull(), 1).otherwise(0)).cast(
            "long"
        ).alias("VALUE_MAPPED_ROW_COUNT"),
        F.sum(
            F.when(
                F.col("QUESTION_MAPPING_CONFLICT_IND")
                | F.col("VALUE_MAPPING_CONFLICT_IND")
                | F.col("UNIT_MAPPING_CONFLICT_IND"),
                1,
            ).otherwise(0)
        ).cast("long").alias("MAPPING_CONFLICT_ROW_COUNT"),
        F.min(response_time).alias("FIRST_RESPONSE_DT_TM"),
        F.max(response_time).alias("LAST_RESPONSE_DT_TM"),
        F.max("ADC_UPDT").alias("ADC_UPDT"),
    )
    conflict_fields = F.filter(
        F.array(
            *[
                F.when(F.col(f"_{label}_cardinality") > 1, F.lit(column_name))
                for column_name, label in conflict_specs
            ]
        ),
        lambda value: value.isNotNull(),
    )
    result = grouped.withColumn("CONTEXT_CONFLICT_FIELDS", conflict_fields)
    result = result.withColumn(
        "CONTEXT_CONFLICT_IND", F.size("CONTEXT_CONFLICT_FIELDS") > 0
    ).withColumn("CONTEXT_QUARANTINED_IND", F.col("CONTEXT_CONFLICT_IND"))
    for column_name, label in conflict_specs:
        result = result.withColumn(
            column_name,
            F.when(
                F.col(f"_{label}_cardinality") <= 1,
                F.col(f"_{label}_value"),
            ).cast(dict(ASSESSMENT_COLUMN_TYPES)[column_name]),
        )
    result = (
        result.withColumn("SOURCE_PRESENT_IND", F.lit(True))
        .withColumn("PIPELINE_RUN_ID", F.lit(POWERFORM_RUN_ID))
        .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
    )
    result = align_to_schema(result, ASSESSMENT_SCHEMA)
    result = result.withColumn(
        "ROW_HASH",
        stable_hash(
            result,
            ["ROW_HASH", "PIPELINE_RUN_ID", "PIPELINE_UPDT_DT_TM"],
        ),
    ).select(*ASSESSMENT_SCHEMA.fieldNames())
    metrics_row = result.agg(
        F.count(F.lit(1)).alias("activities"),
        F.sum(F.when(F.col("CONTEXT_CONFLICT_IND"), 1).otherwise(0)).alias(
            "context_conflicts"
        ),
        F.sum("RESPONSE_ROW_COUNT").alias("response_rows"),
    ).first().asDict()
    metrics = {key: int(value or 0) for key, value in metrics_row.items()}
    metrics["context_conflict_rate"] = (
        metrics["context_conflicts"] / metrics["activities"]
        if metrics["activities"]
        else 0.0
    )
    if (
        metrics["context_conflicts"] > MAX_CONTEXT_CONFLICT_COUNT
        or metrics["context_conflict_rate"] > MAX_CONTEXT_CONFLICT_RATE
    ):
        raise RuntimeError(
            "assessment context conflict gate failed before merge: "
            f"{metrics}"
        )
    return result, metrics


def merge_assessment_rows(
    rows: DataFrame, activity_scope: Optional[DataFrame] = None
) -> dict:
    assert_unique_non_null(
        rows, ["DCP_FORMS_ACTIVITY_ID"], "assessment merge batch"
    )
    assignments = {name: f"s.{qident(name)}" for name in rows.columns}
    (
        DeltaTable.forName(spark, TGT_ASSESSMENT)
        .alias("t")
        .merge(
            rows.alias("s"),
            "t.DCP_FORMS_ACTIVITY_ID = s.DCP_FORMS_ACTIVITY_ID",
        )
        .whenMatchedUpdate(
            condition="NOT (t.ROW_HASH <=> s.ROW_HASH) OR NOT t.SOURCE_PRESENT_IND",
            set=assignments,
        )
        .whenNotMatchedInsert(values=assignments)
        .execute()
    )
    tombstoned = 0
    if activity_scope is not None:
        absent = (
            spark.table(TGT_ASSESSMENT)
            .where(F.col("SOURCE_PRESENT_IND"))
            .join(
                activity_scope.select("DCP_FORMS_ACTIVITY_ID").distinct(),
                "DCP_FORMS_ACTIVITY_ID",
                "left_semi",
            )
            .join(
                rows.select("DCP_FORMS_ACTIVITY_ID"),
                "DCP_FORMS_ACTIVITY_ID",
                "left_anti",
            )
        )
        if not is_empty(absent):
            tombstoned = absent.count()
            tombstones = (
                absent.withColumn("SOURCE_PRESENT_IND", F.lit(False))
                .withColumn("ASSESSMENT_ACTIVE_IND", F.lit(False))
                .withColumn("PIPELINE_RUN_ID", F.lit(POWERFORM_RUN_ID))
                .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
            )
            tombstones = tombstones.withColumn(
                "ROW_HASH",
                stable_hash(
                    tombstones,
                    ["ROW_HASH", "PIPELINE_RUN_ID", "PIPELINE_UPDT_DT_TM"],
                ),
            )
            merge_assessment_rows(
                align_to_schema(tombstones, ASSESSMENT_SCHEMA), None
            )
    metrics = latest_operation_metrics(TGT_ASSESSMENT)
    metrics["tombstoned"] = int(tombstoned)
    return metrics


# COMMAND ----------

# MAGIC %md
# MAGIC ## Version-driven incremental candidates

# COMMAND ----------


MAP_CURATION_CHANGE_COLUMNS = [
    *MAP_SELECTOR_COLUMNS,
    "ANSWER_KIND",
    "ANSWER_NOMENCLATURE_ID",
    "ANSWER_CODE_VALUE_ID",
    "ANSWER_TEXT_NORMALIZED",
    "QUESTION_CONCEPT_ID",
    "VALUE_CONCEPT_ID",
    "UNIT_CONCEPT_ID",
    "TARGET_OMOP_TABLE",
    "TARGET_OMOP_FIELD",
    "CURATED_STATUS",
    "CURATED_BY",
    "CURATED_AT",
    "CURATED_NOTES",
    "MAPPING_VERSION",
    "VALID_FROM_DT",
    "VALID_TO_DT",
]


def captured_versions_for_run(run_id: str, run_type: str) -> Dict[str, int]:
    if run_type == "full_rebuild":
        row = (
            spark.table(RUNS_TABLE)
            .where(F.col("RUN_ID") == run_id)
            .select("STATUS", "SOURCE_VERSIONS_JSON")
            .first()
        )
        if row is not None and row["SOURCE_VERSIONS_JSON"]:
            return {
                key: int(value)
                for key, value in json.loads(row["SOURCE_VERSIONS_JSON"]).items()
            }
    return current_versions()


def candidate_frame(rows: DataFrame, reason: str) -> DataFrame:
    available = set(rows.columns)
    doc_key = (
        F.col("DOC_RESPONSE_KEY").cast("string")
        if "DOC_RESPONSE_KEY" in available
        else F.lit(None).cast("string")
    )
    activity = (
        F.col("DCP_FORMS_ACTIVITY_ID").cast("long")
        if "DCP_FORMS_ACTIVITY_ID" in available
        else F.lit(None).cast("long")
    )
    event_id = (
        F.col("ELEMENT_EVENT_ID_LONG").cast("long")
        if "ELEMENT_EVENT_ID_LONG" in available
        else F.lit(None).cast("long")
    )
    shard_key = F.coalesce(event_id.cast("string"), doc_key)
    return (
        rows.select(
            F.lit(POWERFORM_RUN_ID).alias("RUN_ID"),
            doc_key.alias("DOC_RESPONSE_KEY"),
            activity.alias("DCP_FORMS_ACTIVITY_ID"),
            event_id.alias("ELEMENT_EVENT_ID_LONG"),
            F.lit(reason).alias("REASON"),
            F.pmod(F.xxhash64(shard_key), F.lit(FULL_BUILD_SHARDS))
            .cast("int")
            .alias("SHARD_ID"),
            F.lit("pending").alias("STATUS"),
            F.current_timestamp().alias("CREATED_AT"),
            F.lit(None).cast("timestamp").alias("PROCESSED_AT"),
        )
        .where(F.col("DOC_RESPONSE_KEY").isNotNull())
        .dropDuplicates(["RUN_ID", "DOC_RESPONSE_KEY", "REASON"])
    )


def fixed_source_subset_for_events(
    event_ids: DataFrame, versions: Dict[str, int]
) -> DataFrame:
    ids = event_ids.select(F.col("EVENT_ID").cast("long").alias("EVENT_ID")).distinct()
    source = read_snapshot(SRC_POWERFORM, versions[SRC_POWERFORM])
    return source.join(
        F.broadcast(ids),
        F.col("ELEMENT_EVENT_ID_LONG").cast("long") == F.col("EVENT_ID"),
        "left_semi",
    )


def changed_lane_candidates(
    table_name: str,
    lane_name: str,
    previous_version: int,
    current_version: int,
    versions: Dict[str, int],
) -> Optional[DataFrame]:
    cdf = read_cdf(table_name, previous_version + 1, current_version)
    if cdf is None:
        return None
    events = (
        cdf.where(
            F.col("_change_type").isin("insert", "update_postimage", "delete")
        )
        .select(F.col("EVENT_ID").cast("long").alias("EVENT_ID"))
        .where(F.col("EVENT_ID").isNotNull())
        .distinct()
    )
    if is_empty(events):
        return None
    classified = classify_responses(fixed_source_subset_for_events(events, versions))
    return candidate_frame(
        classified.where(F.col("CANONICAL_LANE") == lane_name),
        f"{lane_name}_LANE_CDF",
    )


def meaningful_map_change_records(
    previous_version: int, current_version: int
) -> Optional[DataFrame]:
    cdf = read_cdf(TGT_MAP, previous_version + 1, current_version)
    if cdf is None:
        return None
    change_rows = cdf.where(
        F.col("_change_type").isin(
            "insert", "delete", "update_preimage", "update_postimage"
        )
    )
    curator_hash = F.sha2(
        F.to_json(F.struct(*[F.col(name) for name in MAP_CURATION_CHANGE_COLUMNS])),
        256,
    )
    hashed = change_rows.withColumn("_CURATOR_HASH", curator_hash)
    commits = hashed.groupBy("MAPPING_RULE_ID", "_commit_version").agg(
        F.max(
            F.when(
                F.col("_change_type").isin("update_preimage", "delete"),
                F.col("_CURATOR_HASH"),
            )
        ).alias("_PRE_HASH"),
        F.max(
            F.when(
                F.col("_change_type").isin("update_postimage", "insert"),
                F.col("_CURATOR_HASH"),
            )
        ).alias("_POST_HASH"),
        F.max(
            F.when(
                F.col("_change_type").isin("update_preimage", "delete"),
                F.col("CURATED_STATUS"),
            )
        ).alias("_PRE_STATUS"),
        F.max(
            F.when(
                F.col("_change_type").isin("update_postimage", "insert"),
                F.col("CURATED_STATUS"),
            )
        ).alias("_POST_STATUS"),
    )
    meaningful = commits.where(
        (~F.col("_PRE_HASH").eqNullSafe(F.col("_POST_HASH")))
        & (
            (F.col("_PRE_STATUS") == "APPROVED")
            | (F.col("_POST_STATUS") == "APPROVED")
        )
    ).select("MAPPING_RULE_ID", "_commit_version")
    if is_empty(meaningful):
        return None
    return (
        hashed.join(
            meaningful,
            ["MAPPING_RULE_ID", "_commit_version"],
            "inner",
        )
        .where(
            F.col("_change_type").isin(
                "insert", "delete", "update_preimage", "update_postimage"
            )
        )
        .drop("_CURATOR_HASH")
    )


def rules_for_candidate_resolution(records: DataFrame) -> DataFrame:
    answer_key = (
        F.when(
            F.col("ANSWER_KIND") == "NOMENCLATURE",
            F.concat(
                F.lit("NOMENCLATURE|"),
                F.col("ANSWER_NOMENCLATURE_ID").cast("string"),
            ),
        )
        .when(
            F.col("ANSWER_KIND") == "CODE_VALUE",
            F.concat(
                F.lit("CODE_VALUE|"), F.col("ANSWER_CODE_VALUE_ID").cast("string")
            ),
        )
        .when(
            F.col("ANSWER_KIND") == "TEXT_EXACT",
            F.concat(F.lit("TEXT_EXACT|"), F.col("ANSWER_TEXT_NORMALIZED")),
        )
    )
    specificity = reduce(
        lambda left, right: left + right,
        [F.col(name).isNotNull().cast("int") for name in MAP_SELECTOR_COLUMNS],
    ) + (F.col("ANSWER_KIND") != "ANY").cast("int")
    return records.select(
        F.col("MAPPING_RULE_ID").alias("_PF_RULE_ID"),
        *[F.col(name).alias(f"_M_{name}") for name in MAP_SELECTOR_COLUMNS],
        F.col("ANSWER_KIND").alias("_M_ANSWER_KIND"),
        answer_key.alias("_M_ANSWER_KEY"),
        F.col("QUESTION_CONCEPT_ID").cast("long").alias("_PF_Q_CONCEPT"),
        F.col("VALUE_CONCEPT_ID").cast("long").alias("_PF_V_CONCEPT"),
        F.col("UNIT_CONCEPT_ID").cast("long").alias("_PF_U_CONCEPT"),
        F.col("MAPPING_VERSION").alias("_PF_MAPPING_VERSION"),
        specificity.alias("_PF_SPECIFICITY"),
    ).dropDuplicates()


def resolve_map_records_to_candidates(
    records: Optional[DataFrame],
    versions: Dict[str, int],
    reason: str,
) -> Optional[DataFrame]:
    if records is None or is_empty(records):
        return None
    source = classify_responses(read_snapshot(SRC_POWERFORM, versions[SRC_POWERFORM]))
    items = source.withColumn("_PF_SOURCE_ANSWER_KEY", _source_answer_key())
    rules = rules_for_candidate_resolution(records)
    matches = reduce(
        lambda left, right: left.unionByName(right),
        [
            _mapping_branch(items, rules, "INPUT", True, 1),
            _mapping_branch(items, rules, "FORM", True, 2),
            _mapping_branch(items, rules, "GLOBAL", True, 3),
            _mapping_branch(items, rules, "INPUT", False, 4),
            _mapping_branch(items, rules, "FORM", False, 5),
            _mapping_branch(items, rules, "GLOBAL", False, 6),
        ],
    ).select("DOC_RESPONSE_KEY").distinct()
    resolved = source.join(matches, "DOC_RESPONSE_KEY", "left_semi")
    return candidate_frame(resolved, reason)


def concept_changed_ids(
    previous_version: int, current_version: int
) -> Tuple[DataFrame, bool]:
    fields = [
        "concept_id",
        "standard_concept",
        "invalid_reason",
        "valid_start_date",
        "valid_end_date",
        "domain_id",
    ]

    def projection(version: int, side: str) -> DataFrame:
        source = read_snapshot(SRC_CONCEPT, version)
        names = {name.lower(): name for name in source.columns}
        expressions = []
        for field in fields:
            actual = names.get(field)
            if actual:
                expressions.append(F.col(actual).cast("string").alias(field))
            else:
                expressions.append(F.lit(None).cast("string").alias(field))
        projected = source.select(*expressions)
        return projected.select(
            F.col("concept_id").cast("long").alias("CONCEPT_ID"),
            F.sha2(
                F.to_json(F.struct(*[F.col(name) for name in fields[1:]])), 256
            ).alias(f"{side}_HASH"),
        )

    try:
        before = projection(previous_version, "OLD")
        after = projection(current_version, "NEW")
        changed = (
            before.join(after, "CONCEPT_ID", "full")
            .where(~F.col("OLD_HASH").eqNullSafe(F.col("NEW_HASH")))
            .select("CONCEPT_ID")
            .where(F.col("CONCEPT_ID").isNotNull())
            .distinct()
        )
        return changed, False
    except Exception as exc:
        print(
            "Prior OMOP concept version is unreadable after replacement; "
            f"falling back to the compact referenced-ID set: {exc}"
        )
        frames: List[DataFrame] = []
        if table_exists(TGT_ITEM):
            item = spark.table(TGT_ITEM)
            for name in [
                "CANONICAL_QUESTION_CONCEPT_ID",
                "CANONICAL_VALUE_CONCEPT_ID",
                "CANONICAL_UNIT_CONCEPT_ID",
                "POWERFORM_QUESTION_CONCEPT_ID",
                "POWERFORM_VALUE_CONCEPT_ID",
                "POWERFORM_UNIT_CONCEPT_ID",
                "QUESTION_CONCEPT_ID",
                "VALUE_CONCEPT_ID",
                "UNIT_CONCEPT_ID",
            ]:
                frames.append(item.select(F.col(name).alias("CONCEPT_ID")))
            for name in [
                "CANONICAL_QUESTION_CANDIDATE_CONCEPT_IDS",
                "CANONICAL_VALUE_CANDIDATE_CONCEPT_IDS",
                "CANONICAL_UNIT_CANDIDATE_CONCEPT_IDS",
            ]:
                frames.append(item.select(F.explode_outer(name).alias("CONCEPT_ID")))
        mapping = read_snapshot(TGT_MAP, source_version(TGT_MAP))
        for name in [
            "QUESTION_CONCEPT_ID",
            "VALUE_CONCEPT_ID",
            "UNIT_CONCEPT_ID",
            "SUGGESTED_QUESTION_CONCEPT_ID",
            "SUGGESTED_VALUE_CONCEPT_ID",
            "SUGGESTED_UNIT_CONCEPT_ID",
        ]:
            frames.append(mapping.select(F.col(name).alias("CONCEPT_ID")))
        referenced = reduce(lambda left, right: left.unionByName(right), frames)
        return referenced.where(F.col("CONCEPT_ID").isNotNull()).distinct(), True


def candidates_for_changed_concepts(
    changed_ids: DataFrame,
    versions: Dict[str, int],
) -> Tuple[Optional[DataFrame], Optional[DataFrame]]:
    if is_empty(changed_ids):
        return None, None
    item_candidates = None
    if table_exists(TGT_ITEM):
        ids = F.broadcast(changed_ids)
        item = spark.table(TGT_ITEM).where(F.col("SOURCE_PRESENT_IND"))
        scalar_columns = [
            "CANONICAL_QUESTION_CONCEPT_ID",
            "CANONICAL_VALUE_CONCEPT_ID",
            "CANONICAL_UNIT_CONCEPT_ID",
            "POWERFORM_QUESTION_CONCEPT_ID",
            "POWERFORM_VALUE_CONCEPT_ID",
            "POWERFORM_UNIT_CONCEPT_ID",
            "QUESTION_CONCEPT_ID",
            "VALUE_CONCEPT_ID",
            "UNIT_CONCEPT_ID",
        ]
        scalar_refs = reduce(
            lambda left, right: left.unionByName(right),
            [
                item.select(
                    "DOC_RESPONSE_KEY",
                    F.col(name).alias("CONCEPT_ID"),
                )
                for name in scalar_columns
            ],
        )
        array_refs = reduce(
            lambda left, right: left.unionByName(right),
            [
                item.select(
                    "DOC_RESPONSE_KEY", F.explode_outer(name).alias("CONCEPT_ID")
                )
                for name in [
                    "CANONICAL_QUESTION_CANDIDATE_CONCEPT_IDS",
                    "CANONICAL_VALUE_CANDIDATE_CONCEPT_IDS",
                    "CANONICAL_UNIT_CANDIDATE_CONCEPT_IDS",
                ]
            ],
        )
        keys = scalar_refs.unionByName(array_refs).join(
            ids, "CONCEPT_ID", "left_semi"
        ).select("DOC_RESPONSE_KEY").distinct()
        item_candidates = candidate_frame(
            item.join(keys, "DOC_RESPONSE_KEY", "left_semi"),
            "OMOP_CONCEPT_VERSION_DIFF",
        )

    mapping = read_snapshot(TGT_MAP, versions[TGT_MAP])
    map_refs = reduce(
        lambda left, right: left.unionByName(right),
        [
            mapping.select("MAPPING_RULE_ID", F.col(name).alias("CONCEPT_ID"))
            for name in [
                "QUESTION_CONCEPT_ID",
                "VALUE_CONCEPT_ID",
                "UNIT_CONCEPT_ID",
                "SUGGESTED_QUESTION_CONCEPT_ID",
                "SUGGESTED_VALUE_CONCEPT_ID",
                "SUGGESTED_UNIT_CONCEPT_ID",
            ]
        ],
    )
    changed_rules = mapping.join(
        map_refs.join(changed_ids, "CONCEPT_ID", "left_semi")
        .select("MAPPING_RULE_ID")
        .distinct(),
        "MAPPING_RULE_ID",
        "left_semi",
    )
    map_candidates = resolve_map_records_to_candidates(
        changed_rules, versions, "OMOP_CONCEPT_MAP_REFERENCE"
    )
    return item_candidates, map_candidates


def incremental_candidates(
    versions: Dict[str, int], previous: Dict[str, int]
) -> Tuple[Optional[DataFrame], dict]:
    required_state = [
        SRC_POWERFORM,
        SRC_NOMEN,
        SRC_CODED,
        SRC_NUMERIC,
        SRC_TEXT,
        SRC_DATE,
        SRC_CONCEPT,
        TGT_MAP,
    ]
    missing_state = [name for name in required_state if name not in previous]
    if missing_state:
        raise RuntimeError(
            "Incremental state is not seeded for all inputs. Run a validated "
            f"full_rebuild first; missing={missing_state}"
        )
    frames: List[Optional[DataFrame]] = []
    ranges = {}

    for table_name in required_state:
        ranges[table_name] = {
            "from": int(previous[table_name]),
            "to": int(versions[table_name]),
        }

    if versions[SRC_POWERFORM] > previous[SRC_POWERFORM]:
        source_cdf = read_cdf(
            SRC_POWERFORM,
            previous[SRC_POWERFORM] + 1,
            versions[SRC_POWERFORM],
        )
        source_changes = source_cdf.where(
            F.col("_change_type").isin("insert", "update_postimage", "delete")
        )
        frames.append(candidate_frame(source_changes, "POWERFORM_SOURCE_CDF"))

    lane_specs = [
        (SRC_NOMEN, "NOMEN"),
        (SRC_CODED, "CODED"),
        (SRC_NUMERIC, "NUMERIC"),
        (SRC_TEXT, "TEXT"),
        (SRC_DATE, "DATE"),
    ]
    for table_name, lane_name in lane_specs:
        if versions[table_name] > previous[table_name]:
            frames.append(
                changed_lane_candidates(
                    table_name,
                    lane_name,
                    previous[table_name],
                    versions[table_name],
                    versions,
                )
            )

    if versions[TGT_MAP] > previous[TGT_MAP]:
        records = meaningful_map_change_records(
            previous[TGT_MAP], versions[TGT_MAP]
        )
        frames.append(
            resolve_map_records_to_candidates(
                records, versions, "POWERFORM_MAP_CURATION_CDF"
            )
        )

    concept_fallback = False
    if versions[SRC_CONCEPT] != previous[SRC_CONCEPT]:
        changed_ids, concept_fallback = concept_changed_ids(
            previous[SRC_CONCEPT], versions[SRC_CONCEPT]
        )
        item_candidates, map_candidates = candidates_for_changed_concepts(
            changed_ids, versions
        )
        frames.extend([item_candidates, map_candidates])

    present = [frame for frame in frames if frame is not None]
    if not present:
        return None, {"ranges": ranges, "concept_reference_fallback": concept_fallback}
    candidates = reduce(lambda left, right: left.unionByName(right), present).dropDuplicates(
        ["RUN_ID", "DOC_RESPONSE_KEY", "REASON"]
    )
    return candidates, {
        "ranges": ranges,
        "concept_reference_fallback": concept_fallback,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build execution, validation and resumable full rebuild

# COMMAND ----------


def coverage_report(items: DataFrame) -> dict:
    rows = (
        items.where(F.col("CANONICAL_LANE") != "NONE")
        .groupBy("RESPONSE_KIND")
        .agg(
            F.count(F.lit(1)).alias("eligible"),
            F.sum(
                F.when(
                    F.col("CANONICAL_MATCH_STATUS").isin(
                        "MATCHED", "VALUE_MISMATCH", "AMBIGUOUS"
                    ),
                    1,
                ).otherwise(0)
            ).alias("matched"),
            F.sum(
                F.when(
                    F.col("CANONICAL_VALUE_CONCORDANCE_IND") == F.lit(False), 1
                ).otherwise(0)
            ).alias("value_mismatch"),
        )
        .collect()
    )
    report = {}
    for row in rows:
        eligible = int(row["eligible"] or 0)
        matched = int(row["matched"] or 0)
        report[row["RESPONSE_KIND"]] = {
            "eligible": eligible,
            "matched": matched,
            "coverage": matched / eligible if eligible else None,
            "value_mismatch": int(row["value_mismatch"] or 0),
        }
    return report


def validate_final_outputs(
    versions: Dict[str, int],
    source_snapshot: Optional[DataFrame] = None,
    enforce_coverage: bool = False,
) -> dict:
    items = spark.table(TGT_ITEM)
    present_items = items.where(F.col("SOURCE_PRESENT_IND"))
    item_row = items.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("DOC_RESPONSE_KEY").alias("distinct_keys"),
        F.sum(F.when(F.col("DOC_RESPONSE_KEY").isNull(), 1).otherwise(0)).alias(
            "null_keys"
        ),
    ).first().asDict()
    item_metrics = {key: int(value or 0) for key, value in item_row.items()}
    item_metrics["present_rows"] = int(present_items.count())
    item_metrics["active_present_rows"] = int(
        present_items.where(F.col("ACTIVE_IND") == 1).count()
    )
    if item_metrics["rows"] != item_metrics["distinct_keys"] or item_metrics[
        "null_keys"
    ]:
        raise RuntimeError(f"item target grain validation failed: {item_metrics}")

    if source_snapshot is not None:
        source_counts = source_snapshot.agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("DOC_RESPONSE_KEY").alias("distinct_keys"),
            F.sum(F.when(F.col("ACTIVE_IND") == 1, 1).otherwise(0)).alias(
                "active_rows"
            ),
        ).first().asDict()
        source_counts = {key: int(value or 0) for key, value in source_counts.items()}
        item_metrics["source_rows"] = source_counts["rows"]
        item_metrics["source_distinct_keys"] = source_counts["distinct_keys"]
        item_metrics["source_active_rows"] = source_counts["active_rows"]
        if (
            source_counts["rows"] != source_counts["distinct_keys"]
            or source_counts["rows"] != item_metrics["present_rows"]
            or source_counts["active_rows"] != item_metrics["active_present_rows"]
        ):
            raise RuntimeError(
                "full source/item reconciliation failed: "
                f"source={source_counts}, target={item_metrics}"
            )

    assessments = spark.table(TGT_ASSESSMENT).where(F.col("SOURCE_PRESENT_IND"))
    assessment_row = assessments.agg(
        F.count(F.lit(1)).alias("present_assessments"),
        F.countDistinct("DCP_FORMS_ACTIVITY_ID").alias("distinct_assessments"),
        F.sum("RESPONSE_ROW_COUNT").alias("header_response_rows"),
        F.sum(F.when(F.col("CONTEXT_CONFLICT_IND"), 1).otherwise(0)).alias(
            "context_conflicts"
        ),
    ).first().asDict()
    assessment_metrics = {
        key: int(value or 0) for key, value in assessment_row.items()
    }
    represented_items = present_items.where(
        F.col("DCP_FORMS_ACTIVITY_ID").isNotNull()
    )
    represented = int(
        represented_items.select("DCP_FORMS_ACTIVITY_ID").distinct().count()
    )
    represented_item_rows = int(represented_items.count())
    assessment_metrics["represented_source_activities"] = represented
    assessment_metrics["represented_item_rows"] = represented_item_rows
    if (
        assessment_metrics["present_assessments"]
        != assessment_metrics["distinct_assessments"]
        or assessment_metrics["present_assessments"] != represented
        or assessment_metrics["header_response_rows"] != represented_item_rows
    ):
        raise RuntimeError(
            "assessment reconciliation failed: "
            f"assessment={assessment_metrics}, item={item_metrics}"
        )

    ambiguity_rows = int(
        present_items.where(F.col("POWERFORM_MAPPING_AMBIGUOUS_IND")).count()
    )
    mapping_metrics = {
        "powerform_mapping_ambiguity_rows": ambiguity_rows,
        "resolved_question_rows": int(
            present_items.where(F.col("QUESTION_CONCEPT_ID").isNotNull()).count()
        ),
        "resolved_value_rows": int(
            present_items.where(F.col("VALUE_CONCEPT_ID").isNotNull()).count()
        ),
        "mapping_conflict_rows": int(
            present_items.where(
                F.col("QUESTION_MAPPING_CONFLICT_IND")
                | F.col("VALUE_MAPPING_CONFLICT_IND")
                | F.col("UNIT_MAPPING_CONFLICT_IND")
            ).count()
        ),
    }
    if ambiguity_rows and TARGET_SCHEMA.startswith("4_prod."):
        raise RuntimeError(
            f"approved PowerForm mappings are ambiguous for {ambiguity_rows} rows"
        )

    concept_refs = reduce(
        lambda left, right: left.unionByName(right),
        [
            present_items.select(F.col(name).alias("CONCEPT_ID"))
            for name in [
                "QUESTION_CONCEPT_ID",
                "VALUE_CONCEPT_ID",
                "UNIT_CONCEPT_ID",
            ]
        ],
    ).where(F.col("CONCEPT_ID").isNotNull()).distinct()
    invalid_resolved = int(
        concept_refs.alias("r")
        .join(
            F.broadcast(concept_dimension(versions[SRC_CONCEPT])).alias("c"),
            F.col("r.CONCEPT_ID") == F.col("c.CONCEPT_ID"),
            "left",
        )
        .where(
            ~F.coalesce(F.col("c.CONCEPT_VALID_STANDARD_IND"), F.lit(False))
        )
        .count()
    )
    mapping_metrics["invalid_resolved_concepts"] = invalid_resolved
    if invalid_resolved:
        raise RuntimeError(
            f"{invalid_resolved} resolved concepts are not current valid standard concepts"
        )

    coverage = coverage_report(present_items)
    if enforce_coverage:
        low = {
            kind: metrics
            for kind, metrics in coverage.items()
            if metrics["eligible"] > 0
            and metrics["coverage"] is not None
            and metrics["coverage"] < MIN_LANE_COVERAGE
        }
        if low:
            raise RuntimeError(
                f"canonical lane coverage below configured gate {MIN_LANE_COVERAGE}: {low}"
            )
    return {
        "item": item_metrics,
        "assessment": assessment_metrics,
        "coverage": coverage,
        "mapping": mapping_metrics,
    }


def create_current_views() -> None:
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {qname(ITEM_CURRENT_VIEW)} AS
        SELECT * FROM {qname(TGT_ITEM)} WHERE SOURCE_PRESENT_IND = true
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {qname(ASSESSMENT_CURRENT_VIEW)} AS
        SELECT * FROM {qname(TGT_ASSESSMENT)} WHERE SOURCE_PRESENT_IND = true
        """
    )


def process_candidate_batch(
    scope: DataFrame, versions: Dict[str, int]
) -> dict:
    keys = scope.select("DOC_RESPONSE_KEY").distinct()
    source_snapshot = read_snapshot(SRC_POWERFORM, versions[SRC_POWERFORM])
    source_rows = source_snapshot.join(keys, "DOC_RESPONSE_KEY", "left_semi")
    classified = classify_responses(source_rows)
    classification = classification_metrics(classified)
    enriched = enrich_with_canonical_lanes(classified, versions)
    mapped, mapping = apply_contextual_mappings(enriched, versions)
    if mapping.get("item_mapping_ambiguity_rows", 0) and TARGET_SCHEMA.startswith(
        "4_prod."
    ):
        raise RuntimeError(
            "approved PowerForm mapping ambiguity blocks production commit: "
            f"{mapping}"
        )
    prepared = prepare_item_rows(mapped, scope)
    item_merge = merge_item_rows(prepared)
    item_tombstones = tombstone_item_scope(
        keys, source_rows, "PHYSICAL_SOURCE_DISAPPEARANCE"
    )

    activity_scope = union_distinct(
        [
            scope.select("DCP_FORMS_ACTIVITY_ID"),
            classified.select("DCP_FORMS_ACTIVITY_ID"),
            spark.table(TGT_ITEM)
            .join(keys, "DOC_RESPONSE_KEY", "left_semi")
            .select("DCP_FORMS_ACTIVITY_ID"),
        ],
        ["DCP_FORMS_ACTIVITY_ID"],
    ).where(F.col("DCP_FORMS_ACTIVITY_ID").isNotNull())
    headers, header_profile = build_assessment_rows(activity_scope)
    assessment_merge = merge_assessment_rows(headers, activity_scope)

    map_candidate_metrics = {"operation": "skipped_empty"}
    if not is_empty(prepared):
        generated = generated_mapping_candidates(prepared)
        if not is_empty(generated):
            map_candidate_metrics = merge_mapping_candidates(
                generated, exact_full=False
            )
    return {
        "classification": classification,
        "mapping": mapping,
        "item_merge": item_merge,
        "item_tombstones": item_tombstones,
        "assessment_profile": header_profile,
        "assessment_merge": assessment_merge,
        "map_candidate_merge": map_candidate_metrics,
    }


def full_stage_names(run_id: str) -> dict:
    token = safe_token(run_id)
    prefix = f"{TARGET_SCHEMA}._powerform_full_{token}"
    return {
        "source": f"{prefix}_source",
        "NOMEN": f"{prefix}_nomen",
        "CODED": f"{prefix}_coded",
        "NUMERIC": f"{prefix}_numeric",
        "TEXT": f"{prefix}_text",
        "DATE": f"{prefix}_date",
    }


def maybe_create_full_backups(run_id: str) -> List[str]:
    if not CREATE_BACKUPS:
        return []
    created = []
    token = safe_token(run_id)
    for table_name, label in [(TGT_ITEM, "item"), (TGT_ASSESSMENT, "assessment")]:
        if not table_exists(table_name) or spark.table(table_name).limit(1).count() == 0:
            continue
        backup = f"{TARGET_SCHEMA}._powerform_backup_{label}_{token}"
        if not table_exists(backup):
            spark.sql(
                f"CREATE TABLE {qname(backup)} SHALLOW CLONE {qname(table_name)}"
            )
            created.append(backup)
    return created


def materialise_full_stages(
    versions: Dict[str, int], run_id: str
) -> Tuple[dict, dict]:
    names = full_stage_names(run_id)
    validation_property = "powerform.full_stage.validated"
    profile_property = "powerform.full_stage.profile_json"

    source_created = False
    if not table_exists(names["source"]):
        powerform_run_progress(
            run_id,
            "source_stage_write_started",
            table=names["source"],
            source_table=SRC_POWERFORM,
            source_version=int(versions[SRC_POWERFORM]),
            shard_count=FULL_BUILD_SHARDS,
        )
        classified = classify_responses(
            read_snapshot(SRC_POWERFORM, versions[SRC_POWERFORM])
        )
        staged_source = classified.withColumn(
            "SHARD_ID",
            F.pmod(
                F.xxhash64(
                    F.coalesce(
                        F.col("ELEMENT_EVENT_ID_LONG").cast("string"),
                        F.col("DOC_RESPONSE_KEY"),
                    )
                ),
                F.lit(FULL_BUILD_SHARDS),
            ).cast("int"),
        )
        (
            staged_source.repartition(FULL_BUILD_SHARDS, "SHARD_ID")
            .write.format("delta")
            .mode("error")
            .partitionBy("SHARD_ID")
            .saveAsTable(names["source"])
        )
        source_created = True
        set_table_comment(
            names["source"],
            f"Run-scoped fixed-version PowerForm source stage for {run_id}.",
        )
        powerform_run_progress(
            run_id,
            "source_stage_write_completed",
            table=names["source"],
        )

    source_stage = spark.table(names["source"])
    source_properties = table_properties(names["source"])
    profile = None
    if str(source_properties.get(validation_property, "false")).lower() == "true":
        raw_profile = source_properties.get(profile_property)
        if raw_profile:
            try:
                profile = json.loads(raw_profile)
            except Exception:
                profile = None

    if profile is None:
        powerform_run_progress(
            run_id,
            "source_stage_validation_started",
            table=names["source"],
            reused=not source_created,
        )
        profile = classification_metrics(source_stage)
        ensure_table_properties(
            names["source"],
            {
                validation_property: "true",
                "powerform.full_stage.run_id": run_id,
                "powerform.full_stage.kind": "source",
                profile_property: json.dumps(profile, sort_keys=True),
            },
        )
        powerform_run_progress(
            run_id,
            "source_stage_validation_completed",
            table=names["source"],
            rows=profile.get("rows"),
            distinct_keys=profile.get("distinct_keys"),
        )
    else:
        powerform_run_progress(
            run_id,
            "source_stage_reused",
            table=names["source"],
            rows=profile.get("rows"),
        )

    table_for_lane = {
        "NOMEN": SRC_NOMEN,
        "CODED": SRC_CODED,
        "NUMERIC": SRC_NUMERIC,
        "TEXT": SRC_TEXT,
        "DATE": SRC_DATE,
    }
    for lane_name in ["NOMEN", "CODED", "NUMERIC", "TEXT", "DATE"]:
        keys = ["_L_EVENT_ID"] + (
            ["_L_SEQUENCE_NBR"] if lane_name in {"NOMEN", "CODED"} else []
        )
        lane_table = names[lane_name]
        if table_exists(lane_table):
            lane_properties = table_properties(lane_table)
            if str(lane_properties.get(validation_property, "false")).lower() != "true":
                powerform_run_progress(
                    run_id,
                    "lane_stage_validation_started",
                    lane=lane_name,
                    table=lane_table,
                    reused=True,
                )
                assert_unique_non_null(
                    spark.table(lane_table), keys, f"full {lane_name} stage"
                )
                ensure_table_properties(
                    lane_table,
                    {
                        validation_property: "true",
                        "powerform.full_stage.run_id": run_id,
                        "powerform.full_stage.kind": lane_name,
                    },
                )
            powerform_run_progress(
                run_id,
                "lane_stage_reused",
                lane=lane_name,
                table=lane_table,
            )
            continue

        powerform_run_progress(
            run_id,
            "lane_stage_write_started",
            lane=lane_name,
            table=lane_table,
            source_table=table_for_lane[lane_name],
            source_version=int(versions[table_for_lane[lane_name]]),
        )
        lane_scope = F.col("CANONICAL_LANE") == lane_name
        if lane_name == "NUMERIC":
            lane_scope = F.col("CANONICAL_LANE").isin("NUMERIC", "TEXT")
        event_ids = (
            source_stage.where(lane_scope)
            .select(F.col("ELEMENT_EVENT_ID_LONG").alias("EVENT_ID"))
            .distinct()
        )
        projected = lane_projection(
            lane_name,
            versions[table_for_lane[lane_name]],
            event_ids,
            broadcast_ids=False,
        ).withColumn(
            "LANE_SHARD_ID",
            F.pmod(
                F.xxhash64(F.col("_L_EVENT_ID").cast("string")),
                F.lit(FULL_BUILD_SHARDS),
            ).cast("int"),
        )
        (
            projected.repartition(FULL_BUILD_SHARDS, "LANE_SHARD_ID")
            .write.format("delta")
            .mode("error")
            .partitionBy("LANE_SHARD_ID")
            .saveAsTable(lane_table)
        )
        set_table_comment(
            lane_table,
            f"Run-scoped narrow {lane_name} canonical lane stage for {run_id}.",
        )
        powerform_run_progress(
            run_id,
            "lane_stage_validation_started",
            lane=lane_name,
            table=lane_table,
            reused=False,
        )
        assert_unique_non_null(
            spark.table(lane_table), keys, f"full {lane_name} stage"
        )
        ensure_table_properties(
            lane_table,
            {
                validation_property: "true",
                "powerform.full_stage.run_id": run_id,
                "powerform.full_stage.kind": lane_name,
            },
        )
        powerform_run_progress(
            run_id,
            "lane_stage_write_completed",
            lane=lane_name,
            table=lane_table,
        )

    powerform_run_progress(
        run_id,
        "full_stage_materialisation_completed",
        stage_tables=names,
    )
    return names, profile

def ensure_full_shard_queue(run_id: str, source_stage: DataFrame) -> None:
    shard_ids = [
        int(row["SHARD_ID"])
        for row in source_stage.select("SHARD_ID").distinct().collect()
    ]
    if not shard_ids:
        raise RuntimeError("full source stage is empty")
    rows = [
        (
            run_id,
            f"__FULL_SHARD__{shard_id:06d}",
            None,
            None,
            "FULL_REBUILD_SHARD",
            shard_id,
            "pending",
            datetime.now(timezone.utc).replace(tzinfo=None),
            None,
        )
        for shard_id in shard_ids
    ]
    queue = spark.createDataFrame(rows, spark.table(CANDIDATE_TABLE).schema)
    write_candidate_rows(queue)


def pending_full_shards(run_id: str, limit: Optional[int] = None) -> List[int]:
    frame = (
        spark.table(CANDIDATE_TABLE)
        .where(
            (F.col("RUN_ID") == run_id)
            & (F.col("REASON") == "FULL_REBUILD_SHARD")
            & F.col("STATUS").isin("pending", "processing")
        )
        .select("SHARD_ID")
        .distinct()
        .orderBy("SHARD_ID")
    )
    if limit is not None:
        frame = frame.limit(limit)
    return [int(row["SHARD_ID"]) for row in frame.collect()]


def mark_full_shard(run_id: str, shard_id: int, status: str) -> None:
    assignments = {"STATUS": F.lit(status)}
    if status == "completed":
        assignments["PROCESSED_AT"] = F.current_timestamp()
    DeltaTable.forName(spark, CANDIDATE_TABLE).update(
        condition=(F.col("RUN_ID") == run_id)
        & (F.col("REASON") == "FULL_REBUILD_SHARD")
        & (F.col("SHARD_ID") == int(shard_id)),
        set=assignments,
    )


def mark_full_shards(run_id: str, shard_ids: Sequence[int], status: str) -> None:
    shard_ids = [int(shard_id) for shard_id in shard_ids]
    if not shard_ids:
        return
    assignments = {"STATUS": F.lit(status)}
    if status == "completed":
        assignments["PROCESSED_AT"] = F.current_timestamp()
    DeltaTable.forName(spark, CANDIDATE_TABLE).update(
        condition=(F.col("RUN_ID") == run_id)
        & (F.col("REASON") == "FULL_REBUILD_SHARD")
        & F.col("SHARD_ID").isin(shard_ids),
        set=assignments,
    )


def cleanup_full_stages(names: dict) -> None:
    for table_name in names.values():
        if table_exists(table_name):
            spark.sql(f"DROP TABLE {qname(table_name)}")


def run_full_rebuild(versions: Dict[str, int]) -> dict:
    powerform_run_progress(
        POWERFORM_RUN_ID,
        "full_rebuild_started",
        full_build_shards=FULL_BUILD_SHARDS,
        max_shards_per_run=MAX_SHARDS_PER_RUN,
    )
    if is_serverless_compute():
        override_is_small = bool(_source_overrides) and (
            read_snapshot(SRC_POWERFORM, versions[SRC_POWERFORM])
            .limit(5_000_001)
            .count()
            <= 5_000_000
        )
        if not override_is_small:
            raise RuntimeError(
                "full_rebuild requires classic compute; serverless is allowed only for "
                "a measured small isolated source override"
            )
    ensure_table_properties(TGT_ITEM, {"powerform.pipeline.complete": "false"})
    ensure_table_properties(TGT_ASSESSMENT, {"powerform.pipeline.complete": "false"})
    backups = maybe_create_full_backups(POWERFORM_RUN_ID)
    names, classification = materialise_full_stages(versions, POWERFORM_RUN_ID)
    source_stage = spark.table(names["source"])
    ensure_full_shard_queue(POWERFORM_RUN_ID, source_stage)
    selected = pending_full_shards(POWERFORM_RUN_ID, MAX_SHARDS_PER_RUN)
    powerform_run_progress(
        POWERFORM_RUN_ID,
        "shard_batch_selected",
        selected_shards=selected,
        max_shards_per_run=MAX_SHARDS_PER_RUN,
    )
    shard_metrics = []
    if selected:
        powerform_run_progress(
            POWERFORM_RUN_ID,
            "shard_batch_processing_started",
            shard_ids=selected,
            shard_count=len(selected),
        )
        mark_full_shards(POWERFORM_RUN_ID, selected, "processing")
        source_rows = source_stage.where(F.col("SHARD_ID").isin(selected)).drop(
            "SHARD_ID"
        )
        staged_lanes = {
            lane_name: spark.table(names[lane_name])
            .where(F.col("LANE_SHARD_ID").isin(selected))
            .drop("LANE_SHARD_ID")
            for lane_name in ["NOMEN", "CODED", "NUMERIC", "TEXT", "DATE"]
        }
        enriched = enrich_with_canonical_lanes(
            source_rows, versions, staged_lanes=staged_lanes
        )
        mapped, mapping = apply_contextual_mappings(enriched, versions)
        if mapping.get("item_mapping_ambiguity_rows", 0) and TARGET_SCHEMA.startswith(
            "4_prod."
        ):
            raise RuntimeError(
                f"approved map ambiguity in full shard batch {selected}: {mapping}"
            )
        item_metrics = merge_item_rows(prepare_item_rows(mapped))
        mark_full_shards(POWERFORM_RUN_ID, selected, "completed")
        shard_metrics.append(
            {"shard_ids": selected, "item": item_metrics, "mapping": mapping}
        )
        powerform_run_progress(
            POWERFORM_RUN_ID,
            "shard_batch_processing_completed",
            shard_ids=selected,
            shard_count=len(selected),
            item_metrics=item_metrics,
        )

    remaining = pending_full_shards(POWERFORM_RUN_ID)
    if remaining:
        return {
            "status": "partial",
            "classification": classification,
            "backups": backups,
            "processed_shards": selected,
            "remaining_shard_count": len(remaining),
            "next_shards": remaining[: min(10, len(remaining))],
            "shard_metrics": shard_metrics,
            "stage_tables": names,
        }

    powerform_run_progress(
        POWERFORM_RUN_ID,
        "item_reconciliation_started",
    )
    all_target_keys = spark.table(TGT_ITEM).select("DOC_RESPONSE_KEY")
    tombstones = tombstone_item_scope(
        all_target_keys, source_stage, "FULL_REBUILD_SOURCE_RECONCILIATION"
    )
    powerform_run_progress(
        POWERFORM_RUN_ID,
        "item_reconciliation_completed",
        tombstones=tombstones,
    )

    powerform_run_progress(
        POWERFORM_RUN_ID,
        "assessment_build_started",
    )
    all_activity_scope = union_distinct(
        [
            source_stage.select("DCP_FORMS_ACTIVITY_ID"),
            spark.table(TGT_ASSESSMENT).select("DCP_FORMS_ACTIVITY_ID"),
        ],
        ["DCP_FORMS_ACTIVITY_ID"],
    ).where(F.col("DCP_FORMS_ACTIVITY_ID").isNotNull())
    headers, header_profile = build_assessment_rows()
    assessment_merge = merge_assessment_rows(headers, all_activity_scope)
    powerform_run_progress(
        POWERFORM_RUN_ID,
        "assessment_build_completed",
        assessment_profile=header_profile,
        assessment_merge=assessment_merge,
    )

    powerform_run_progress(
        POWERFORM_RUN_ID,
        "mapping_candidate_build_started",
    )
    current_items = spark.table(TGT_ITEM).where(F.col("SOURCE_PRESENT_IND"))
    map_candidates = generated_mapping_candidates(current_items)
    map_merge = merge_mapping_candidates(map_candidates, exact_full=True)
    powerform_run_progress(
        POWERFORM_RUN_ID,
        "mapping_candidate_build_completed",
        mapping_merge=map_merge,
    )

    powerform_run_progress(
        POWERFORM_RUN_ID,
        "final_validation_started",
    )
    validation = validate_final_outputs(
        versions, source_snapshot=source_stage, enforce_coverage=True
    )
    powerform_run_progress(
        POWERFORM_RUN_ID,
        "final_validation_completed",
    )

    powerform_run_progress(
        POWERFORM_RUN_ID,
        "publication_started",
    )
    create_current_views()
    ensure_table_properties(TGT_ITEM, {"powerform.pipeline.complete": "true"})
    ensure_table_properties(TGT_ASSESSMENT, {"powerform.pipeline.complete": "true"})
    powerform_run_progress(
        POWERFORM_RUN_ID,
        "publication_completed",
    )
    return {
        "status": "success",
        "classification": classification,
        "backups": backups,
        "processed_shards": selected,
        "remaining_shard_count": 0,
        "shard_metrics": shard_metrics,
        "item_tombstones": tombstones,
        "assessment_profile": header_profile,
        "assessment_merge": assessment_merge,
        "map_candidate_merge": map_merge,
        "validation": validation,
        "stage_tables": names,
    }


def target_is_complete() -> bool:
    return (
        table_exists(TGT_ITEM)
        and table_exists(TGT_ASSESSMENT)
        and str(
            table_properties(TGT_ITEM).get("powerform.pipeline.complete", "false")
        ).lower()
        == "true"
        and str(
            table_properties(TGT_ASSESSMENT).get(
                "powerform.pipeline.complete", "false"
            )
        ).lower()
        == "true"
    )


def run_incremental_build(
    versions: Dict[str, int], previous: Dict[str, int]
) -> dict:
    if not target_is_complete():
        raise RuntimeError(
            "incremental mode requires a validated completed full_rebuild; "
            "target completion properties are not true"
        )
    candidates, change_info = incremental_candidates(versions, previous)
    if candidates is None or is_empty(candidates):
        return {
            "status": "noop",
            "change_info": change_info,
            "candidate_count": 0,
        }
    candidate_count = int(
        candidates.select("DOC_RESPONSE_KEY").distinct().count()
    )
    write_candidate_rows(candidates)
    if candidate_count > MAX_INCREMENTAL_CANDIDATES:
        raise RuntimeError(
            f"incremental candidate count {candidate_count:,} exceeds "
            f"max_incremental_candidates={MAX_INCREMENTAL_CANDIDATES:,}. "
            "Candidates remain pending; use a classic recode/full-rebuild recovery run."
        )
    mark_candidates(POWERFORM_RUN_ID, "processing")
    scope = candidate_scope(POWERFORM_RUN_ID)
    processing = process_candidate_batch(scope, versions)
    mark_candidates(POWERFORM_RUN_ID, "completed")
    post_validation = None
    if RUN_POST_CHECKS:
        post_validation = validate_final_outputs(
            versions, source_snapshot=None, enforce_coverage=False
        )
    create_current_views()
    return {
        "status": "success",
        "change_info": change_info,
        "candidate_count": candidate_count,
        "processing": processing,
        "post_validation": post_validation,
    }


def run_recode_build(versions: Dict[str, int]) -> dict:
    if not target_is_complete():
        raise RuntimeError("recode mode requires completed item and assessment targets")
    source = read_snapshot(SRC_POWERFORM, versions[SRC_POWERFORM]).select(
        "DOC_RESPONSE_KEY", "DCP_FORMS_ACTIVITY_ID", "ELEMENT_EVENT_ID_LONG"
    )
    candidates = candidate_frame(source, "EXPLICIT_RECODE")
    candidate_count = int(candidates.count())
    write_candidate_rows(candidates)
    if candidate_count > MAX_INCREMENTAL_CANDIDATES:
        raise RuntimeError(
            f"recode candidate count {candidate_count:,} exceeds the configured safe "
            f"single-run bound {MAX_INCREMENTAL_CANDIDATES:,}. Candidates remain pending; "
            "run a classic sharded full_rebuild rather than truncating the recode."
        )
    mark_candidates(POWERFORM_RUN_ID, "processing")
    processing = process_candidate_batch(candidate_scope(POWERFORM_RUN_ID), versions)
    mark_candidates(POWERFORM_RUN_ID, "completed")
    validation = (
        validate_final_outputs(versions, enforce_coverage=False)
        if RUN_POST_CHECKS
        else None
    )
    create_current_views()
    return {
        "status": "success",
        "candidate_count": candidate_count,
        "processing": processing,
        "post_validation": validation,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## Mode driver and auditable notebook result

# COMMAND ----------


EFFECTIVE_RUN_MODE = (
    "full_rebuild"
    if FORCE_FULL_REFRESH and RUN_MODE in {"incremental", "recode"}
    else RUN_MODE
)
RUN_STARTED = False
FINAL_RESULT = {
    "pipeline": PIPELINE_NAME,
    "run_id": POWERFORM_RUN_ID,
    "requested_mode": RUN_MODE,
    "effective_mode": EFFECTIVE_RUN_MODE,
    "write_environment": WRITE_ENVIRONMENT,
    "target_schema": TARGET_SCHEMA,
    "started_at": STARTED_AT,
    "preflight": PREFLIGHT,
}

try:
    if EFFECTIVE_RUN_MODE == "define_only":
        FINAL_RESULT.update(
            {
                "status": "defined",
                "message": (
                    "Control plane and combined mapping table are defined. "
                    "No item/header build was attempted."
                ),
                "target_versions": target_versions(),
            }
        )
    else:
        PREFLIGHT = preflight_report(strict=True)
        versions = captured_versions_for_run(
            POWERFORM_RUN_ID, EFFECTIVE_RUN_MODE
        )
        begin_run(POWERFORM_RUN_ID, EFFECTIVE_RUN_MODE, versions)
        RUN_STARTED = True
        powerform_run_progress(
            POWERFORM_RUN_ID,
            "target_initialisation_started",
            target_schema=TARGET_SCHEMA,
        )
        ensure_user_tables()
        powerform_run_progress(
            POWERFORM_RUN_ID,
            "target_initialisation_completed",
            target_schema=TARGET_SCHEMA,
        )
        previous = state_versions()

        if EFFECTIVE_RUN_MODE == "full_rebuild":
            execution = run_full_rebuild(versions)
        elif EFFECTIVE_RUN_MODE == "incremental":
            execution = run_incremental_build(versions, previous)
        elif EFFECTIVE_RUN_MODE == "recode":
            execution = run_recode_build(versions)
        elif EFFECTIVE_RUN_MODE == "validate":
            execution = {
                "status": "success",
                "validation": validate_final_outputs(
                    versions,
                    source_snapshot=read_snapshot(
                        SRC_POWERFORM, versions[SRC_POWERFORM]
                    ),
                    enforce_coverage=True,
                ),
            }
            create_current_views()
        else:
            raise ValueError(f"unsupported effective mode {EFFECTIVE_RUN_MODE}")

        status = execution["status"]
        if status in {"success", "noop"} and EFFECTIVE_RUN_MODE != "validate":
            write_state(versions, POWERFORM_RUN_ID)
        if status == "success" and EFFECTIVE_RUN_MODE == "full_rebuild":
            cleanup_full_stages(execution["stage_tables"])
        update_run(
            POWERFORM_RUN_ID,
            status,
            message=(
                "Run completed and checkpoints advanced."
                if status in {"success", "noop"}
                else "Full rebuild is resumable; rerun with the same pipeline_run_id."
            ),
            changed_ranges=execution.get("change_info", {}).get("ranges"),
            candidate_counts={
                "candidate_count": execution.get("candidate_count"),
                "remaining_shard_count": execution.get("remaining_shard_count"),
            },
            item_metrics=execution.get("processing", {}).get("item_merge")
            or execution.get("validation", {}).get("item"),
            assessment_metrics=execution.get("processing", {}).get(
                "assessment_merge"
            )
            or execution.get("validation", {}).get("assessment"),
            coverage=execution.get("validation", {}).get("coverage")
            or (execution.get("post_validation") or {}).get("coverage"),
            mapping=execution.get("processing", {}).get("mapping")
            or execution.get("validation", {}).get("mapping"),
            target_versions_after=target_versions(),
        )
        FINAL_RESULT.update(
            {
                "status": status,
                "fixed_versions": versions,
                "previous_versions": previous,
                "execution": execution,
                "target_versions": target_versions(),
            }
        )
except Exception as exc:
    if not RUN_STARTED and table_exists(RUNS_TABLE):
        try:
            diagnostic_versions = {
                key: int(value)
                for key, value in PREFLIGHT.get("versions", {}).items()
            }
            begin_run(
                POWERFORM_RUN_ID,
                f"{EFFECTIVE_RUN_MODE}_preflight",
                diagnostic_versions,
            )
            RUN_STARTED = True
        except Exception as preflight_audit_exc:
            print(f"Failed to begin preflight failure audit: {preflight_audit_exc}")
    if RUN_STARTED:
        try:
            update_run(
                POWERFORM_RUN_ID,
                "failed",
                message="PowerForm canonical run failed closed.",
                error=exc,
                target_versions_after=target_versions(),
            )
        except Exception as audit_exc:
            print(f"Failed to audit run failure: {audit_exc}")
    FINAL_RESULT.update(
        {
            "status": "failed",
            "error_class": type(exc).__name__,
            "error_message": str(exc),
            "target_versions": target_versions(),
        }
    )
    print(bronze_json(FINAL_RESULT))
    raise

print(bronze_json(FINAL_RESULT))
dbutils.notebook.exit(bronze_json(FINAL_RESULT))











