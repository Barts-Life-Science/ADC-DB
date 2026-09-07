# Databricks notebook source
# Root fix v3: resolve classifiable mill_* rows when evidence exists, but prefer
# raw completeness over indefinite staging retention. Valid-key rows that remain
# unresolved after the configured retry cap are audited and landed with NULL
# Trust. Null merge-key rows retain the abandoned-queue policy because they
# cannot be upserted safely. Non-mill sources remain outside this classifier.

import hashlib
import json
import re
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone
from functools import reduce
from concurrent.futures import ThreadPoolExecutor, as_completed

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType

# COMMAND ----------

def _widget(name, default, choices=None):
    try:
        if choices:
            dbutils.widgets.dropdown(name, default, choices)
        else:
            dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name)
    except Exception:
        return default


TARGET_ENV = _widget("target_env", "prod", ["prod", "dev"]).lower()
DRY_RUN = _widget("dry_run", "false", ["false", "true"]).lower() == "true"
TABLE_FILTER = _widget("table_filter", "").strip().lower()
BACKFILL_PENDING_LOOKUPS = (
    _widget("backfill_pending_lookups", "true", ["true", "false"]).lower() == "true"
)
MAX_CLASSIFICATION_ATTEMPTS = int(_widget("max_classification_attempts", "15"))
MATERIALIZE_MIN_ROWS = int(_widget("materialize_min_rows", "10000"))
VALIDATE_TARGET_UNIQUENESS = (
    _widget("validate_target_uniqueness", "false", ["false", "true"]).lower() == "true"
)
MAX_PARALLEL_TABLES = max(1, int(_widget("max_parallel_tables", "4")))
BOOTSTRAP_EXISTING = (
    _widget("bootstrap_existing", "false", ["false", "true"]).lower() == "true"
)

INCLUDE_LOOKUP_PASSTHROUGH = (
    _widget(
        "include_lookup_passthrough",
        "true" if TARGET_ENV == "prod" else "false",
        ["true", "false"],
    ).lower() == "true"
)

CATALOG = "4_prod" if TARGET_ENV == "prod" else "8_dev"
RAW_SCHEMA = "raw"
STAGING_SCHEMA = "staging"
TMP_SCHEMA = "tmp"
ABANDONED_SCHEMA = "staging_abandoned"
EXCLUDED_SCHEMA = "staging_excluded"
LOG_CATALOG = "6_mgmt" if TARGET_ENV == "prod" else "8_dev"
LOG_SCHEMA = "logs" if TARGET_ENV == "prod" else "trust_classification"

RUN_ID = str(uuid.uuid4())
RUN_TOKEN = RUN_ID.replace("-", "")[:16]
RUN_STARTED_AT = datetime.now(timezone.utc)
SCRATCH_TABLES = []
MAPPING_LOCK = threading.Lock()
RUN_DEPENDENCIES = {}

TRUST_MAP_TBL = f"{CATALOG}.{TMP_SCHEMA}.org_to_trust_map"
POLICY_TBL = f"{CATALOG}.{TMP_SCHEMA}.trust_table_policy"
ENC_ORG_TBL = f"{CATALOG}.{TMP_SCHEMA}.sw_enc_org_map"
ENC_ORG_SOURCE_TABLE = "mill_encounter_org"
HUB_TBL = f"{CATALOG}.{TMP_SCHEMA}.sw_mapping_hub"
CHANGED_ENC_TBL = f"{CATALOG}.{TMP_SCHEMA}.sw_changed_encounters"
CONTROL_TBL = f"{CATALOG}.{TMP_SCHEMA}.incr_updt_trust_control"
CLASSIFICATION_CONTROL_TBL = f"{CATALOG}.{TMP_SCHEMA}.trust_classification_control_v3"
FLAG_TBL = f"{LOG_CATALOG}.{LOG_SCHEMA}.organization_flags"
RUN_LOG_TBL = f"{LOG_CATALOG}.{LOG_SCHEMA}.trust_classification_run_log_v2"
TABLE_LOG_TBL = f"{LOG_CATALOG}.{LOG_SCHEMA}.trust_classification_table_log_v2"

LOOKUP_CATALOG = "3_lookup"
LOOKUP_STAGING_SCHEMA = "staging"
LOOKUP_TARGET_SCHEMA = "mill"

# These tables are global reference/master entities. Their own identifier is
# not evidence that every row belongs to a trust, so unresolved rows must not
# be held back or abandoned.
GLOBAL_PASSTHROUGH_TABLES = {
    "mill_organization",
    # Patient tracking/location reference tables have no patient-level trust route.
    "mill_bed", "mill_nurse_unit", "mill_room",
    "mill_pm_loc_attrib", "mill_pm_loc_attrib_hist",
    "mill_track_event", "mill_track_group",
    # Minimal unfiltered ENCNTR_ID -> ORGANIZATION_ID resolver feed.
    "mill_encounter_org",
}

# Pass-through updates must not erase enrichment already present in a target.
# New rows naturally receive NULL for target-only enrichment columns.
PASSTHROUGH_PRESERVE_TARGET_COLUMNS = {
    "mill_organization": {"TRUST", "ENCNTR_ID"},
}

PAYLOAD_CHANGE_ONLY_TABLES = {
      "path_patient_resultlevel": {"TFCResultSeq", "ADC_PAYLOAD_HASH"},
      "path_patient_samplelevel": set(),
  }



CANONICAL_ORG_TRUST = {
    873843: "Barts", 8367658: "Barts", 669849: "Barts", 9073614: "Barts",
    2681833: "Barts", 4401825: "Barts", 3203824: "Barts", 2681830: "Barts",
    8061679: "Barts", 669848: "Barts", 8467812: "Barts", 2681824: "Barts",
    2619824: "Barts", 2681827: "Barts", 3203825: "Barts", 691988: "Barts",
    3125827: "Barts", 8061682: "Barts", 8061694: "Barts", 2641824: "Barts",
    2641827: "Barts", 669847: "Barts", 8056759: "Barts", 8061685: "Barts",
    2641830: "Barts", 3201824: "Barts", 691989: "Barts", 669845: "Barts",
    669843: "Barts", 8061691: "Barts", 669846: "Barts", 3199824: "Barts",
    669850: "Barts", 6333825: "Barts", 669844: "Barts", 8397458: "Barts",
    8152502: "Barts", 671843: "Barts", 613843: "Barts",
    9161976: "BHRUT", 9163579: "BHRUT", 9161983: "BHRUT", 723896: "BHRUT",
    9161987: "BHRUT", 9161988: "BHRUT", 9163583: "BHRUT", 9161989: "BHRUT",
}

# This is the maintenance notebook's former protection policy, now used by both
# notebooks through POLICY_TBL. BHRUT rows outside this set are archived to
# staging_excluded before removal from staging/raw.
KEEP_BHRUT_TABLES = {
    "mill_person", "mill_address", "mill_person_alias", "mill_person_name",
    "mill_person_patient", "mill_person_info", "mill_person_org_reltn",
    "mill_person_person_reltn", "mill_person_prsnl_reltn", "mill_prsnl",
    "mill_prsnl_alias", "mill_prsnl_org_reltn", "mill_prsnl_reltn",
    "mill_encounter_org",
}

# Used only when staging history is unavailable. The normal path discovers the
# exact key used by the upstream IncrUpdtV2 staging MERGE.
KEY_OVERRIDES = {
    # Disposable 8_dev smoke fixture; the production catalog has no such table.
    "mill_trust_repair_fixture": ["RECORD_ID"],
    "mill_trust_long_text_fixture": ["LONG_TEXT_ID"],
    "mill_order_detail": ["ORDER_ID", "ACTION_SEQUENCE", "DETAIL_SEQUENCE"],
    "mill_order_comment": ["ORDER_ID", "ACTION_SEQUENCE", "COMMENT_TYPE_CD"],
    "mill_order_ingredient": ["ORDER_ID", "ACTION_SEQUENCE", "COMP_SEQUENCE"],
    "mill_episode_encntr_reltn": ["EPISODE_ENCNTR_RELTN_ID"],
    "mill_sch_location": ["SCHEDULE_ID"],
    "mill_sn_surg_case_proc_doc": ["SURG_CASE_PROC_DOC_ID"],
    "mill_ce_blob": ["EVENT_ID", "BLOB_SEQ_NUM"],
}

LOOKUP_SPECS = {
    "SURG_CASE_ID": ("mill_surgical_case", "SURG_CASE_ID"),
    "SURG_CASE_PROC_ID": ("mill_surg_case_procedure", "SURG_CASE_PROC_ID"),
    "DCP_FORMS_ACTIVITY_ID": ("mill_dcp_forms_activity", "DCP_FORMS_ACTIVITY_ID"),
    "EPISODE_ID": ("mill_episode_encntr_reltn", "EPISODE_ID"),
    "ORDER_ID": ("mill_orders", "ORDER_ID"),
    "PROBLEM_ID": ("mill_problem", "PROBLEM_ID"),
    "SCH_EVENT_ID": ("mill_sch_event_patient", "SCH_EVENT_ID"),
    "SCHEDULE_ID": ("mill_sch_schedule", "SCHEDULE_ID"),
    "IM_STUDY_ID": ("mill_im_study", "IM_STUDY_ID"),
    "IM_ACQUIRED_STUDY_ID": ("mill_im_acquired_study", "IM_ACQUIRED_STUDY_ID"),
    "CV_PROC_ID": ("mill_cv_proc", "CV_PROC_ID"),
    "EVENT_ID": ("mill_clinical_event", "EVENT_ID"),
    "REFERRAL_ID": ("mill_referral", "REFERRAL_ID"),
    "PM_WAIT_LIST_ID": ("mill_pm_wait_list", "PM_WAIT_LIST_ID"),
    "TRACKING_ID": ("mill_tracking_item", "TRACKING_ID"),
}

# Tables whose encounter evidence lives in differently-named column(s), tried in
# order (first non-null, non-zero wins). Used by the direct-encounter route, the
# changed-encounter candidate scan, and the hub build.
DIRECT_ENC_COLUMN_OVERRIDES = {
    "mill_referral": ["OUTBOUND_ENCNTR_ID"],
    "mill_referral_hist": ["OUTBOUND_ENCNTR_ID"],
    "mill_problem": ["ORIGINATING_ENCNTR_ID", "UPDATE_ENCNTR_ID"],
    "mill_cds_batch_content_hist": ["ENCOUNTER_ID"],
    "mill_tracking_prearrival": ["ATTACHED_ENCNTR_ID"],
}

REVERSE_REFERENCE_SPECS = {
    "mill_long_text": {
        "target_key": "LONG_TEXT_ID",
        "sources": [
            ("mill_ce_string_result", "STRING_LONG_TEXT_ID"),
            ("mill_clinical_event", "MODIFIER_LONG_TEXT_ID"),
            ("mill_encntr_info", "LONG_TEXT_ID"),
            ("mill_order_comment", "LONG_TEXT_ID"),
            ("mill_order_ingredient", "DOSE_CALCULATOR_LONG_TEXT_ID"),
            ("mill_pm_wait_list", "COMMENT_LONG_TEXT_ID"),
            ("mill_pm_wait_list_future_status", "COMMENT_LONG_TEXT_ID"),
            ("mill_pm_wait_list_status", "COMMENT_LONG_TEXT_ID"),
            ("mill_procedure", "LONG_TEXT_ID"),
            ("mill_rx_med_request", "REQUEST_REASON_LONG_TEXT_ID"),
            ("mill_rx_med_request", "RX_REASON_LONG_TEXT_ID"),
        ],
    },
}
if TARGET_ENV == "dev":
    REVERSE_REFERENCE_SPECS["mill_trust_long_text_fixture"] = {
        "target_key": "LONG_TEXT_ID",
        "sources": [("mill_trust_long_text_ref_fixture", "LONG_TEXT_ID")],
    }

SOURCE_PRIORITY = [
    "mill_encounter_org",
    "mill_encounter",
    "mill_tracking_item",
    "mill_surgical_case",
    "mill_surg_case_procedure",
    "mill_dcp_forms_activity",
    "mill_episode_encntr_reltn",
    "mill_orders",
    "mill_problem",
    "mill_sch_event_patient",
    "mill_sch_schedule",
    "mill_cv_proc",
    "mill_im_study",
    "mill_im_acquired_study",
    "mill_clinical_event",
    "mill_referral",
    "mill_pm_wait_list",
    "mill_long_text",
]

spark.sql(f"USE CATALOG {CATALOG}")

print("=" * 88)
print("TRUST CLASSIFICATION INCREMENTAL - PASSTHROUGH")
print(f"run_id={RUN_ID} env={TARGET_ENV} dry_run={DRY_RUN}")
print("=" * 88)

# COMMAND ----------

def table_exists(fqn):
    return spark.catalog.tableExists(fqn)


def table_columns(fqn):
    return spark.table(fqn).columns


def upper_map(columns):
    return {c.upper(): c for c in columns}


def nonzero_long(column):
    return F.when(
        column.isNotNull() & (column.cast("long") != 0),
        column.cast("long"),
    )


def sql_string(value):
    return str(value).replace("'", "''")


def latest_metrics(fqn, operation=None):
    rows = spark.sql(f"DESCRIBE HISTORY {fqn} LIMIT 20").collect()
    for row in rows:
        if operation is None or row["operation"] == operation:
            return dict(row["operationMetrics"] or {})
    return {}


def metric_int(metrics, name):
    try:
        return int((metrics or {}).get(name, 0) or 0)
    except (TypeError, ValueError):
        return 0


def best_order_columns(columns):
    cu = upper_map(columns)
    preferred = [
        "VALID_UNTIL_DT_TM", "VALID_FROM_DT_TM", "LAST_UTC_TS",
        "ADC_UPDT", "UPDT_DT_TM", "_STAGED_AT",
    ]
    result = [cu[c] for c in preferred if c in cu]
    return result


def key_condition(left_alias, right_alias, keys):
    conditions = [
        F.col(f"{left_alias}.{k}").eqNullSafe(F.col(f"{right_alias}.{k}"))
        for k in keys
    ]
    return reduce(lambda a, b: a & b, conditions)


def any_null_key(keys, alias=None):
    cols = [F.col(f"{alias}.{k}" if alias else k).isNull() for k in keys]
    return reduce(lambda a, b: a | b, cols)


def row_fingerprint(columns, prefix=""):
    normalized = [
        F.coalesce(
            F.col(f"{prefix}`{column}`").cast("string"),
            F.lit("<NULL>"),
        ).alias(column)
        for column in columns
    ]
    return F.sha2(
        F.to_json(F.struct(*normalized), {"ignoreNullFields": "false"}),
        256,
    )


def actual_table_count(fqn):
    return spark.table(fqn).count()


def write_run_log(status, table_count=0, failed_count=0, notes=None):
    if DRY_RUN:
        print(
            f"DRY RUN: run_log status={status} tables={table_count} "
            f"failures={failed_count}"
        )
        return
    values = [(RUN_ID, RUN_STARTED_AT, datetime.now(timezone.utc), TARGET_ENV, status,
               int(table_count), int(failed_count), notes)]
    schema = "run_id string, started_at timestamp, ended_at timestamp, target_env string, status string, table_count long, failed_count long, notes string"
    spark.createDataFrame(values, schema=schema).write.mode("append").saveAsTable(RUN_LOG_TBL)


def write_table_log(
    table_name,
    status,
    merge_keys,
    staged_rows,
    resolved_keys=0,
    promoted_keys=0,
    excluded_keys=0,
    abandoned_keys=0,
    remaining_rows=0,
    error_message=None,
):
    if DRY_RUN:
        print(
            f"DRY RUN: table_log table={table_name} status={status} "
            f"staged={staged_rows} promoted={promoted_keys} "
            f"remaining={remaining_rows}"
        )
        return
    values = [(
        RUN_ID, datetime.now(timezone.utc), table_name, status,
        json.dumps(merge_keys), int(staged_rows), int(resolved_keys),
        int(promoted_keys), int(excluded_keys), int(abandoned_keys),
        int(remaining_rows), error_message,
    )]
    schema = (
        "run_id string, logged_at timestamp, table_name string, status string, "
        "merge_keys string, staged_rows long, resolved_keys long, promoted_keys long, "
        "excluded_keys long, abandoned_keys long, remaining_rows long, error_message string"
    )
    spark.createDataFrame(values, schema=schema).write.mode("append").saveAsTable(TABLE_LOG_TBL)


def ensure_column(fqn, name, data_type):
    if name.upper() not in upper_map(table_columns(fqn)):
        spark.sql(f"ALTER TABLE {fqn} ADD COLUMNS ({name} {data_type})")


def ensure_setup():
    if DRY_RUN:
        required_tables = [
            TRUST_MAP_TBL,
            POLICY_TBL,
            ENC_ORG_TBL,
            HUB_TBL,
            CHANGED_ENC_TBL,
            CONTROL_TBL,
            CLASSIFICATION_CONTROL_TBL,
            FLAG_TBL,
            RUN_LOG_TBL,
            TABLE_LOG_TBL,
        ]
        missing = [fqn for fqn in required_tables if not table_exists(fqn)]
        if missing:
            raise ValueError(
                "DRY RUN requires existing pipeline tables; missing: "
                + ", ".join(missing)
            )
        print("DRY RUN: setup verified; no schemas or tables were mutated")
        return

    for schema in [TMP_SCHEMA, STAGING_SCHEMA, ABANDONED_SCHEMA, EXCLUDED_SCHEMA]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LOG_CATALOG}.{LOG_SCHEMA}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TRUST_MAP_TBL} (
            organization_id BIGINT,
            trust STRING,
            updated_at TIMESTAMP
        ) USING DELTA
    """)
    ensure_column(TRUST_MAP_TBL, "updated_at", "TIMESTAMP")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {POLICY_TBL} (
            table_name STRING,
            keep_bhrut BOOLEAN,
            updated_at TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ENC_ORG_TBL} (
            ENCNTR_ID BIGINT,
            ORGANIZATION_ID BIGINT,
            updated_at TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {HUB_TBL} (
            key_type STRING,
            key_id BIGINT,
            ENCNTR_ID BIGINT,
            updated_at TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CHANGED_ENC_TBL} (
            ENCNTR_ID BIGINT,
            change_type STRING,
            detected_at TIMESTAMP
        ) USING DELTA
    """)
    for name, typ in [
        ("old_organization_id", "BIGINT"),
        ("new_organization_id", "BIGINT"),
        ("run_id", "STRING"),
        ("processed_at", "TIMESTAMP"),
    ]:
        ensure_column(CHANGED_ENC_TBL, name, typ)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_TBL} (
            table_name STRING,
            checkpoint_key STRING,
            last_version BIGINT,
            last_timestamp TIMESTAMP,
            updated_at TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CLASSIFICATION_CONTROL_TBL} (
            table_name STRING,
            staging_version BIGINT,
            staged_at_watermark TIMESTAMP,
            enc_org_watermark TIMESTAMP,
            hub_watermark TIMESTAMP,
            trust_map_hash STRING,
            run_id STRING,
            updated_at TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FLAG_TBL} (
            organization_id BIGINT,
            organization_name STRING,
            flag_type STRING,
            flag_reason STRING,
            first_seen TIMESTAMP,
            last_seen TIMESTAMP,
            resolved BOOLEAN,
            resolved_at TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {RUN_LOG_TBL} (
            run_id STRING,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            target_env STRING,
            status STRING,
            table_count BIGINT,
            failed_count BIGINT,
            notes STRING
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_LOG_TBL} (
            run_id STRING,
            logged_at TIMESTAMP,
            table_name STRING,
            status STRING,
            merge_keys STRING,
            staged_rows BIGINT,
            resolved_keys BIGINT,
            promoted_keys BIGINT,
            excluded_keys BIGINT,
            abandoned_keys BIGINT,
            remaining_rows BIGINT,
            error_message STRING
        ) USING DELTA
    """)

    trust_rows = [
        (int(key), value, datetime.now(timezone.utc))
        for key, value in CANONICAL_ORG_TRUST.items()
    ]
    trust_df = spark.createDataFrame(
        trust_rows, "organization_id long, trust string, updated_at timestamp"
    )
    (
        DeltaTable.forName(spark, TRUST_MAP_TBL)
        .alias("t")
        .merge(
            trust_df.alias("s"),
            F.col("t.organization_id") == F.col("s.organization_id"),
        )
        .whenMatchedUpdate(
            condition=~F.col("t.trust").eqNullSafe(F.col("s.trust")),
            set={"trust": F.col("s.trust"), "updated_at": F.col("s.updated_at")},
        )
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )

    all_policy_tables = sorted(KEEP_BHRUT_TABLES)
    policy_df = spark.createDataFrame(
        [(name, True, datetime.now(timezone.utc)) for name in all_policy_tables],
        "table_name string, keep_bhrut boolean, updated_at timestamp",
    )
    (
        DeltaTable.forName(spark, POLICY_TBL)
        .alias("t")
        .merge(
            policy_df.alias("s"),
            F.col("t.table_name") == F.col("s.table_name"),
        )
        .whenMatchedUpdate(
            condition=~F.col("t.keep_bhrut").eqNullSafe(F.col("s.keep_bhrut")),
            set={
                "keep_bhrut": F.col("s.keep_bhrut"),
                "updated_at": F.col("s.updated_at"),
            },
        )
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )


def discover_merge_keys(table_name, staging_fqn):
    cols = table_columns(staging_fqn)
    cu = upper_map(cols)

    try:
        history = spark.sql(f"DESCRIBE HISTORY {staging_fqn} LIMIT 200")
        predicates = (
            history.filter(F.col("operation") == "MERGE")
            .orderBy(F.col("version").desc())
            .select(F.col("operationParameters").getItem("predicate").alias("predicate"))
            .collect()
        )
        for row in predicates:
            predicate = row["predicate"] or ""
            names = re.findall(r"([A-Za-z_][A-Za-z0-9_]*)#[0-9]+L?", predicate)
            keys = []
            for name in names:
                upper = name.upper()
                if upper in cu and upper not in keys:
                    keys.append(upper)
            if keys:
                return [cu[k] for k in keys]
    except Exception as exc:
        print(f"  Key-history warning for {table_name}: {type(exc).__name__}: {exc}")

    override = KEY_OVERRIDES.get(table_name.lower())
    if override and all(k.upper() in cu for k in override):
        return [cu[k.upper()] for k in override]

    raise ValueError(
        f"No authoritative merge key found for {table_name}. "
        "Add it to KEY_OVERRIDES; heuristic keys are intentionally disabled."
    )


def ensure_raw_target(table_name, staging_fqn, raw_fqn):
    staging_cols = table_columns(staging_fqn)
    business_cols = [
        c for c in staging_cols
        if c.upper() not in {"_STAGED_AT", "_CLASSIFICATION_ATTEMPTS"}
    ]

    if not table_exists(raw_fqn):
        if DRY_RUN:
            print(f"  DRY RUN: would create {raw_fqn}")
            return business_cols
        spark.table(staging_fqn).select(*business_cols).limit(0).write.format("delta").saveAsTable(raw_fqn)

    raw_cols = table_columns(raw_fqn)
    raw_upper = upper_map(raw_cols)
    staging_types = {f.name.upper(): f.dataType.simpleString() for f in spark.table(staging_fqn).schema.fields}

    for enrichment in ["TRUST", "ENCNTR_ID", "ORGANIZATION_ID"]:
        if enrichment in upper_map(staging_cols) and enrichment not in raw_upper:
            if DRY_RUN:
                print(f"  DRY RUN: would add {enrichment} to {raw_fqn}")
            else:
                spark.sql(f"ALTER TABLE {raw_fqn} ADD COLUMNS ({enrichment} {staging_types[enrichment]})")

    raw_cols = table_columns(raw_fqn)
    raw_upper = upper_map(raw_cols)
    missing_business = [
        c for c in business_cols
        if c.upper() not in raw_upper and c.upper() not in {"TRUST", "ENCNTR_ID", "ORGANIZATION_ID"}
    ]
    if missing_business:
        raise ValueError(f"Raw target {raw_fqn} is missing business columns: {missing_business}")
    return raw_cols


def merge_unique(
    target_fqn,
    source_df,
    keys,
    update_columns=None,
    known_nonempty=False,
    source_is_unique=False,
    compare_columns=None,
):
    if not known_nonempty and source_df.limit(1).count() == 0:
        return {"source_rows": 0}

    merge_source = source_df
    if not source_is_unique:
        order_columns = best_order_columns(source_df.columns)
        if order_columns:
            source_window = Window.partitionBy(*keys).orderBy(
                *[F.col(column).desc_nulls_last() for column in order_columns]
            )
            merge_source = (
                source_df.withColumn("_merge_source_rn", F.row_number().over(source_window))
                .filter(F.col("_merge_source_rn") == 1)
                .drop("_merge_source_rn")
            )
        else:
            merge_source = source_df.dropDuplicates(keys)

    # This audit is deliberately optional. Running it for every table performs a
    # second full target scan before the Delta MERGE and was the dominant cost on
    # large raw tables and on the shared mapping hub. The standalone duplicate
    # repair notebook certifies/repairs legacy collisions before normal runs.
    if VALIDATE_TARGET_UNIQUENESS and table_exists(target_fqn):
        source_keys = merge_source.select(*keys).dropDuplicates()
        target_duplicates = (
            spark.table(target_fqn)
            .join(source_keys, keys, "inner")
            .groupBy(*keys)
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )
        if target_duplicates:
            raise ValueError(
                f"Target {target_fqn} already contains duplicate rows on {keys}. "
                "Repair or rebuild those keys before classification is resumed."
            )

    if DRY_RUN:
        return {"source_rows": merge_source.count(), "dry_run": True}

    target_cols = table_columns(target_fqn)
    target_upper = upper_map(target_cols)
    source_upper = upper_map(merge_source.columns)
    common = [target_upper[u] for u in target_upper if u in source_upper]
    update_cols = update_columns or [c for c in common if c.upper() not in {k.upper() for k in keys}]
    update_map = {c: F.col(f"s.{source_upper[c.upper()]}") for c in update_cols}
    insert_map = {c: F.col(f"s.{source_upper[c.upper()]}") for c in common}

    compared = compare_columns or update_cols
    differences = [
        ~F.col(f"t.`{column}`").eqNullSafe(
            F.col(f"s.`{source_upper[column.upper()]}`")
        )
        for column in compared
        if column.upper() in source_upper
    ]
    changed_condition = (
        reduce(lambda left, right: left | right, differences)
        if differences
        else F.lit(False)
    )
    (
        DeltaTable.forName(spark, target_fqn)
        .alias("t")
        .merge(merge_source.alias("s"), key_condition("t", "s", keys))
        .whenMatchedUpdate(condition=changed_condition, set=update_map)
        .whenNotMatchedInsert(values=insert_map)
        .execute()
    )
    return latest_metrics(target_fqn, "MERGE")


def payload_compare_columns(table_name, target_cols, source_cols, keys):
      """Columns whose change should trigger a raw update, for opted-in tables.

      Excludes ADC_UPDT (restamped by the landing job on every run) plus whatever
      the table lists in PAYLOAD_CHANGE_ONLY_TABLES. Returns None for every other
      table, which leaves merge_unique on its default full-column comparison.
      """
      extra = PAYLOAD_CHANGE_ONLY_TABLES.get(table_name.lower())
      if extra is None:
          return None
      source_upper = {c.upper() for c in source_cols}
      key_upper = {k.upper() for k in keys}
      excluded = {"ADC_UPDT", "_STAGED_AT", "_CLASSIFICATION_ATTEMPTS"}
      excluded |= {c.upper() for c in extra}
      compared = [
          c for c in target_cols
          if c.upper() in source_upper
          and c.upper() not in key_upper
          and c.upper() not in excluded
      ]
      return compared or None



def append_encounter_changes(changes_df):
    if changes_df.limit(1).count() == 0 or DRY_RUN:
        return
    out = (
        changes_df.select(
            F.col("ENCNTR_ID").cast("long"),
            F.lit("organization_remap").alias("change_type"),
            F.current_timestamp().alias("detected_at"),
            F.col("old_organization_id").cast("long"),
            F.col("new_organization_id").cast("long"),
            F.lit(RUN_ID).alias("run_id"),
            F.lit(None).cast("timestamp").alias("processed_at"),
        )
        .dropDuplicates(["ENCNTR_ID", "old_organization_id", "new_organization_id", "run_id"])
    )
    out.write.mode("append").saveAsTable(CHANGED_ENC_TBL)


def merge_encounter_map(source_df, track_changes=True):
    source = (
        source_df.select(
            F.col("ENCNTR_ID").cast("long").alias("ENCNTR_ID"),
            F.col("ORGANIZATION_ID").cast("long").alias("ORGANIZATION_ID"),
        )
        .filter((F.col("ENCNTR_ID").isNotNull()) & (F.col("ENCNTR_ID") != 0))
        .dropDuplicates(["ENCNTR_ID"])
    )
    if source.limit(1).count() == 0:
        return

    existing = spark.table(ENC_ORG_TBL).select(
        "ENCNTR_ID", F.col("ORGANIZATION_ID").alias("old_organization_id")
    )
    compared = source.join(existing, "ENCNTR_ID", "left")
    changes = compared.filter(
        F.col("old_organization_id").isNotNull()
        & ~F.col("old_organization_id").eqNullSafe(F.col("ORGANIZATION_ID"))
    ).select(
        "ENCNTR_ID",
        "old_organization_id",
        F.col("ORGANIZATION_ID").alias("new_organization_id"),
    )
    if track_changes:
        append_encounter_changes(changes)

    source = source.withColumn("updated_at", F.current_timestamp())
    merge_unique(
        ENC_ORG_TBL,
        source,
        ["ENCNTR_ID"],
        ["ORGANIZATION_ID", "updated_at"],
        known_nonempty=True,
        source_is_unique=True,
        compare_columns=["ORGANIZATION_ID"],
    )


def merge_hub(source_df, key_type):
    source = (
        source_df.select(
            F.lit(key_type).alias("key_type"),
            F.col("key_id").cast("long").alias("key_id"),
            F.col("ENCNTR_ID").cast("long").alias("ENCNTR_ID"),
        )
        .filter(
            F.col("key_id").isNotNull()
            & (F.col("key_id") != 0)
            & F.col("ENCNTR_ID").isNotNull()
            & (F.col("ENCNTR_ID") != 0)
        )
        .dropDuplicates(["key_type", "key_id"])
        .withColumn("updated_at", F.current_timestamp())
    )
    merge_unique(
        HUB_TBL,
        source,
        ["key_type", "key_id"],
        ["ENCNTR_ID", "updated_at"],
        source_is_unique=True,
        compare_columns=["ENCNTR_ID"],
    )


def raw_lookup_mapping(key_type, needed_keys):
    source_table, source_key = LOOKUP_SPECS[key_type]
    source_fqn = f"{CATALOG}.{RAW_SCHEMA}.{source_table}"
    if not table_exists(source_fqn):
        return None

    needed = needed_keys.select(F.col(key_type).alias("needed_key")).dropDuplicates()
    original_source_columns = table_columns(source_fqn)
    src = spark.table(source_fqn).alias("src").join(
        needed.alias("n"),
        F.col(f"src.{source_key}") == F.col("n.needed_key"),
        "inner",
    )

    if key_type == "SURG_CASE_PROC_ID":
        sc = spark.table(f"{CATALOG}.{RAW_SCHEMA}.mill_surgical_case").select(
            "SURG_CASE_ID", "ENCNTR_ID"
        )
        src = src.join(sc, "SURG_CASE_ID", "left")
        enc_expr = nonzero_long(F.col("ENCNTR_ID"))
    elif key_type == "IM_STUDY_ID":
        cp = spark.table(f"{CATALOG}.{RAW_SCHEMA}.mill_cv_proc").select(
            F.col("CV_PROC_ID").alias("_CV_PROC_ID"),
            F.col("ENCNTR_ID").alias("_CP_ENCNTR_ID"),
        )
        src = src.join(cp, F.col("ORIG_ENTITY_ID") == F.col("_CV_PROC_ID"), "left")
        enc_expr = F.coalesce(
            nonzero_long(F.col("src.ENCNTR_ID")),
            nonzero_long(F.col("_CP_ENCNTR_ID")),
        )
    elif key_type == "IM_ACQUIRED_STUDY_ID":
        ims = spark.table(f"{CATALOG}.{RAW_SCHEMA}.mill_im_study").select(
            F.col("IM_STUDY_ID").alias("_IMS_STUDY_ID"),
            F.col("ENCNTR_ID").alias("_IMS_ENCNTR_ID"),
            F.col("ORIG_ENTITY_ID").alias("_IMS_ORIG_ENTITY_ID"),
        )
        cp = spark.table(f"{CATALOG}.{RAW_SCHEMA}.mill_cv_proc").select(
            F.col("CV_PROC_ID").alias("_CV_PROC_ID"),
            F.col("ENCNTR_ID").alias("_CP_ENCNTR_ID"),
        )
        src = src.join(
            ims,
            F.col("src.MATCHED_STUDY_ID") == F.col("_IMS_STUDY_ID"),
            "left",
        )
        src = src.join(
            cp,
            F.col("_IMS_ORIG_ENTITY_ID") == F.col("_CV_PROC_ID"),
            "left",
        )
        enc_expr = F.coalesce(
            nonzero_long(F.col("_IMS_ENCNTR_ID")),
            nonzero_long(F.col("_CP_ENCNTR_ID")),
        )
    elif key_type == "PROBLEM_ID":
        enc_expr = F.coalesce(
            nonzero_long(F.col("src.ORIGINATING_ENCNTR_ID")),
            nonzero_long(F.col("src.UPDATE_ENCNTR_ID")),
        )
    elif key_type == "REFERRAL_ID":
        # Non-zero-aware coalesce: OUTBOUND_ENCNTR_ID first, then the enrichment
        # ENCNTR_ID classification stamps on raw rows (carries the ORDER_ID-derived
        # encounter for referrals with no outbound encounter, without which their
        # children would stay unresolved). The enrichment column exists only after
        # the first promotion, so guard on the actual raw columns.
        _ref_cols = {c.upper() for c in original_source_columns}
        _enc_candidates = [
            nonzero_long(F.col(f"src.{c}"))
            for c in ("OUTBOUND_ENCNTR_ID", "ENCNTR_ID")
            if c in _ref_cols
        ]
        enc_expr = F.coalesce(*_enc_candidates)
    else:
        enc_expr = nonzero_long(F.col("src.ENCNTR_ID"))

    order_cols = best_order_columns(original_source_columns)
    ordering = [F.col(f"src.{c}").desc_nulls_last() for c in order_cols] or [
        F.col(f"src.{source_key}").desc()
    ]
    window = Window.partitionBy(F.col(f"src.{source_key}")).orderBy(*ordering)
    return (
        src.select(
            F.col(f"src.{source_key}").alias("key_id"),
            enc_expr.alias("ENCNTR_ID"),
            F.row_number().over(window).alias("_rn"),
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def applicable_lookup_types(table_name, columns):
    cu = upper_map(columns)
    has_direct_route = (
        "ENCNTR_ID" in cu
        or "ORGANIZATION_ID" in cu
        or table_name.lower() in DIRECT_ENC_COLUMN_OVERRIDES
    )
    result = []
    for key_type, (source_table, _) in LOOKUP_SPECS.items():
        if key_type not in cu:
            continue
        # A source table's own hub entry is derived from that same table. Joining
        # it back to itself is redundant when the row already carries encounter
        # or organization data, and it caused large avoidable hub scans.
        if has_direct_route and source_table.lower() == table_name.lower():
            continue
        result.append(key_type)
    return result


def classification_routes(table_name, columns):
    cu = upper_map(columns)
    routes = []
    if "ORGANIZATION_ID" in cu:
        routes.append("ORGANIZATION_ID")
    if any(
        column in cu
        for column in DIRECT_ENC_COLUMN_OVERRIDES.get(
            table_name.lower(), ["ENCNTR_ID"]
        )
    ):
        routes.append("ENCNTR_ID")
    routes.extend(applicable_lookup_types(table_name, columns))
    if table_name.lower() in REVERSE_REFERENCE_SPECS:
        routes.append("REVERSE_REFERENCE")
    return routes


def passthrough_decision(table_name, columns, force_passthrough=False):
    routes = classification_routes(table_name, columns)
    if force_passthrough:
        return True, "catalog_passthrough", routes
    if table_name.lower() in GLOBAL_PASSTHROUGH_TABLES:
        return True, "global_reference_table", routes
    if not table_name.lower().startswith("mill_"):
        return True, "Non Millenium", routes
    if not routes:
        return True, "no_classification_route", routes
    return False, None, routes


def backfill_pending_lookups(table_name, staging, columns):
    if not BACKFILL_PENDING_LOOKUPS:
        return
    cu = upper_map(columns)

    encounter_columns = [
        cu[column]
        for column in DIRECT_ENC_COLUMN_OVERRIDES.get(
            table_name.lower(), ["ENCNTR_ID"]
        )
        if column in cu
    ]
    encounter_source_fqn = f"{CATALOG}.{RAW_SCHEMA}.{ENC_ORG_SOURCE_TABLE}"
    if encounter_columns and table_exists(encounter_source_fqn):
        needed_frames = [
            staging.select(
                nonzero_long(F.col(column)).alias("ENCNTR_ID")
            ).filter(F.col("ENCNTR_ID").isNotNull())
            for column in encounter_columns
        ]
        needed = reduce(
            lambda left, right: left.unionByName(right), needed_frames
        ).dropDuplicates()
        current = spark.table(ENC_ORG_TBL).select("ENCNTR_ID")
        missing = needed.join(current, "ENCNTR_ID", "left_anti")
        if missing.limit(1).count() > 0:
            encounters = spark.table(encounter_source_fqn).select(
                F.col("ENCNTR_ID").cast("long").alias("ENCNTR_ID"),
                F.col("ORGANIZATION_ID").cast("long").alias("ORGANIZATION_ID"),
            ).filter(F.col("ENCNTR_ID").isNotNull() & (F.col("ENCNTR_ID") != 0))
            merge_encounter_map(missing.join(encounters, "ENCNTR_ID", "inner"), track_changes=False)

    for key_type in applicable_lookup_types(table_name, columns):
        needed = staging.select(F.col(cu[key_type]).alias(key_type)).filter(
            F.col(key_type).isNotNull() & (F.col(key_type) != 0)
        ).dropDuplicates()
        current = spark.table(HUB_TBL).filter(F.col("key_type") == key_type).select(
            F.col("key_id").alias(key_type)
        )
        missing = needed.join(current, key_type, "left_anti")
        if missing.limit(1).count() == 0:
            continue
        mapping = raw_lookup_mapping(key_type, missing)
        if mapping is not None:
            merge_hub(mapping, key_type)


def reverse_reference_candidates(table_name, needed_keys):
    spec = REVERSE_REFERENCE_SPECS.get(table_name.lower())
    if not spec:
        return None
    frames = []
    for source_table, reference_column in spec["sources"]:
        source_fqn = f"{CATALOG}.{RAW_SCHEMA}.{source_table}"
        if not table_exists(source_fqn):
            continue
        source_upper = upper_map(table_columns(source_fqn))
        if reference_column not in source_upper or "TRUST" not in source_upper:
            continue
        frames.append(
            spark.table(source_fqn).alias("src")
            .join(
                F.broadcast(needed_keys.alias("n")),
                F.col(f"src.{source_upper[reference_column]}").cast("long")
                == F.col("n.key_id"),
                "inner",
            )
            .select(
                F.col("n.key_id"),
                F.trim(F.col(f"src.{source_upper['TRUST']}")).alias("trust"),
            )
            .filter(F.col("trust").isNotNull())
            .dropDuplicates()
        )
    if not frames:
        return None
    candidates = reduce(lambda left, right: left.unionByName(right), frames)
    return candidates.groupBy("key_id").agg(
        F.countDistinct(F.upper(F.col("trust"))).alias("trust_count"),
        F.first(F.col("trust"), ignorenulls=True).alias("trust"),
    )


def current_version(fqn):
    return int(spark.sql(f"DESCRIBE HISTORY {fqn} LIMIT 1").first()["version"])


def get_checkpoint(fqn, checkpoint_key):
    rows = spark.sql(f"""
        SELECT last_version
        FROM {CONTROL_TBL}
        WHERE table_name = '{sql_string(fqn)}'
          AND checkpoint_key = '{sql_string(checkpoint_key)}'
        ORDER BY updated_at DESC
        LIMIT 1
    """).collect()
    return rows[0]["last_version"] if rows else None


def set_checkpoint(fqn, checkpoint_key, version):
    if DRY_RUN:
        return
    source = spark.createDataFrame(
        [(fqn, checkpoint_key, int(version), datetime.now(timezone.utc), datetime.now(timezone.utc))],
        "table_name string, checkpoint_key string, last_version long, last_timestamp timestamp, updated_at timestamp",
    )
    DeltaTable.forName(spark, CONTROL_TBL).alias("t").merge(
        source.alias("s"),
        (F.col("t.table_name") == F.col("s.table_name"))
        & (F.col("t.checkpoint_key") == F.col("s.checkpoint_key")),
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def refresh_encounters_from_cdf():
    fqn = f"{CATALOG}.{RAW_SCHEMA}.{ENC_ORG_SOURCE_TABLE}"
    if not table_exists(fqn):
        return
    end_version = current_version(fqn)
    last_version = get_checkpoint(fqn, "hub_encounter_v2")
    if last_version is None:
        # The resolver feed is deliberately unfiltered. Bootstrap the complete
        # historical encounter map before classifying child rows, including BHRUT.
        merge_encounter_map(
            spark.table(fqn).select("ENCNTR_ID", "ORGANIZATION_ID"),
            track_changes=False,
        )
        set_checkpoint(fqn, "hub_encounter_v2", end_version)
        print(f"Encounter map bootstrapped from {fqn} at Delta version {end_version}.")
        return
    if last_version >= end_version:
        return

    try:
        changes = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", int(last_version) + 1)
            .option("endingVersion", end_version)
            .table(fqn)
            .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
        )
    except Exception as exc:
        print(
            f"Encounter CDF unavailable for versions {int(last_version) + 1}..{end_version}; "
            f"pending staging keys will be backfilled on demand: {type(exc).__name__}: {exc}"
        )
        return
    window = Window.partitionBy("ENCNTR_ID").orderBy(
        F.col("_commit_version").desc(),
        F.when(F.col("_change_type") == "delete", 2).otherwise(1).desc(),
    )
    latest = changes.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1)

    deleted = latest.filter(F.col("_change_type") == "delete").select(
        F.col("ENCNTR_ID").cast("long").alias("ENCNTR_ID")
    )
    if deleted.limit(1).count() > 0 and not DRY_RUN:
        old = spark.table(ENC_ORG_TBL).join(deleted, "ENCNTR_ID", "inner").select(
            "ENCNTR_ID",
            F.col("ORGANIZATION_ID").alias("old_organization_id"),
            F.lit(None).cast("long").alias("new_organization_id"),
        )
        append_encounter_changes(old)
        DeltaTable.forName(spark, ENC_ORG_TBL).alias("t").merge(
            deleted.alias("s"), F.col("t.ENCNTR_ID") == F.col("s.ENCNTR_ID")
        ).whenMatchedDelete().execute()

    upserts = latest.filter(F.col("_change_type") != "delete").select(
        "ENCNTR_ID", "ORGANIZATION_ID"
    )
    merge_encounter_map(upserts, track_changes=True)
    set_checkpoint(fqn, "hub_encounter_v2", end_version)


def refresh_hub_from_cdf():
    for key_type, (table_name, source_key) in LOOKUP_SPECS.items():
        fqn = f"{CATALOG}.{RAW_SCHEMA}.{table_name}"
        if not table_exists(fqn):
            continue
        checkpoint_key = f"hub_{key_type}_v2"
        end_version = current_version(fqn)
        last_version = get_checkpoint(fqn, checkpoint_key)
        if last_version is None:
            set_checkpoint(fqn, checkpoint_key, end_version)
            continue
        if last_version >= end_version:
            continue

        try:
            changes = (
                spark.read.format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", int(last_version) + 1)
                .option("endingVersion", end_version)
                .table(fqn)
                .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
            )
        except Exception as exc:
            print(
                f"{key_type} CDF unavailable for versions "
                f"{int(last_version) + 1}..{end_version}; demand backfill remains enabled: "
                f"{type(exc).__name__}: {exc}"
            )
            continue
        window = Window.partitionBy(source_key).orderBy(
            F.col("_commit_version").desc(),
            F.when(F.col("_change_type") == "delete", 2).otherwise(1).desc(),
        )
        latest = changes.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1)

        deleted = latest.filter(F.col("_change_type") == "delete").select(
            F.col(source_key).cast("long").alias("key_id")
        )
        if deleted.limit(1).count() > 0 and not DRY_RUN:
            (
                DeltaTable.forName(spark, HUB_TBL)
                .alias("t")
                .merge(
                    deleted.alias("s"),
                    (F.col("t.key_type") == F.lit(key_type))
                    & (F.col("t.key_id") == F.col("s.key_id")),
                )
                .whenMatchedUpdate(
                    set={
                        "ENCNTR_ID": F.lit(None).cast("long"),
                        "updated_at": F.current_timestamp(),
                    }
                )
                .execute()
            )

        changed_keys = latest.filter(F.col("_change_type") != "delete").select(
            F.col(source_key).alias(key_type)
        ).dropDuplicates()
        if changed_keys.limit(1).count() > 0:
            mapping = raw_lookup_mapping(key_type, changed_keys)
            if mapping is not None:
                merge_hub(mapping, key_type)
        set_checkpoint(fqn, checkpoint_key, end_version)




def trust_map_digest():
    rows = (
        spark.table(TRUST_MAP_TBL)
        .select("organization_id", "trust")
        .orderBy("organization_id")
        .collect()
    )
    payload = [(int(row["organization_id"]), row["trust"]) for row in rows]
    return hashlib.sha256(
        json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    ).hexdigest()


def capture_dependency_watermarks():
    enc_row = spark.table(ENC_ORG_TBL).agg(F.max("updated_at").alias("m")).first()
    enc_change_row = (
        spark.table(CHANGED_ENC_TBL)
        .agg(F.max("detected_at").alias("m"))
        .first()
    )
    hub_row = spark.table(HUB_TBL).agg(F.max("updated_at").alias("m")).first()
    enc_values = [
        value for value in (enc_row["m"], enc_change_row["m"]) if value is not None
    ]
    return {
        "enc_org_watermark": max(enc_values) if enc_values else None,
        "hub_watermark": hub_row["m"],
        "trust_map_hash": trust_map_digest(),
    }


def get_classification_control(table_name):
    rows = (
        spark.table(CLASSIFICATION_CONTROL_TBL)
        .filter(F.col("table_name") == table_name)
        .orderBy(F.col("updated_at").desc_nulls_last())
        .limit(1)
        .collect()
    )
    return rows[0] if rows else None


def set_classification_control(payload):
    if DRY_RUN:
        return
    schema = spark.table(CLASSIFICATION_CONTROL_TBL).schema
    frame = spark.createDataFrame(
        [(
            payload["table_name"],
            int(payload["staging_version"]),
            payload.get("staged_at_watermark"),
            payload.get("enc_org_watermark"),
            payload.get("hub_watermark"),
            payload.get("trust_map_hash"),
            RUN_ID,
            datetime.now(timezone.utc),
        )],
        schema=schema,
    )
    (
        DeltaTable.forName(spark, CLASSIFICATION_CONTROL_TBL)
        .alias("t")
        .merge(frame.alias("s"), F.col("t.table_name") == F.col("s.table_name"))
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _max_staged_at(staging, column_name):
    return staging.agg(F.max(F.col(column_name)).alias("m")).first()["m"]


def classification_candidates(table_name, staging_fqn, columns, keys):
    """Select only rows changed in staging or affected by changed mapping content."""
    staging = spark.table(staging_fqn)
    column_map = upper_map(columns)
    # Every row still in staging is pending by definition. Include legacy rows
    # whose attempt counter already equals/exceeds the new cap so deployment of
    # this replacement drains them into raw instead of stranding them forever.
    pending_retry = staging
    staging_version = current_version(staging_fqn)
    control = get_classification_control(table_name)
    staged_column = column_map.get("_STAGED_AT")
    staged_watermark = (
        _max_staged_at(staging, staged_column)
        if control is None and staged_column
        else (control["staged_at_watermark"] if control else None)
    )
    payload = {
        "table_name": table_name,
        "staging_version": staging_version,
        "staged_at_watermark": staged_watermark,
        **RUN_DEPENDENCIES,
    }

    if control is None:
        if BOOTSTRAP_EXISTING:
            print(f"{table_name}: baselining existing unresolved rows without a full retry")
            return staging.limit(0), payload
        print(f"{table_name}: no classification control row; processing complete staging table")
        if staged_column:
            payload["staged_at_watermark"] = _max_staged_at(staging, staged_column)
        return staging, payload

    staging_changed = int(control["staging_version"]) != staging_version
    enc_changed = control["enc_org_watermark"] != RUN_DEPENDENCIES["enc_org_watermark"]
    hub_changed = control["hub_watermark"] != RUN_DEPENDENCIES["hub_watermark"]
    trust_changed = control["trust_map_hash"] != RUN_DEPENDENCIES["trust_map_hash"]
    if not (staging_changed or enc_changed or hub_changed or trust_changed):
        # Attempt-counter writes are intentionally captured in the control row,
        # so Delta-version comparison alone cannot drive retries. Pending rows
        # must be reconsidered on every classifier execution until they resolve
        # or the final attempt lands them in raw with NULL Trust.
        return pending_retry, payload

    parts = [pending_retry]
    if staging_changed:
        if staged_column and control["staged_at_watermark"] is not None:
            current_max = _max_staged_at(staging, staged_column)
            payload["staged_at_watermark"] = current_max
            if current_max is not None and current_max > control["staged_at_watermark"]:
                parts.append(
                    staging.filter(F.col(staged_column) > F.lit(control["staged_at_watermark"]))
                )
            else:
                print(
                    f"{table_name}: Delta version changed without an advancing _STAGED_AT; "
                    "using correctness-first full fallback"
                )
                parts.append(staging)
        else:
            if staged_column:
                payload["staged_at_watermark"] = _max_staged_at(staging, staged_column)
            parts.append(staging)

    candidate_enc_cols = [
        c for c in DIRECT_ENC_COLUMN_OVERRIDES.get(table_name.lower(), ["ENCNTR_ID"])
        if c in column_map
    ]
    if enc_changed and candidate_enc_cols:
        changed_from_map = (
            spark.table(ENC_ORG_TBL)
            .filter(
                F.col("updated_at") > F.lit(control["enc_org_watermark"])
                if control["enc_org_watermark"] is not None
                else F.lit(True)
            )
            .select(F.col("ENCNTR_ID").alias("_changed_encntr_id"))
        )
        changed_from_log = (
            spark.table(CHANGED_ENC_TBL)
            .filter(
                F.col("detected_at") > F.lit(control["enc_org_watermark"])
                if control["enc_org_watermark"] is not None
                else F.lit(True)
            )
            .select(F.col("ENCNTR_ID").alias("_changed_encntr_id"))
        )
        changed_encounters = changed_from_map.unionByName(
            changed_from_log
        ).distinct()
        parts.append(
            staging.join(
                F.broadcast(changed_encounters),
                reduce(
                    lambda a, b: a | b,
                    [
                        F.col(column_map[c]) == F.col("_changed_encntr_id")
                        for c in candidate_enc_cols
                    ],
                ),
                "left_semi",
            )
        )

    if hub_changed:
        for key_type in applicable_lookup_types(table_name, columns):
            changed_keys = (
                spark.table(HUB_TBL)
                .filter(F.col("key_type") == key_type)
                .filter(
                    F.col("updated_at") > F.lit(control["hub_watermark"])
                    if control["hub_watermark"] is not None
                    else F.lit(True)
                )
                .select(F.col("key_id").alias("_changed_key"))
                .distinct()
            )
            parts.append(
                staging.join(
                    F.broadcast(changed_keys),
                    F.col(column_map[key_type]) == F.col("_changed_key"),
                    "left_semi",
                )
            )

    if trust_changed and "ORGANIZATION_ID" in column_map:
        mapped_orgs = (
            spark.table(TRUST_MAP_TBL)
            .select(F.col("organization_id").alias("_changed_org"))
            .distinct()
        )
        parts.append(
            staging.join(
                F.broadcast(mapped_orgs),
                F.col(column_map["ORGANIZATION_ID"]) == F.col("_changed_org"),
                "left_semi",
            )
        )

    if not parts:
        return staging.limit(0), payload
    candidates = reduce(lambda left, right: left.unionByName(right), parts)
    return candidates.dropDuplicates(keys), payload



def build_resolved(table_name, staging_fqn, keys, staging_df=None):
    staging = staging_df if staging_df is not None else spark.table(staging_fqn)
    metadata = {"_STAGED_AT", "_CLASSIFICATION_ATTEMPTS"}
    columns = staging.columns
    cu = upper_map(columns)

    valid = staging.filter(~any_null_key(keys))
    ordering = [F.col(c).desc_nulls_last() for c in best_order_columns(columns)]
    if not ordering:
        ordering = [F.col(k).desc_nulls_last() for k in keys]
    window = Window.partitionBy(*keys).orderBy(*ordering)
    resolved = valid.withColumn("_key_rn", F.row_number().over(window)).filter(
        F.col("_key_rn") == 1
    ).drop("_key_rn")

    trust_map = (
        spark.table(TRUST_MAP_TBL)
        .select(
            F.col("organization_id").cast("long").alias("organization_id"),
            F.trim(F.col("trust")).alias("trust"),
        )
        .filter(F.col("organization_id").isNotNull() & F.col("trust").isNotNull())
    )
    org_conflicts = (
        trust_map.groupBy("organization_id")
        .agg(F.countDistinct(F.upper(F.col("trust"))).alias("_trust_count"))
        .filter(F.col("_trust_count") > 1)
        .limit(1)
        .count()
    )
    if org_conflicts:
        raise RuntimeError(f"{TRUST_MAP_TBL} has organizations mapped to conflicting trusts")
    trust_map = trust_map.groupBy("organization_id").agg(
        F.min(F.col("trust")).alias("trust")
    )
    trust_map_join = trust_map.select(
        F.col("organization_id").alias("_tm_organization_id"),
        F.col("trust").alias("_tm_trust"),
    )
    mapped_encounters = (
        spark.table(ENC_ORG_TBL).alias("eo")
        .select(
            F.col("eo.ENCNTR_ID").cast("long").alias("ENCNTR_ID"),
            F.col("eo.ORGANIZATION_ID").cast("long").alias("ORGANIZATION_ID"),
        )
        .filter(F.col("ENCNTR_ID").isNotNull() & (F.col("ENCNTR_ID") != 0))
    )
    raw_encounter_fqn = f"{CATALOG}.{RAW_SCHEMA}.mill_encounter"
    if table_exists(raw_encounter_fqn):
        raw_encounters = (
            spark.table(raw_encounter_fqn)
            .select(
                F.col("ENCNTR_ID").cast("long").alias("ENCNTR_ID"),
                F.col("ORGANIZATION_ID").cast("long").alias("ORGANIZATION_ID"),
            )
            .filter(F.col("ENCNTR_ID").isNotNull() & (F.col("ENCNTR_ID") != 0))
        )
        encounter_organizations = mapped_encounters.unionByName(raw_encounters)
    else:
        encounter_organizations = mapped_encounters

    enc_candidates = (
        encounter_organizations.alias("eo")
        .join(
            F.broadcast(trust_map_join.alias("tm")),
            F.col("eo.ORGANIZATION_ID") == F.col("tm._tm_organization_id"),
            "left",
        )
        .select(
            F.col("eo.ENCNTR_ID").alias("ENCNTR_ID"),
            F.col("eo.ORGANIZATION_ID").alias("ORGANIZATION_ID"),
            F.col("tm._tm_trust").alias("trust"),
        )
        .filter(F.col("ENCNTR_ID").isNotNull() & F.col("trust").isNotNull())
    )
    enc_conflicts = (
        enc_candidates.groupBy("ENCNTR_ID")
        .agg(F.countDistinct(F.upper(F.col("trust"))).alias("_trust_count"))
        .filter(F.col("_trust_count") > 1)
        .limit(1)
        .count()
    )
    if enc_conflicts:
        raise RuntimeError(f"{ENC_ORG_TBL} resolves encounters to conflicting trusts")
    enc_trust = enc_candidates.groupBy("ENCNTR_ID").agg(
        F.min(F.col("ORGANIZATION_ID")).alias("ORGANIZATION_ID"),
        F.min(F.col("trust")).alias("trust"),
    )

    enc_col_names = [
        c for c in DIRECT_ENC_COLUMN_OVERRIDES.get(table_name.lower(), ["ENCNTR_ID"])
        if c in cu
    ]
    original_enc = (
        F.coalesce(*[
            F.when(
                F.col(cu[c]).isNotNull() & (F.col(cu[c]) != 0),
                F.col(cu[c]),
            )
            for c in enc_col_names
        ])
        if enc_col_names
        else F.lit(None).cast("long")
    )
    original_org = (
        F.when(
            F.col(cu["ORGANIZATION_ID"]).isNotNull()
            & (F.col(cu["ORGANIZATION_ID"]) != 0),
            F.col(cu["ORGANIZATION_ID"]),
        )
        if "ORGANIZATION_ID" in cu
        else F.lit(None).cast("long")
    )
    route_candidates = []

    if "ORGANIZATION_ID" in cu:
        direct_org = trust_map.select(
            F.col("organization_id").alias("_direct_org_id"),
            F.col("trust").alias("_direct_org_trust"),
        )
        resolved = resolved.join(
            F.broadcast(direct_org),
            original_org == F.col("_direct_org_id"),
            "left",
        )
        route_candidates.append(
            (F.col("_direct_org_trust"), original_enc, original_org, F.lit(False))
        )

    if enc_col_names:
        direct_enc = enc_trust.select(
            F.col("ENCNTR_ID").alias("_direct_enc_id"),
            F.col("ORGANIZATION_ID").alias("_direct_enc_org"),
            F.col("trust").alias("_direct_enc_trust"),
        )
        resolved = resolved.join(
            direct_enc,
            original_enc == F.col("_direct_enc_id"),
            "left",
        )
        route_candidates.append(
            (
                F.col("_direct_enc_trust"),
                F.col("_direct_enc_id"),
                F.col("_direct_enc_org"),
                F.lit(False),
            )
        )

    for index, key_type in enumerate(applicable_lookup_types(table_name, columns)):
        lookup_candidates = (
            spark.table(HUB_TBL)
            .filter(F.col("key_type") == key_type)
            .join(enc_trust, "ENCNTR_ID", "left")
        )
        lookup = (
            lookup_candidates.groupBy("key_id")
            .agg(
                F.countDistinct(F.upper(F.col("trust"))).alias(f"_hub_trust_count_{index}"),
                F.min(F.col("ENCNTR_ID")).alias(f"_hub_enc_{index}"),
                F.min(F.col("ORGANIZATION_ID")).alias(f"_hub_org_{index}"),
                F.min(F.col("trust")).alias(f"_hub_trust_{index}"),
            )
            .select(
                F.col("key_id").alias(f"_hub_key_{index}"),
                F.col(f"_hub_enc_{index}"),
                F.col(f"_hub_org_{index}"),
                F.col(f"_hub_trust_{index}"),
                (F.col(f"_hub_trust_count_{index}") > 1).alias(f"_hub_conflict_{index}"),
            )
        )
        resolved = resolved.join(
            lookup,
            F.col(cu[key_type]) == F.col(f"_hub_key_{index}"),
            "left",
        )
        route_candidates.append(
            (
                F.col(f"_hub_trust_{index}"),
                F.col(f"_hub_enc_{index}"),
                F.col(f"_hub_org_{index}"),
                F.coalesce(F.col(f"_hub_conflict_{index}"), F.lit(False)),
            )
        )

    reverse_spec = REVERSE_REFERENCE_SPECS.get(table_name.lower())
    if reverse_spec:
        reverse_key = reverse_spec["target_key"]
        if reverse_key in cu:
            needed_reverse = (
                resolved.select(
                    F.col(cu[reverse_key]).cast("long").alias("key_id")
                )
                .filter(F.col("key_id").isNotNull() & (F.col("key_id") != 0))
                .dropDuplicates()
            )
            reverse = reverse_reference_candidates(table_name, needed_reverse)
            if reverse is not None:
                resolved = resolved.join(
                    reverse.select(
                        F.col("key_id").alias("_reverse_key"),
                        F.col("trust").alias("_reverse_trust"),
                        (F.col("trust_count") > 1).alias("_reverse_conflict"),
                    ),
                    F.col(cu[reverse_key]).cast("long") == F.col("_reverse_key"),
                    "left",
                )
                route_candidates.append(
                    (
                        F.col("_reverse_trust"),
                        F.lit(None).cast("long"),
                        F.lit(None).cast("long"),
                        F.coalesce(F.col("_reverse_conflict"), F.lit(False)),
                    )
                )

    trust_candidates = [candidate[0] for candidate in route_candidates]
    routed_encounters = [
        F.when(trust.isNotNull(), encounter)
        for trust, encounter, _, _ in route_candidates
    ]
    routed_organizations = [
        F.when(trust.isNotNull(), organization)
        for trust, _, organization, _ in route_candidates
    ]
    route_conflicts = [candidate[3] for candidate in route_candidates]
    normalized_trusts = F.array(
        *[F.upper(F.trim(candidate)) for candidate in trust_candidates]
    )
    distinct_trusts = F.array_distinct(
        F.filter(normalized_trusts, lambda value: value.isNotNull())
    )
    trust_conflict = (F.size(distinct_trusts) > 1)
    for route_conflict in route_conflicts:
        trust_conflict = trust_conflict | route_conflict
    resolved = (
        resolved.withColumn(
            "_resolved_encntr_id",
            F.coalesce(*(routed_encounters + [original_enc])),
        )
        .withColumn(
            "_resolved_organization_id",
            F.coalesce(*(routed_organizations + [original_org])),
        )
        .withColumn(
            "_resolved_trust",
            F.when(~trust_conflict, F.coalesce(*trust_candidates)).otherwise(
                F.lit(None).cast("string")
            )
            if trust_candidates
            else F.lit(None).cast("string"),
        )
        .withColumn("_trust_conflict", trust_conflict)
    )
    return resolved.select(
        *columns,
        "_resolved_encntr_id",
        "_resolved_organization_id",
        "_resolved_trust",
        "_trust_conflict",
    )


def ensure_archive_table(target_fqn, source_df):
    if not table_exists(target_fqn):
        source_df.limit(0).write.format("delta").saveAsTable(target_fqn)
        return
    target_upper = upper_map(table_columns(target_fqn))
    for field in source_df.schema.fields:
        if field.name.upper() not in target_upper:
            spark.sql(
                f"ALTER TABLE {target_fqn} ADD COLUMNS "
                f"({field.name} {field.dataType.simpleString()})"
            )


def archive_rows(df, staging_columns, target_fqn, reason, expected_count=None):
    if expected_count is not None:
        if expected_count <= 0:
            return 0
    elif df.limit(1).count() == 0:
        return 0
    archived = (
        df.select(*staging_columns)
        .withColumn("_row_fingerprint", row_fingerprint(staging_columns))
        .withColumn("_archive_reason", F.lit(reason))
        .withColumn("_archived_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(RUN_ID))
        .dropDuplicates(["_row_fingerprint"])
    )
    count = int(expected_count) if expected_count is not None else archived.count()
    if DRY_RUN:
        return count
    ensure_archive_table(target_fqn, archived)
    target_upper = upper_map(table_columns(target_fqn))
    source_upper = upper_map(archived.columns)
    insert_values = {
        target_upper[u]: F.col(f"s.{source_upper[u]}")
        for u in target_upper
        if u in source_upper
    }
    DeltaTable.forName(spark, target_fqn).alias("t").merge(
        archived.alias("s"),
        F.col("t._row_fingerprint") == F.col("s._row_fingerprint"),
    ).whenNotMatchedInsert(values=insert_values).execute()
    return count


def delete_by_fingerprint(staging_fqn, rows_df, staging_columns, known_nonempty=False):
    if DRY_RUN:
        return 0
    if not known_nonempty and rows_df.limit(1).count() == 0:
        return 0
    fingerprints = rows_df.select(
        row_fingerprint(staging_columns).alias("_row_fingerprint")
    ).dropDuplicates()
    target_normalized = [
        F.coalesce(F.col(f"t.{c}").cast("string"), F.lit("<NULL>")).alias(c)
        for c in staging_columns
    ]
    target_fingerprint = F.sha2(F.to_json(F.struct(*target_normalized)), 256)
    DeltaTable.forName(spark, staging_fqn).alias("t").merge(
        fingerprints.alias("s"),
        target_fingerprint == F.col("s._row_fingerprint"),
    ).whenMatchedDelete().execute()
    return metric_int(latest_metrics(staging_fqn, "MERGE"), "numTargetRowsDeleted")


def delete_by_keys(staging_fqn, keys_df, keys, known_nonempty=False):
    if DRY_RUN:
        return 0
    if not known_nonempty and keys_df.limit(1).count() == 0:
        return 0
    DeltaTable.forName(spark, staging_fqn).alias("t").merge(
        keys_df.select(*keys).dropDuplicates().alias("s"),
        key_condition("t", "s", keys),
    ).whenMatchedDelete().execute()
    return metric_int(latest_metrics(staging_fqn, "MERGE"), "numTargetRowsDeleted")


def increment_attempts_by_keys(staging_fqn, keys_df, keys, known_nonempty=False):
    if DRY_RUN:
        return
    if not known_nonempty and keys_df.limit(1).count() == 0:
        return
    DeltaTable.forName(spark, staging_fqn).alias("t").merge(
        keys_df.select(*keys).dropDuplicates().alias("s"),
        key_condition("t", "s", keys),
    ).whenMatchedUpdate(
        set={
            "_classification_attempts": F.coalesce(
                F.col("t._classification_attempts"), F.lit(0)
            ) + F.lit(1)
        }
    ).execute()


def increment_attempts_by_fingerprint(staging_fqn, rows_df, staging_columns):
    if DRY_RUN or rows_df.limit(1).count() == 0:
        return
    fingerprints = (
        rows_df.select(*staging_columns)
        .withColumn("_row_fingerprint", row_fingerprint(staging_columns))
        .select("_row_fingerprint")
        .dropDuplicates()
    )
    target_fingerprint = row_fingerprint(
        staging_columns, prefix="t."
    )
    (
        DeltaTable.forName(spark, staging_fqn)
        .alias("t")
        .merge(
            fingerprints.alias("s"),
            target_fingerprint == F.col("s._row_fingerprint"),
        )
        .whenMatchedUpdate(
            set={
                "_classification_attempts": F.coalesce(
                    F.col("t._classification_attempts"), F.lit(0)
                ) + F.lit(1)
            }
        )
        .execute()
    )



def prepare_passthrough_source(
    source_df,
    target_fqn,
    keys,
    target_cols=None,
    preserve_target_columns=None,
):
    target_cols = target_cols or table_columns(target_fqn)
    source_upper = upper_map(source_df.columns)
    preserved = {value.upper() for value in (preserve_target_columns or set())}
    preserved.add("TRUST")
    metadata = {
        "_STAGED_AT",
        "_CLASSIFICATION_ATTEMPTS",
        "_ROW_FINGERPRINT",
        "_ARCHIVE_REASON",
        "_ARCHIVED_AT",
        "_RUN_ID",
        "_RECOVERED_AT",
        "_RECOVERED_RUN_ID",
    }
    expressions = [
        F.col(source_upper[target_col.upper()]).alias(target_col)
        for target_col in target_cols
        if target_col.upper() in source_upper
        and target_col.upper() not in metadata
        and target_col.upper() not in preserved
    ]
    prepared = source_df.select(*expressions)
    prepared_upper = upper_map(prepared.columns)
    missing_keys = [key for key in keys if key.upper() not in prepared_upper]
    if missing_keys:
        raise ValueError(
            f"Pass-through source for {target_fqn} lacks merge keys {missing_keys}"
        )
    return prepared


def unrecovered_archive_rows(archive_fqn):
    archived = spark.table(archive_fqn)
    archived_upper = upper_map(archived.columns)
    if "_RECOVERED_AT" in archived_upper:
        archived = archived.filter(F.col(archived_upper["_RECOVERED_AT"]).isNull())
    return archived


def mark_archive_recovered_by_keys(archive_fqn, keys_df, keys):
    if DRY_RUN:
        return
    ensure_column(archive_fqn, "_recovered_at", "TIMESTAMP")
    ensure_column(archive_fqn, "_recovered_run_id", "STRING")
    (
        DeltaTable.forName(spark, archive_fqn)
        .alias("t")
        .merge(
            keys_df.select(*keys).dropDuplicates().alias("s"),
            key_condition("t", "s", keys),
        )
        .whenMatchedUpdate(
            set={
                "_recovered_at": F.current_timestamp(),
                "_recovered_run_id": F.lit(RUN_ID),
            }
        )
        .execute()
    )


def passthrough_table(
    table_name,
    log_name,
    staging_fqn,
    target_fqn,
    reason,
    abandoned_fqn=None,
):
    started = time.time()
    staging_columns = table_columns(staging_fqn)
    keys = discover_merge_keys(table_name, staging_fqn)
    live = spark.table(staging_fqn)
    live_nonempty = live.limit(1).count() > 0

    archived = None
    archived_nonempty = False
    if abandoned_fqn and table_exists(abandoned_fqn):
        archived = unrecovered_archive_rows(abandoned_fqn)
        archived_nonempty = archived.limit(1).count() > 0

    if not live_nonempty and not archived_nonempty:
        print(f"{log_name}: pass-through skipped; no pending rows")
        return None

    target_columns = ensure_raw_target(table_name, staging_fqn, target_fqn)
    preserved = PASSTHROUGH_PRESERVE_TARGET_COLUMNS.get(
        table_name.lower(), set()
    )
    prepared_sources = []
    live_source = None
    archive_source = None

    if live_nonempty:
        live_source = prepare_passthrough_source(
            live,
            target_fqn,
            keys,
            target_cols=target_columns,
            preserve_target_columns=preserved,
        )
        prepared_sources.append(
            live_source.withColumn("_passthrough_source_priority", F.lit(2))
        )

    if archived_nonempty:
        archive_source = prepare_passthrough_source(
            archived,
            target_fqn,
            keys,
            target_cols=target_columns,
            preserve_target_columns=preserved,
        )
        prepared_sources.append(
            archive_source.withColumn("_passthrough_source_priority", F.lit(1))
        )

    passthrough = reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True),
        prepared_sources,
    )
    order_columns = best_order_columns(passthrough.columns)
    ordering = [
        F.col(column).desc_nulls_last() for column in order_columns
    ] + [F.col("_passthrough_source_priority").desc()]
    window = Window.partitionBy(*keys).orderBy(*ordering)
    passthrough = (
        passthrough.withColumn("_passthrough_rn", F.row_number().over(window))
        .filter(F.col("_passthrough_rn") == 1)
        .drop("_passthrough_rn", "_passthrough_source_priority")
    )

    merge_metrics = merge_unique(
          target_fqn,
          passthrough,
          keys,
          known_nonempty=True,
          source_is_unique=True,
          compare_columns=payload_compare_columns(
              table_name, target_columns, passthrough.columns, keys
          ),
    )
    promoted_keys = metric_int(merge_metrics, "numSourceRows")
    if DRY_RUN:
        promoted_keys = metric_int(merge_metrics, "source_rows")

    if live_nonempty:
        delete_by_keys(staging_fqn, live_source, keys, known_nonempty=True)
    if archived_nonempty:
        mark_archive_recovered_by_keys(abandoned_fqn, archive_source, keys)

    write_table_log(
        table_name=log_name,
        status="DRY_RUN" if DRY_RUN else "PASSTHROUGH",
        merge_keys=keys,
        staged_rows=promoted_keys,
        promoted_keys=promoted_keys,
        remaining_rows=promoted_keys if DRY_RUN else 0,
    )
    print(
        f"{log_name}: pass-through reason={reason} promoted_keys={promoted_keys:,} "
        f"recovered_archive={archived_nonempty} completed in "
        f"{time.time() - started:.1f}s"
    )
    return None



def prepare_raw_source(
    resolved_df,
    raw_fqn,
    keys,
    raw_cols=None,
):
    raw_cols = raw_cols or table_columns(raw_fqn)
    source_upper = upper_map(resolved_df.columns)
    expressions = []
    for raw_col in raw_cols:
        upper = raw_col.upper()
        if upper == "TRUST":
            expressions.append(F.col("_resolved_trust").alias(raw_col))
        elif upper == "ENCNTR_ID":
            original = (
                F.when(
                    F.col(source_upper["ENCNTR_ID"]).isNotNull()
                    & (F.col(source_upper["ENCNTR_ID"]) != 0),
                    F.col(source_upper["ENCNTR_ID"]),
                )
                if "ENCNTR_ID" in source_upper
                else F.lit(None).cast("long")
            )
            expressions.append(
                F.coalesce(F.col("_resolved_encntr_id"), original).alias(raw_col)
            )
        elif upper == "ORGANIZATION_ID":
            original = (
                F.when(
                    F.col(source_upper["ORGANIZATION_ID"]).isNotNull()
                    & (F.col(source_upper["ORGANIZATION_ID"]) != 0),
                    F.col(source_upper["ORGANIZATION_ID"]),
                )
                if "ORGANIZATION_ID" in source_upper
                else F.lit(None).cast("long")
            )
            expressions.append(
                F.coalesce(F.col("_resolved_organization_id"), original).alias(raw_col)
            )
        elif upper in source_upper:
            expressions.append(F.col(source_upper[upper]).alias(raw_col))
    source = resolved_df.select(*expressions)
    source_upper = upper_map(source.columns)
    missing_keys = [k for k in keys if k.upper() not in source_upper]
    if missing_keys:
        raise ValueError(f"Raw merge source lacks keys {missing_keys}")
    return source


def classify_table(table_name, staging_fqn, raw_fqn):
    started = time.time()
    staging_columns = table_columns(staging_fqn)
    keys = discover_merge_keys(table_name, staging_fqn)
    staging, control_payload = classification_candidates(
        table_name, staging_fqn, staging_columns, keys
    )
    if staging.limit(1).count() == 0:
        print(f"{table_name}: skipped; no staging or mapping content changed")
        return control_payload

    candidate_scratch_fqn = None
    scratch_fqn = None
    try:
        if not DRY_RUN:
            # Spark cache/persist APIs are unsupported on serverless. Preserve the
            # candidate snapshot by materializing it to a run-scoped Delta table.
            candidate_scratch_fqn = (
                f"{CATALOG}.{TMP_SCHEMA}._trust_candidates_v3_"
                f"{re.sub(r'[^A-Za-z0-9_]', '_', table_name)}_{RUN_TOKEN}"
            )
            SCRATCH_TABLES.append(candidate_scratch_fqn)
            spark.sql(f"DROP TABLE IF EXISTS {candidate_scratch_fqn}")
            (
                staging.write.format("delta")
                .mode("overwrite")
                .saveAsTable(candidate_scratch_fqn)
            )
            staging = spark.table(candidate_scratch_fqn)

        null_key_condition = any_null_key(keys)
        attempts_col = F.coalesce(F.col("_classification_attempts"), F.lit(0))
        staging_stats = staging.agg(
            F.count(F.lit(1)).alias("staged_rows"),
            F.sum(F.when(null_key_condition, 1).otherwise(0)).alias("null_key_rows"),
            F.sum(
                F.when(
                    null_key_condition
                    & (attempts_col >= MAX_CLASSIFICATION_ATTEMPTS - 1),
                    1,
                ).otherwise(0)
            ).alias("null_key_rows_at_cap"),
        ).first()
        staged_rows = int(staging_stats["staged_rows"] or 0)
        null_key_count = int(staging_stats["null_key_rows"] or 0)
        null_cap_count = int(staging_stats["null_key_rows_at_cap"] or 0)
        null_retry_count = max(0, null_key_count - null_cap_count)
        valid_rows = staged_rows - null_key_count
        print(f"\n{table_name}: candidate_rows={staged_rows:,} keys={keys}")

        raw_columns = ensure_raw_target(table_name, staging_fqn, raw_fqn)
        with MAPPING_LOCK:
            backfill_pending_lookups(table_name, staging, staging_columns)

        if null_key_count:
            null_key_df = staging.filter(null_key_condition)
            null_at_cap = null_key_df.filter(
                attempts_col >= MAX_CLASSIFICATION_ATTEMPTS - 1
            )
            if null_cap_count:
                archive_fqn = f"{CATALOG}.{ABANDONED_SCHEMA}.{table_name}"
                archive_rows(
                    null_at_cap,
                    staging_columns,
                    archive_fqn,
                    "null_merge_key",
                    expected_count=null_cap_count,
                )
                delete_by_fingerprint(
                    staging_fqn,
                    null_at_cap,
                    staging_columns,
                    known_nonempty=True,
                )
            if null_retry_count:
                increment_attempts_by_fingerprint(
                    staging_fqn,
                    null_key_df.filter(
                        attempts_col < MAX_CLASSIFICATION_ATTEMPTS - 1
                    ),
                    staging_columns,
                )

        if valid_rows == 0:
            write_table_log(
                table_name,
                "DRY_RUN" if DRY_RUN else "SUCCESS",
                keys,
                staged_rows,
                abandoned_keys=null_cap_count,
                remaining_rows=null_retry_count,
            )
            control_payload["staging_version"] = current_version(staging_fqn)
            print(f"  completed in {time.time() - started:.1f}s")
            return control_payload

        resolved_plan = build_resolved(
            table_name, staging_fqn, keys, staging_df=staging
        )
        use_scratch = not DRY_RUN and valid_rows >= MATERIALIZE_MIN_ROWS
        if use_scratch:
            scratch_fqn = (
                f"{CATALOG}.{TMP_SCHEMA}._trust_resolved_v3_"
                f"{re.sub(r'[^A-Za-z0-9_]', '_', table_name)}_{RUN_TOKEN}"
            )
            SCRATCH_TABLES.append(scratch_fqn)
            spark.sql(f"DROP TABLE IF EXISTS {scratch_fqn}")
            (
                resolved_plan.write.format("delta")
                .mode("overwrite")
                .saveAsTable(scratch_fqn)
            )
            resolved = spark.table(scratch_fqn)
        else:
            resolved = resolved_plan

        keep_bhrut = table_name.lower() in KEEP_BHRUT_TABLES
        trust_upper = F.upper(F.trim(F.col("_resolved_trust")))
        conflict_condition = F.coalesce(F.col("_trust_conflict"), F.lit(False))
        promoted_condition = F.col("_resolved_trust").isNotNull() & (
            (trust_upper != "BHRUT") | F.lit(keep_bhrut)
        )
        excluded_condition = (trust_upper == "BHRUT") & F.lit(not keep_bhrut)
        unclassified_condition = F.col("_resolved_trust").isNull()
        land_null_condition = unclassified_condition & (
            attempts_col >= MAX_CLASSIFICATION_ATTEMPTS - 1
        )
        retry_condition = unclassified_condition & (
            attempts_col < MAX_CLASSIFICATION_ATTEMPTS - 1
        )

        outcome_stats = resolved.agg(
            F.count(F.lit(1)).alias("resolved_keys"),
            F.sum(F.when(promoted_condition, 1).otherwise(0)).alias("trusted_promoted_keys"),
            F.sum(F.when(excluded_condition, 1).otherwise(0)).alias("excluded_keys"),
            F.sum(F.when(conflict_condition, 1).otherwise(0)).alias("conflict_keys"),
            F.sum(F.when(land_null_condition, 1).otherwise(0)).alias("landed_null_keys"),
            F.sum(F.when(retry_condition, 1).otherwise(0)).alias("retry_keys"),
        ).first()
        resolved_keys = int(outcome_stats["resolved_keys"] or 0)
        trusted_promoted_keys = int(outcome_stats["trusted_promoted_keys"] or 0)
        excluded_keys = int(outcome_stats["excluded_keys"] or 0)
        conflict_keys = int(outcome_stats["conflict_keys"] or 0)
        landed_null_keys = int(outcome_stats["landed_null_keys"] or 0)
        retry_keys = int(outcome_stats["retry_keys"] or 0)
        promoted_keys = trusted_promoted_keys + landed_null_keys

        promoted = resolved.filter(promoted_condition)
        excluded = resolved.filter(excluded_condition)
        landed_null = resolved.filter(land_null_condition)
        to_retry = resolved.filter(retry_condition)

        if trusted_promoted_keys:
            raw_source = prepare_raw_source(promoted, raw_fqn, keys, raw_columns)
            merge_unique(
                raw_fqn,
                raw_source,
                keys,
                known_nonempty=True,
                source_is_unique=True,
            )
            delete_by_keys(staging_fqn, promoted, keys, known_nonempty=True)

        if landed_null_keys:
            # Keep an immutable evidence snapshot, but do not sacrifice raw
            # completeness. Only valid-key rows reach this branch.
            archive_fqn = f"{CATALOG}.{ABANDONED_SCHEMA}.{table_name}"
            archive_rows(
                landed_null,
                staging_columns,
                archive_fqn,
                "UNRESOLVED_LANDED_NULL_TRUST",
                expected_count=landed_null_keys,
            )
            landed_source = prepare_raw_source(
                landed_null
                .withColumn("_resolved_encntr_id", F.lit(None).cast("long"))
                .withColumn("_resolved_organization_id", F.lit(None).cast("long")),
                raw_fqn,
                keys,
                raw_columns,
            )
            key_names = {key.upper() for key in keys}
            fallthrough_update_columns = [
                column
                for column in raw_columns
                if column.upper() not in key_names and column.upper() != "TRUST"
            ]
            merge_unique(
                raw_fqn,
                landed_source,
                keys,
                update_columns=fallthrough_update_columns,
                known_nonempty=True,
                source_is_unique=True,
            )
            delete_by_keys(staging_fqn, landed_null, keys, known_nonempty=True)

        if excluded_keys:
            excluded_fqn = f"{CATALOG}.{EXCLUDED_SCHEMA}.{table_name}"
            archive_rows(
                excluded,
                staging_columns,
                excluded_fqn,
                "BHRUT_FILTERED",
                expected_count=excluded_keys,
            )
            delete_by_keys(staging_fqn, excluded, keys, known_nonempty=True)

        if retry_keys:
            increment_attempts_by_keys(
                staging_fqn, to_retry, keys, known_nonempty=True
            )

        abandoned_keys = null_cap_count
        table_status = (
            "DRY_RUN"
            if DRY_RUN
            else ("SUCCESS_WITH_NULL_TRUST" if landed_null_keys else "SUCCESS")
        )
        write_table_log(
            table_name=table_name,
            status=table_status,
            merge_keys=keys,
            staged_rows=staged_rows,
            resolved_keys=resolved_keys,
            promoted_keys=promoted_keys,
            excluded_keys=excluded_keys,
            abandoned_keys=abandoned_keys,
            remaining_rows=(
                null_retry_count + retry_keys
            ),
            error_message=(
                f"evidence_conflicts={conflict_keys}; "
                f"unresolved_landed_null={landed_null_keys}"
                if conflict_keys or landed_null_keys
                else None
            ),
        )
        control_payload["staging_version"] = current_version(staging_fqn)
        print(
            f"  completed in {time.time() - started:.1f}s "
            f"(candidate_rows={staged_rows:,}, "
            f"trusted_promoted={trusted_promoted_keys:,}, "
            f"unresolved_retry={retry_keys:,}, "
            f"unresolved_landed_null={landed_null_keys:,}, "
            f"evidence_conflicts={conflict_keys:,}, "
            f"materialized={use_scratch})"
        )
        return control_payload
    finally:
        for cleanup_fqn in (scratch_fqn, candidate_scratch_fqn):
            if cleanup_fqn and not DRY_RUN:
                try:
                    spark.sql(f"DROP TABLE IF EXISTS {cleanup_fqn}")
                finally:
                    if cleanup_fqn in SCRATCH_TABLES:
                        SCRATCH_TABLES.remove(cleanup_fqn)


def table_filter_allows(table_name, *qualified_names):
    if not TABLE_FILTER:
        return True
    requested = {
        value.strip().lower()
        for value in TABLE_FILTER.split(",")
        if value.strip()
    }
    candidates = {table_name.lower()}
    candidates.update(value.lower() for value in qualified_names)
    return bool(requested & candidates)



def discover_staging_tables():
    rows = spark.sql(f"SHOW TABLES IN {CATALOG}.{STAGING_SCHEMA}").collect()
    names = sorted(
        row["tableName"] for row in rows
    )
    names = [
        name for name in names
        if table_filter_allows(
            name,
            f"{CATALOG}.{STAGING_SCHEMA}.{name}",
            f"{CATALOG}.{RAW_SCHEMA}.{name}",
        )
    ]

    priority = {name: index for index, name in enumerate(SOURCE_PRIORITY)}
    return sorted(
        names,
        key=lambda name: (
            100000
            if name.lower() == "mill_long_text"
            else priority.get(name.lower(), 9999),
            name.lower(),
        ),
    )


def discover_lookup_passthrough_tables():
    if not INCLUDE_LOOKUP_PASSTHROUGH:
        return []
    try:
        rows = spark.sql(
            f"SHOW TABLES IN {LOOKUP_CATALOG}.{LOOKUP_STAGING_SCHEMA}"
        ).collect()
    except Exception as exc:
        print(
            "Lookup pass-through discovery skipped: "
            f"{type(exc).__name__}: {exc}"
        )
        return []
    return sorted(
        row["tableName"]
        for row in rows
        if row["tableName"].lower().startswith("mill_")
        and table_filter_allows(
            row["tableName"],
            f"{LOOKUP_CATALOG}.{LOOKUP_STAGING_SCHEMA}.{row['tableName']}",
            f"{LOOKUP_CATALOG}.{LOOKUP_TARGET_SCHEMA}.{row['tableName']}",
        )
    )


def discover_work_items():
    items = []
    for table_name in discover_staging_tables():
        staging_fqn = f"{CATALOG}.{STAGING_SCHEMA}.{table_name}"
        target_fqn = f"{CATALOG}.{RAW_SCHEMA}.{table_name}"
        passthrough, reason, routes = passthrough_decision(
            table_name, table_columns(staging_fqn)
        )
        items.append(
            {
                "table_name": table_name,
                "log_name": table_name,
                "staging_fqn": staging_fqn,
                "target_fqn": target_fqn,
                "abandoned_fqn": (
                    f"{CATALOG}.{ABANDONED_SCHEMA}.{table_name}"
                    if passthrough
                    else None
                ),
                "mode": "passthrough" if passthrough else "classify",
                "reason": reason,
                "routes": routes,
            }
        )

    for table_name in discover_lookup_passthrough_tables():
        items.append(
            {
                "table_name": table_name,
                "log_name": (
                    f"{LOOKUP_CATALOG}.{LOOKUP_TARGET_SCHEMA}.{table_name}"
                ),
                "staging_fqn": (
                    f"{LOOKUP_CATALOG}.{LOOKUP_STAGING_SCHEMA}.{table_name}"
                ),
                "target_fqn": (
                    f"{LOOKUP_CATALOG}.{LOOKUP_TARGET_SCHEMA}.{table_name}"
                ),
                "abandoned_fqn": None,
                "mode": "passthrough",
                "reason": "lookup_catalog",
                "routes": [],
            }
        )
    return items



def flag_unknown_organizations():
    enc_orgs = (
        spark.table(ENC_ORG_TBL)
        .select(F.col("ORGANIZATION_ID").alias("_enc_org_id"))
        .filter(F.col("_enc_org_id").isNotNull() & (F.col("_enc_org_id") != 0))
        .dropDuplicates()
    )
    mapped_orgs = spark.table(TRUST_MAP_TBL).select(
        F.col("organization_id").alias("_mapped_org_id")
    )
    unknown = (
        enc_orgs.join(
            F.broadcast(mapped_orgs),
            F.col("_enc_org_id") == F.col("_mapped_org_id"),
            "left_anti",
        )
        .select(F.col("_enc_org_id").alias("ORGANIZATION_ID"))
    )
    if unknown.limit(1).count() == 0 or DRY_RUN:
        return

    org_fqn = f"{CATALOG}.{RAW_SCHEMA}.mill_organization"
    if table_exists(org_fqn) and "ORG_NAME" in upper_map(table_columns(org_fqn)):
        details = unknown.join(
            spark.table(org_fqn).select("ORGANIZATION_ID", "ORG_NAME"),
            "ORGANIZATION_ID",
            "left",
        ).select(
            F.col("ORGANIZATION_ID").alias("organization_id"),
            F.coalesce(F.col("ORG_NAME"), F.lit("UNKNOWN")).alias("organization_name"),
        )
    else:
        details = unknown.select(
            F.col("ORGANIZATION_ID").alias("organization_id"),
            F.lit("UNKNOWN").alias("organization_name"),
        )

    source = details.select(
        "organization_id",
        "organization_name",
        F.lit("UNKNOWN_TRUST").alias("flag_type"),
        F.lit("Organization is absent from the canonical trust map").alias("flag_reason"),
        F.current_timestamp().alias("first_seen"),
        F.current_timestamp().alias("last_seen"),
        F.lit(False).alias("resolved"),
        F.lit(None).cast("timestamp").alias("resolved_at"),
    )
    DeltaTable.forName(spark, FLAG_TBL).alias("t").merge(
        source.alias("s"),
        (F.col("t.organization_id") == F.col("s.organization_id"))
        & (F.col("t.flag_type") == F.col("s.flag_type"))
        & (F.col("t.resolved") == F.lit(False)),
    ).whenMatchedUpdate(
        set={
            "organization_name": F.col("s.organization_name"),
            "last_seen": F.col("s.last_seen"),
        }
    ).whenNotMatchedInsertAll().execute()


# COMMAND ----------

errors = []
tables = []
control_updates = []
classifiable_items = []
fatal_error = None

try:
    ensure_setup()
    tables = discover_work_items()
    classifiable_items = [
        item for item in tables if item["mode"] == "classify"
    ]
    passthrough_count = len(tables) - len(classifiable_items)
    print(
        f"work items={len(tables)} classify={len(classifiable_items)} "
        f"passthrough={passthrough_count}"
    )
    for item in tables:
        if item["mode"] == "passthrough":
            print(
                f"  pass-through {item['log_name']}: "
                f"reason={item['reason']} routes={item['routes']}"
            )

    if classifiable_items:
        refresh_encounters_from_cdf()
        refresh_hub_from_cdf()
        RUN_DEPENDENCIES.update(capture_dependency_watermarks())
        print(
            "dependency watermarks="
            + json.dumps(RUN_DEPENDENCIES, default=str)
        )
    else:
        print(
            "No classifiable work items; skipping encounter/hub refresh and "
            "dependency scans."
        )

    def process_one_table(item):
        operation_tag = (
            f"trust-{RUN_TOKEN}-"
            + re.sub(r"[^A-Za-z0-9_]", "_", item["log_name"])
        )[:240]
        # Spark Connect/serverless has no SparkContext; SparkSession tags are
        # thread-scoped and preserve per-table operation grouping.
        spark.addTag(operation_tag)
        try:
            if item["mode"] == "passthrough":
                return passthrough_table(
                    table_name=item["table_name"],
                    log_name=item["log_name"],
                    staging_fqn=item["staging_fqn"],
                    target_fqn=item["target_fqn"],
                    reason=item["reason"],
                    abandoned_fqn=item["abandoned_fqn"],
                )
            return classify_table(
                item["table_name"],
                item["staging_fqn"],
                item["target_fqn"],
            )
        finally:
            spark.removeTag(operation_tag)

    with ThreadPoolExecutor(
        max_workers=min(MAX_PARALLEL_TABLES, max(1, len(tables)))
    ) as executor:
        futures = {
            executor.submit(process_one_table, item): item
            for item in tables
        }
        for future in as_completed(futures):
            item = futures[future]
            log_name = item["log_name"]
            try:
                payload = future.result()
                if payload:
                    control_updates.append(payload)
            except Exception as exc:
                message = f"{type(exc).__name__}: {str(exc)}"
                errors.append((log_name, message))
                try:
                    write_table_log(
                        table_name=log_name,
                        status="FAILED",
                        merge_keys=[],
                        staged_rows=0,
                        remaining_rows=0,
                        error_message=message[:8000],
                    )
                except Exception:
                    pass
                print(f"FAILED {log_name}: {message}")

    # A single writer avoids Delta conflicts on the shared control table.
    for payload in sorted(
        control_updates, key=lambda value: value["table_name"]
    ):
        set_classification_control(payload)

    if classifiable_items:
        flag_unknown_organizations()
    status = "FAILED" if errors else ("DRY_RUN" if DRY_RUN else "SUCCESS")
    write_run_log(
        status=status,
        table_count=len(tables),
        failed_count=len(errors),
        notes=json.dumps(errors)[:16000] if errors else None,
    )
except Exception as exc:
    fatal_error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
    errors.append(("__NOTEBOOK__", fatal_error))
    try:
        write_run_log(
            status="FAILED",
            table_count=len(tables),
            failed_count=len(errors),
            notes=fatal_error[:16000],
        )
    except Exception as log_exc:
        print(f"Unable to persist fatal classification error: {log_exc}")
finally:
    if not DRY_RUN:
        for scratch_fqn in list(SCRATCH_TABLES):
            try:
                spark.sql(f"DROP TABLE IF EXISTS {scratch_fqn}")
            except Exception:
                pass
    print("=" * 88)
    print(
        f"run_id={RUN_ID} tables={len(tables)} failures={len(errors)} "
        f"parallelism={MAX_PARALLEL_TABLES} dry_run={DRY_RUN}"
    )
    print("=" * 88)

if fatal_error:
    raise RuntimeError(fatal_error)

if errors:
    raise RuntimeError(
        f"Trust classification failed for {len(errors)} table(s): "
        + "; ".join(f"{table}: {message}" for table, message in errors[:10])
    )

result = {
    "run_id": RUN_ID,
    "status": "DRY_RUN" if DRY_RUN else "SUCCESS",
    "tables": len(tables),
    "failures": 0,
}
try:
    dbutils.notebook.exit(json.dumps(result))
except Exception:
    print(json.dumps(result))
