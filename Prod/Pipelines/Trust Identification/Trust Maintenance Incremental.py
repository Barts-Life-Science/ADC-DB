# Databricks notebook source
# Root fix candidate v2: replay all 14 Millennium parent-key routes against full raw history.
# Non-mill sources remain intentionally out of scope.
# Trust Maintenance Incremental - corrected implementation
#
# This notebook consumes durable encounter-remapping events produced by the
# updated classification notebook. It never refreshes the trust map from a
# second hard-coded list, never overwrites ADC_UPDT, resolves NULL trust before
# BHRUT cleanup, archives exclusions, records table-level metrics, and fails the
# task when any table operation fails.

# COMMAND ----------

import json
import time
import uuid
from datetime import datetime, timezone
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

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
OPTIMIZE_LOOKUPS = _widget("optimize_lookups", "false", ["false", "true"]).lower() == "true"

CATALOG = "4_prod" if TARGET_ENV == "prod" else "8_dev"
RAW_SCHEMA = "raw"
TMP_SCHEMA = "tmp"
RAW_EXCLUDED_SCHEMA = "raw_excluded"
STAGING_SCHEMA = "staging"
LOG_CATALOG = "6_mgmt" if TARGET_ENV == "prod" else "8_dev"
LOG_SCHEMA = "logs" if TARGET_ENV == "prod" else "trust_maintenance"

RUN_ID = str(uuid.uuid4())
RUN_STARTED_AT = datetime.now(timezone.utc)

TRUST_MAP_TBL = f"{CATALOG}.{TMP_SCHEMA}.org_to_trust_map"
POLICY_TBL = f"{CATALOG}.{TMP_SCHEMA}.trust_table_policy"
ENC_ORG_TBL = f"{CATALOG}.{TMP_SCHEMA}.sw_enc_org_map"
HUB_TBL = f"{CATALOG}.{TMP_SCHEMA}.sw_mapping_hub"
CHANGED_ENC_TBL = f"{CATALOG}.{TMP_SCHEMA}.sw_changed_encounters"
RUN_LOG_TBL = f"{LOG_CATALOG}.{LOG_SCHEMA}.trust_maintenance_run_log_v2"
TABLE_LOG_TBL = f"{LOG_CATALOG}.{LOG_SCHEMA}.trust_maintenance_table_log_v2"

# Keep this Millennium-only routing contract aligned with Trust Classification
# Incremental. Non-mill sources intentionally remain outside Trust maintenance.
GLOBAL_PASSTHROUGH_TABLES = {
    "mill_organization",
    "mill_bed", "mill_nurse_unit", "mill_room",
    "mill_pm_loc_attrib", "mill_pm_loc_attrib_hist",
    "mill_track_event", "mill_track_group",
    "mill_encounter_org",
}

# Historical mutation is deliberately deferred for assets whose attribution or
# row-version policy is not safe for the generic resolver. Classification still
# protects new classifiable rows; these historical populations require their
# dedicated review/sub-plan.
HISTORICAL_REPAIR_BLOCKED_TABLES = {
    "mill_ce_blob",
    "mill_organization_alias",
    "mill_org_org_reltn",
    "mill_org_type_reltn",
    "mill_location",
}

DIRECT_ENC_COLUMN_OVERRIDES = {
    "mill_referral": ["OUTBOUND_ENCNTR_ID"],
    "mill_referral_hist": ["OUTBOUND_ENCNTR_ID"],
    "mill_problem": ["ORIGINATING_ENCNTR_ID", "UPDATE_ENCNTR_ID"],
    "mill_cds_batch_content_hist": ["ENCOUNTER_ID"],
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
}

SOURCE_PRIORITY = [
    "mill_encounter",
    "mill_surgical_case",
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
print("TRUST MAINTENANCE INCREMENTAL - UPDATED")
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


def latest_metrics(fqn, operation=None):
    for row in spark.sql(f"DESCRIBE HISTORY {fqn} LIMIT 20").collect():
        if operation is None or row["operation"] == operation:
            return dict(row["operationMetrics"] or {})
    return {}


def row_fingerprint(columns, alias=None):
    prefix = f"{alias}." if alias else ""
    values = [
        F.coalesce(F.col(f"{prefix}{c}").cast("string"), F.lit("<NULL>")).alias(c)
        for c in columns
    ]
    return F.sha2(F.to_json(F.struct(*values)), 256)


def write_run_log(status, table_count, failed_count, notes=None):
    if DRY_RUN:
        print(
            f"DRY RUN: run_log status={status} tables={table_count} "
            f"failures={failed_count}"
        )
        return
    values = [(
        RUN_ID, RUN_STARTED_AT, datetime.now(timezone.utc), TARGET_ENV,
        status, int(table_count), int(failed_count), notes,
    )]
    schema = (
        "run_id string, started_at timestamp, ended_at timestamp, target_env string, "
        "status string, table_count long, failed_count long, notes string"
    )
    spark.createDataFrame(values, schema).write.mode("append").saveAsTable(RUN_LOG_TBL)


def write_table_log(table_name, action, status, rows_affected=0, error_message=None):
    if DRY_RUN:
        print(
            f"DRY RUN: table_log table={table_name} action={action} "
            f"status={status} rows={int(rows_affected or 0)}"
        )
        return
    values = [(
        RUN_ID, datetime.now(timezone.utc), table_name, action, status,
        int(rows_affected or 0), error_message,
    )]
    schema = (
        "run_id string, logged_at timestamp, table_name string, action string, "
        "status string, rows_affected long, error_message string"
    )
    spark.createDataFrame(values, schema).write.mode("append").saveAsTable(TABLE_LOG_TBL)


def ensure_setup():
    if DRY_RUN:
        required = [TRUST_MAP_TBL, POLICY_TBL, ENC_ORG_TBL, CHANGED_ENC_TBL]
        missing = [fqn for fqn in required if not table_exists(fqn)]
        if missing:
            raise RuntimeError(
                "DRY RUN requires existing shared trust infrastructure: "
                + ", ".join(missing)
            )
        print("DRY RUN: setup verified; no schemas, tables or logs were mutated")
        return

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LOG_CATALOG}.{LOG_SCHEMA}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{RAW_EXCLUDED_SCHEMA}")
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
            action STRING,
            status STRING,
            rows_affected BIGINT,
            error_message STRING
        ) USING DELTA
    """)
    required = [TRUST_MAP_TBL, POLICY_TBL, ENC_ORG_TBL, CHANGED_ENC_TBL]
    missing = [fqn for fqn in required if not table_exists(fqn)]
    if missing:
        raise RuntimeError(
            "Missing shared trust infrastructure. Run the updated classification "
            f"notebook setup first. Missing: {missing}"
        )
    changed_cols = upper_map(table_columns(CHANGED_ENC_TBL))
    required_changed = {"NEW_ORGANIZATION_ID", "RUN_ID", "PROCESSED_AT"}
    if not required_changed.issubset(changed_cols):
        raise RuntimeError(
            f"{CHANGED_ENC_TBL} does not have the durable-v2 columns. "
            "Run the updated classification notebook setup first."
        )

    # Normalize historical casing without replacing the table or its identity.
    if not DRY_RUN:
        spark.sql(f"""
            UPDATE {TRUST_MAP_TBL}
            SET trust = CASE
                WHEN UPPER(trust) = 'BHRUT' THEN 'BHRUT'
                WHEN UPPER(trust) = 'BARTS' THEN 'Barts'
                ELSE trust
            END
            WHERE trust IS NOT NULL
        """)


def discover_raw_tables():
    requested = {x.strip() for x in TABLE_FILTER.split(",") if x.strip()}
    names = []
    for row in spark.sql(f"SHOW TABLES IN {CATALOG}.{RAW_SCHEMA}").collect():
        name = row["tableName"]
        if not name.lower().startswith("mill_"):
            continue
        if requested and name.lower() not in requested:
            continue
        try:
            if "TRUST" in upper_map(table_columns(f"{CATALOG}.{RAW_SCHEMA}.{name}")):
                names.append(name)
        except Exception:
            continue
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


def policy_map():
    return {
        row["table_name"].lower(): bool(row["keep_bhrut"])
        for row in spark.table(POLICY_TBL).collect()
    }


def pending_encounter_changes():
    pending = spark.table(CHANGED_ENC_TBL).filter(F.col("processed_at").isNull())
    if pending.limit(1).count() == 0:
        return pending

    window = Window.partitionBy("ENCNTR_ID").orderBy(
        F.col("detected_at").desc_nulls_last(),
        F.col("run_id").desc_nulls_last(),
    )
    latest = pending.withColumn("_rn", F.row_number().over(window)).filter(
        F.col("_rn") == 1
    )
    current_enc = spark.table(ENC_ORG_TBL).select(
        "ENCNTR_ID", F.col("ORGANIZATION_ID").alias("_current_organization_id")
    )
    trust_map = spark.table(TRUST_MAP_TBL).select(
        F.col("organization_id").alias("_trust_organization_id"),
        F.col("trust").alias("_new_trust"),
    )
    return (
        latest.join(current_enc, "ENCNTR_ID", "left")
        .withColumn(
            "_effective_organization_id",
            F.coalesce(F.col("new_organization_id"), F.col("_current_organization_id")),
        )
        .join(
            F.broadcast(trust_map),
            F.col("_effective_organization_id") == F.col("_trust_organization_id"),
            "left",
        )
        .select(
            F.col("ENCNTR_ID").cast("long").alias("ENCNTR_ID"),
            F.col("_new_trust").alias("Trust"),
        )
        .dropDuplicates(["ENCNTR_ID"])
    )


def propagate_remaps(raw_tables):
    pending = pending_encounter_changes()
    pending_count = pending.count()
    if pending_count == 0:
        print("No unprocessed encounter remaps.")
        return []

    print(f"Propagating {pending_count:,} encounter remaps.")
    errors = []
    for table_name in raw_tables:
        fqn = f"{CATALOG}.{RAW_SCHEMA}.{table_name}"
        cols = upper_map(table_columns(fqn))
        if "ENCNTR_ID" not in cols:
            continue
        try:
            source = pending.select(
                F.col("ENCNTR_ID").alias(cols["ENCNTR_ID"]),
                F.col("Trust").alias(cols["TRUST"]),
            )
            if DRY_RUN:
                affected = (
                    spark.table(fqn).alias("t")
                    .join(source.alias("s"), cols["ENCNTR_ID"])
                    .filter(~F.col(f"t.{cols['TRUST']}").eqNullSafe(F.col(f"s.{cols['TRUST']}")))
                    .count()
                )
            else:
                DeltaTable.forName(spark, fqn).alias("t").merge(
                    source.alias("s"),
                    F.col(f"t.{cols['ENCNTR_ID']}") == F.col(f"s.{cols['ENCNTR_ID']}"),
                ).whenMatchedUpdate(
                    condition=~F.col(f"t.{cols['TRUST']}").eqNullSafe(F.col(f"s.{cols['TRUST']}")),
                    set={cols["TRUST"]: F.col(f"s.{cols['TRUST']}")},
                ).execute()
                metrics = latest_metrics(fqn, "MERGE")
                affected = int(metrics.get("numTargetRowsUpdated", 0))
            write_table_log(table_name, "PROPAGATE_REMAP", "DRY_RUN" if DRY_RUN else "SUCCESS", affected)
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            errors.append((table_name, message))
            write_table_log(table_name, "PROPAGATE_REMAP", "FAILED", 0, message[:8000])

    if not errors and not DRY_RUN:
        DeltaTable.forName(spark, CHANGED_ENC_TBL).update(
            condition=F.col("processed_at").isNull(),
            set={"processed_at": F.current_timestamp()},
        )
    return errors



def best_order_columns(columns):
    upper = upper_map(columns)
    preferred = [
        "ADC_UPDT",
        "LAST_UTC_TS",
        "UPDT_DT_TM",
        "RECORD_UPDATED_DT",
        "_STAGED_AT",
    ]
    return [upper[name] for name in preferred if name in upper]


def direct_encounter_columns(table_name, columns):
    cu = upper_map(columns)
    candidates = DIRECT_ENC_COLUMN_OVERRIDES.get(
        table_name.lower(), ["ENCNTR_ID"]
    )
    return [cu[name] for name in candidates if name in cu]


def direct_encounter_expr(table_name, columns, alias):
    candidates = direct_encounter_columns(table_name, columns)
    if not candidates:
        return None
    values = [
        F.when(
            F.col(f"{alias}.{column}").isNotNull()
            & (F.col(f"{alias}.{column}") != 0),
            F.col(f"{alias}.{column}").cast("long"),
        )
        for column in candidates
    ]
    return F.coalesce(*values)


def applicable_lookup_types(table_name, columns):
    cu = upper_map(columns)
    has_direct_route = bool(direct_encounter_columns(table_name, columns)) or (
        "ORGANIZATION_ID" in cu
    )
    routes = []
    for key_type, (source_table, _) in LOOKUP_SPECS.items():
        if key_type not in cu:
            continue
        if has_direct_route and source_table.lower() == table_name.lower():
            continue
        routes.append(key_type)
    return routes


def raw_lookup_mapping(key_type, needed_keys):
    source_table, source_key = LOOKUP_SPECS[key_type]
    source_fqn = f"{CATALOG}.{RAW_SCHEMA}.{source_table}"
    if not table_exists(source_fqn):
        return None

    source_columns = table_columns(source_fqn)
    source_upper = upper_map(source_columns)
    if source_key not in source_upper:
        return None

    needed = (
        needed_keys.select(F.col(key_type).alias("_needed_key"))
        .filter(F.col("_needed_key").isNotNull() & (F.col("_needed_key") != 0))
        .dropDuplicates()
    )
    src = spark.table(source_fqn).alias("src").join(
        needed.alias("n"),
        F.col(f"src.{source_upper[source_key]}") == F.col("n._needed_key"),
        "inner",
    )
    parent_trust = (
        F.col(f"src.{source_upper['TRUST']}")
        if "TRUST" in source_upper
        else F.lit(None).cast("string")
    )

    if key_type == "SURG_CASE_PROC_ID":
        surgical_case_fqn = f"{CATALOG}.{RAW_SCHEMA}.mill_surgical_case"
        sc = spark.table(surgical_case_fqn).select(
            F.col("SURG_CASE_ID").alias("_sc_case_id"),
            F.col("ENCNTR_ID").alias("_sc_encntr_id"),
            F.col("Trust").alias("_sc_trust"),
        )
        src = src.join(
            sc,
            F.col(f"src.{source_upper['SURG_CASE_ID']}") == F.col("_sc_case_id"),
            "left",
        )
        enc_expr = nonzero_long(F.col("_sc_encntr_id"))
        parent_trust = F.coalesce(parent_trust, F.col("_sc_trust"))
    elif key_type == "IM_STUDY_ID":
        cv_fqn = f"{CATALOG}.{RAW_SCHEMA}.mill_cv_proc"
        cv = spark.table(cv_fqn).select(
            F.col("CV_PROC_ID").alias("_cv_proc_id"),
            F.col("ENCNTR_ID").alias("_cv_encntr_id"),
            F.col("Trust").alias("_cv_trust"),
        )
        src = src.join(
            cv,
            F.col(f"src.{source_upper['ORIG_ENTITY_ID']}") == F.col("_cv_proc_id"),
            "left",
        )
        direct_enc = (
            nonzero_long(F.col(f"src.{source_upper['ENCNTR_ID']}"))
            if "ENCNTR_ID" in source_upper
            else F.lit(None).cast("long")
        )
        enc_expr = F.coalesce(direct_enc, nonzero_long(F.col("_cv_encntr_id")))
        parent_trust = F.coalesce(parent_trust, F.col("_cv_trust"))
    elif key_type == "IM_ACQUIRED_STUDY_ID":
        study_fqn = f"{CATALOG}.{RAW_SCHEMA}.mill_im_study"
        cv_fqn = f"{CATALOG}.{RAW_SCHEMA}.mill_cv_proc"
        study = spark.table(study_fqn).select(
            F.col("IM_STUDY_ID").alias("_study_id"),
            F.col("ENCNTR_ID").alias("_study_encntr_id"),
            F.col("ORIG_ENTITY_ID").alias("_study_orig_entity_id"),
            F.col("Trust").alias("_study_trust"),
        )
        cv = spark.table(cv_fqn).select(
            F.col("CV_PROC_ID").alias("_cv_proc_id"),
            F.col("ENCNTR_ID").alias("_cv_encntr_id"),
            F.col("Trust").alias("_cv_trust"),
        )
        src = src.join(
            study,
            F.col(f"src.{source_upper['MATCHED_STUDY_ID']}") == F.col("_study_id"),
            "left",
        ).join(
            cv,
            F.col("_study_orig_entity_id") == F.col("_cv_proc_id"),
            "left",
        )
        enc_expr = F.coalesce(
            nonzero_long(F.col("_study_encntr_id")),
            nonzero_long(F.col("_cv_encntr_id")),
        )
        parent_trust = F.coalesce(
            parent_trust, F.col("_study_trust"), F.col("_cv_trust")
        )
    elif key_type == "PROBLEM_ID":
        enc_expr = F.coalesce(
            nonzero_long(F.col(f"src.{source_upper['ORIGINATING_ENCNTR_ID']}")),
            nonzero_long(F.col(f"src.{source_upper['UPDATE_ENCNTR_ID']}")),
        )
    elif key_type == "REFERRAL_ID":
        enc_expr = nonzero_long(F.col(f"src.{source_upper['OUTBOUND_ENCNTR_ID']}"))
    elif "ENCNTR_ID" in source_upper:
        enc_expr = nonzero_long(F.col(f"src.{source_upper['ENCNTR_ID']}"))
    else:
        enc_expr = F.lit(None).cast("long")

    order_columns = best_order_columns(source_columns)
    ordering = [
        F.col(f"src.{column}").desc_nulls_last()
        for column in order_columns
    ] or [F.col(f"src.{source_upper[source_key]}").desc()]
    window = Window.partitionBy(
        F.col(f"src.{source_upper[source_key]}")
    ).orderBy(*ordering)
    return (
        src.select(
            F.col(f"src.{source_upper[source_key]}").alias("key_id"),
            enc_expr.cast("long").alias("_parent_encntr_id"),
            parent_trust.alias("_parent_trust"),
            F.row_number().over(window).alias("_rn"),
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def historical_parent_trust(key_type, needed_keys, enc_trust):
    mapping = raw_lookup_mapping(key_type, needed_keys)
    if mapping is None:
        return None

    candidates = (
        mapping.alias("m")
        .join(
            enc_trust.alias("e"),
            F.col("m._parent_encntr_id") == F.col("e._encntr_id"),
            "left",
        )
        .select(
            F.col("m.key_id").alias("key_id"),
            F.coalesce(
                F.col("m._parent_trust"), F.col("e._trust")
            ).alias("_trust"),
        )
        .filter(F.col("_trust").isNotNull())
    )
    grouped = candidates.groupBy("key_id").agg(
        F.countDistinct(F.upper(F.col("_trust"))).alias("_trust_count"),
        F.first(F.col("_trust"), ignorenulls=True).alias("_trust"),
    )
    conflict_count = grouped.filter(F.col("_trust_count") > 1).count()
    if conflict_count:
        raise RuntimeError(
            f"{key_type}: {conflict_count} parent keys resolve to conflicting trusts"
        )
    return grouped.filter(F.col("_trust_count") == 1).select(
        "key_id", "_trust"
    )


def reverse_reference_trust(table_name, target_fqn, target_columns):
    spec = REVERSE_REFERENCE_SPECS.get(table_name.lower())
    if not spec:
        return None
    target_upper = upper_map(target_columns)
    target_key = spec["target_key"]
    if target_key not in target_upper:
        return None

    needed = (
        spark.table(target_fqn)
        .filter(F.col(target_upper["TRUST"]).isNull())
        .select(F.col(target_upper[target_key]).cast("long").alias("key_id"))
        .filter(F.col("key_id").isNotNull() & (F.col("key_id") != 0))
        .dropDuplicates()
    )
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
                F.broadcast(needed.alias("n")),
                F.col(f"src.{source_upper[reference_column]}").cast("long")
                == F.col("n.key_id"),
                "inner",
            )
            .select(
                F.col("n.key_id"),
                F.trim(F.col(f"src.{source_upper['TRUST']}")).alias("_trust"),
            )
            .filter(F.col("_trust").isNotNull())
            .dropDuplicates()
        )

    if not frames:
        return None
    candidates = reduce(lambda left, right: left.unionByName(right), frames)
    grouped = candidates.groupBy("key_id").agg(
        F.countDistinct(F.upper(F.col("_trust"))).alias("_trust_count"),
        F.first(F.col("_trust"), ignorenulls=True).alias("_trust"),
    )
    conflict_count = grouped.filter(F.col("_trust_count") > 1).count()
    if conflict_count:
        print(
            f"{table_name}: leaving {conflict_count:,} reverse-reference keys "
            "unresolved because referring rows disagree on Trust"
        )
    return grouped.filter(F.col("_trust_count") == 1).select("key_id", "_trust")


def merge_null_trust(table_name, fqn, cols, source, condition, action):
    source = source.filter(F.col("_trust").isNotNull())
    if source.limit(1).count() == 0:
        return 0
    if DRY_RUN:
        affected = (
            spark.table(fqn)
            .alias("t")
            .join(source.alias("s"), condition, "inner")
            .filter(F.col(f"t.{cols['TRUST']}").isNull())
            .count()
        )
    else:
        (
            DeltaTable.forName(spark, fqn)
            .alias("t")
            .merge(source.alias("s"), condition)
            .whenMatchedUpdate(
                condition=F.col(f"t.{cols['TRUST']}").isNull(),
                set={cols["TRUST"]: F.col("s._trust")},
            )
            .execute()
        )
        affected = int(
            latest_metrics(fqn, "MERGE").get("numTargetRowsUpdated", 0)
        )
    write_table_log(
        table_name,
        action,
        "DRY_RUN" if DRY_RUN else "SUCCESS",
        affected,
    )
    return affected


def resolve_null_trust(raw_tables):
    errors = []
    trust_map = (
        spark.table(TRUST_MAP_TBL)
        .select(
            F.col("organization_id").cast("long").alias("_organization_id"),
            F.trim(F.col("trust")).alias("_trust"),
        )
        .filter(F.col("_organization_id").isNotNull() & F.col("_trust").isNotNull())
    )
    org_conflicts = (
        trust_map.groupBy("_organization_id")
        .agg(F.countDistinct(F.upper(F.col("_trust"))).alias("_trust_count"))
        .filter(F.col("_trust_count") > 1)
        .limit(1)
        .count()
    )
    if org_conflicts:
        raise RuntimeError(f"{TRUST_MAP_TBL} has organizations mapped to conflicting trusts")
    trust_map = trust_map.groupBy("_organization_id").agg(
        F.min(F.col("_trust")).alias("_trust")
    )
    mapped_encounters = (
        spark.table(ENC_ORG_TBL).alias("eo")
        .select(
            F.col("eo.ENCNTR_ID").cast("long").alias("_encntr_id"),
            F.col("eo.ORGANIZATION_ID").cast("long").alias("_organization_id"),
        )
        .filter(F.col("_encntr_id").isNotNull() & (F.col("_encntr_id") != 0))
    )
    raw_encounter_fqn = f"{CATALOG}.{RAW_SCHEMA}.mill_encounter"
    if table_exists(raw_encounter_fqn):
        raw_encounters = (
            spark.table(raw_encounter_fqn)
            .select(
                F.col("ENCNTR_ID").cast("long").alias("_encntr_id"),
                F.col("ORGANIZATION_ID").cast("long").alias("_organization_id"),
            )
            .filter(F.col("_encntr_id").isNotNull() & (F.col("_encntr_id") != 0))
        )
        encounter_organizations = mapped_encounters.unionByName(raw_encounters)
    else:
        encounter_organizations = mapped_encounters

    enc_candidates = (
        encounter_organizations.alias("eo")
        .join(
            F.broadcast(trust_map.alias("tm")),
            F.col("eo._organization_id") == F.col("tm._organization_id"),
            "left",
        )
        .select(
            F.col("eo._encntr_id").alias("_encntr_id"),
            F.col("tm._trust").alias("_trust"),
        )
        .filter(F.col("_encntr_id").isNotNull() & F.col("_trust").isNotNull())
    )
    enc_conflicts = (
        enc_candidates.groupBy("_encntr_id")
        .agg(F.countDistinct(F.upper(F.col("_trust"))).alias("_trust_count"))
        .filter(F.col("_trust_count") > 1)
        .limit(1)
        .count()
    )
    if enc_conflicts:
        raise RuntimeError(f"{ENC_ORG_TBL} resolves encounters to conflicting trusts")
    enc_trust = enc_candidates.groupBy("_encntr_id").agg(
        F.min(F.col("_trust")).alias("_trust")
    )

    for table_name in raw_tables:
        if table_name.lower() in HISTORICAL_REPAIR_BLOCKED_TABLES:
            write_table_log(
                table_name,
                "RESOLVE_NULL_TRUST",
                "SKIPPED_DEDICATED_REPAIR_REQUIRED",
                0,
            )
            continue
        if table_name.lower() in GLOBAL_PASSTHROUGH_TABLES:
            write_table_log(
                table_name,
                "RESOLVE_NULL_TRUST",
                "SKIPPED_GLOBAL_PASSTHROUGH",
                0,
            )
            continue

        fqn = f"{CATALOG}.{RAW_SCHEMA}.{table_name}"
        cols = upper_map(table_columns(fqn))
        try:
            total_updated = 0

            if "ORGANIZATION_ID" in cols:
                source = trust_map.filter(F.col("_trust").isNotNull())
                total_updated += merge_null_trust(
                    table_name,
                    fqn,
                    cols,
                    source,
                    F.col(f"t.{cols['ORGANIZATION_ID']}") == F.col("s._organization_id"),
                    "RESOLVE_NULL_TRUST_ORGANIZATION",
                )

            encounter_expr = direct_encounter_expr(
                table_name, table_columns(fqn), "t"
            )
            if encounter_expr is not None:
                total_updated += merge_null_trust(
                    table_name,
                    fqn,
                    cols,
                    enc_trust,
                    encounter_expr == F.col("s._encntr_id"),
                    "RESOLVE_NULL_TRUST_ENCOUNTER",
                )

            for key_type in applicable_lookup_types(
                table_name, table_columns(fqn)
            ):
                key_column = cols[key_type]
                needed_keys = (
                    spark.table(fqn)
                    .filter(F.col(cols["TRUST"]).isNull())
                    .select(F.col(key_column).alias(key_type))
                    .filter(
                        F.col(key_type).isNotNull()
                        & (F.col(key_type) != 0)
                    )
                    .dropDuplicates()
                )
                parent_source = historical_parent_trust(
                    key_type, needed_keys, enc_trust
                )
                if parent_source is None:
                    continue
                total_updated += merge_null_trust(
                    table_name,
                    fqn,
                    cols,
                    parent_source,
                    F.col(f"t.{key_column}") == F.col("s.key_id"),
                    f"RESOLVE_NULL_TRUST_PARENT_{key_type}",
                )

            reverse_source = reverse_reference_trust(
                table_name, fqn, table_columns(fqn)
            )
            if reverse_source is not None:
                target_key = REVERSE_REFERENCE_SPECS[table_name.lower()]["target_key"]
                total_updated += merge_null_trust(
                    table_name,
                    fqn,
                    cols,
                    reverse_source,
                    F.col(f"t.{cols[target_key]}").cast("long")
                    == F.col("s.key_id"),
                    "RESOLVE_NULL_TRUST_REVERSE_REFERENCE",
                )

            write_table_log(
                table_name,
                "RESOLVE_NULL_TRUST",
                "DRY_RUN" if DRY_RUN else "SUCCESS",
                total_updated,
            )
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            errors.append((table_name, message))
            write_table_log(
                table_name,
                "RESOLVE_NULL_TRUST",
                "FAILED",
                0,
                message[:8000],
            )
    return errors

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


def archive_raw_exclusions(table_name, rows_df):
    if rows_df.limit(1).count() == 0:
        return 0
    source_columns = rows_df.columns
    archived = (
        rows_df.withColumn("_row_fingerprint", row_fingerprint(source_columns))
        .withColumn("_archive_reason", F.lit("BHRUT_FILTERED"))
        .withColumn("_archived_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(RUN_ID))
        .dropDuplicates(["_row_fingerprint"])
    )
    count = archived.count()
    if DRY_RUN:
        return count

    target_fqn = f"{CATALOG}.{RAW_EXCLUDED_SCHEMA}.{table_name}"
    ensure_archive_table(target_fqn, archived)
    target_upper = upper_map(table_columns(target_fqn))
    source_upper = upper_map(archived.columns)
    values = {
        target_upper[u]: F.col(f"s.{source_upper[u]}")
        for u in target_upper
        if u in source_upper
    }
    DeltaTable.forName(spark, target_fqn).alias("t").merge(
        archived.alias("s"),
        F.col("t._row_fingerprint") == F.col("s._row_fingerprint"),
    ).whenNotMatchedInsert(values=values).execute()
    return count


def cleanup_bhrut(raw_tables, policies):
    errors = []
    for table_name in raw_tables:
        if table_name.lower() in HISTORICAL_REPAIR_BLOCKED_TABLES:
            write_table_log(table_name, "CLEANUP_BHRUT", "SKIPPED_DEDICATED_REPAIR_REQUIRED", 0)
            continue
        if table_name.lower() in GLOBAL_PASSTHROUGH_TABLES:
            write_table_log(table_name, "CLEANUP_BHRUT", "SKIPPED_GLOBAL_PASSTHROUGH", 0)
            continue
        if policies.get(table_name.lower(), False):
            write_table_log(table_name, "CLEANUP_BHRUT", "PROTECTED", 0)
            continue
        fqn = f"{CATALOG}.{RAW_SCHEMA}.{table_name}"
        cols = upper_map(table_columns(fqn))
        try:
            bhrut = spark.table(fqn).filter(F.upper(F.col(cols["TRUST"])) == "BHRUT")
            count = bhrut.count()
            if count:
                archive_raw_exclusions(table_name, bhrut)
                if not DRY_RUN:
                    DeltaTable.forName(spark, fqn).delete(
                        F.upper(F.col(cols["TRUST"])) == "BHRUT"
                    )
            write_table_log(
                table_name, "CLEANUP_BHRUT",
                "DRY_RUN" if DRY_RUN else "SUCCESS", count,
            )
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            errors.append((table_name, message))
            write_table_log(table_name, "CLEANUP_BHRUT", "FAILED", 0, message[:8000])
    return errors


def staging_health_report():
    rows = []
    for row in spark.sql(f"SHOW TABLES IN {CATALOG}.{STAGING_SCHEMA}").collect():
        table_name = row["tableName"]
        if not table_name.lower().startswith("mill_"):
            continue
        fqn = f"{CATALOG}.{STAGING_SCHEMA}.{table_name}"
        cols = upper_map(table_columns(fqn))
        if "_STAGED_AT" not in cols or "_CLASSIFICATION_ATTEMPTS" not in cols:
            continue
        stats = spark.table(fqn).agg(
            F.count("*").alias("rows"),
            F.min(F.col(cols["_STAGED_AT"])).alias("oldest"),
            F.max(F.col(cols["_CLASSIFICATION_ATTEMPTS"])).alias("max_attempts"),
        ).first()
        if stats["rows"]:
            rows.append((table_name, stats["rows"], stats["oldest"], stats["max_attempts"]))
    print(f"Staging tables with pending rows: {len(rows)}")
    for table_name, count, oldest, max_attempts in sorted(rows, key=lambda x: -x[1])[:30]:
        print(
            f"  {table_name:<42} rows={count:>12,} "
            f"oldest={str(oldest)[:19]} max_attempts={max_attempts}"
        )


def optimize_lookups():
    if not OPTIMIZE_LOOKUPS or DRY_RUN:
        return
    spark.sql(f"OPTIMIZE {ENC_ORG_TBL} ZORDER BY (ENCNTR_ID)")
    spark.sql(f"OPTIMIZE {HUB_TBL} ZORDER BY (key_type, key_id)")
    # VACUUM is intentionally omitted so recovery/time-travel is not shortened.


# COMMAND ----------

errors = []
raw_tables = []

try:
    ensure_setup()
    raw_tables = discover_raw_tables()
    policies = policy_map()

    errors.extend(propagate_remaps(raw_tables))
    errors.extend(resolve_null_trust(raw_tables))
    errors.extend(cleanup_bhrut(raw_tables, policies))
    staging_health_report()
    optimize_lookups()

    status = "FAILED" if errors else ("DRY_RUN" if DRY_RUN else "SUCCESS")
    write_run_log(
        status, len(raw_tables), len(errors),
        json.dumps(errors)[:16000] if errors else None,
    )
finally:
    print("=" * 88)
    print(f"run_id={RUN_ID} tables={len(raw_tables)} failures={len(errors)} dry_run={DRY_RUN}")
    print("=" * 88)

if errors:
    raise RuntimeError(
        f"Trust maintenance failed for {len(errors)} table action(s): "
        + "; ".join(f"{table}: {message}" for table, message in errors[:10])
    )

result = {
    "run_id": RUN_ID,
    "status": "DRY_RUN" if DRY_RUN else "SUCCESS",
    "tables": len(raw_tables),
    "failures": 0,
}
try:
    dbutils.notebook.exit(json.dumps(result))
except Exception:
    print(json.dumps(result))
