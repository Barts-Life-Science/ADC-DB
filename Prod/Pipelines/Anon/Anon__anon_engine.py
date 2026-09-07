# Databricks notebook source
# MAGIC %run ./Anon__anon_matcher_lib

# COMMAND ----------

"""Config-driven bronze anonymous-text engine.

The default registry targets dev twins only. Production table names belong in the
human-gated promotion runbook and must never be introduced here implicitly.
"""

# RDE_REPAIR_20260901_BOUNDED_ANON
# ANON_B1_PERF_20260904_V1
# ANON_B1_MERGE_METRIC_20260905_V1
import json
import math
import random
import re
import time
import uuid
from datetime import datetime, timedelta, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, LongType, StringType, StructField, StructType


DEFAULT_MAX_RUNTIME_SECONDS = 14400
DEADLINE_RESERVE_SECONDS = 900

if "ANON_ACTION" in globals():
    ACTION = str(ANON_ACTION).strip()
    FEED = str(ANON_FEED).strip()
    CONTROL_SCHEMA = str(ANON_CONTROL_SCHEMA).strip()
    TARGET_SCHEMA = str(
        globals().get("ANON_TARGET_SCHEMA", "4_prod.bronze")
    ).strip()
    MAX_BATCHES = int(ANON_MAX_BATCHES)
    MAX_RUNTIME_SECONDS = int(
        globals().get("ANON_MAX_RUNTIME_SECONDS", DEFAULT_MAX_RUNTIME_SECONDS)
    )
    WORK_CAP_ROWS = int(globals().get("ANON_WORK_CAP_ROWS", 0))
    RUN_ID = str(ANON_RUN_ID).strip() or uuid.uuid4().hex
    PRODUCTION_CONFIRMATION = str(
        globals().get("ANON_PRODUCTION_CONFIRMATION", "")
    ).strip()
    SKIP_FINGERPRINT_REFRESH = str(
        globals().get("ANON_SKIP_FINGERPRINT_REFRESH", "false")
    ).strip().lower() in {"1", "true", "yes"}
else:
    for name, default in [
        ("action", "run"),
        ("feed", "mill_blob_text"),
        ("control_schema", "6_mgmt.anon"),
        ("target_schema", "4_prod.bronze"),
        ("max_batches", "1000"),
        ("max_runtime_seconds", str(DEFAULT_MAX_RUNTIME_SECONDS)),
        ("work_cap_rows", "0"),
        ("run_id", ""),
        ("production_confirmation", ""),
        ("skip_fingerprint_refresh", "false"),
    ]:
        dbutils.widgets.text(name, default)

    ACTION = dbutils.widgets.get("action").strip()
    FEED = dbutils.widgets.get("feed").strip()
    CONTROL_SCHEMA = dbutils.widgets.get("control_schema").strip()
    TARGET_SCHEMA = dbutils.widgets.get("target_schema").strip() or "4_prod.bronze"
    MAX_BATCHES = int(dbutils.widgets.get("max_batches") or "1000")
    MAX_RUNTIME_SECONDS = int(
        dbutils.widgets.get("max_runtime_seconds") or str(DEFAULT_MAX_RUNTIME_SECONDS)
    )
    WORK_CAP_ROWS = int(dbutils.widgets.get("work_cap_rows") or "0")
    RUN_ID = dbutils.widgets.get("run_id").strip() or uuid.uuid4().hex
    PRODUCTION_CONFIRMATION = dbutils.widgets.get("production_confirmation").strip()
    SKIP_FINGERPRINT_REFRESH = dbutils.widgets.get(
        "skip_fingerprint_refresh"
    ).strip().lower() in {"1", "true", "yes"}

if MAX_BATCHES < 1:
    raise ValueError("max_batches must be positive")
if MAX_RUNTIME_SECONDS < DEADLINE_RESERVE_SECONDS + 60:
    raise ValueError(
        f"max_runtime_seconds must be at least {DEADLINE_RESERVE_SECONDS + 60}"
    )
if WORK_CAP_ROWS < 0:
    raise ValueError("work_cap_rows must be zero (uncapped) or positive")

ENGINE_STARTED_MONOTONIC = time.monotonic()


def _elapsed_seconds():
    return round(time.monotonic() - ENGINE_STARTED_MONOTONIC, 1)


def _deadline_reached():
    return (
        time.monotonic() - ENGINE_STARTED_MONOTONIC
        >= MAX_RUNTIME_SECONDS - DEADLINE_RESERVE_SECONDS
    )


REDACTOR_VERSION = "v3.2"
FINGERPRINT_OVERLAP_DAYS = 7
# person_identifier_current.refreshed_at is assigned when the 7-day-overlapped
# source refresh actually changes a bundle, so a second overlap here would replay
# the same persons on every run for a day and prevent immediate quiescence.
DRIFT_OVERLAP_DAYS = 0
_RETRYABLE_MERGE_TOKENS = (
    "ConcurrentAppendException", "ConcurrentDeleteReadException",
    "ConcurrentDeleteDeleteException", "ConcurrentTransactionException",
    "MetadataChangedException", "ProtocolChangedException",
    "DELTA_CONCURRENT_APPEND", "DELTA_CONCURRENT_DELETE_READ",
    "DELTA_CONCURRENT_DELETE_DELETE", "DELTA_CONCURRENT_TRANSACTION",
    "DELTA_METADATA_CHANGED", "DELTA_PROTOCOL_CHANGED",
)


def _merge_with_retry(sql, label, attempts=6):
    """Run a Delta MERGE, retrying only on optimistic-concurrency conflicts."""
    delay = 30
    for attempt in range(1, attempts + 1):
        try:
            return spark.sql(sql).first()
        except Exception as exc:
            text = f"{type(exc).__name__}: {exc}"
            if attempt == attempts or not any(
                token in text for token in _RETRYABLE_MERGE_TOKENS
            ):
                raise
            print(json.dumps({
                "event": "anon_merge_retry", "label": label, "attempt": attempt,
                "sleep_seconds": delay, "error": text[:300],
            }, sort_keys=True))
            time.sleep(delay + random.uniform(0, delay))
            delay = min(delay * 2, 300)


def _verify_merged_batch(cfg, stage, update_columns, expected_rows):
    """Verify exact target state when Delta's MERGE row metric is inconsistent."""
    staged = spark.table(stage).alias("s")
    target = (
        spark.table(cfg["table"])
        .select(
            *cfg["key_cols"],
            *update_columns,
            F.lit(1).alias("_anon_target_match"),
        )
        .alias("t")
    )
    join_condition = None
    for column in cfg["key_cols"]:
        term = F.col(f"t.`{column}`").eqNullSafe(F.col(f"s.`{column}`"))
        join_condition = term if join_condition is None else join_condition & term
    mismatch = F.col("t._anon_target_match").isNull()
    for column in update_columns:
        mismatch = mismatch | ~F.col(f"t.`{column}`").eqNullSafe(
            F.col(f"s.`{column}`")
        )
    metrics = (
        staged.join(target, join_condition, "left")
        .agg(
            F.count("*").alias("joined_rows"),
            F.count(F.col("t._anon_target_match")).alias("matched_rows"),
            F.sum(mismatch.cast("long")).alias("mismatch_rows"),
        )
        .first()
    )
    result = {
        "expected_rows": int(expected_rows),
        "joined_rows": int(metrics.joined_rows or 0),
        "matched_rows": int(metrics.matched_rows or 0),
        "mismatch_rows": int(metrics.mismatch_rows or 0),
    }
    result["exact"] = (
        result["joined_rows"] == result["expected_rows"]
        and result["matched_rows"] == result["expected_rows"]
        and result["mismatch_rows"] == 0
    )
    return result


def _accept_verified_merge_metric_mismatch(label, merge_result, verification):
    if not verification["exact"]:
        raise AssertionError(
            f"merge fan-out/miss: label={label}, merge_result={merge_result}, "
            f"verification={verification}"
        )
    print(json.dumps({
        "event": "anon_merge_metric_mismatch_verified",
        "label": label,
        "merge_result": merge_result,
        "verification": verification,
    }, default=str, sort_keys=True))


if TARGET_SCHEMA == "4_prod.bronze":
    if CONTROL_SCHEMA != "6_mgmt.anon":
        raise RuntimeError("production target requires control_schema=6_mgmt.anon")
    if PRODUCTION_CONFIRMATION != "RUN_PRODUCTION_ANON_V3_2":
        raise RuntimeError(
            "Production execution is disabled without "
            "production_confirmation=RUN_PRODUCTION_ANON_V3_2"
        )
elif TARGET_SCHEMA.startswith("8_dev."):
    if not CONTROL_SCHEMA.startswith("8_dev."):
        raise RuntimeError("dev target_schema requires an 8_dev control_schema")
else:
    raise RuntimeError(
        f"target_schema {TARGET_SCHEMA!r} must be exactly 4_prod.bronze or under 8_dev."
    )

STATE_COLS = [
    "anon_status",
    "anon_redactor_version",
    "anon_source_text_sha",
    "anon_identity_fingerprint",
    "anon_redaction_count",
    "anon_processed_at",
]
IDENTITY_VALUE_COLUMNS = [
    "first_names", "middle_names", "last_names", "nickname_tokens", "aliases",
    "relatives", "informants", "addresses", "dob", "identity_fingerprint",
]
WORK_BUCKET_COUNT = 256
# Keep identity-table bucketing stable; use finer independent work buckets.
WORK_BATCH_BUCKET_COUNT = 4096
MIN_WORK_BATCH_BUCKET_COUNT = 64
TARGET_ROWS_PER_WORK_BUCKET = 5000

FEED_REGISTRY = {
    "mill_blob_text": dict(
        table="4_prod.bronze.mill_blob_text",
        key_cols=["BLOB_VERSION_ID"],
        text_cols={"BLOB_TEXT": "anon_text"},
        eligibility="STATUS = 'Decoded' AND BLOB_TEXT IS NOT NULL AND BLOB_TEXT != ''",
        person_routes=[("mill_event", "EVENT_ID"), ("encounter", "ENCNTR_ID")],
        priority_col="ADC_UPDT",
        accepted_versions={"v3.2", "v2-prod-legacy"},
        unresolved_retry_days=28,
        batch_persons=10000,
        batch_resolved_rows=100000,
        batch_text_chars=200000000,
        legacy_state_batch_rows=500000,
        batch_unresolved_rows=50000,
        grain_validated=True,
        grain_evidence="Production repair applied 2026-08-25; BLOB_VERSION_ID is unique and non-null",
    ),
    "neonatal_episode_narrative": dict(
        table="4_prod.bronze.map_neonatal_episode_narrative",
        key_cols=["EntityID"],
        text_cols={
            "FinalSummaryText": "anon_final_summary_text",
            "BirthSummary": "anon_birth_summary",
            "EpisodeSummary": "anon_episode_summary",
            "DiagnosisDuringStay": "anon_diagnosis_during_stay",
            "DrugsDuringStay": "anon_drugs_during_stay",
            "MaternalMedicalNotes": "anon_maternal_medical_notes",
        },
        eligibility=" OR ".join([
            f"coalesce(trim({column}), '') != ''"
            for column in [
                "FinalSummaryText", "BirthSummary", "EpisodeSummary",
                "DiagnosisDuringStay", "DrugsDuringStay", "MaternalMedicalNotes",
            ]
        ]),
        person_routes=[("direct_person", "BABY_PERSON_ID")],
        priority_col="ADC_UPDT",
        accepted_versions={"v3.2"}, unresolved_retry_days=28,
        batch_persons=100000, batch_unresolved_rows=100000,
        grain_validated=True,
        grain_evidence="A1 registry grain EntityID; A3 twin validates non-null uniqueness",
    ),
    "elective_access_comment": dict(
        table="4_prod.bronze.map_elective_access_list",
        key_cols=["SOURCE_SYSTEM_OID", "WAITING_LIST_OID"],
        text_cols={"COMMENTS": "anon_comments"},
        eligibility="coalesce(trim(COMMENTS), '') != ''",
        person_routes=[("direct_person", "PERSON_ID")],
        priority_col="ADC_UPDT",
        accepted_versions={"v3.2"}, unresolved_retry_days=28,
        batch_persons=100000, batch_unresolved_rows=100000,
        grain_validated=True,
        grain_evidence="A1 composite grain; A3 twin validates non-null uniqueness",
    ),
    "endobase_exam": dict(
        table="4_prod.bronze.map_endobase_exam_term",
        key_cols=["ENDOBASE_EXAM_TERM_ID"],
        text_cols={"TERM_TEXT": "anon_term_text"},
        eligibility="coalesce(SOURCE_PRESENT_IND, true) AND coalesce(trim(TERM_TEXT), '') != ''",
        person_routes=[
            ("direct_person", "PERSON_ID"),
            ("lookup", {
                "source": "ENDOBASE_EXAM_ID",
                "table": "4_prod.bronze.map_endobase_exam",
                "key": "ENDOBASE_EXAM_ID",
                "person": "MILL_ENCNTR_ID",
                "via_encounter": True,
            }),
        ],
        priority_col="ADC_UPDT",
        accepted_versions={"v3.2"}, unresolved_retry_days=28,
        batch_persons=100000, batch_unresolved_rows=100000,
        grain_validated=True,
        grain_evidence=(
            "A1 term grain; 2026-09-04: PERSON_ID populated on 99.85% of terms, "
            "exam encounter as agreement route"
        ),
    ),
    "order_comment": dict(
        table="4_prod.bronze.map_order_comment",
        key_cols=["ORDER_ID", "ACTION_SEQUENCE", "COMMENT_TYPE_CD"],
        text_cols={"COMMENT_TEXT": "anon_comment_text"},
        eligibility="TEXT_AVAILABLE_IND = true AND length(nullif(trim(COMMENT_TEXT), '')) > 100",
        person_routes=[
            ("lookup", {
                "source": "ORDER_ID", "table": "4_prod.bronze.map_orders",
                "key": "ORDER_ID", "person": "PERSON_ID",
            }),
            ("lookup", {
                "source": "ORDER_ID", "table": "4_prod.bronze.map_orders",
                "key": "ORDER_ID", "person": "ENCNTR_ID", "via_encounter": True,
            }),
        ],
        priority_col="PIPELINE_UPDT_DT_TM",
        accepted_versions={"v3.2"}, unresolved_retry_days=28,
        batch_persons=100000, batch_unresolved_rows=100000,
        grain_validated=True,
        grain_evidence=(
            "A1 composite grain; map_orders ORDER_ID unique on 643,578,167 rows "
            "(2026-09-04)"
        ),
    ),
    "pacs_report": dict(
        table="4_prod.bronze.map_pacs_report",
        key_cols=["PACS_REPORT_ID"],
        text_cols={"REPORT_TEXT": "anon_report_text"},
        eligibility="coalesce(trim(REPORT_TEXT), '') != ''",
        person_routes=[("direct_person", "PERSON_ID")],
        priority_col="ADC_UPDT",
        accepted_versions={"v3.2"}, unresolved_retry_days=28,
        batch_persons=100000, batch_unresolved_rows=100000,
        grain_validated=True, grain_evidence="Bronze PACS report grain PACS_REPORT_ID",
    ),
    "pacs_report_bridge": dict(
        table="4_prod.bronze.map_pacs_report_text_bridge",
        key_cols=["REPORT_ID"],
        text_cols={"BRIDGED_TEXT": "anon_bridged_text"},
        eligibility="coalesce(trim(BRIDGED_TEXT), '') != ''",
        person_routes=[("mill_event", "EVENT_ID")],
        priority_col="PIPELINE_UPDT_DT_TM",
        accepted_versions={"v3.2"}, unresolved_retry_days=28,
        batch_persons=100000, batch_unresolved_rows=100000,
        grain_validated=True,
        grain_evidence="Bronze PACS bridge grain REPORT_ID; identity routes through EVENT_ID",
    ),
    "text_event": dict(
        table="4_prod.bronze.map_text_events",
        key_cols=["EVENT_ID"],
        text_cols={"TEXT_RESULT": "anon_text_result"},
        eligibility="length(nullif(trim(TEXT_RESULT), '')) > 100",
        person_routes=[("direct_person", "PERSON_ID"), ("encounter", "ENCNTR_ID")],
        priority_col="ADC_UPDT",
        accepted_versions={"v3.2"}, unresolved_retry_days=28,
        batch_persons=100000, batch_unresolved_rows=100000,
        grain_validated=True,
        grain_evidence=(
            "A1 registry grain EVENT_ID; floor matches silver document constructor "
            "(>100 chars)"
        ),
    ),
    "pathology_report": dict(
        table="4_prod.bronze.map_pathology_report",
        key_cols=["report_version_id"],
        text_cols={"report_text": "anon_report_text"},
        eligibility="coalesce(trim(report_text), '') != ''",
        person_routes=[
            ("lookup", {
                "source": "pathology_accession_id",
                "table": "4_prod.bronze.map_pathology_accession",
                "key": "pathology_accession_id",
                "person": "canonical_person_id",
                "key_type": "string",
                "filter": "lower(trim(person_resolution_status)) = 'eligible'",
            }),
        ],
        priority_col="ADC_UPDT",
        accepted_versions={"v3.2"}, unresolved_retry_days=28,
        batch_persons=250000, batch_resolved_rows=250000,
        batch_unresolved_rows=250000, batch_text_chars=40000000,
        grain_validated=True,
        grain_evidence=(
            "A1 report-version grain; eligible accession person via "
            "map_pathology_accession (151,143,528 eligible of 172,518,977)"
        ),
    ),
}

if "ANON_FEED_REGISTRY" in globals():
    FEED_REGISTRY = ANON_FEED_REGISTRY


def _apply_target_schema(registry, target_schema):
    """Re-point feed tables for dev; route and identifier sources stay read-only prod."""
    if not target_schema or target_schema == "4_prod.bronze":
        return registry
    repointed = {}
    for feed, cfg in registry.items():
        cfg = dict(cfg)
        if cfg["table"].startswith("4_prod.bronze."):
            cfg["table"] = target_schema + cfg["table"][len("4_prod.bronze"):]
        repointed[feed] = cfg
    return repointed


FEED_REGISTRY = _apply_target_schema(FEED_REGISTRY, TARGET_SCHEMA)


def _safe_name(value):
    return re.sub(r"[^A-Za-z0-9_]", "_", value)


def _assert_feed_grain_approved(feed):
    cfg = FEED_REGISTRY[feed]
    if not cfg.get("grain_validated", False):
        raise RuntimeError(f"{feed} grain gate blocked: {cfg.get('grain_blocker', 'not validated')}")


def ensure_control_tables(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CONTROL_SCHEMA}")
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.person_identifier_current (
          person_id BIGINT NOT NULL,
          first_names ARRAY<STRING>,
          middle_names ARRAY<STRING>,
          last_names ARRAY<STRING>,
          nickname_tokens ARRAY<STRING>,
          aliases ARRAY<STRING>,
          relatives ARRAY<STRING>,
          informants ARRAY<STRING>,
          addresses ARRAY<STRUCT<
            STREET_ADDR: STRING, STREET_ADDR2: STRING, STREET_ADDR3: STRING,
            STREET_ADDR4: STRING, CITY: STRING, COUNTY: STRING, STATE: STRING,
            COUNTRY: STRING, ZIPCODE: STRING, POSTAL_IDENTIFIER: STRING>>,
          dob DATE,
          identity_fingerprint STRING NOT NULL,
          refreshed_at TIMESTAMP NOT NULL,
          _anon_bucket INT NOT NULL
        ) USING DELTA
        PARTITIONED BY (_anon_bucket)"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.engine_progress (
          feed STRING NOT NULL,
          lane STRING NOT NULL,
          cursor STRING,
          batch_rows BIGINT,
          run_id STRING NOT NULL,
          started_at TIMESTAMP,
          finished_at TIMESTAMP,
          last_refresh_ts TIMESTAMP,
          status STRING,
          detail STRING
        ) USING DELTA"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.engine_errors (
          feed STRING, run_id STRING, row_key STRING, error_detail STRING, recorded_at TIMESTAMP
        ) USING DELTA"""
    )


def tag_control_tables(spark):
    """Apply IG tags once from init_controls, avoiding concurrent metadata writes."""
    tag_levels = {
        "person_identifier_current": {
            "person_id": ("4", "2"),
            "first_names": ("4", "2"),
            "middle_names": ("4", "2"),
            "last_names": ("4", "2"),
            "nickname_tokens": ("4", "2"),
            "aliases": ("4", "2"),
            "relatives": ("4", "2"),
            "informants": ("4", "2"),
            "addresses": ("4", "2"),
            "dob": ("4", "2"),
            "identity_fingerprint": ("2", "1"),
            "refreshed_at": ("1", "1"),
            "_anon_bucket": ("1", "1"),
        },
        "engine_progress": {name: ("1", "1") for name in [
            "feed", "lane", "cursor", "batch_rows", "run_id", "started_at",
            "finished_at", "last_refresh_ts", "status", "detail",
        ]},
        "engine_errors": {
            "feed": ("1", "1"), "run_id": ("1", "1"), "row_key": ("1", "1"),
            "error_detail": ("2", "1"), "recorded_at": ("1", "1"),
        },
    }
    for table, columns in tag_levels.items():
        for column, (risk, severity) in columns.items():
            spark.sql(
                f"ALTER TABLE {CONTROL_SCHEMA}.{table} ALTER COLUMN {column} "
                f"SET TAGS ('ig_risk'='{risk}','ig_severity'='{severity}')"
            )


def _unique_map(df, key_col, person_col, prefix):
    grouped = (
        df.select(
            F.col(key_col).alias("_route_key"),
            F.col(person_col).cast("long").alias("_candidate_person"),
        )
        .where(F.col("_route_key").isNotNull() & F.col("_candidate_person").isNotNull())
        .groupBy("_route_key")
        .agg(
            F.countDistinct("_candidate_person").alias(f"_{prefix}_person_count"),
            F.max("_candidate_person").alias(f"_{prefix}_person"),
        )
    )
    return grouped.withColumn(
        f"_{prefix}_person",
        F.when(F.col(f"_{prefix}_person_count") == 1, F.col(f"_{prefix}_person")),
    )


def _route_map(spark, rows_df, route_kind, source_column, index):
    prefix = f"route_{index}"
    if route_kind == "direct_person":
        return None, prefix
    if route_kind == "encounter":
        route_keys = rows_df.select(
            F.col(source_column).cast("long").alias("_scope_route_key")
        ).where(F.col("_scope_route_key").isNotNull()).distinct()
        encounters = spark.table("4_prod.raw.mill_encounter").select(
            F.col("ENCNTR_ID").cast("long").alias("route_key"),
            F.col("PERSON_ID").cast("long").alias("route_person"),
        ).join(
            F.broadcast(route_keys),
            F.col("route_key") == F.col("_scope_route_key"),
            "left_semi",
        )
        return _unique_map(encounters, "route_key", "route_person", prefix), prefix
    if route_kind == "mill_event":
        event_keys = rows_df.select(
            F.col(source_column).cast("long").alias("_scope_event_id")
        ).where(F.col("_scope_event_id").isNotNull()).distinct()
        scoped_events = (
            spark.table("4_prod.raw.mill_clinical_event")
            .select(
                F.col("EVENT_ID").cast("long").alias("route_key"),
                F.col("ENCNTR_ID").cast("long").alias("_event_encounter_key"),
                F.col("PERSON_ID").cast("long").alias("_event_person"),
            )
            .join(
                F.broadcast(event_keys),
                F.col("route_key") == F.col("_scope_event_id"),
                "left_semi",
            )
        )
        encounter_keys = scoped_events.select(
            F.col("_event_encounter_key").alias("_scope_encounter_key")
        ).where(F.col("_scope_encounter_key").isNotNull()).distinct()
        encounter_map = _unique_map(
            spark.table("4_prod.raw.mill_encounter").select(
                F.col("ENCNTR_ID").cast("long").alias("encounter_key"),
                F.col("PERSON_ID").cast("long").alias("encounter_person"),
            ).join(
                F.broadcast(encounter_keys),
                F.col("encounter_key") == F.col("_scope_encounter_key"),
                "left_semi",
            ),
            "encounter_key",
            "encounter_person",
            "event_encounter",
        ).select(
            F.col("_route_key").alias("_encounter_key"),
            F.col("_event_encounter_person").alias("_encounter_person"),
        )
        events = (
            scoped_events
            .join(encounter_map, F.col("_event_encounter_key") == F.col("_encounter_key"), "left")
            .select("route_key", F.coalesce("_event_person", "_encounter_person").alias("route_person"))
        )
        return _unique_map(events, "route_key", "route_person", prefix), prefix
    if route_kind == "lookup":
        spec = source_column
        key_type = spec.get("key_type", "long")
        route_keys = rows_df.select(
            F.col(spec["source"]).cast(key_type).alias("_scope_route_key")
        ).where(F.col("_scope_route_key").isNotNull()).distinct()
        lookup = spark.table(spec["table"])
        if spec.get("filter"):
            lookup = lookup.where(F.expr(spec["filter"]))
        values = lookup.select(
            F.col(spec["key"]).cast(key_type).alias("route_key"),
            F.col(spec["person"]).cast("long").alias("route_person"),
        ).join(
            route_keys,
            F.col("route_key") == F.col("_scope_route_key"),
            "left_semi",
        )
        if spec.get("via_encounter"):
            encounter_keys = values.select(
                F.col("route_person").alias("_scope_encounter_key")
            ).where(F.col("_scope_encounter_key").isNotNull()).distinct()
            encounter_map = _unique_map(
                spark.table("4_prod.raw.mill_encounter").select(
                    F.col("ENCNTR_ID").cast("long").alias("encounter_key"),
                    F.col("PERSON_ID").cast("long").alias("encounter_person"),
                ).join(
                    encounter_keys,
                    F.col("encounter_key") == F.col("_scope_encounter_key"),
                    "left_semi",
                ),
                "encounter_key", "encounter_person", "lookup_encounter",
            ).select(
                F.col("_route_key").alias("_encounter_key"),
                F.col("_lookup_encounter_person").alias("_encounter_person"),
            )
            values = values.join(
                encounter_map,
                F.col("route_person") == F.col("_encounter_key"),
                "left",
            ).select("route_key", F.col("_encounter_person").alias("route_person"))
        return _unique_map(values, "route_key", "route_person", prefix), prefix
    if route_kind in {"mrn", "nhs", "mrn_nhs"}:
        route_keys = rows_df.select(
            F.upper(F.trim(F.col(source_column).cast("string"))).alias("_scope_route_key")
        ).where(F.col("_scope_route_key").isNotNull()).distinct()
        identifiers = spark.table("4_prod.bronze.map_patient_identifier")
        predicate = F.lit(True)
        if route_kind != "mrn_nhs":
            predicate = F.upper(F.col("ALIAS_TYPE")) == F.lit(route_kind.upper())
        values = identifiers.where(predicate).select(
            F.upper(F.trim(F.col("ALIAS_VALUE"))).alias("route_key"),
            F.col("PERSON_ID").cast("long").alias("route_person"),
        ).join(
            F.broadcast(route_keys),
            F.col("route_key") == F.col("_scope_route_key"),
            "left_semi",
        )
        return _unique_map(values, "route_key", "route_person", prefix), prefix
    raise ValueError(f"unsupported person route: {route_kind}")


def resolve_persons(rows_df, routes):
    """Apply grouped unique-only routes and fail closed on fan-out/disagreement."""
    resolved = rows_df
    person_columns = []
    ambiguity_columns = []
    for index, (route_kind, source_column) in enumerate(routes):
        prefix = f"route_{index}"
        if route_kind == "direct_person":
            person_column = f"_{prefix}_person"
            resolved = resolved.withColumn(person_column, F.col(source_column).cast("long"))
            person_columns.append(person_column)
            continue
        mapping, prefix = _route_map(spark, rows_df, route_kind, source_column, index)
        mapping = mapping.select(
            F.col("_route_key").alias(f"_{prefix}_key"),
            F.col(f"_{prefix}_person"),
            F.col(f"_{prefix}_person_count"),
        )
        if route_kind == "lookup":
            source_key = F.col(source_column["source"]).cast(
                source_column.get("key_type", "long")
            )
        elif route_kind in {"mrn", "nhs", "mrn_nhs"}:
            source_key = F.upper(F.trim(F.col(source_column).cast("string")))
        else:
            source_key = F.col(source_column).cast("long")
        resolved = resolved.join(mapping, source_key == F.col(f"_{prefix}_key"), "left")
        person_columns.append(f"_{prefix}_person")
        ambiguity_columns.append(
            F.coalesce(F.col(f"_{prefix}_person_count"), F.lit(0)) > 1
        )

    candidates = F.array(*[F.col(column).cast("long") for column in person_columns])
    distinct_candidates = F.array_distinct(F.filter(candidates, lambda value: value.isNotNull()))
    route_ambiguous = F.lit(False)
    for expression in ambiguity_columns:
        route_ambiguous = route_ambiguous | expression
    disagreement = F.size(distinct_candidates) > 1
    resolved = (
        resolved.withColumn(
            "person_id",
            F.when(~route_ambiguous & ~disagreement & (F.size(distinct_candidates) == 1),
                   F.element_at(distinct_candidates, 1)),
        )
        .withColumn(
            "resolution_status",
            F.when(route_ambiguous | disagreement, F.lit("ambiguous"))
            .when(F.col("person_id").isNotNull(), F.lit("resolved"))
            .otherwise(F.lit("unresolved")),
        )
    )
    drop_columns = [column for column in resolved.columns if column.startswith("_route_")]
    return resolved.drop(*drop_columns)


def _latest_fingerprint_refresh():
    row = spark.sql(
        f"""SELECT max(coalesce(last_refresh_ts, finished_at)) AS last_refresh_ts
        FROM {CONTROL_SCHEMA}.engine_progress
        WHERE feed='__fingerprints__' AND lane='refresh' AND status='success'"""
    ).first()
    return row[0] if row and row[0] else datetime(1900, 1, 1, tzinfo=timezone.utc)


def refresh_person_fingerprints(candidate_person_ids=None, include_source_deltas=True):
    """Refresh changed identifiers, or only missing candidate bundles for feed tasks."""
    ensure_control_tables(spark)
    started = datetime.now(timezone.utc)
    last_refresh = _latest_fingerprint_refresh()
    overlap_start = last_refresh - timedelta(days=FINGERPRINT_OVERLAP_DAYS)
    current_identifiers = spark.table(
        f"{CONTROL_SCHEMA}.person_identifier_current"
    ).select("person_id")
    candidate_scope = None
    missing_candidates = None
    if candidate_person_ids is not None:
        candidate_scope = candidate_person_ids.select(
            F.col(candidate_person_ids.columns[0]).cast("long").alias("person_id")
        ).where(F.col("person_id").isNotNull()).distinct()
        missing_candidates = candidate_scope.join(
            current_identifiers, "person_id", "left_anti"
        )
    changed = []
    source_specs = [
        ("4_prod.raw.mill_person_name", "PERSON_ID", "UPDT_DT_TM"),
        ("4_prod.raw.mill_person_alias", "PERSON_ID", "UPDT_DT_TM"),
        ("4_prod.raw.mill_address", "PARENT_ENTITY_ID", "UPDT_DT_TM"),
        ("4_prod.raw.mill_person_person_reltn", "PERSON_ID", "UPDT_DT_TM"),
        ("4_prod.raw.mill_person", "PERSON_ID", "UPDT_DT_TM"),
        ("4_prod.raw.mill_encounter", "PERSON_ID", "UPDT_DT_TM"),
    ]
    if include_source_deltas:
        for table, person_column, update_column in source_specs:
            source = spark.table(table).where(
                F.col(update_column) > F.lit(overlap_start)
            )
            if table.endswith("mill_address"):
                source = source.where(F.col("PARENT_ENTITY_NAME") == "PERSON")
            source = source.select(F.col(person_column).cast("long").alias("person_id"))
            if candidate_scope is not None:
                source = source.join(F.broadcast(candidate_scope), "person_id", "left_semi")
            changed.append(source)
    if changed:
        worklist = changed[0]
        for frame in changed[1:]:
            worklist = worklist.unionByName(frame)
    else:
        worklist = spark.createDataFrame([], "person_id long")
    if missing_candidates is not None:
        worklist = worklist.unionByName(missing_candidates)
    worklist = worklist.where(F.col("person_id").isNotNull()).distinct()
    if not include_source_deltas and worklist.limit(1).count() == 0:
        return 0

    identifiers = with_identity_fingerprint(build_identifier_frame(spark, worklist)).select(
        "person_id", *IDENTITY_VALUE_COLUMNS,
        F.current_timestamp().alias("refreshed_at"),
        F.pmod(F.xxhash64("person_id"), F.lit(WORK_BUCKET_COUNT))
        .cast("int").alias("_anon_bucket"),
    )
    stage = f"{CONTROL_SCHEMA}._stage_identifier_{_safe_name(RUN_ID)}"
    identifiers.write.mode("overwrite").partitionBy("_anon_bucket").saveAsTable(stage)
    _merge_with_retry(
        f"""MERGE INTO {CONTROL_SCHEMA}.person_identifier_current t
        USING {stage} s ON t.person_id = s.person_id AND t._anon_bucket = s._anon_bucket
        WHEN MATCHED AND t.identity_fingerprint <> s.identity_fingerprint THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""",
        "fingerprints",
    )
    count = spark.table(stage).count()
    spark.sql(f"DROP TABLE IF EXISTS {stage}")
    lane = "refresh" if include_source_deltas else "candidates"
    spark.createDataFrame(
        [("__fingerprints__", lane, None, count, RUN_ID, started, datetime.now(timezone.utc),
          datetime.now(timezone.utc) if include_source_deltas else None, "success",
          f"previous_refresh={last_refresh.isoformat()};overlap_days={FINGERPRINT_OVERLAP_DAYS}")],
        "feed string, lane string, cursor string, batch_rows long, run_id string, started_at timestamp, "
        "finished_at timestamp, last_refresh_ts timestamp, status string, detail string",
    ).write.mode("append").saveAsTable(f"{CONTROL_SCHEMA}.engine_progress")
    return count


def _text_hash(cfg):
    return F.sha2(
        F.to_json(
            F.struct(*[
                F.coalesce(F.col(column).cast("string"), F.lit("")).alias(column)
                for column in cfg["text_cols"]
            ])
        ),
        256,
    )


def eligible_rows(feed):
    _assert_feed_grain_approved(feed)
    cfg = FEED_REGISTRY[feed]
    source = spark.table(cfg["table"]).where(F.expr(cfg["eligibility"]))
    source = resolve_persons(source, cfg["person_routes"])
    fingerprints = spark.table(f"{CONTROL_SCHEMA}.person_identifier_current").select(
        F.col("person_id").alias("_fingerprint_person_id"),
        F.col("identity_fingerprint").alias("_current_identity_fingerprint"),
    )
    source = source.join(
        fingerprints,
        F.col("person_id") == F.col("_fingerprint_person_id"),
        "left",
    ).withColumn("_current_source_text_sha", _text_hash(cfg))
    accepted = list(cfg["accepted_versions"])
    retry_cutoff = F.current_timestamp() - F.expr(
        f"INTERVAL {int(cfg['unresolved_retry_days'])} DAYS"
    )
    eligible = (
        F.col("anon_status").isNull()
        | ~F.col("anon_source_text_sha").eqNullSafe(F.col("_current_source_text_sha"))
        | F.col("anon_redactor_version").isNull()
        | ~F.col("anon_redactor_version").isin(*accepted)
        | (
            (F.col("anon_status") == "anonymized")
            & ~F.col("anon_identity_fingerprint").eqNullSafe(F.col("_current_identity_fingerprint"))
        )
        | (
            (F.col("anon_status") == "unresolved_person")
            & F.col("anon_processed_at").isNotNull()
            & (F.col("anon_processed_at") < retry_cutoff)
        )
    )
    return source.where(eligible)


def initialize_legacy_blob_state():
    """Attach v2 state to existing blob outputs without re-running redaction."""
    feed = "mill_blob_text"
    cfg = FEED_REGISTRY[feed]
    if "v2-prod-legacy" not in cfg["accepted_versions"]:
        raise RuntimeError("mill_blob_text must explicitly accept v2-prod-legacy")
    source = (
        spark.table(cfg["table"])
        .where(F.expr(cfg["eligibility"]))
        .where(F.col("anon_text").isNotNull() & (F.trim("anon_text") != ""))
        .where(F.col("anon_status").isNull())
    )
    resolved = resolve_persons(source, cfg["person_routes"])
    refresh_person_fingerprints(
        resolved.select("person_id").where(F.col("person_id").isNotNull()),
        include_source_deltas=not SKIP_FINGERPRINT_REFRESH,
    )
    if _deadline_reached():
        return {
            "status": "partial", "stop_reason": "deadline",
            "processed": 0, "batches": 0, "elapsed_seconds": _elapsed_seconds(),
        }
    fingerprints = spark.table(
        f"{CONTROL_SCHEMA}.person_identifier_current"
    ).select(
        F.col("person_id").alias("_fingerprint_person_id"),
        F.col("identity_fingerprint").alias("_current_identity_fingerprint"),
    )
    work_stage = (
        resolved.join(
            fingerprints,
            F.col("person_id") == F.col("_fingerprint_person_id"),
            "left",
        )
        .withColumn("_current_source_text_sha", _text_hash(cfg))
        .withColumn("_anon_lane", F.lit("legacy"))
        .withColumn("_row_key", _row_key_json(cfg))
        .withColumn(
            "_anon_work_bucket",
            F.pmod(
                F.xxhash64(*[F.col(column) for column in cfg["key_cols"]]),
                F.lit(WORK_BATCH_BUCKET_COUNT),
            ).cast("int"),
        )
    )
    stage = f"{CONTROL_SCHEMA}._work_legacy_blob_{_safe_name(RUN_ID)}"
    work_stage.write.mode("overwrite").partitionBy("_anon_work_bucket").saveAsTable(stage)
    processed = 0
    batches = 0
    stop_reason = None
    try:
        worklist = spark.table(stage)
        groups = _bucket_groups(
            worklist,
            lane="legacy",
            target_items=int(cfg["legacy_state_batch_rows"]),
            target_rows=int(cfg["legacy_state_batch_rows"]),
            target_text_chars=None,
            text_columns=[],
        )
        for group in groups:
            if batches >= MAX_BATCHES:
                stop_reason = "max_batches"
                break
            if _deadline_reached():
                stop_reason = "deadline"
                break
            started = datetime.now(timezone.utc)
            bucket_ids = group["bucket_ids"]
            batch = worklist.where(F.col("_anon_work_bucket").isin(bucket_ids)).select(
                *cfg["key_cols"],
                F.lit("anonymized").alias("anon_status"),
                F.lit("v2-prod-legacy").alias("anon_redactor_version"),
                F.col("_current_source_text_sha").alias("anon_source_text_sha"),
                F.col("_current_identity_fingerprint")
                 .alias("anon_identity_fingerprint"),
                F.lit(None).cast("long").alias("anon_redaction_count"),
                F.current_timestamp().alias("anon_processed_at"),
            )
            batch_stage = (
                f"{CONTROL_SCHEMA}._stage_legacy_blob_{_safe_name(RUN_ID)}"
            )
            batch.write.mode("overwrite").saveAsTable(batch_stage)
            staged = spark.table(batch_stage)
            metrics = staged.agg(
                F.count("*").alias("rows"),
                F.countDistinct(F.struct(*[
                    F.col(column) for column in cfg["key_cols"]
                ])).alias("distinct_rows"),
            ).first()
            if int(metrics.rows) != int(metrics.distinct_rows):
                raise AssertionError("legacy blob state batch contains duplicate keys")
            join_clause = " AND ".join(
                f"t.`{column}` <=> s.`{column}`" for column in cfg["key_cols"]
            )
            update_clause = ", ".join(
                f"t.`{column}` = s.`{column}`" for column in STATE_COLS
            )
            result = _merge_with_retry(
                f"MERGE INTO {cfg['table']} t USING {batch_stage} s ON {join_clause} "
                f"WHEN MATCHED AND t.anon_status IS NULL THEN UPDATE SET {update_clause}",
                "legacy_blob_state",
            )
            merge_result = result.asDict(recursive=True)
            updated = int(merge_result["num_updated_rows"])
            if updated != int(metrics.rows):
                verification = _verify_merged_batch(
                    cfg, batch_stage, STATE_COLS, int(metrics.rows)
                )
                _accept_verified_merge_metric_mismatch(
                    "legacy_blob_state", merge_result, verification
                )
                updated = int(metrics.rows)
            processed += updated
            batches += 1
            spark.sql(f"DROP TABLE IF EXISTS {batch_stage}")
            spark.createDataFrame(
                [(feed, "legacy", json.dumps(bucket_ids), updated, RUN_ID, started,
                  datetime.now(timezone.utc), None, "success", None)],
                "feed string, lane string, cursor string, batch_rows long, run_id string, "
                "started_at timestamp, finished_at timestamp, last_refresh_ts timestamp, "
                "status string, detail string",
            ).write.mode("append").saveAsTable(f"{CONTROL_SCHEMA}.engine_progress")
        has_more = batches < len(groups)
        return {
            "status": "partial" if has_more else "success",
            "stop_reason": stop_reason,
            "processed": processed,
            "batches": batches,
            "total_groups": len(groups),
            "elapsed_seconds": _elapsed_seconds(),
        }
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {stage}")

REDACTION_RESULT_SCHEMA = StructType(
    [
        StructField("texts", ArrayType(StringType()), True),
        StructField("redaction_count", LongType(), False),
        StructField("success", BooleanType(), False),
        StructField("error_detail", StringType(), True),
    ]
)


def _redact_row(texts, text_modes, first_names, middle_names, last_names, nickname_tokens,
                aliases, relatives, informants, addresses, dob):
    try:
        output, total = [], 0
        for value, text_mode in zip(texts or [], text_modes or []):
            redact_fn = (
                redact_variant_json
                if text_mode == "json_string_leaves"
                else redact_with_count
            )
            redacted, count = redact_fn(
                value,
                list(first_names or []) + list(nickname_tokens or []),
                middle_names or [],
                last_names or [],
                dob,
                addresses or [],
                aliases or [],
                relatives or [],
                informants or [],
                DEFAULT_WHITELIST,
            )
            output.append(redacted)
            total += count
        return output, total, True, None
    except Exception as exc:
        return None, 0, False, f"{type(exc).__name__}: {exc}"[:4000]


redact_row_udf = F.udf(_redact_row, REDACTION_RESULT_SCHEMA)


def _row_key_json(cfg):
    return F.to_json(F.struct(*[F.col(column).alias(column) for column in cfg["key_cols"]]))


def _prepare_resolved(feed, rows):
    cfg = FEED_REGISTRY[feed]
    identity = spark.table(f"{CONTROL_SCHEMA}.person_identifier_current").select(
        "person_id", *IDENTITY_VALUE_COLUMNS, "_anon_bucket"
    )
    enriched = rows.join(identity, ["person_id", "_anon_bucket"], "inner")
    texts = F.array(*[F.col(column).cast("string") for column in cfg["text_cols"]])
    text_modes = F.array(*[
        F.lit(cfg.get("text_modes", {}).get(column, "scalar"))
        for column in cfg["text_cols"]
    ])
    redacted = enriched.withColumn(
        "_redacted",
        redact_row_udf(
            texts, text_modes, "first_names", "middle_names", "last_names", "nickname_tokens",
            "aliases", "relatives", "informants", "addresses", "dob",
        ),
    )
    output = redacted
    for index, anon_column in enumerate(cfg["text_cols"].values(), 1):
        output = output.withColumn(
            anon_column,
            F.when(F.col("_redacted.success"), F.element_at("_redacted.texts", index)),
        )
    return output.select(
        *cfg["key_cols"],
        *cfg["text_cols"].values(),
        F.when(F.col("_redacted.success"), F.lit("anonymized")).otherwise(F.lit("failed")).alias("anon_status"),
        F.lit(REDACTOR_VERSION).alias("anon_redactor_version"),
        F.col("_current_source_text_sha").alias("anon_source_text_sha"),
        F.col("identity_fingerprint").alias("anon_identity_fingerprint"),
        F.col("_redacted.redaction_count").cast("long").alias("anon_redaction_count"),
        F.current_timestamp().alias("anon_processed_at"),
        F.col("_redacted.error_detail").alias("_error_detail"),
    )


def _prepare_unresolved(feed, rows):
    cfg = FEED_REGISTRY[feed]
    output = rows
    for anon_column in cfg["text_cols"].values():
        output = output.withColumn(anon_column, F.lit(None).cast("string"))
    return output.select(
        *cfg["key_cols"],
        *cfg["text_cols"].values(),
        F.lit("unresolved_person").alias("anon_status"),
        F.lit(REDACTOR_VERSION).alias("anon_redactor_version"),
        F.col("_current_source_text_sha").alias("anon_source_text_sha"),
        F.lit(None).cast("string").alias("anon_identity_fingerprint"),
        F.lit(0).cast("long").alias("anon_redaction_count"),
        F.current_timestamp().alias("anon_processed_at"),
        F.lit(None).cast("string").alias("_error_detail"),
    )


def merge_batch(feed, output, lane, cursor, started_at):
    cfg = FEED_REGISTRY[feed]
    stage = f"{CONTROL_SCHEMA}._stage_{_safe_name(feed)}_{_safe_name(RUN_ID)}_{lane}"
    output.select(
        *cfg["key_cols"], *cfg["text_cols"].values(), *STATE_COLS, "_error_detail"
    ).write.mode("overwrite").saveAsTable(stage)
    staged = spark.table(stage)
    batch_metrics = staged.agg(
        F.count("*").alias("rows"),
        F.countDistinct(F.struct(*[F.col(column) for column in cfg["key_cols"]]))
        .alias("distinct_rows"),
        F.count_if(F.col("_error_detail").isNotNull()).alias("error_rows"),
    ).first()
    batch_rows = int(batch_metrics.rows)
    if batch_rows != int(batch_metrics.distinct_rows):
        spark.sql(f"DROP TABLE IF EXISTS {stage}")
        raise AssertionError(
            f"grain violation in batch: {batch_rows - int(batch_metrics.distinct_rows)} "
            "duplicate keys"
        )
    join_clause = " AND ".join(f"t.`{column}` <=> s.`{column}`" for column in cfg["key_cols"])
    update_columns = list(cfg["text_cols"].values()) + STATE_COLS
    update_clause = ", ".join(f"t.`{column}` = s.`{column}`" for column in update_columns)
    result = _merge_with_retry(
        f"MERGE INTO {cfg['table']} t USING {stage} s ON {join_clause} "
        f"WHEN MATCHED THEN UPDATE SET {update_clause}",
        f"{feed}:{lane}",
    )
    merge_result = result.asDict(recursive=True)
    updated = int(merge_result["num_updated_rows"])
    if updated != batch_rows:
        verification = _verify_merged_batch(
            cfg, stage, update_columns, batch_rows
        )
        _accept_verified_merge_metric_mismatch(
            f"{feed}:{lane}", merge_result, verification
        )
    errors = staged.where(F.col("_error_detail").isNotNull()).select(
        F.lit(feed).alias("feed"),
        F.lit(RUN_ID).alias("run_id"),
        _row_key_json(cfg).alias("row_key"),
        F.col("_error_detail").alias("error_detail"),
        F.current_timestamp().alias("recorded_at"),
    )
    if int(batch_metrics.error_rows):
        errors.write.mode("append").saveAsTable(f"{CONTROL_SCHEMA}.engine_errors")
    spark.sql(f"DROP TABLE IF EXISTS {stage}")

    spark.createDataFrame(
        [(feed, lane, cursor, batch_rows, RUN_ID, started_at, datetime.now(timezone.utc),
          None, "success", None)],
        "feed string, lane string, cursor string, batch_rows long, run_id string, started_at timestamp, "
        "finished_at timestamp, last_refresh_ts timestamp, status string, detail string",
    ).write.mode("append").saveAsTable(f"{CONTROL_SCHEMA}.engine_progress")
    return batch_rows


def _bucket_groups(
    worklist,
    lane,
    target_items,
    target_rows,
    target_text_chars,
    text_columns,
):
    """Pack fine deterministic buckets while enforcing persons, rows and text caps."""
    count_column = "person_id" if lane == "resolved" else "_row_key"
    text_chars = F.lit(0).cast("long")
    for column in text_columns:
        text_chars = text_chars + F.coalesce(
            F.length(F.col(column).cast("string")).cast("long"), F.lit(0)
        )
    counts = (
        worklist.where(F.col("_anon_lane") == lane)
        .groupBy("_anon_work_bucket")
        .agg(
            F.countDistinct(count_column).alias("items"),
            F.count("*").alias("rows"),
            F.sum(text_chars).alias("text_chars"),
        )
        .orderBy("_anon_work_bucket")
        .collect()
    )
    item_cap = int(target_items)
    row_cap = int(target_rows) if target_rows is not None else 2 ** 63 - 1
    char_cap = (
        int(target_text_chars) if target_text_chars is not None else 2 ** 63 - 1
    )
    groups = []
    current = []
    current_items = current_rows = current_chars = 0
    for row in counts:
        bucket_id = int(row._anon_work_bucket)
        bucket_items = int(row.items)
        bucket_rows = int(row.rows)
        bucket_chars = int(row.text_chars or 0)
        if bucket_items > item_cap or bucket_rows > row_cap or bucket_chars > char_cap:
            raise RuntimeError(
                "ANON_BUCKET_TOO_LARGE: "
                f"lane={lane}, bucket={bucket_id}, items={bucket_items}/{item_cap}, "
                f"rows={bucket_rows}/{row_cap}, text_chars={bucket_chars}/{char_cap}; "
                "increase WORK_BATCH_BUCKET_COUNT before processing"
            )
        would_exceed = current and (
            current_items + bucket_items > item_cap
            or current_rows + bucket_rows > row_cap
            or current_chars + bucket_chars > char_cap
        )
        if would_exceed:
            groups.append({
                "bucket_ids": current,
                "items": current_items,
                "rows": current_rows,
                "text_chars": current_chars,
            })
            current = []
            current_items = current_rows = current_chars = 0
        current.append(bucket_id)
        current_items += bucket_items
        current_rows += bucket_rows
        current_chars += bucket_chars
    if current:
        groups.append({
            "bucket_ids": current,
            "items": current_items,
            "rows": current_rows,
            "text_chars": current_chars,
        })
    return groups


def _anon_output_missing_for_present_source(cfg):
    """Only flag a NULL anon output when its corresponding source carries text."""
    anon_output_null = F.lit(False)
    for source_column, anon_column in cfg["text_cols"].items():
        source_present = (
            F.coalesce(F.trim(F.col(source_column).cast("string")), F.lit(""))
            != F.lit("")
        )
        anon_output_null = anon_output_null | (
            source_present & F.col(anon_column).isNull()
        )
    return anon_output_null


def _cheap_candidate_predicate(cfg):
    """Source-column-only test for rows that may need work."""
    accepted = list(cfg["accepted_versions"])
    retry_cutoff = F.current_timestamp() - F.expr(
        f"INTERVAL {int(cfg['unresolved_retry_days'])} DAYS"
    )
    anon_output_null = _anon_output_missing_for_present_source(cfg)
    return (
        F.col("anon_status").isNull()
        | F.col("anon_processed_at").isNull()
        | F.col("anon_redactor_version").isNull()
        | ~F.col("anon_redactor_version").isin(*accepted)
        | ((F.col("anon_status") == "anonymized") & anon_output_null)
        | (
            (F.col("anon_status") == "unresolved_person")
            & (F.col("anon_processed_at") < retry_cutoff)
        )
        | (F.col(cfg["priority_col"]).cast("timestamp") > F.col("anon_processed_at"))
    )


def _drift_watermark(feed):
    row = spark.sql(
        f"""SELECT max(last_refresh_ts) FROM {CONTROL_SCHEMA}.engine_progress
        WHERE feed='{feed}' AND lane='drift' AND status='success'"""
    ).first()
    # On first adoption, start from the latest global fingerprint refresh. Using
    # 1900 would classify every already-anonymized row as drift; with a cap, the
    # newest rows would be selected repeatedly and the watermark could never advance.
    return row[0] if row and row[0] else _latest_fingerprint_refresh()


def _record_drift_watermark(feed, through):
    spark.createDataFrame(
        [(feed, "drift", None, None, RUN_ID, through, datetime.now(timezone.utc),
          through, "success", f"overlap_days={DRIFT_OVERLAP_DAYS}")],
        "feed string, lane string, cursor string, batch_rows long, run_id string, "
        "started_at timestamp, finished_at timestamp, last_refresh_ts timestamp, "
        "status string, detail string",
    ).write.mode("append").saveAsTable(f"{CONTROL_SCHEMA}.engine_progress")


def _encounters_of(persons):
    return (
        spark.table("4_prod.raw.mill_encounter")
        .select(
            F.col("ENCNTR_ID").cast("long").alias("_e"),
            F.col("PERSON_ID").cast("long").alias("_p"),
        )
        .join(persons, F.col("_p") == F.col("person_id"), "left_semi")
        .select("_e")
    )


def _rows_for_persons(cfg, source, persons):
    """Return feed rows belonging to persons through any declared route."""
    matches = None
    for route_kind, spec in cfg["person_routes"]:
        if route_kind == "direct_person":
            keys = persons.select(F.col("person_id").alias("_k"))
            condition = F.col(spec).cast("long") == F.col("_k")
        elif route_kind == "encounter":
            keys = _encounters_of(persons).select(F.col("_e").alias("_k"))
            condition = F.col(spec).cast("long") == F.col("_k")
        elif route_kind == "mill_event":
            events = spark.table("4_prod.raw.mill_clinical_event").select(
                F.col("EVENT_ID").cast("long").alias("_k"),
                F.col("PERSON_ID").cast("long").alias("_p"),
                F.col("ENCNTR_ID").cast("long").alias("_e"),
            )
            by_person = events.join(
                persons, F.col("_p") == F.col("person_id"), "left_semi"
            ).select("_k")
            by_encounter = events.join(
                _encounters_of(persons), "_e", "left_semi"
            ).select("_k")
            keys = by_person.unionByName(by_encounter)
            condition = F.col(spec).cast("long") == F.col("_k")
        elif route_kind == "lookup":
            key_type = spec.get("key_type", "long")
            lookup = spark.table(spec["table"])
            if spec.get("filter"):
                lookup = lookup.where(F.expr(spec["filter"]))
            if spec.get("via_encounter"):
                lookup = lookup.join(
                    _encounters_of(persons),
                    F.col(spec["person"]).cast("long") == F.col("_e"),
                    "left_semi",
                )
            else:
                person_keys = persons.select(
                    F.col("person_id").alias("_wanted_person_id")
                )
                lookup = lookup.join(
                    person_keys,
                    F.col(spec["person"]).cast("long") == F.col("_wanted_person_id"),
                    "left_semi",
                )
            keys = lookup.select(F.col(spec["key"]).cast(key_type).alias("_k"))
            condition = F.col(spec["source"]).cast(key_type) == F.col("_k")
        elif route_kind in {"mrn", "nhs", "mrn_nhs"}:
            identifiers = spark.table("4_prod.bronze.map_patient_identifier")
            if route_kind != "mrn_nhs":
                identifiers = identifiers.where(
                    F.upper(F.col("ALIAS_TYPE")) == F.lit(route_kind.upper())
                )
            keys = identifiers.join(
                persons,
                F.col("PERSON_ID").cast("long") == F.col("person_id"),
                "left_semi",
            ).select(F.upper(F.trim(F.col("ALIAS_VALUE"))).alias("_k"))
            condition = F.upper(F.trim(F.col(spec).cast("string"))) == F.col("_k")
        else:
            raise ValueError(f"unsupported person route: {route_kind}")
        subset = source.join(keys.distinct(), condition, "left_semi")
        matches = subset if matches is None else matches.unionByName(subset)
    return matches.dropDuplicates(cfg["key_cols"])


def _drift_rows(feed, cfg, source):
    """Anonymized rows whose person's identifier bundle changed since watermark."""
    anonymized = source.where(F.col("anon_status") == "anonymized")
    if anonymized.limit(1).count() == 0:
        return anonymized.limit(0)
    since = _drift_watermark(feed) - timedelta(days=DRIFT_OVERLAP_DAYS)
    changed = (
        spark.table(f"{CONTROL_SCHEMA}.person_identifier_current")
        .where(F.col("refreshed_at") > F.lit(since))
        .select("person_id")
    )
    if changed.limit(1).count() == 0:
        return anonymized.limit(0)
    return _rows_for_persons(cfg, anonymized, changed)


def touch_rows(feed, rows):
    """Advance anon_processed_at for inspected rows whose text and identity are unchanged."""
    cfg = FEED_REGISTRY[feed]
    started_at = datetime.now(timezone.utc)
    stage = f"{CONTROL_SCHEMA}._stage_{_safe_name(feed)}_{_safe_name(RUN_ID)}_touch"
    rows.select(*cfg["key_cols"]).write.mode("overwrite").saveAsTable(stage)
    touched = int(spark.table(stage).count())
    if touched:
        join_clause = " AND ".join(
            f"t.`{column}` <=> s.`{column}`" for column in cfg["key_cols"]
        )
        result = _merge_with_retry(
            f"MERGE INTO {cfg['table']} t USING {stage} s ON {join_clause} "
            "WHEN MATCHED THEN UPDATE SET t.anon_processed_at = current_timestamp()",
            f"{feed}:touch",
        )
        if int(result["num_updated_rows"]) != touched:
            raise AssertionError(
                f"touch fan-out/miss: {result['num_updated_rows']} != {touched}"
            )
    spark.sql(f"DROP TABLE IF EXISTS {stage}")
    spark.createDataFrame(
        [(feed, "touch", None, touched, RUN_ID, started_at,
          datetime.now(timezone.utc), None, "success", None)],
        "feed string, lane string, cursor string, batch_rows long, run_id string, "
        "started_at timestamp, finished_at timestamp, last_refresh_ts timestamp, "
        "status string, detail string",
    ).write.mode("append").saveAsTable(f"{CONTROL_SCHEMA}.engine_progress")
    return touched


def _adaptive_work_bucket_count(row_count):
    """Keep work-stage partitions useful for both bounded tests and full backlog runs."""
    wanted = max(
        MIN_WORK_BATCH_BUCKET_COUNT,
        int(math.ceil(max(int(row_count), 1) / TARGET_ROWS_PER_WORK_BUCKET)),
    )
    power_of_two = 1 << (wanted - 1).bit_length()
    return min(WORK_BATCH_BUCKET_COUNT, power_of_two)


def _bucket_cursor(bucket_ids):
    """Compact audit cursor; processing never resumes from the verbose bucket list."""
    return json.dumps({
        "bucket_count": len(bucket_ids),
        "first_bucket": min(bucket_ids) if bucket_ids else None,
        "last_bucket": max(bucket_ids) if bucket_ids else None,
    }, sort_keys=True)


def run_feed(feed):
    ensure_control_tables(spark)
    _assert_feed_grain_approved(feed)
    cfg = FEED_REGISTRY[feed]
    text_columns = list(cfg["text_cols"])
    planned = datetime.now(timezone.utc)
    source = spark.table(cfg["table"]).where(F.expr(cfg["eligibility"]))
    work_stage = f"{CONTROL_SCHEMA}._work_{_safe_name(feed)}_{_safe_name(RUN_ID)}"

    key_priority = [
        *cfg["key_cols"],
        F.col(cfg["priority_col"]).cast("timestamp").alias("_priority"),
    ]
    cheap = (
        source.where(_cheap_candidate_predicate(cfg))
        .select(*key_priority)
        .withColumn("_drift", F.lit(False))
    )
    drift = (
        _drift_rows(feed, cfg, source)
        .select(*key_priority)
        .withColumn("_drift", F.lit(True))
    )
    candidates = (
        drift.unionByName(cheap)
        .withColumn(
            "_rank",
            F.row_number().over(
                Window.partitionBy(*cfg["key_cols"]).orderBy(F.col("_drift").desc())
            ),
        )
        .where(F.col("_rank") == 1)
        .drop("_rank")
    )
    try:
        candidate_metrics = candidates.agg(
            F.count("*").alias("eligible_total"),
            F.sum(F.col("_drift").cast("long")).alias("drift_total"),
        ).first()
        eligible_total = int(candidate_metrics.eligible_total or 0)
        drift_total = int(candidate_metrics.drift_total or 0)
        if eligible_total == 0:
            finished = datetime.now(timezone.utc)
            spark.createDataFrame(
                [(feed, "plan", json.dumps({
                    "eligible_total": 0, "drift_total": 0, "work_rows": 0,
                    "touched": 0, "work_cap_rows": WORK_CAP_ROWS,
                    "work_bucket_count": 0,
                }), 0, RUN_ID, planned, finished, None, "success", None)],
                "feed string, lane string, cursor string, batch_rows long, run_id string, "
                "started_at timestamp, finished_at timestamp, last_refresh_ts timestamp, "
                "status string, detail string",
            ).write.mode("append").saveAsTable(f"{CONTROL_SCHEMA}.engine_progress")
            _record_drift_watermark(feed, finished)
            return {
                "status": "success", "stop_reason": None,
                "processed": 0, "touched": 0, "batches": 0, "total_groups": 0,
                "eligible_total": 0, "drift_total": 0, "work_rows": 0,
                "remaining_estimate": 0, "elapsed_seconds": _elapsed_seconds(),
            }
        slice_df = candidates
        if WORK_CAP_ROWS and eligible_total > WORK_CAP_ROWS:
            slice_df = (
                candidates
                .orderBy(F.col("_drift").desc(), F.col("_priority").desc_nulls_last(), *[F.col(c) for c in cfg["key_cols"]])
                .limit(WORK_CAP_ROWS)
            )
        slice_target_rows = (
            min(eligible_total, WORK_CAP_ROWS) if WORK_CAP_ROWS else eligible_total
        )
        work_bucket_count = _adaptive_work_bucket_count(slice_target_rows)
        if _deadline_reached():
            return {
                "status": "partial", "stop_reason": "deadline",
                "processed": 0, "batches": 0,
                "eligible_total": eligible_total, "drift_total": drift_total,
                "elapsed_seconds": _elapsed_seconds(),
            }

        slice_rows = (
            slice_df.select(*cfg["key_cols"], "_drift")
            .join(source, cfg["key_cols"], "inner")
        )
        resolved = resolve_persons(slice_rows, cfg["person_routes"])
        refresh_person_fingerprints(
            resolved.select("person_id").where(F.col("person_id").isNotNull()),
            include_source_deltas=not SKIP_FINGERPRINT_REFRESH,
        )
        fingerprints = spark.table(
            f"{CONTROL_SCHEMA}.person_identifier_current"
        ).select(
            F.col("person_id").alias("_fingerprint_person_id"),
            F.col("identity_fingerprint").alias("_current_identity_fingerprint"),
        )
        inspected = (
            resolved.join(
                fingerprints,
                F.col("person_id") == F.col("_fingerprint_person_id"),
                "left",
            )
            .withColumn("_current_source_text_sha", _text_hash(cfg))
        )
        accepted = list(cfg["accepted_versions"])
        retry_cutoff = F.current_timestamp() - F.expr(
            f"INTERVAL {int(cfg['unresolved_retry_days'])} DAYS"
        )
        anon_output_null = _anon_output_missing_for_present_source(cfg)
        needs_work = (
            F.col("anon_status").isNull()
            | ~F.col("anon_source_text_sha").eqNullSafe(
                F.col("_current_source_text_sha")
            )
            | F.col("anon_redactor_version").isNull()
            | ~F.col("anon_redactor_version").isin(*accepted)
            | (
                (F.col("anon_status") == "anonymized")
                & ~F.col("anon_identity_fingerprint").eqNullSafe(
                    F.col("_current_identity_fingerprint")
                )
            )
            | ((F.col("anon_status") == "anonymized") & anon_output_null)
            | (
                (F.col("anon_status") == "unresolved_person")
                & F.col("anon_processed_at").isNotNull()
                & (F.col("anon_processed_at") < retry_cutoff)
            )
        )
        inspected_stage = (
            inspected
            .withColumn("_needs_work", needs_work)
            .withColumn(
                "_anon_lane",
                F.when(
                    F.col("resolution_status") == "resolved", F.lit("resolved")
                ).otherwise(F.lit("unresolved")),
            )
            .withColumn("_row_key", _row_key_json(cfg))
            .withColumn(
                "_anon_bucket",
                F.when(
                    F.col("resolution_status") == "resolved",
                    F.pmod(F.xxhash64("person_id"), F.lit(WORK_BUCKET_COUNT)),
                ).otherwise(
                    F.pmod(F.xxhash64("_row_key"), F.lit(WORK_BUCKET_COUNT))
                ).cast("int"),
            )
            .withColumn(
                "_anon_work_bucket",
                F.when(
                    F.col("resolution_status") == "resolved",
                    F.pmod(
                        F.xxhash64("person_id"), F.lit(work_bucket_count)
                    ),
                ).otherwise(
                    F.pmod(
                        F.xxhash64("_row_key"), F.lit(work_bucket_count)
                    )
                ).cast("int"),
            )
            .select(
                *cfg["key_cols"],
                *text_columns,
                "person_id",
                "resolution_status",
                "_current_source_text_sha",
                "_row_key",
                "_anon_bucket",
                "_anon_work_bucket",
                "_anon_lane",
                "_needs_work",
            )
        )
        (
            inspected_stage.write.mode("overwrite")
            .partitionBy("_anon_work_bucket")
            .saveAsTable(work_stage)
        )
        inspected_rows = spark.table(work_stage)
        worklist = inspected_rows.where(F.col("_needs_work"))
        resolved_groups = _bucket_groups(
            worklist,
            lane="resolved",
            target_items=int(cfg["batch_persons"]),
            target_rows=int(cfg.get("batch_resolved_rows", 100000)),
            target_text_chars=cfg.get("batch_text_chars"),
            text_columns=text_columns,
        )
        unresolved_groups = _bucket_groups(
            worklist,
            lane="unresolved",
            target_items=int(cfg["batch_unresolved_rows"]),
            target_rows=int(cfg["batch_unresolved_rows"]),
            target_text_chars=cfg.get("batch_text_chars"),
            text_columns=text_columns,
        )
        work_rows = sum(
            group["rows"] for group in resolved_groups + unresolved_groups
        )
        touched = touch_rows(feed, inspected_rows.where(~F.col("_needs_work")))
        spark.createDataFrame(
            [(feed, "plan", json.dumps({
                "eligible_total": eligible_total,
                "drift_total": drift_total,
                "work_rows": work_rows,
                "touched": touched,
                "work_cap_rows": WORK_CAP_ROWS,
                "work_bucket_count": work_bucket_count,
                "batch_resolved_rows": int(cfg.get("batch_resolved_rows", 100000)),
                "batch_unresolved_rows": int(cfg["batch_unresolved_rows"]),
                "batch_text_chars": cfg.get("batch_text_chars"),
            }), work_rows, RUN_ID, planned, datetime.now(timezone.utc), None, "success", None)],
            "feed string, lane string, cursor string, batch_rows long, run_id string, "
            "started_at timestamp, finished_at timestamp, last_refresh_ts timestamp, "
            "status string, detail string",
        ).write.mode("append").saveAsTable(f"{CONTROL_SCHEMA}.engine_progress")
        processed = 0
        batches = 0
        stop_reason = None
        for lane, groups in [
            ("resolved", resolved_groups),
            ("unresolved", unresolved_groups),
        ]:
            for group in groups:
                if batches >= MAX_BATCHES:
                    stop_reason = "max_batches"
                    break
                if _deadline_reached():
                    stop_reason = "deadline"
                    break
                bucket_ids = group["bucket_ids"]
                batch = worklist.where(
                    (F.col("_anon_lane") == lane)
                    & F.col("_anon_work_bucket").isin(bucket_ids)
                )
                started = datetime.now(timezone.utc)
                output = (
                    _prepare_resolved(feed, batch)
                    if lane == "resolved"
                    else _prepare_unresolved(feed, batch)
                )
                processed += merge_batch(
                    feed, output, lane, _bucket_cursor(bucket_ids), started
                )
                batches += 1
            if stop_reason:
                break
        total_groups = len(resolved_groups) + len(unresolved_groups)
        slice_complete = batches == total_groups
        capped = bool(WORK_CAP_ROWS) and eligible_total > WORK_CAP_ROWS
        drift_through = datetime.now(timezone.utc)
        if slice_complete and not capped:
            _record_drift_watermark(feed, drift_through)
        elif slice_complete and capped and drift_total <= WORK_CAP_ROWS:
            _record_drift_watermark(feed, drift_through)
        has_more = (not slice_complete) or capped
        if not stop_reason and capped:
            stop_reason = "work_cap"
        return {
            "status": "partial" if has_more else "success",
            "stop_reason": stop_reason,
            "processed": processed,
            "touched": touched,
            "batches": batches,
            "total_groups": total_groups,
            "eligible_total": eligible_total,
            "drift_total": drift_total,
            "work_rows": work_rows,
            "remaining_estimate": max(eligible_total - processed - touched, 0),
            "elapsed_seconds": _elapsed_seconds(),
        }
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {work_stage}")

ensure_control_tables(spark)
if ACTION == "init_controls":
    tag_control_tables(spark)
    result = {"status": "success", "elapsed_seconds": _elapsed_seconds()}
elif ACTION == "refresh_fingerprints":
    refresh_person_fingerprints()
    result = {"status": "success", "elapsed_seconds": _elapsed_seconds()}
elif ACTION == "eligible_count":
    count = eligible_rows(FEED).count()
    spark.createDataFrame(
        [(FEED, "eligible_count", None, count, RUN_ID, datetime.now(timezone.utc),
          datetime.now(timezone.utc), None, "success", None)],
        "feed string, lane string, cursor string, batch_rows long, run_id string, started_at timestamp, "
        "finished_at timestamp, last_refresh_ts timestamp, status string, detail string",
    ).write.mode("append").saveAsTable(f"{CONTROL_SCHEMA}.engine_progress")
    result = {
        "status": "success", "eligible_count": int(count),
        "elapsed_seconds": _elapsed_seconds(),
    }
elif ACTION == "initialize_legacy_blob_state":
    result = initialize_legacy_blob_state()
elif ACTION == "run":
    result = run_feed(FEED)
else:
    raise ValueError(f"unknown action: {ACTION}")

payload = {"action": ACTION, "feed": FEED, "run_id": RUN_ID, **result}
print(json.dumps(payload, default=str, sort_keys=True))
dbutils.notebook.exit(json.dumps(payload, default=str, sort_keys=True))

