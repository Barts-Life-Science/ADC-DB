# Databricks notebook source
# MAGIC %md
# MAGIC # Patient flow
# MAGIC Tracking episodes, attendance, location stays, milestones, pre-arrivals, pending movements and bed observations.
# MAGIC
# MAGIC
# MAGIC Reading order: 7 of 8. Numbers guide navigation; Lakeflow schedules datasets by their dependencies.
# MAGIC Shared helpers live in `silver_journey_shared.py`, an importable Python file.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identity, source normalization and dataset registration

# COMMAND ----------

from pyspark.sql import Column, functions as F
from silver_active_rows import active_rows  # ACTIVE_ROWS_V1
from silver_value_policy import column_comments, finish_public, mill_fk  # INSR_W2 W2.02

try:
    from pyspark import pipelines as dp
except ImportError:
    import dlt
    class _DpShim:
        @staticmethod
        def materialized_view(**options):
            # Adapt the materialized-view declaration to classic DLT by removing unsupported
            # options.
            options.pop("refresh_policy", None)
            options.pop("cluster_by", None)
            return dlt.table(**options)
    dp = _DpShim()

PUBLIC_SCHEMA = spark.conf.get("journey.public_schema")
INTERNAL_SCHEMA = spark.conf.get("journey.internal_schema")
BRONZE = spark.conf.get("journey.tdx_bronze_schema")
SPINE = spark.conf.get("journey.tdx_spine_schema")
NULL_TOKEN = "~"

def _enc(value):
    # Encode a key component as trimmed base64, using a distinct token for nulls so separators
    # cannot collide.
    column = value if isinstance(value, Column) else F.lit(value)
    text_value = F.trim(column.cast("string"))
    return F.when(column.isNull(), F.lit(NULL_TOKEN)).otherwise(F.base64(text_value.cast("binary")))

def stable_id(namespace, *columns):
    # Hash the namespace and encoded key components into a deterministic SHA-256 identifier.
    return F.sha2(F.concat_ws(":", _enc(namespace), *[_enc(column) for column in columns]), 256)

def subject_key_with_system(person_id, source_table, source_row_id):
    # Use the Cerner person identifier when present; otherwise create a source-row-specific
    # unresolved subject key.
    present = person_id.isNotNull() & (F.trim(person_id.cast("string")) != "")
    return (
        F.when(present, stable_id("subject", "urn:cerner:person_id", person_id))
         .otherwise(stable_id("subject", "nosubject", source_table, source_row_id)),
        F.when(present, F.lit("urn:cerner:person_id")).otherwise(F.lit("nosubject")),
    )

def _with_comments(frame, product):
    # INSR_W8_COMMENTS_V1 (W8.04): every column's comment comes from the canonical dictionary in silver_tdx_comments.py.
    # A column missing from it raises KeyError while the graph is built, so a template comment cannot be
    # published again; assert_not_boilerplate rejects template text that reached the dictionary.
    from silver_tdx_comments import SILVER_TDX_COLUMN_COMMENTS, assert_not_boilerplate
    comments = SILVER_TDX_COLUMN_COMMENTS[product]
    for column in frame.columns:
        text = comments[column]
        assert_not_boilerplate(text, f"{product}.{column}")
        frame = frame.withMetadata(column, {"comment": text})
    return frame


def _tdx_table_comment(target_name):
    # INSR_W8_COMMENTS_V1 (W8.04): the grain statement used as the product's MV table comment.
    from silver_tdx_comments import SILVER_TDX_TABLE_COMMENTS
    return SILVER_TDX_TABLE_COMMENTS[target_name]

def canonical(source_name, target_name, keys, key_name, namespace):
    # Normalize a configured patient-flow bronze feed: standardize names, construct stable keys,
    # and carry identity, record status and source-derived load timestamps.
    # ACTIVE_ROWS_V1: bronze rows that are not active never enter silver; the rule lives in silver_active_rows.py.
    source = active_rows(spark.read.table(f"{BRONZE}.{source_name}"), f"{BRONZE}.{source_name}")
    df = source.select(*[F.col(column).alias(column.lower()) for column in source.columns])
    if target_name == "clinical_prearrival":
        df = df.withColumnRenamed("linked_person_id", "person_id").withColumnRenamed("linked_encntr_id", "encounter_id")
    elif "encntr_id" in df.columns and "encounter_id" not in df.columns:
        df = df.withColumnRenamed("encntr_id", "encounter_id")
    if target_name in {"clinical_bed_status_observation", "reference_location_unit"} and "location_cd" in df.columns:
        df = df.withColumnRenamed("location_cd", "location_code")
        keys = ["location_code", "observation_date"] if target_name == "clinical_bed_status_observation" else ["location_code"]

    source_row = F.concat_ws(":", *[F.col(key).cast("string") for key in keys])
    df = df.withColumn(key_name, stable_id(namespace, *[F.col(key) for key in keys]))
    if target_name == "clinical_appointment_action":
        df = df.withColumn("appointment_patient_event_id", stable_id("appointment:mill_scheduling", F.col("sch_event_id")))

    if "source_present_ind" in df.columns:
        absent = ~F.coalesce(F.col("source_present_ind"), F.lit(True))
        df = df.withColumn("record_status", F.when(absent, "superseded").otherwise("active"))
        df = df.withColumn("record_status_effective_to", F.when(absent, F.col("source_absent_detected_ts")))
    else:
        df = df.withColumn("record_status", F.lit("active"))
        df = df.withColumn("record_status_effective_to", F.lit(None).cast("timestamp"))
    df = df.withColumn("record_status_effective_from", F.lit(None).cast("timestamp"))

    if "person_id" in df.columns:
        # INSR_W2 W2.02: subject_key hashes the source text form exactly as before; the published person_id is
        # BIGINT with Millennium's 0 ("no person") as NULL.
        person_text = F.col("person_id").cast("string")
        subject_key, subject_system = subject_key_with_system(person_text, F.lit(f"{BRONZE}.{source_name}"), source_row)
        df = df.withColumn("subject_key", subject_key).withColumn("subject_id_system", subject_system)
        df = df.withColumn("person_id", mill_fk("person_id"))
        df = df.withColumn("identity_status", F.when(F.col("person_id").isNotNull(), "resolved").otherwise("unresolved"))
    else:
        df = df.withColumn("identity_status", F.lit("not_applicable"))
    if "encounter_id" in df.columns:
        # INSR_W2 W2.02: BIGINT, 0 -> NULL (no key hashes encounter_id in this notebook).
        df = df.withColumn("encounter_id", mill_fk("encounter_id"))
    if "source_adc_updt" in df.columns:
        df = df.withColumn("source_update_timestamp", F.col("source_adc_updt"))

    stable_loaded_at = (
        F.col("adc_updt") if "adc_updt" in df.columns
        else F.col("observed_at") if "observed_at" in df.columns
        else F.col("source_update_timestamp") if "source_update_timestamp" in df.columns
        else F.lit(None).cast("timestamp")
    )
    df = df.withColumn("loaded_at", stable_loaded_at)
    df = df.withColumn("load_batch_id", F.coalesce(F.date_format(stable_loaded_at, "yyyyMMddHHmmss"), F.lit("tdx-v21")))

    if target_name == "clinical_tracking_attendance":
        valid = F.col("checkin_dt_tm").isNotNull() & F.col("checkout_dt_tm_clean").isNotNull() & (F.col("checkout_dt_tm_clean") >= F.col("checkin_dt_tm"))
        df = df.withColumn("period_start", F.col("checkin_dt_tm")).withColumn("period_end", F.col("checkout_dt_tm_clean"))
        df = df.withColumn("dwell_minutes", F.when(valid, ((F.unix_timestamp("checkout_dt_tm_clean") - F.unix_timestamp("checkin_dt_tm")) / 60).cast("int")))
    elif target_name == "clinical_tracking_location_stay":
        valid = F.col("arrive_dt_tm").isNotNull() & F.col("depart_dt_tm_clean").isNotNull() & (F.col("depart_dt_tm_clean") >= F.col("arrive_dt_tm"))
        df = df.withColumn("period_start", F.col("arrive_dt_tm")).withColumn("period_end", F.col("depart_dt_tm_clean"))
        df = df.withColumn("stay_minutes", F.when(valid, ((F.unix_timestamp("depart_dt_tm_clean") - F.unix_timestamp("arrive_dt_tm")) / 60).cast("int")))
    elif target_name == "clinical_tracking_milestone":
        df = df.withColumn("milestone_datetime", F.coalesce("complete_dt_tm", "onset_dt_tm", "requested_dt_tm"))
    # INSR_W2 W2.02: zero codes/ids, system user and derived columns (silver_value_policy).
    df = finish_public(df, target_name)
    df = _with_comments(df, target_name)
    # INSR_W2 W2.02: _with_comments writes a generic comment on every column; put back the policy's own
    # comments (<role>_system_ind, milestone_config_era, ...) so UC carries the real meaning.
    for column, text in column_comments(target_name).items():
        if column in df.columns:
            df = df.withMetadata(column, {"comment": text})
    return df

def declare_product(source_name, target_name, keys, key_name, namespace):
    # Register a configured public product and its internal lifecycle and batch-metadata
    # companion.
    def build():
        # Build the configured product using the source, key columns and namespace captured by
        # this declaration.
        return canonical(source_name, target_name, list(keys), key_name, namespace)
    build.__name__ = f"build_{target_name}"
    globals()[build.__name__] = dp.materialized_view(
        name=f"{PUBLIC_SCHEMA}.{target_name}", comment=_tdx_table_comment(target_name), refresh_policy="auto"
    )(build)

    def metadata():
        # Select the available identity, lifecycle and batch fields from the configured public
        # product.
        frame = spark.read.table(f"{PUBLIC_SCHEMA}.{target_name}")
        columns = [key_name, "identity_status", "linkage_route", "record_status", "record_status_effective_to", "load_batch_id", "loaded_at"]
        return _with_comments(frame.select(*[column for column in columns if column in frame.columns]), f"{target_name} metadata")
    metadata.__name__ = f"build_{target_name}_metadata"
    globals()[metadata.__name__] = dp.materialized_view(
        name=f"{INTERNAL_SCHEMA}._{target_name}_metadata",
        comment=f"Internal TDX v2.1 quality and batch metadata for {target_name}.",
        refresh_policy="auto",
    )(metadata)

FLOW_SPECS = [
    ("map_tracking_episode", "clinical_tracking_episode", ["tracking_id"], "tracking_episode_key", "tracking_episode:mill"),
    ("map_tracking_attendance", "clinical_tracking_attendance", ["tracking_checkin_id"], "tracking_attendance_key", "tracking_attendance:mill"),
    ("map_tracking_location_stay", "clinical_tracking_location_stay", ["tracking_locator_id"], "tracking_location_stay_key", "tracking_location_stay:mill"),
    ("map_tracking_milestone", "clinical_tracking_milestone", ["tracking_event_id"], "tracking_milestone_key", "tracking_milestone:mill"),
    ("map_prearrival", "clinical_prearrival", ["tracking_prearrival_id"], "prearrival_key", "prearrival:mill"),
    ("map_pending_movement", "clinical_pending_movement", ["encntr_pending_id"], "pending_movement_key", "pending_movement:mill"),
    ("map_appointment_action", "clinical_appointment_action", ["sch_action_id"], "appointment_action_key", "appointment_action:mill_scheduling"),
    ("map_bed_status_observation", "clinical_bed_status_observation", ["location_cd", "observation_date"], "bed_status_observation_key", "bed_status_observation:mill"),
    ("map_bed_observation_run", "clinical_bed_status_observation_coverage", ["observation_date"], "bed_status_coverage_key", "bed_status_coverage:mill"),
    ("map_location_unit", "reference_location_unit", ["location_cd"], "location_unit_key", "location_unit:mill"),
    ("map_location_attribute_history", "reference_location_attribute_history", ["attribute_row_id"], "location_attribute_key", "location_attribute:mill"),
]
for _spec in FLOW_SPECS:
    declare_product(*_spec)
