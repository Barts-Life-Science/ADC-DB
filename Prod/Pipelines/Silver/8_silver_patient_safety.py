# Databricks notebook source
# MAGIC %md
# MAGIC # Patient safety
# MAGIC Safety incidents, participants, injuries, factors, actions, status and incident text.

# MAGIC
# MAGIC Reading order: 8 of 8. Numbers guide navigation; Lakeflow schedules datasets by their dependencies.
# MAGIC Shared helpers live in `silver_journey_shared.py`, an importable Python file.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identity, source normalization and dataset registration

# COMMAND ----------

from pyspark.sql import Column, functions as F

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
    # Attach the product and source description to every output column's schema metadata.
    for column in frame.columns:
        frame = frame.withMetadata(column, {"comment": f"{product} field {column}; derived from the admitted TDX bronze source."})
    return frame

def canonical(source_name, target_name, keys, key_name, namespace):
    # Normalize a configured patient-safety bronze feed: standardize names, construct stable
    # keys, and carry identity, record status and source-derived load timestamps.
    source = spark.read.table(f"{BRONZE}.{source_name}")
    df = source.select(*[F.col(column).alias(column.lower()) for column in source.columns])
    native_renames = {
        "clinical_safety_incident": ("recordid", "incident_id"),
        "clinical_safety_incident_injury": ("recordid", "injury_id"),
        "clinical_safety_incident_lfpse": ("recordid", "lfpse_id"),
        "clinical_safety_incident_action": ("recordid", "action_id"),
        "clinical_safety_incident_participant": ("link_recordid", "participant_row_id"),
    }
    if target_name in native_renames:
        old, new = native_renames[target_name]
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
            keys = [new if key == old else key for key in keys]
    if "encntr_id" in df.columns and "encounter_id" not in df.columns:
        df = df.withColumnRenamed("encntr_id", "encounter_id")

    source_row = F.concat_ws(":", *[F.col(key).cast("string") for key in keys])
    df = df.withColumn(key_name, stable_id(namespace, *[F.col(key) for key in keys]))
    if "source_present_ind" in df.columns:
        absent = ~F.coalesce(F.col("source_present_ind"), F.lit(True))
        df = df.withColumn("record_status", F.when(absent, "superseded").otherwise("active"))
        df = df.withColumn("record_status_effective_to", F.when(absent, F.col("source_absent_detected_ts")))
    else:
        df = df.withColumn("record_status", F.lit("active"))
        df = df.withColumn("record_status_effective_to", F.lit(None).cast("timestamp"))
    df = df.withColumn("record_status_effective_from", F.lit(None).cast("timestamp"))

    if "person_id" in df.columns:
        df = df.withColumn("person_id", F.col("person_id").cast("string"))
        subject_key, subject_system = subject_key_with_system(F.col("person_id"), F.lit(f"{BRONZE}.{source_name}"), source_row)
        df = df.withColumn("identity_status", F.when(F.col("person_id").isNotNull(), "resolved").otherwise("unresolved"))
        df = df.withColumn("subject_key", subject_key).withColumn("subject_id_system", subject_system)
    else:
        df = df.withColumn("identity_status", F.lit("not_applicable"))
    if "encounter_id" in df.columns:
        df = df.withColumn("encounter_id", F.col("encounter_id").cast("string"))
    if "source_adc_updt" in df.columns:
        df = df.withColumn("source_update_timestamp", F.col("source_adc_updt"))

    stable_loaded_at = (
        F.col("adc_updt") if "adc_updt" in df.columns
        else F.col("source_update_timestamp") if "source_update_timestamp" in df.columns
        else F.lit(None).cast("timestamp")
    )
    df = df.withColumn("loaded_at", stable_loaded_at)
    df = df.withColumn("load_batch_id", F.coalesce(F.date_format(stable_loaded_at, "yyyyMMddHHmmss"), F.lit("tdx-v21")))

    if target_name == "text_safety_document":
        df = df.withColumn("incident_id", F.col("inc_id").cast("string"))
        df = df.withColumn("person_id", F.col("incident_person_id").cast("string"))
        subject_key, subject_system = subject_key_with_system(F.col("person_id"), F.lit(f"{BRONZE}.{source_name}"), source_row)
        df = df.withColumn("identity_status", F.when(F.col("person_id").isNotNull(), "resolved").otherwise("unresolved"))
        df = df.withColumn("subject_key", subject_key).withColumn("subject_id_system", subject_system)
        df = df.withColumn("source_object", F.regexp_replace(F.lower("source_object_table"), "^datix_", ""))
        df = df.withColumn("document_type_code", F.col("fragment_role"))
        df = df.withColumn("document_type_display", F.initcap(F.regexp_replace("fragment_role", "_", " ")))
        df = df.withColumn("document_text", F.col("text"))
        df = df.withColumn("document_text_anonymised", F.col("anon_text"))
        df = df.withColumn(
            "text_is_anonymised",
            (F.col("anon_status") == "anonymized")
            & F.col("anon_redactor_version").isin("v3.3")
            & F.col("anon_source_text_sha").eqNullSafe(F.col("anon_input_digest"))
            & F.col("anon_context_fingerprint").eqNullSafe(F.col("context_fingerprint_current")),
        )
        df = df.withColumn("text_sha256", F.sha2("document_text", 256))
        df = df.withColumn("text_length", F.length("document_text"))
    return _with_comments(df, target_name)

def declare_product(source_name, target_name, keys, key_name, namespace):
    # Register a configured public product and its internal lifecycle and batch-metadata
    # companion.
    def build():
        # Build the configured product using the source, key columns and namespace captured by
        # this declaration.
        return canonical(source_name, target_name, list(keys), key_name, namespace)
    build.__name__ = f"build_{target_name}"
    globals()[build.__name__] = dp.materialized_view(
        name=f"{PUBLIC_SCHEMA}.{target_name}", comment=f"TDX v2.1 Silver research product {target_name}.", refresh_policy="incremental"
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
        refresh_policy="incremental",
    )(metadata)

SAFETY_SPECS = [
    ("map_safety_incident", "clinical_safety_incident", ["recordid"], "safety_incident_key", "safety_incident:datix"),
    ("map_safety_incident_participant", "clinical_safety_incident_participant", ["link_recordid"], "safety_incident_participant_key", "safety_incident_participant:datix"),
    ("map_safety_incident_injury", "clinical_safety_incident_injury", ["recordid"], "safety_incident_injury_key", "safety_incident_injury:datix"),
    ("map_safety_incident_factor", "clinical_safety_incident_factor", ["factor_row_id"], "safety_incident_factor_key", "safety_incident_factor:datix"),
    ("map_safety_incident_lfpse", "clinical_safety_incident_lfpse", ["recordid"], "safety_incident_lfpse_key", "safety_incident_lfpse:datix"),
    ("map_safety_incident_action", "clinical_safety_incident_action", ["recordid"], "safety_incident_action_key", "safety_incident_action:datix"),
    ("map_safety_incident_status", "clinical_safety_incident_status", ["status_row_id"], "safety_incident_status_key", "safety_incident_status:datix"),
    ("map_safety_text_fragment", "text_safety_document", ["fragment_id"], "safety_document_key", "safety_document:datix"),
]
for _spec in SAFETY_SPECS:
    declare_product(*_spec)

@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._clinical_safety_incident_index_lane",
    comment="Internal S4-compatible event-index projection lane for one row per Datix safety incident.",
    refresh_policy="incremental",
)
def build_clinical_safety_incident_index_lane():
    # Build the declared dataset: Internal S4-compatible event-index projection lane for one row
    # per Datix safety incident.
    source = spark.read.table(f"{PUBLIC_SCHEMA}.clinical_safety_incident")
    columns = set(source.columns)
    def optional(name, data_type):
        # Return a typed source column when present, or a typed null for a missing optional
        # field.
        return F.col(name).cast(data_type) if name in columns else F.lit(None).cast(data_type)
    return source.select(
        F.col("safety_incident_key").cast("string").alias("patient_event_key"),
        optional("subject_key", "string").alias("subject_key"),
        optional("subject_id_system", "string").alias("subject_id_system"),
        optional("person_id", "string").alias("person_id"),
        optional("identity_status", "string").alias("identity_status"),
        optional("encounter_id", "string").alias("encounter_id"),
        F.coalesce(optional("incident_dt_tm_clean", "timestamp"), optional("incident_dt_tm", "timestamp")).alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("datix").alias("_source_system"),
        optional("source_table", "string").alias("_source_table"),
        optional("source_row_id", "string").alias("_source_row_id"),
        F.lit("urn:datix:incident-type").alias("source_coding_system"),
        optional("inc_type", "string").alias("source_code"),
        optional("inc_type_display", "string").alias("source_display"),
        optional("record_status", "string").alias("record_status"),
        optional("seclevel", "string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        optional("load_batch_id", "string").alias("load_batch_id"),
        optional("loaded_at", "timestamp").alias("loaded_at"),
    )

