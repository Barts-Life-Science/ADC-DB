"""Silver specialist.registry_observations: 1 dataset(s).

- datasets: _clinical_registry_observations
- moved verbatim from: 2_silver_clinical_events:117, 2_silver_clinical_events:4190, 2_silver_clinical_events:4198, 2_silver_clinical_events:4237

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, spark, stable_id


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


SRC_S3B_REGISTRY_ROUTE = "3_lookup.omop.registry_field_route"


# Clinical /  registry observations

# S3b Task 15: unpivot governed registry fields once, preserving the registry record,
# field identity and the registry's own event time. Public registry products remain wide.
S3B_REGISTRY_PRODUCTS = (
    "clinical_registry_acs_transfer", "clinical_registry_cardiac_mdt",
    "clinical_registry_coronary_lesion", "clinical_registry_coronary_procedure",
    "clinical_registry_eracs_episode", "clinical_registry_mortality_review",
    "clinical_registry_noncoronary_procedure", "clinical_registry_surgery_episode",
    "clinical_registry_surgery_followup", "clinical_registry_surgery_procedure",
)


def _registry_observation_query():
    # Use registry-field routing rules and parent registry entries to construct configured
    # observations.
    route = read_source(SRC_S3B_REGISTRY_ROUTE).select(
        "registry_field_id", "product", "field", "route", "question_display", "unit_column"
    )
    parent = spark.read.table(_n("journey_clinical.registry_entry")).select(
        F.col("patient_event_key").alias("registry_entry_key"), "subject_key", "subject_id_system",
        "person_id", "encounter_id", "event_datetime", "event_end_datetime", "event_datetime_local", "event_end_datetime_local", "record_status",
        "record_status_effective_from", "record_status_effective_to", "source_update_timestamp", "loaded_at",
    )
    frames = []
    for product in S3B_REGISTRY_PRODUCTS:
        relation = product.removeprefix("clinical_")
        source = spark.read.table(_n(f"journey_clinical.{relation}"))
        values = []
        for name in source.columns:
            values.extend((F.lit(name), source[name].cast("string")))
        long = (source.select(
                    F.col("patient_event_key").alias("registry_entry_key"),
                    F.create_map(*values).alias("_values"))
                .select("registry_entry_key", "_values", F.explode(F.map_entries("_values")).alias("_field_value"))
                .select("registry_entry_key", "_values", F.col("_field_value.key").alias("field"),
                        F.col("_field_value.value").alias("answer_text"))
                .where(F.col("answer_text").isNotNull() & (F.trim("answer_text") != ""))
                .withColumn("product", F.lit(product)))
        frames.append(long)
    observations = frames[0]
    for frame in frames[1:]:
        observations = observations.unionByName(frame)
    observations = observations.join(F.broadcast(route), ["product", "field"], "inner").join(parent, "registry_entry_key", "left")
    return (observations
        .withColumn("patient_event_key", stable_id("registry_observation:iweb", F.col("registry_entry_key"), F.col("registry_field_id")))
        .withColumn("registry_product", F.col("product"))
        .withColumn("registry_field", F.col("field"))
        .withColumn("value_number", F.when(F.col("route") == "measurement", F.expr("try_cast(answer_text as decimal(38,10))")))
        .withColumn("unit", F.element_at(F.col("_values"), F.col("unit_column")))
        .withColumn("event_datetime_status", F.when(F.col("event_datetime").isNull(), F.lit("registry_undated")).otherwise(F.lit("registry_dated"))))


@materialized_view(
    name=_n("journey_clinical._registry_observations"),
    refresh_policy="auto",
)
def _registry_observations():
    # Build the declared dataset: Source-linked long observations for governed fields in the ten
    # typed iWeb registry products.
    return _registry_observation_query().select(
        "patient_event_key", "registry_entry_key", "registry_field_id", "registry_product", "registry_field",
        "route", "question_display", "answer_text", "value_number", "unit", "event_datetime_status",
        "subject_key", "subject_id_system", "person_id", "encounter_id", "event_datetime", "event_end_datetime", "event_datetime_local", "event_end_datetime_local",
        "record_status", "record_status_effective_from", "record_status_effective_to", "source_update_timestamp", "loaded_at",
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_registry_observations': {'columns': {},
                                     'table': 'Source-linked long observations for governed fields in the '
                                              'ten typed iWeb registry products.'}}
