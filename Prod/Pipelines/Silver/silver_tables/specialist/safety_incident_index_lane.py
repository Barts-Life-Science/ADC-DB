"""Silver specialist.safety_incident_index_lane: 1 dataset(s).

- datasets: _clinical_safety_incident_index_lane
- moved verbatim from: 8_silver_patient_safety:197

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import spark
from silver_tables.specialist._shared import INTERNAL_SCHEMA, PUBLIC_SCHEMA


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._clinical_safety_incident_index_lane",
    refresh_policy="auto",
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
        F.coalesce(optional("incident_dt_tm_clean", "timestamp"), optional("incident_dt_tm", "timestamp")).alias("event_datetime_local"),
        F.lit(None).cast("timestamp").alias("event_end_datetime_local"),
        # TZ_SILVER_LOCAL_V1: Europe/London companions (bronze *_LOCAL for Millennium; local-clock sources unchanged)
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


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_safety_incident_index_lane': {'columns': {},
                                          'table': 'Internal S4-compatible event-index projection lane for '
                                                   'one row per Datix safety incident.'}}
