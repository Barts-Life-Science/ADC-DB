"""Silver spine.pathology_accession_identity: 1 dataset(s).

- datasets: _events_pathology_accession_identity
- moved verbatim from: 2_silver_clinical_events:1672

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source
from silver_tables._common.journey import SRC_PATHOLOGY_ACCESSION, _alias_resolved_accession
from silver_tables.spine._shared import SRC_PATHOLOGY_ACCESSION_SOURCE


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Events /  pathology accession identity
@materialized_view(
    name=_n("journey_events._pathology_accession_identity"),
    refresh_policy="auto",
)
def _pathology_accession_identity():
    # Build the declared dataset: INTERNAL accession identity: status-honest registry person
    # columns plus MRN/NHS evidence aggregated from accession_source. Survivor accessions only;
    # aliases resolve before grouping so retired evidence follows the survivor.
    acc = _alias_resolved_accession(read_source(SRC_PATHOLOGY_ACCESSION))
    survivors = acc.where(F.col("pathology_accession_id") == F.col("_resolved_accession_id"))
    ev = (
        _alias_resolved_accession(read_source(SRC_PATHOLOGY_ACCESSION_SOURCE))
        .groupBy(F.col("_resolved_accession_id").alias("pathology_accession_id"))
        .agg(
            F.max("mrn").alias("evidence_mrn"),
            F.max("nhs_number").alias("evidence_nhs"),
            F.count(F.lit(1)).cast("long").alias("source_row_count"),
            F.max("ADC_UPDT").alias("evidence_loaded_at"),
            # PATH_ENCOUNTER_V1: the encounters named by the accession's current (Millennium) sources.
            F.min(F.when(F.col("is_current"), F.col("encounter_id"))).cast("bigint").alias("_encounter_lo"),
            F.max(F.when(F.col("is_current"), F.col("encounter_id"))).cast("bigint").alias("_encounter_hi"),
        )
        .withColumn("accession_encounter_id",
                    F.when(F.col("_encounter_lo") == F.col("_encounter_hi"), F.col("_encounter_lo")))
        .withColumn("accession_encounter_conflict",
                    F.coalesce(F.col("_encounter_lo") != F.col("_encounter_hi"), F.lit(False)))
        .drop("_encounter_lo", "_encounter_hi")
    )
    return survivors.select(
        "pathology_accession_id", "primary_source_accession_id", "canonical_accession_status",
        "canonical_person_id", "person_resolution_status", "normalized_lab_no",
        "lab_series", "discipline", "request_dt", "sample_dt", "report_dt",
        "request_dt_local", "sample_dt_local", "report_dt_local",
        "clinical_details", "tlcs_requested", "conditions", "reason", "urgent_flag",
        "body_site_code", "body_site_snomed_code", "specimen_type_code",
        "specimen_type_snomed_code", "source_site_code", "lifecycle_status",
        "research_qi_only", "created_at", "ADC_UPDT",
        # STM_SPECIMEN_TYPE_V1/silver-identity: specimen-type namespace + display (the lookup key) and the discipline rule id.
        "specimen_type_source_system", "specimen_type_display", "body_site_source_system", "discipline_rule_id",
    ).join(ev, "pathology_accession_id", "left")


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_events_pathology_accession_identity': {'columns': {},
                                          'table': 'INTERNAL accession identity: status-honest registry '
                                                   'person columns plus MRN/NHS evidence aggregated from '
                                                   'accession_source. Survivor accessions only; aliases '
                                                   'resolve before grouping so retired evidence follows the '
                                                   'survivor.'}}
