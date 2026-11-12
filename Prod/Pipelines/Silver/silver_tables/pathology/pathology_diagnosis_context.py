"""Silver pathology.pathology_diagnosis_context: 1 dataset(s).

- datasets: clinical_pathology_diagnosis_context
- moved verbatim from: 2_silver_clinical_events:1574, 2_silver_clinical_events:1600, silver_journey_shared.py:4080, silver_journey_shared.py:4083

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _clamped_ts, _n, read_source, stable_id
from silver_tables._common.journey import _alias_resolved_accession


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# CONTEXT_LINK_V1 (BRONZE_FIX_20261025D): the diagnosis context of a pathology accession comes from the narrow bronze
# link. Bronze no longer writes clinical_context rows into map_pathology_indication, so clinical_indication now holds
# rule-based indications only (none are active today).
SRC_PATHOLOGY_DIAGNOSIS_CONTEXT = "4_prod.bronze.map_pathology_diagnosis_context"


def _pathology_diagnosis_context_canonical():
    """One row per bronze link: the specimen and condition keys and how the diagnosis relates to the accession."""
    x = _alias_resolved_accession(read_source(SRC_PATHOLOGY_DIAGNOSIS_CONTEXT))
    return x.select(
        stable_id("pathology_diagnosis_context", F.col("pathology_accession_id"), F.col("diagnosis_id"))
        .alias("pathology_diagnosis_context_key"),
        stable_id("specimen:accession", F.col("_resolved_accession_id")).alias("specimen_patient_event_key"),
        stable_id("condition:mill:diagnosis", F.col("diagnosis_id")).alias("condition_patient_event_key"),
        F.col("_resolved_accession_id").alias("accession_identifier"),
        F.col("diagnosis_id").cast("bigint").alias("diagnosis_id"),
        F.col("person_id").cast("bigint").alias("person_id"),
        F.col("accession_encounter_id").cast("bigint").alias("accession_encounter_id"),
        F.col("diagnosis_encounter_id").cast("bigint").alias("diagnosis_encounter_id"),
        F.col("same_encounter").alias("same_encounter_ind"),
        F.col("day_offset").cast("int").alias("day_offset"),
        _clamped_ts(F.col("accession_dt")).alias("accession_datetime"),
        _clamped_ts(F.col("diagnosis_dt")).alias("diagnosis_datetime"),
        F.col("rule_id"),
        F.col("rule_version"),
        F.col("ADC_UPDT").alias("loaded_at"),
    )


# ==== journey_clinical.pathology_diagnosis_context (CONTEXT_LINK_V1, BRONZE_FIX_20261025D) ====
#
# The patient's diagnoses recorded in a pathology accession's encounter, or dated 30 days before to 7 days after the
# accession. This is context around the test, not the reason for it (until 2026-10-25 these rows were published as
# clinical_indication). Built in bronze (map_pathology_diagnosis_context) as a narrow link; nothing is copied here
# except keys and the relation, so the diagnosis text and codes are read from clinical_condition.
PATHOLOGY_DIAGNOSIS_CONTEXT_PUBLIC_COLUMNS = [
    "pathology_diagnosis_context_key", "specimen_patient_event_key", "condition_patient_event_key",
    "accession_identifier", "diagnosis_id", "person_id", "accession_encounter_id", "diagnosis_encounter_id",
    "same_encounter_ind", "day_offset", "accession_datetime", "diagnosis_datetime", "rule_id", "rule_version",
    "loaded_at",
]


@materialized_view(
    name=_n("journey_clinical.pathology_diagnosis_context"),
    cluster_by=["person_id"],
    refresh_policy="auto",
)
def pathology_diagnosis_context():
    # Build the declared dataset: the bronze diagnosis-context link with silver keys.
    return _pathology_diagnosis_context_canonical().select(*PATHOLOGY_DIAGNOSIS_CONTEXT_PUBLIC_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_pathology_diagnosis_context': {'columns': {'accession_datetime': 'Accession date (sample, else '
                                                                            'request, else report datetime) '
                                                                            'of the source row that gave the '
                                                                            'strongest evidence (NULL '
                                                                            'outside 1950-2099).',
                                                      'accession_encounter_id': 'Millennium ENCNTR_ID of the '
                                                                                'accession source row that '
                                                                                'gave the strongest '
                                                                                'evidence; NULL for '
                                                                                'LIMS-only rows.',
                                                      'accession_identifier': 'Alias-resolved pathology '
                                                                              'accession identifier (the '
                                                                              'clinical_specimen accession).',
                                                      'condition_patient_event_key': 'patient_event_key of '
                                                                                     'the clinical_condition '
                                                                                     'row of the Millennium '
                                                                                     'diagnosis; join '
                                                                                     'clinical_condition for '
                                                                                     'its code, display and '
                                                                                     'status. Gold publishes '
                                                                                     'only pairs whose '
                                                                                     'specimen and condition '
                                                                                     'Gold admitted.',
                                                      'day_offset': 'Days from accession_datetime to '
                                                                    'diagnosis_datetime by calendar date '
                                                                    '(negative = diagnosis before the '
                                                                    'accession); NULL when either date is '
                                                                    'missing.',
                                                      'diagnosis_datetime': 'Diagnosis date: DIAG_DT_TM, '
                                                                            'else ASSERTED_DT_TM, else '
                                                                            'BEG_EFFECTIVE_DT_TM (NULL '
                                                                            'outside 1950-2099).',
                                                      'diagnosis_encounter_id': 'Millennium ENCNTR_ID of the '
                                                                                'diagnosis.',
                                                      'diagnosis_id': 'Millennium DIAGNOSIS_ID.',
                                                      'loaded_at': 'ADC_UPDT of the bronze link row (when '
                                                                   'the link was last materially changed).',
                                                      'pathology_diagnosis_context_key': 'Deterministic '
                                                                                         'SHA-256 key: '
                                                                                         "stable_id('pathology_diagnosis_context', "
                                                                                         'bronze '
                                                                                         'pathology_accession_id, '
                                                                                         'diagnosis_id). '
                                                                                         'Primary key: one '
                                                                                         'row per '
                                                                                         '(accession, '
                                                                                         'diagnosis).',
                                                      'person_id': 'Millennium PERSON_ID shared by the '
                                                                   'accession and the diagnosis.',
                                                      'rule_id': 'Linking rule: diagnosis_context_window_v1 '
                                                                 '(same encounter, or diagnosis dated 30 '
                                                                 'days before to 7 days after the '
                                                                 'accession).',
                                                      'rule_version': 'Rule version.',
                                                      'same_encounter_ind': 'True when the diagnosis is '
                                                                            "recorded in the accession's "
                                                                            'encounter; false when it is '
                                                                            'linked only by date (30 days '
                                                                            'before to 7 days after the '
                                                                            'accession).',
                                                      'specimen_patient_event_key': 'patient_event_key of '
                                                                                    'the clinical_specimen '
                                                                                    'row of the '
                                                                                    '(alias-resolved) '
                                                                                    'pathology accession.'},
                                          'table': 'Diagnosis context of pathology accessions '
                                                   "(CONTEXT_LINK_V1): the patient's diagnoses in the "
                                                   "accession's encounter or dated 30 days before to 7 days "
                                                   'after it. One row per (accession, diagnosis), keyed to '
                                                   'clinical_specimen and clinical_condition. Context, not '
                                                   'the indication for the test. Rule-based, no manual '
                                                   'curation.'}}
