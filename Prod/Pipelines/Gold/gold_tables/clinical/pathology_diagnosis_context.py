"""Gold clinical.pathology_diagnosis_context: 2 dataset(s).

- datasets: _qc_clinical_pathology_diagnosis_context, clinical_pathology_diagnosis_context
- moved verbatim from: 2_gold_clinical_events:7866, 2_gold_clinical_events:7872, 2_gold_clinical_events:7907, 2_gold_clinical_events:7913, 2_gold_clinical_events:7933

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from gold_core.gold_core import _n, _src, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.pathology_diagnosis_context (CONTEXT_LINK_V1, BRONZE_FIX_20261025D) ====

# Child of both clinical_specimen and clinical_condition: a pair is admitted only when Gold admitted both parents.
CLINICAL_PATHOLOGY_DIAGNOSIS_CONTEXT_COLUMNS = [
    "pathology_diagnosis_context_key", "specimen_patient_event_key", "condition_patient_event_key",
    "accession_identifier", "diagnosis_id", "person_id", "accession_encounter_id", "diagnosis_encounter_id",
    "same_encounter_ind", "day_offset", "accession_datetime", "diagnosis_datetime", "rule_id", "rule_version",
    "loaded_at",
]


CLINICAL_PATHOLOGY_DIAGNOSIS_CONTEXT_SELECT = [  # INSR_W3 W3.06: expanded from CLINICAL_PATHOLOGY_DIAGNOSIS_CONTEXT_COLUMNS so each column's transform is visible
    '`pathology_diagnosis_context_key` AS `pathology_diagnosis_context_key`',
    '`specimen_patient_event_key` AS `specimen_patient_event_key`',
    '`condition_patient_event_key` AS `condition_patient_event_key`',
    '`accession_identifier` AS `accession_identifier`',
    '`diagnosis_id` AS `diagnosis_id`',
    '`person_id` AS `person_id`',
    '`accession_encounter_id` AS `accession_encounter_id`',
    '`diagnosis_encounter_id` AS `diagnosis_encounter_id`',
    '`same_encounter_ind` AS `same_encounter_ind`',
    '`day_offset` AS `day_offset`',
    # INSR_W3 W3.06: Gold: NULL where a pinned LIMS placeholder day (P3e).
    "CASE WHEN CAST(`accession_datetime` AS DATE) IN (DATE'1972-01-19', DATE'1972-03-25') THEN NULL ELSE `accession_datetime` END AS `accession_datetime`",
    '`diagnosis_datetime` AS `diagnosis_datetime`',
    '`rule_id` AS `rule_id`',
    '`rule_version` AS `rule_version`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_PATHOLOGY_DIAGNOSIS_CONTEXT_MANDATORY_RULES = {
    # The pair belongs in Gold only when both its specimen and its condition were admitted to Gold.
    "gold.clinical_pathology_diagnosis_context.specimen_admitted": "COALESCE(__gold_specimen_present, FALSE)",
    "gold.clinical_pathology_diagnosis_context.condition_admitted": "COALESCE(__gold_condition_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_pathology_diagnosis_context"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_PATHOLOGY_DIAGNOSIS_CONTEXT_MANDATORY_RULES)
def _gold_qc_clinical_pathology_diagnosis_context():
    """CONTEXT_LINK_V1 Gold child of clinical_specimen and clinical_condition; parent-rejected pairs are excluded."""
    specimens = (spark.read.table(_n("gold_clinical.specimen"))
                 .select(F.col("patient_event_key").alias("specimen_patient_event_key")).dropDuplicates()
                 .withColumn("__gold_specimen_present", F.lit(True)))
    conditions = (spark.read.table(_n("gold_clinical.condition"))
                  .select(F.col("patient_event_key").alias("condition_patient_event_key")).dropDuplicates()
                  .withColumn("__gold_condition_present", F.lit(True)))
    child = spark.read.table(_src("clinical_pathology_diagnosis_context"))
    df = (child.join(specimens, "specimen_patient_event_key", "left")
          .join(conditions, "condition_patient_event_key", "left")
          .selectExpr(*CLINICAL_PATHOLOGY_DIAGNOSIS_CONTEXT_SELECT, "__gold_specimen_present", "__gold_condition_present"))
    return df


@dp.materialized_view(
    name=_n("gold_clinical.pathology_diagnosis_context"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_pathology_diagnosis_context():
    """Publish clinical_pathology_diagnosis_context without the internal parent-admission markers."""
    return (spark.read.table(_n("gold_qc._clinical_pathology_diagnosis_context"))
            .drop("__gold_specimen_present", "__gold_condition_present"))


finalise()


COMMENTS = {'_qc_clinical_pathology_diagnosis_context': {'columns': {'accession_datetime': 'Accession date (sample, '
                                                                                'else request, else report '
                                                                                'datetime) of the source row '
                                                                                'that gave the strongest '
                                                                                'evidence (NULL outside '
                                                                                '1950-2099).',
                                                          'accession_encounter_id': 'Millennium ENCNTR_ID of '
                                                                                    'the accession source '
                                                                                    'row that gave the '
                                                                                    'strongest evidence; '
                                                                                    'NULL for LIMS-only '
                                                                                    'rows.',
                                                          'accession_identifier': 'Alias-resolved pathology '
                                                                                  'accession identifier (the '
                                                                                  'clinical_specimen '
                                                                                  'accession).',
                                                          'condition_patient_event_key': 'patient_event_key '
                                                                                         'of the '
                                                                                         'clinical_condition '
                                                                                         'row of the '
                                                                                         'diagnosis; join '
                                                                                         'clinical_condition '
                                                                                         'for its code, '
                                                                                         'display and '
                                                                                         'status.',
                                                          'day_offset': 'Days from accession_datetime to '
                                                                        'diagnosis_datetime by calendar date '
                                                                        '(negative = diagnosis before the '
                                                                        'accession); NULL when either date '
                                                                        'is missing.',
                                                          'diagnosis_datetime': 'Diagnosis date: DIAG_DT_TM, '
                                                                                'else ASSERTED_DT_TM, else '
                                                                                'BEG_EFFECTIVE_DT_TM (NULL '
                                                                                'outside 1950-2099).',
                                                          'diagnosis_encounter_id': 'Millennium ENCNTR_ID of '
                                                                                    'the diagnosis.',
                                                          'diagnosis_id': 'Millennium DIAGNOSIS_ID.',
                                                          'loaded_at': 'ADC_UPDT of the bronze link row.',
                                                          'pathology_diagnosis_context_key': 'Deterministic '
                                                                                             'SHA-256 key: '
                                                                                             "stable_id('pathology_diagnosis_context', "
                                                                                             'bronze '
                                                                                             'pathology_accession_id, '
                                                                                             'diagnosis_id). '
                                                                                             'Primary key: '
                                                                                             'one row per '
                                                                                             '(accession, '
                                                                                             'diagnosis).',
                                                          'person_id': 'Millennium PERSON_ID shared by the '
                                                                       'accession and the diagnosis.',
                                                          'rule_id': 'Linking rule: '
                                                                     'diagnosis_context_window_v1.',
                                                          'rule_version': 'Rule version.',
                                                          'same_encounter_ind': 'True when the diagnosis is '
                                                                                "recorded in the accession's "
                                                                                'encounter; false when it is '
                                                                                'linked only by date (30 '
                                                                                'days before to 7 days after '
                                                                                'the accession).',
                                                          'specimen_patient_event_key': 'patient_event_key '
                                                                                        'of the '
                                                                                        'clinical_specimen '
                                                                                        'row of the '
                                                                                        'pathology '
                                                                                        'accession.'},
                                              'quantity_override': True,
                                              'table': 'Internal QC of clinical_pathology_diagnosis_context; '
                                                       'counts pairs whose specimen or condition Gold '
                                                       'rejected.'},
 'clinical_pathology_diagnosis_context': {'columns': {},
                                          'quantity_override': False,
                                          'table': "Diagnosis context of pathology accessions: the patient's "
                                                   "diagnoses in the accession's encounter or dated 30 days "
                                                   'before to 7 days after it, one row per (accession, '
                                                   'diagnosis), keyed to clinical_specimen and '
                                                   'clinical_condition. Context, not the indication for the '
                                                   'test. Rule-based (diagnosis_context_window_v1), no '
                                                   'manual curation.'}}
