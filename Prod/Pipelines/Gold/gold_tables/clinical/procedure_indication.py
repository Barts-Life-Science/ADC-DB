"""Gold clinical.procedure_indication: 2 dataset(s).

- datasets: _qc_clinical_procedure_indication, clinical_procedure_indication
- moved verbatim from: 2_gold_clinical_events:7792, 2_gold_clinical_events:7799, 2_gold_clinical_events:7824, 2_gold_clinical_events:7831, 2_gold_clinical_events:7851

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from gold_core.gold_core import _n, _src, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.procedure_indication (PROCEDURE_INDICATION_V1) ====

# Child of both clinical_procedure and clinical_condition: a candidate is admitted only when Gold admitted both parents.
CLINICAL_PROCEDURE_INDICATION_COLUMNS = [
    "procedure_indication_key",  # SILVER_RELEASE_20261025 item 3
    "procedure_patient_event_key", "procedure_source_object", "procedure_id", "implant_event_id", "implant_sequence",
    "condition_patient_event_key", "diagnosis_id", "problem_id", "person_id", "encounter_id",
    "procedure_site_code", "procedure_site_display", "condition_site_code", "condition_site_display",
    "match_type", "match_levels", "diagnosis_priority", "indication_rank", "rule_id", "rule_version", "loaded_at",
]


CLINICAL_PROCEDURE_INDICATION_SELECT = [f"`{c}` AS `{c}`" for c in CLINICAL_PROCEDURE_INDICATION_COLUMNS]


CLINICAL_PROCEDURE_INDICATION_MANDATORY_RULES = {
    # The candidate belongs in Gold only when both its procedure and its condition were admitted to Gold.
    # The left joins keep rejected rows long enough for Lakeflow to count them.
    "gold.clinical_procedure_indication.procedure_admitted": "COALESCE(__gold_procedure_present, FALSE)",
    "gold.clinical_procedure_indication.condition_admitted": "COALESCE(__gold_condition_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_procedure_indication"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_PROCEDURE_INDICATION_MANDATORY_RULES)
def _gold_qc_clinical_procedure_indication():
    """PROCEDURE_INDICATION_V1 Gold child of clinical_procedure and clinical_condition; parent-rejected rows are excluded."""
    procedures = (spark.read.table(_n("gold_clinical.procedure"))
                  .select(F.col("patient_event_key").alias("procedure_patient_event_key")).dropDuplicates()
                  .withColumn("__gold_procedure_present", F.lit(True)))
    conditions = (spark.read.table(_n("gold_clinical.condition"))
                  .select(F.col("patient_event_key").alias("condition_patient_event_key")).dropDuplicates()
                  .withColumn("__gold_condition_present", F.lit(True)))
    child = spark.read.table(_src("clinical_procedure_indication"))
    df = (child.join(procedures, "procedure_patient_event_key", "left")
          .join(conditions, "condition_patient_event_key", "left")
          .selectExpr(*CLINICAL_PROCEDURE_INDICATION_SELECT, "__gold_procedure_present", "__gold_condition_present"))
    return df


@dp.materialized_view(
    name=_n("gold_clinical.procedure_indication"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_procedure_indication():
    """Publish clinical_procedure_indication without the internal parent-admission markers."""
    return (spark.read.table(_n("gold_qc._clinical_procedure_indication"))
            .drop("__gold_procedure_present", "__gold_condition_present"))


finalise()


COMMENTS = {'_qc_clinical_procedure_indication': {'columns': {'condition_patient_event_key': 'patient_event_key of the '
                                                                                  'clinical_condition row '
                                                                                  'proposed as the '
                                                                                  'indication.',
                                                   'condition_site_code': 'SNOMED finding site of the '
                                                                          "condition's standard concept "
                                                                          "('Has finding site').",
                                                   'condition_site_display': 'SNOMED name of '
                                                                             'condition_site_code.',
                                                   'diagnosis_id': 'Millennium DIAGNOSIS_ID of the condition '
                                                                   'when it is a diagnosis.',
                                                   'diagnosis_priority': 'Millennium DIAG_PRIORITY of the '
                                                                         'diagnosis as recorded: 1 = coded '
                                                                         'primary, 2+ = secondary positions, '
                                                                         '0 = not ranked (Discharge/Working '
                                                                         'diagnoses such as ED); NULL for '
                                                                         'problems.',
                                                   'encounter_id': 'Millennium ENCNTR_ID shared by the '
                                                                   'procedure and the condition.',
                                                   'implant_event_id': 'Millennium implant EVENT_ID for '
                                                                       'implant rows.',
                                                   'implant_sequence': 'Millennium IMPLANT_SEQUENCE for '
                                                                       'implant rows.',
                                                   'indication_rank': '1 = best candidate for this '
                                                                      'procedure: fewest match_levels, then '
                                                                      'lowest positive diagnosis_priority (0 '
                                                                      'and NULL last), then '
                                                                      'condition_patient_event_key.',
                                                   'loaded_at': 'Greatest load timestamp of the procedure '
                                                                'and condition rows.',
                                                   'match_levels': 'is-a levels between the two sites (0 = '
                                                                   'exact; at most 2).',
                                                   'match_type': 'exact (same structure), '
                                                                 'condition_site_narrower (the condition '
                                                                 'site is a descendant of the procedure '
                                                                 'site) or condition_site_broader (an '
                                                                 'ancestor).',
                                                   'person_id': 'Millennium PERSON_ID.',
                                                   'problem_id': 'Millennium PROBLEM_ID of the condition '
                                                                 'when it is a problem.',
                                                   'procedure_id': 'Millennium PROCEDURE_ID of the operation '
                                                                   'row (NULL for implant rows).',
                                                   'procedure_indication_key': 'Deterministic SHA-256 key of '
                                                                               'the candidate: '
                                                                               "stable_id('procedure_indication', "
                                                                               'procedure_patient_event_key, '
                                                                               'condition_patient_event_key). '
                                                                               'Primary key: the table holds '
                                                                               'one row per (procedure, '
                                                                               'condition) pair.',
                                                   'procedure_patient_event_key': 'patient_event_key of the '
                                                                                  'clinical_procedure row: '
                                                                                  'an OPCS-4 operation, or '
                                                                                  'an implant linked to '
                                                                                  'exactly one operation.',
                                                   'procedure_site_code': 'SNOMED body structure of the '
                                                                          'operation (an OPCS-4 site code of '
                                                                          'the coded operation via OMOP '
                                                                          "'Maps to').",
                                                   'procedure_site_display': 'SNOMED name of '
                                                                             'procedure_site_code.',
                                                   'procedure_source_object': 'source_object of the '
                                                                              'procedure row: '
                                                                              'millennium_procedure or '
                                                                              'implant.',
                                                   'rule_id': 'Matching rule: '
                                                              'procedure_site_finding_site_v1.',
                                                   'rule_version': 'Rule version.'},
                                       'quantity_override': True,
                                       'table': 'Internal QC of clinical_procedure_indication; counts '
                                                'candidates whose procedure or condition Gold rejected.'},
 'clinical_procedure_indication': {'columns': {},
                                   'quantity_override': False,
                                   'table': 'Candidate indications for coded operations and singly-linked '
                                            'implants: same-encounter conditions whose SNOMED finding site '
                                            "matches the operation's coded body site exactly or within two "
                                            'is-a levels; indication_rank 1 = closest anatomy then lowest '
                                            'diagnosis priority. Rule-based '
                                            '(procedure_site_finding_site_v1), no manual curation.'}}
