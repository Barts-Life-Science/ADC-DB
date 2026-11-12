"""Silver spine.journey_participant: 1 dataset(s).

- datasets: spine_journey_participant
- moved verbatim from: 4_silver_care_pathways:579, 4_silver_care_pathways:687

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n
from silver_tables._common.care_pathways import _lifecycle_source_journey_participant


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
JOURNEY_PARTICIPANT_PUBLIC_COLUMNS = [
    'journey_participant_key',
    'journey_key',
    'subject_key',
    'subject_id_system',
    'person_id',
    'role_code',
    'role_display',
    'valid_from',
    'valid_to',
    'construction_rule',
    'construction_version',
    'source_system',
    'source_table',
    'source_row_id',
    'record_status',
    'source_update_timestamp',
    'loaded_at',
]


@materialized_view(
    name=_n("journey_spine.journey_participant"),
    refresh_policy="auto",
)
def journey_participant():
    # Build the declared dataset: Journey participant roles with subject-key support for
    # unresolved babies.
    return _lifecycle_source_journey_participant().select(*JOURNEY_PARTICIPANT_PUBLIC_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'spine_journey_participant': {'columns': {'construction_rule': 'Governed participant-construction rule.',
                                           'construction_version': 'Governed participant-construction '
                                                                   'version.',
                                           'journey_key': 'Deterministic SHA-256 journey key.',
                                           'journey_participant_key': 'Deterministic SHA-256 '
                                                                      'journey-participant key.',
                                           'loaded_at': 'Mother participants inherit the pregnancy/trimester '
                                                        'journey load clock: greatest of selected '
                                                        'map_mat_pregnancy.ADC_UPDT and maximum '
                                                        'map_mat_birth.ADC_UPDT for the Pregnancy_ID group. '
                                                        'Baby participants use only their own '
                                                        'map_mat_birth.ADC_UPDT. The two arms are unioned '
                                                        'without a mother/baby clock maximum or substitution '
                                                        'of clinical validity times or the current Silver '
                                                        'refresh time.',
                                           'person_id': 'Millennium PERSON_ID; native participant foreign '
                                                        'key when resolved.',
                                           'record_status': 'Mother rows are superseded only when their '
                                                            'constructed pregnancy/trimester journey '
                                                            'status_code is superseded; completed and all '
                                                            'other journey statuses map to active. Baby rows '
                                                            'are superseded when '
                                                            'map_mat_birth.PregnancySource_DELETE_IND cast '
                                                            'to LONG is nonzero, otherwise active; null '
                                                            'defaults zero. Neither arm emits retracted, and '
                                                            'the union does not reconcile statuses between '
                                                            'mother and baby.',
                                           'role_code': 'Controlled role in the journey.',
                                           'role_display': 'Human-readable participant role.',
                                           'source_row_id': 'Stable source-row identifier supporting the '
                                                            'role.',
                                           'source_system': 'Source-system label for these maternity-derived '
                                                            'records: millennium-maternity.',
                                           'source_table': 'Fully qualified configured bronze source table.',
                                           'source_update_timestamp': 'Mother participants inherit the '
                                                                      'pregnancy/trimester journey clock: '
                                                                      'greatest of the selected pregnancy '
                                                                      "row's MAT_RECORD_UPDATED_DT, "
                                                                      'MSDS_RECORD_UPDATED_DT and '
                                                                      'BIRTH_ADC_UPDT, plus maximum '
                                                                      'BirthSourceRecordUpdatedDateTime '
                                                                      'across its map_mat_birth Pregnancy_ID '
                                                                      'group. Baby participants use greatest '
                                                                      'of their own map_mat_birth '
                                                                      'BirthSourceRecordUpdatedDateTime, '
                                                                      'PregnancySourceRecordUpdatedDateTime '
                                                                      'and NNUSourceLastUpdate. These clocks '
                                                                      'are carried per arm without a '
                                                                      'cross-arm maximum.',
                                           'subject_id_system': 'Identifier system used for participant '
                                                                'subject_key.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'valid_from': 'Role validity start.',
                                           'valid_to': 'Role validity end.'},
                               'table': 'Journey participant roles with subject-key support for unresolved '
                                        'babies.'}}
