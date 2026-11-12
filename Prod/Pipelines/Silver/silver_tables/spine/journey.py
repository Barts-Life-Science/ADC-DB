"""Silver spine.journey: 1 dataset(s).

- datasets: spine_journey
- moved verbatim from: 4_silver_care_pathways:507, 4_silver_care_pathways:511, 4_silver_care_pathways:526

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n
from silver_tables._common.care_pathways import _lifecycle_source_journey


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


JOURNEY_LIFECYCLE_FIELDS = [
    'load_batch_id',
]


JOURNEY_RETIRED_COLUMNS = [

]


@materialized_view(
    name=_n("journey_spine.journey"),
    cluster_by=["person_id", "period_start"],
    refresh_policy="auto",
)
def journey():
    # Build the declared dataset: Deterministic longitudinal journeys; pregnancy-v1 supplies
    # parent and trimester rows.
    return _lifecycle_source_journey().drop(*JOURNEY_LIFECYCLE_FIELDS, *JOURNEY_RETIRED_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'spine_journey': {'columns': {'construction_rule': 'Governed constructor name.',
                               'construction_version': 'Governed constructor version.',
                               'defining_code': 'Constructor-defining concept code.',
                               'defining_coding_system': 'Coding system for the constructor-defining '
                                                         'concept.',
                               'defining_display': 'Constructor-defining concept display.',
                               'journey_key': 'Deterministic SHA-256 journey key.',
                               'journey_type_code': 'Controlled journey type.',
                               'journey_type_display': 'Human-readable journey type.',
                               'loaded_at': 'Greatest of ADC_UPDT from the selected map_mat_pregnancy row '
                                            'and maximum map_mat_birth.ADC_UPDT across all rows in the '
                                            'Pregnancy_ID group. The selected pregnancy row, not a maximum '
                                            'over all pregnancy versions, contributes its clock. Pregnancy '
                                            'and trimester rows carry the same value; clinical period '
                                            'boundaries and the current Silver refresh time are not used.',
                               'outcome_code': 'Source pregnancy outcome code where supplied.',
                               'outcome_display': 'Source pregnancy outcome display where supplied.',
                               'parent_journey_key': 'Deterministic SHA-256 parent journey key for governed '
                                                     'nesting.',
                               'period_end': 'Journey end. Pregnancy (PREGNANCY_DATES_V2): the last birth; '
                                             'else maternity service discharge, else the expected delivery '
                                             'date, each only within 0-308 days after period_start. If the '
                                             'span still exceeds 308 days (or is negative) the less-trusted '
                                             'side is NULLed, never both. See period_end_basis.',
                               'period_end_basis': 'Rule that set period_end (PREGNANCY_DATES_V2). Pregnancy '
                                                   'rows: last_birth, maternity_service_discharge (only '
                                                   'within 0-308 days after the start), '
                                                   'expected_delivery_date (only within 0-308 days after the '
                                                   'start), or nulled_span_guard. Trimester rows: '
                                                   'trimester_nominal for the fixed 98-day trimester ends, '
                                                   "otherwise their pregnancy's end basis. NULL when no "
                                                   'source date exists or none passed its rule.',
                               'period_start': 'Journey start. Pregnancy (PREGNANCY_DATES_V2): last '
                                               'menstrual period, else expected delivery date - 280 days; '
                                               'the latest birth minus its plausible recorded gestation '
                                               'replaces either when missing or more than 14 days apart; '
                                               'first contact / first antenatal appointment only as a last '
                                               'resort within 308 days before period_end. See '
                                               'period_start_basis.',
                               'period_start_basis': 'Rule that set period_start (PREGNANCY_DATES_V2). '
                                                     'Pregnancy rows: birth_minus_gestation (latest birth '
                                                     'with a plausible recorded gestation of 140-308 days, '
                                                     'used when LMP/EDD dating is missing or differs from it '
                                                     'by more than 14 days), last_menstrual_period, '
                                                     'expected_delivery_date_minus_280, first_contact or '
                                                     'first_antenatal_appointment (last resort, only within '
                                                     '308 days before period_end), or nulled_span_guard (the '
                                                     'span still exceeded 308 days or was negative and this '
                                                     'was the less-trusted side). Trimester rows carry their '
                                                     "pregnancy's start basis. NULL when no source date "
                                                     'exists.',
                               'person_id': 'Millennium PERSON_ID; native primary-subject foreign key when '
                                            'resolved.',
                               'source_journey_identifier': 'Verbatim source pregnancy identifier.',
                               'source_row_id': 'Stable constructor source-row identifier.',
                               'source_system': 'Source-system label for these maternity-derived records: '
                                                'millennium-maternity.',
                               'source_table': 'Fully qualified configured pregnancy source table.',
                               'source_update_timestamp': "Greatest of the selected map_mat_pregnancy row's "
                                                          'MAT_RECORD_UPDATED_DT, MSDS_RECORD_UPDATED_DT and '
                                                          'BIRTH_ADC_UPDT, and the maximum '
                                                          'map_mat_birth.BirthSourceRecordUpdatedDateTime '
                                                          'across its Pregnancy_ID group. Pregnancy rows are '
                                                          'selected by the existing live/load/person/hash '
                                                          'rank before this join. The same value is carried '
                                                          'to the pregnancy and all trimester rows; it is '
                                                          'not just the birth timestamp or the current '
                                                          'Silver refresh time.',
                               'status_code': 'Normalized journey lifecycle state.',
                               'subject_id_system': 'Identifier system used for subject_key.',
                               'subject_key': 'Deterministic SHA-256 over the strongest source identifier. '
                                              'Not salted, not secret; a stable join key across feeds where '
                                              'person_id is unresolved.'},
                   'table': 'Deterministic longitudinal journeys; pregnancy-v1 supplies parent and trimester '
                            'rows.'}}
