"""Gold spine.theatre_attendance: 2 dataset(s).

- datasets: _qc_reference_theatre_attendance, reference_theatre_attendance
- moved verbatim from: 1_gold_spine_and_reference:4473, 1_gold_spine_and_reference:4519, 1_gold_spine_and_reference:4524, 1_gold_spine_and_reference:4529, 1_gold_spine_and_reference:4551

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.theatre_attendance ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_THEATRE_ATTENDANCE_SELECT = [
    '`theatre_attendance_key` AS `theatre_attendance_key`',
    '`theatre_case_key` AS `theatre_case_key`',
    '`case_link_status` AS `case_link_status`',
    '`attendee_practitioner_id` AS `attendee_practitioner_id`',
    '`attendee_personnel_id` AS `attendee_personnel_id`',
    '`role_description` AS `role_description`',
    '`signing_attendee_ind` AS `signing_attendee_ind`',
    '`in_datetime` AS `in_datetime`',
    '`in_datetime_quality` AS `in_datetime_quality`',
    '`out_datetime` AS `out_datetime`',
    '`out_datetime_quality` AS `out_datetime_quality`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`surgical_area_desc` AS `surgical_area_desc`',
    '`active_ind` AS `active_ind`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`loaded_at` AS `loaded_at`',
]


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
REFERENCE_THEATRE_ATTENDANCE_PERSON_SCOPE_RULES = {
    "gold.reference.theatre_attendance.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
REFERENCE_THEATRE_ATTENDANCE_ORPHAN_RULES = {
    "gold.reference.theatre_attendance.encounter_id.orphan_nulled": "NOT COALESCE(`_qc_encounter_id_orphan`, FALSE)",
    "gold.reference.theatre_attendance.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._reference_theatre_attendance"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(REFERENCE_THEATRE_ATTENDANCE_PERSON_SCOPE_RULES)
@dp.expect_all(REFERENCE_THEATRE_ATTENDANCE_ORPHAN_RULES)
def _gold_qc_reference_theatre_attendance():
    """Quality-controlled twin of journey_reference.theatre_attendance."""
    # 101,593 rows point at a encounter_id the spine does not have. The pointer is nulled so
    # it cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    # 7 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "reference_theatre_attendance",
        REFERENCE_THEATRE_ATTENDANCE_SELECT,
        fk_columns=["encounter_id", "person_id"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_reference.theatre_attendance"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_theatre_attendance():
    """Contract-v2 public twin of reference_theatre_attendance; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_theatre_attendance")).selectExpr(
        '`theatre_attendance_key` AS `theatre_attendance_key`',
        '`theatre_case_key` AS `theatre_case_key`',
        '`case_link_status` AS `case_link_status`',
        '`attendee_practitioner_id` AS `attendee_practitioner_id`',
        '`attendee_personnel_id` AS `attendee_personnel_id`',
        '`role_description` AS `role_description`',
        '`signing_attendee_ind` AS `signing_attendee_ind`',
        '`in_datetime` AS `in_datetime`',
        '`in_datetime_quality` AS `in_datetime_quality`',
        '`out_datetime` AS `out_datetime`',
        '`out_datetime_quality` AS `out_datetime_quality`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`surgical_area_desc` AS `surgical_area_desc`',
        '`active_ind` AS `active_ind`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'reference_theatre_attendance': GoldRules(
        table='reference_theatre_attendance',
        select=REFERENCE_THEATRE_ATTENDANCE_SELECT,
        person_scope=REFERENCE_THEATRE_ATTENDANCE_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['theatre_attendance_key'], 'support_columns': ['loaded_at']},
        value_rules={'gold.reference.theatre_attendance.attendee_personnel_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                     'column': 'attendee_personnel_id',
                                                                                     'companions': [],
                                                                                     'match': 'TRIM(`attendee_personnel_id`) '
                                                                                              "= '0'",
                                                                                     'table': 'reference_theatre_attendance'},
         'gold.reference.theatre_attendance.attendee_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                        'column': 'attendee_practitioner_id',
                                                                                        'companions': [],
                                                                                        'match': '`attendee_practitioner_id` '
                                                                                                 '= 0',
                                                                                        'table': 'reference_theatre_attendance'}}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_theatre_attendance': {'columns': {'_source_row_id': 'Journey care_participation; retained as '
                                                                    'a source-faithful bronze input or '
                                                                    'Journey standard-block field.',
                                                  '_source_system': 'Value describing source system for the '
                                                                    'theatre attendance record. It is '
                                                                    'produced by the silver transformation '
                                                                    'and has no direct bronze-column lineage '
                                                                    'entry. Whitespace and source sentinel '
                                                                    'text are retained unless the pipeline '
                                                                    'explicitly normalizes them; null means '
                                                                    'no value was supplied.',
                                                  '_source_table': 'Value describing source table for the '
                                                                   'theatre attendance record. It is '
                                                                   'produced by the silver transformation '
                                                                   'and has no direct bronze-column lineage '
                                                                   'entry. Whitespace and source sentinel '
                                                                   'text are retained unless the pipeline '
                                                                   'explicitly normalizes them; null means '
                                                                   'no value was supplied.',
                                                  'active_ind': 'Journey care_participation; retained as a '
                                                                'source-faithful bronze input or Journey '
                                                                'standard-block field.',
                                                  'attendee_personnel_id': 'Journey care_participation '
                                                                           'practitioner FK; no practitioner '
                                                                           'attributes are re-landed.',
                                                  'attendee_practitioner_id': 'Native Millennium personnel '
                                                                              'PERSON_ID as BIGINT for the '
                                                                              'attendee.',
                                                  'case_link_status': 'S14 theatre feeder and Journey '
                                                                      'standard block; retained as a '
                                                                      'source-faithful bronze input or '
                                                                      'Journey standard-block field.',
                                                  'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT.',
                                                  'in_datetime': 'Journey care_participation; retained as a '
                                                                 'source-faithful bronze input or Journey '
                                                                 'standard-block field.',
                                                  'in_datetime_quality': 'Journey care_participation; '
                                                                         'retained as a source-faithful '
                                                                         'bronze input or Journey '
                                                                         'standard-block field.',
                                                  'loaded_at': 'map_theatre_case_attendance.ADC_UPDT carried '
                                                               'unchanged for the attendance row. The '
                                                               'parent-case existence join adds no '
                                                               'timestamp; this is bronze ingestion '
                                                               'provenance, not attendee in/out time or '
                                                               'Silver refresh time.',
                                                  'out_datetime': 'Journey care_participation; retained as a '
                                                                  'source-faithful bronze input or Journey '
                                                                  'standard-block field.',
                                                  'out_datetime_quality': 'Journey care_participation; '
                                                                          'retained as a source-faithful '
                                                                          'bronze input or Journey '
                                                                          'standard-block field.',
                                                  'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                                  'role_description': 'Journey care_participation; retained '
                                                                      'as a source-faithful bronze input or '
                                                                      'Journey standard-block field.',
                                                  'signing_attendee_ind': 'Journey care_participation; '
                                                                          'retained as a source-faithful '
                                                                          'bronze input or Journey '
                                                                          'standard-block field.',
                                                  'surgical_area_desc': 'Journey care_participation; '
                                                                        'retained as a source-faithful '
                                                                        'bronze input or Journey '
                                                                        'standard-block field.',
                                                  'theatre_attendance_key': 'Deterministic SHA-256 key for '
                                                                            'the theatre attendance row.',
                                                  'theatre_case_key': 'Source-preserved theatre case key.'},
                                      'quantity_override': True,
                                      'table': 'Internal quality-controlled twin of '
                                               'reference_theatre_attendance: the Gold repairs, nulls and '
                                               'expectations are applied here and the published Gold MV '
                                               'reads this object. Materialized rather than temporary so the '
                                               'published MV can refresh incrementally.'},
 'reference_theatre_attendance': {'columns': {'_source_row_id': 'Journey care_participation; retained as a '
                                                                'source-faithful bronze input or Journey '
                                                                'standard-block field.',
                                              '_source_system': 'Value describing source system for the '
                                                                'theatre attendance record. It is produced '
                                                                'by the silver transformation and has no '
                                                                'direct bronze-column lineage entry. '
                                                                'Whitespace and source sentinel text are '
                                                                'retained unless the pipeline explicitly '
                                                                'normalizes them; null means no value was '
                                                                'supplied.',
                                              '_source_table': 'Value describing source table for the '
                                                               'theatre attendance record. It is produced by '
                                                               'the silver transformation and has no direct '
                                                               'bronze-column lineage entry. Whitespace and '
                                                               'source sentinel text are retained unless the '
                                                               'pipeline explicitly normalizes them; null '
                                                               'means no value was supplied.',
                                              'active_ind': 'Journey care_participation; retained as a '
                                                            'source-faithful bronze input or Journey '
                                                            'standard-block field.',
                                              'attendee_personnel_id': 'Journey care_participation '
                                                                       'practitioner FK; no practitioner '
                                                                       'attributes are re-landed.',
                                              'attendee_practitioner_id': 'Native Millennium personnel '
                                                                          'PERSON_ID as BIGINT for the '
                                                                          'attendee.',
                                              'case_link_status': 'S14 theatre feeder and Journey standard '
                                                                  'block; retained as a source-faithful '
                                                                  'bronze input or Journey standard-block '
                                                                  'field.',
                                              'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT.',
                                              'in_datetime': 'Journey care_participation; retained as a '
                                                             'source-faithful bronze input or Journey '
                                                             'standard-block field.',
                                              'in_datetime_quality': 'Journey care_participation; retained '
                                                                     'as a source-faithful bronze input or '
                                                                     'Journey standard-block field.',
                                              'loaded_at': 'map_theatre_case_attendance.ADC_UPDT carried '
                                                           'unchanged for the attendance row. The '
                                                           'parent-case existence join adds no timestamp; '
                                                           'this is bronze ingestion provenance, not '
                                                           'attendee in/out time or Silver refresh time.',
                                              'out_datetime': 'Journey care_participation; retained as a '
                                                              'source-faithful bronze input or Journey '
                                                              'standard-block field.',
                                              'out_datetime_quality': 'Journey care_participation; retained '
                                                                      'as a source-faithful bronze input or '
                                                                      'Journey standard-block field.',
                                              'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                              'role_description': 'Journey care_participation; retained as a '
                                                                  'source-faithful bronze input or Journey '
                                                                  'standard-block field.',
                                              'signing_attendee_ind': 'Journey care_participation; retained '
                                                                      'as a source-faithful bronze input or '
                                                                      'Journey standard-block field.',
                                              'surgical_area_desc': 'Journey care_participation; retained as '
                                                                    'a source-faithful bronze input or '
                                                                    'Journey standard-block field.',
                                              'theatre_attendance_key': 'Deterministic SHA-256 key for the '
                                                                        'theatre attendance row.',
                                              'theatre_case_key': 'Source-preserved theatre case key.'},
                                  'quantity_override': False,
                                  'table': 'One SurgiNet theatre attendance row with parent-case linkage '
                                           'evidence. Gold QC twin of the silver product: 2 columns are '
                                           'repaired or nulled, 0 check(s) are advisory. Each rule states '
                                           'its reason in the pipeline notebook, and Lakeflow expectation '
                                           'metrics report what every rule matched on each update. Coverage: '
                                           'The signing attendee indicator is constant at source (0 on every '
                                           'Millennium attendance row), so it is published as recorded and '
                                           'carries no information; complete-through month per release: '
                                           '6_mgmt.data_quality.release_finding (finding_code '
                                           'F2_complete_through).'}}
