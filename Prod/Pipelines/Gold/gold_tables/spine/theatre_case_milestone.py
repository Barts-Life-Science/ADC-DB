"""Gold spine.theatre_case_milestone: 2 dataset(s).

- datasets: _qc_reference_theatre_case_milestone, reference_theatre_case_milestone
- moved verbatim from: 1_gold_spine_and_reference:4595, 1_gold_spine_and_reference:4633, 1_gold_spine_and_reference:4638, 1_gold_spine_and_reference:4643, 1_gold_spine_and_reference:4665

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.theatre_case_milestone ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_THEATRE_CASE_MILESTONE_SELECT = [
    '`theatre_case_milestone_key` AS `theatre_case_milestone_key`',
    '`theatre_case_key` AS `theatre_case_key`',
    '`case_link_status` AS `case_link_status`',
    "CASE WHEN UPPER(TRIM(CAST(`task_assay_code` AS STRING))) = '0' THEN NULL ELSE `task_assay_code` END AS `task_assay_code`",
    '`task_assay_description` AS `task_assay_description`',
    '`stage_description` AS `stage_description`',
    '`case_time_datetime` AS `case_time_datetime`',
    '`case_time_quality` AS `case_time_quality`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`active_ind` AS `active_ind`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`loaded_at` AS `loaded_at`',
]


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
REFERENCE_THEATRE_CASE_MILESTONE_PERSON_SCOPE_RULES = {
    "gold.reference.theatre_case_milestone.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
REFERENCE_THEATRE_CASE_MILESTONE_ORPHAN_RULES = {
    "gold.reference.theatre_case_milestone.encounter_id.orphan_nulled": "NOT COALESCE(`_qc_encounter_id_orphan`, FALSE)",
    "gold.reference.theatre_case_milestone.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._reference_theatre_case_milestone"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(REFERENCE_THEATRE_CASE_MILESTONE_PERSON_SCOPE_RULES)
@dp.expect_all(REFERENCE_THEATRE_CASE_MILESTONE_ORPHAN_RULES)
def _gold_qc_reference_theatre_case_milestone():
    """Quality-controlled twin of journey_reference.theatre_case_milestone."""
    # 122,160 rows point at a encounter_id the spine does not have. The pointer is nulled so
    # it cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    # 8 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "reference_theatre_case_milestone",
        REFERENCE_THEATRE_CASE_MILESTONE_SELECT,
        fk_columns=["encounter_id", "person_id"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_reference.theatre_case_milestone"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_theatre_case_milestone():
    """Contract-v2 public twin of reference_theatre_case_milestone; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_theatre_case_milestone")).selectExpr(
        '`theatre_case_milestone_key` AS `theatre_case_milestone_key`',
        '`theatre_case_key` AS `theatre_case_key`',
        '`case_link_status` AS `case_link_status`',
        '`task_assay_code` AS `task_assay_code`',
        '`task_assay_description` AS `task_assay_description`',
        '`stage_description` AS `stage_description`',
        '`case_time_datetime` AS `case_time_datetime`',
        '`case_time_quality` AS `case_time_quality`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`active_ind` AS `active_ind`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'reference_theatre_case_milestone': GoldRules(
        table='reference_theatre_case_milestone',
        select=REFERENCE_THEATRE_CASE_MILESTONE_SELECT,
        person_scope=REFERENCE_THEATRE_CASE_MILESTONE_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['theatre_case_milestone_key'], 'support_columns': ['loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_theatre_case_milestone': {'columns': {'_source_row_id': 'S14 theatre activity; retained as a '
                                                                        'source-faithful bronze input or '
                                                                        'Journey standard-block field.',
                                                      '_source_system': 'Value describing source system for '
                                                                        'the theatre case milestone record. '
                                                                        'It is produced by the silver '
                                                                        'transformation and has no direct '
                                                                        'bronze-column lineage entry. '
                                                                        'Whitespace and source sentinel text '
                                                                        'are retained unless the pipeline '
                                                                        'explicitly normalizes them; null '
                                                                        'means no value was supplied.',
                                                      '_source_table': 'Value describing source table for '
                                                                       'the theatre case milestone record. '
                                                                       'It is produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. '
                                                                       'Whitespace and source sentinel text '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly normalizes them; null '
                                                                       'means no value was supplied.',
                                                      'active_ind': 'S14 theatre activity; retained as a '
                                                                    'source-faithful bronze input or Journey '
                                                                    'standard-block field.',
                                                      'case_link_status': 'S14 theatre feeder and Journey '
                                                                          'standard block; retained as a '
                                                                          'source-faithful bronze input or '
                                                                          'Journey standard-block field.',
                                                      'case_time_datetime': 'S14 theatre performed-milestone '
                                                                            'time after deterministic '
                                                                            'quality bounds.',
                                                      'case_time_quality': 'S14 theatre activity; retained '
                                                                           'as a source-faithful bronze '
                                                                           'input or Journey standard-block '
                                                                           'field.',
                                                      'encounter_id': 'Native Millennium ENCNTR_ID as '
                                                                      'BIGINT.',
                                                      'loaded_at': 'map_theatre_case_times.ADC_UPDT carried '
                                                                   'unchanged for the milestone row. The '
                                                                   'parent-case existence join adds no '
                                                                   'timestamp; this is bronze ingestion '
                                                                   'provenance, not CASE_TIME_DT_TM or '
                                                                   'Silver refresh time.',
                                                      'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                                      'stage_description': 'S14 theatre activity; retained '
                                                                           'as a source-faithful bronze '
                                                                           'input or Journey standard-block '
                                                                           'field.',
                                                      'task_assay_code': 'S14 theatre activity; retained as '
                                                                         'a source-faithful bronze input or '
                                                                         'Journey standard-block field.',
                                                      'task_assay_description': 'S14 theatre activity; '
                                                                                'retained as a '
                                                                                'source-faithful bronze '
                                                                                'input or Journey '
                                                                                'standard-block field.',
                                                      'theatre_case_key': 'Source-preserved theatre case '
                                                                          'key.',
                                                      'theatre_case_milestone_key': 'Deterministic SHA-256 '
                                                                                    'key for the theatre '
                                                                                    'milestone row.'},
                                          'quantity_override': True,
                                          'table': 'Internal quality-controlled twin of '
                                                   'reference_theatre_case_milestone: the Gold repairs, '
                                                   'nulls and expectations are applied here and the '
                                                   'published Gold MV reads this object. Materialized rather '
                                                   'than temporary so the published MV can refresh '
                                                   'incrementally.'},
 'reference_theatre_case_milestone': {'columns': {'_source_row_id': 'S14 theatre activity; retained as a '
                                                                    'source-faithful bronze input or Journey '
                                                                    'standard-block field.',
                                                  '_source_system': 'Value describing source system for the '
                                                                    'theatre case milestone record. It is '
                                                                    'produced by the silver transformation '
                                                                    'and has no direct bronze-column lineage '
                                                                    'entry. Whitespace and source sentinel '
                                                                    'text are retained unless the pipeline '
                                                                    'explicitly normalizes them; null means '
                                                                    'no value was supplied.',
                                                  '_source_table': 'Value describing source table for the '
                                                                   'theatre case milestone record. It is '
                                                                   'produced by the silver transformation '
                                                                   'and has no direct bronze-column lineage '
                                                                   'entry. Whitespace and source sentinel '
                                                                   'text are retained unless the pipeline '
                                                                   'explicitly normalizes them; null means '
                                                                   'no value was supplied.',
                                                  'active_ind': 'S14 theatre activity; retained as a '
                                                                'source-faithful bronze input or Journey '
                                                                'standard-block field.',
                                                  'case_link_status': 'S14 theatre feeder and Journey '
                                                                      'standard block; retained as a '
                                                                      'source-faithful bronze input or '
                                                                      'Journey standard-block field.',
                                                  'case_time_datetime': 'S14 theatre performed-milestone '
                                                                        'time after deterministic quality '
                                                                        'bounds.',
                                                  'case_time_quality': 'S14 theatre activity; retained as a '
                                                                       'source-faithful bronze input or '
                                                                       'Journey standard-block field.',
                                                  'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT.',
                                                  'loaded_at': 'map_theatre_case_times.ADC_UPDT carried '
                                                               'unchanged for the milestone row. The '
                                                               'parent-case existence join adds no '
                                                               'timestamp; this is bronze ingestion '
                                                               'provenance, not CASE_TIME_DT_TM or Silver '
                                                               'refresh time.',
                                                  'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                                  'stage_description': 'S14 theatre activity; retained as a '
                                                                       'source-faithful bronze input or '
                                                                       'Journey standard-block field.',
                                                  'task_assay_code': 'S14 theatre activity; retained as a '
                                                                     'source-faithful bronze input or '
                                                                     'Journey standard-block field.',
                                                  'task_assay_description': 'S14 theatre activity; retained '
                                                                            'as a source-faithful bronze '
                                                                            'input or Journey standard-block '
                                                                            'field.',
                                                  'theatre_case_key': 'Source-preserved theatre case key.',
                                                  'theatre_case_milestone_key': 'Deterministic SHA-256 key '
                                                                                'for the theatre milestone '
                                                                                'row.'},
                                      'quantity_override': False,
                                      'table': 'One SurgiNet theatre case-time milestone with parent-case '
                                               'linkage evidence. Gold QC twin of the silver product: 3 '
                                               'columns are repaired or nulled, 0 check(s) are advisory. '
                                               'Each rule states its reason in the pipeline notebook, and '
                                               'Lakeflow expectation metrics report what every rule matched '
                                               'on each update.'}}
