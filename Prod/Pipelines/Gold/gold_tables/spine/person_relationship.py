"""Gold spine.person_relationship: 2 dataset(s).

- datasets: _qc_spine_person_relationship, spine_person_relationship
- moved verbatim from: 1_gold_spine_and_reference:1888, 1_gold_spine_and_reference:1910, 1_gold_spine_and_reference:1942, 1_gold_spine_and_reference:1946, 1_gold_spine_and_reference:1959

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _flag_person_scope_any, _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_spine.person_relationship ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_PERSON_RELATIONSHIP_SELECT = [
    '`person_relationship_key` AS `person_relationship_key`',
    '`birth_row_id` AS `birth_row_id`',
    '`source_subject_key` AS `source_subject_key`',
    '`source_person_id` AS `source_person_id`',
    '`target_subject_key` AS `target_subject_key`',
    '`target_person_id` AS `target_person_id`',
    '`relationship_type_code` AS `relationship_type_code`',
    '`inverse_relationship_type_code` AS `inverse_relationship_type_code`',
    '`valid_from` AS `valid_from`',
    '`valid_to` AS `valid_to`',
    '`construction_rule` AS `construction_rule`',
    '`construction_version` AS `construction_version`',
    '`source_system` AS `source_system`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`record_status` AS `record_status`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _spine_person_relationship_metadata; source history stays on the main research table.
SPINE_PERSON_RELATIONSHIP_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 735 of 186,512
    # rows (0.394%) when profiled on 2026-08-24.
    "gold.spine.person_relationship.valid_from.future_owner":
        "NOT COALESCE((CAST(`valid_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
SPINE_PERSON_RELATIONSHIP_PERSON_SCOPE_RULES = {
    "gold.spine.person_relationship.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._spine_person_relationship"),
    refresh_policy="auto",
)
@dp.expect_all(SPINE_PERSON_RELATIONSHIP_ADVISORY_RULES)
@dp.expect_all_or_drop(SPINE_PERSON_RELATIONSHIP_PERSON_SCOPE_RULES)
def _gold_qc_spine_person_relationship():
    """Quality-controlled twin of journey_spine.person_relationship."""
    df = _qc("spine_person_relationship", SPINE_PERSON_RELATIONSHIP_SELECT)
    df = _flag_person_scope_any(df, ("source_person_id", "target_person_id"))  # INSR_W3 W3.02
    return df


@dp.materialized_view(
    name=_n("gold_spine.person_relationship"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_spine_person_relationship():
    """Contract-v2 public twin of spine_person_relationship; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_person_relationship")).selectExpr(
        '`person_relationship_key` AS `person_relationship_key`',
        '`birth_row_id` AS `birth_row_id`',
        '`source_subject_key` AS `source_subject_key`',
        '`source_person_id` AS `source_person_id`',
        '`target_subject_key` AS `target_subject_key`',
        '`target_person_id` AS `target_person_id`',
        '`relationship_type_code` AS `relationship_type_code`',
        '`inverse_relationship_type_code` AS `inverse_relationship_type_code`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`construction_rule` AS `construction_rule`',
        '`construction_version` AS `construction_version`',
        '`source_system` AS `source_system`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`record_status` AS `record_status`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'spine_person_relationship': GoldRules(
        table='spine_person_relationship',
        select=SPINE_PERSON_RELATIONSHIP_SELECT,
        advisory=SPINE_PERSON_RELATIONSHIP_ADVISORY_RULES,
        person_scope=SPINE_PERSON_RELATIONSHIP_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['person_relationship_key'],
         'support_columns': ['record_status', 'load_batch_id', 'source_update_timestamp', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_spine_person_relationship': {'columns': {'birth_row_id': 'MSDS BirthRow_ID; native source primary key.',
                                               'construction_rule': 'Governed relationship-construction '
                                                                    'rule.',
                                               'construction_version': 'Governed relationship-construction '
                                                                       'version.',
                                               'inverse_relationship_type_code': 'Controlled inverse '
                                                                                 'relationship type.',
                                               'loaded_at': 'map_mat_birth.ADC_UPDT carried unchanged for '
                                                            'the mother-to-child relationship. The three '
                                                            'contributing source-update clocks are published '
                                                            'separately as source_update_timestamp and do '
                                                            'not replace this value; no parent-person or '
                                                            'separate pregnancy load clock is joined.',
                                               'person_relationship_key': 'Deterministic SHA-256 '
                                                                          'relationship key.',
                                               'record_status': 'Superseded when '
                                                                'map_mat_birth.PregnancySource_DELETE_IND '
                                                                'cast to LONG is nonzero, otherwise active; '
                                                                'null defaults to zero. No retracted status '
                                                                'is emitted. This is the '
                                                                'pregnancy-enrichment deletion flag carried '
                                                                'on the birth row, not a Boolean output or '
                                                                'an independent test of birth-source '
                                                                'absence.',
                                               'relationship_type_code': 'Controlled source-to-target '
                                                                         'relationship type.',
                                               'source_person_id': 'Resolved source-side person identifier '
                                                                   'when available.',
                                               'source_row_id': 'Stable source birth-row identifier.',
                                               'source_subject_key': 'Always-populated subject key for the '
                                                                     'relationship source person.',
                                               'source_system': 'Source-system label for these '
                                                                'maternity-derived records: '
                                                                'millennium-maternity.',
                                               'source_table': 'Fully qualified configured bronze source '
                                                               'table.',
                                               'source_update_timestamp': 'Greatest of '
                                                                          'map_mat_birth.BirthSourceRecordUpdatedDateTime, '
                                                                          'PregnancySourceRecordUpdatedDateTime '
                                                                          'and NNUSourceLastUpdate for the '
                                                                          'contributing birth row. Null '
                                                                          'inputs are ignored; all null '
                                                                          'gives null. No separate pregnancy '
                                                                          'or neonatal row is joined to add '
                                                                          'a clock, and the current Silver '
                                                                          'refresh time is not substituted.',
                                               'target_person_id': 'Resolved target-side person identifier '
                                                                   'when available.',
                                               'target_subject_key': 'Always-populated subject key for the '
                                                                     'relationship target person.',
                                               'valid_from': 'Relationship validity start from source '
                                                             'evidence.',
                                               'valid_to': 'Relationship validity end when source evidence '
                                                           'supplies one.'},
                                   'quantity_override': True,
                                   'table': 'Internal quality-controlled twin of spine_person_relationship: '
                                            'the Gold repairs, nulls and expectations are applied here and '
                                            'the published Gold MV reads this object. Materialized rather '
                                            'than temporary so the published MV can refresh incrementally.'},
 'spine_person_relationship': {'columns': {'birth_row_id': 'MSDS BirthRow_ID; native source primary key.',
                                           'construction_rule': 'Governed relationship-construction rule.',
                                           'construction_version': 'Governed relationship-construction '
                                                                   'version.',
                                           'inverse_relationship_type_code': 'Controlled inverse '
                                                                             'relationship type.',
                                           'loaded_at': 'map_mat_birth.ADC_UPDT carried unchanged for the '
                                                        'mother-to-child relationship. The three '
                                                        'contributing source-update clocks are published '
                                                        'separately as source_update_timestamp and do not '
                                                        'replace this value; no parent-person or separate '
                                                        'pregnancy load clock is joined.',
                                           'person_relationship_key': 'Deterministic SHA-256 relationship '
                                                                      'key.',
                                           'record_status': 'Superseded when '
                                                            'map_mat_birth.PregnancySource_DELETE_IND cast '
                                                            'to LONG is nonzero, otherwise active; null '
                                                            'defaults to zero. No retracted status is '
                                                            'emitted. This is the pregnancy-enrichment '
                                                            'deletion flag carried on the birth row, not a '
                                                            'Boolean output or an independent test of '
                                                            'birth-source absence.',
                                           'relationship_type_code': 'Controlled source-to-target '
                                                                     'relationship type.',
                                           'source_person_id': 'Resolved source-side person identifier when '
                                                               'available.',
                                           'source_row_id': 'Stable source birth-row identifier.',
                                           'source_subject_key': 'Always-populated subject key for the '
                                                                 'relationship source person.',
                                           'source_system': 'Source-system label for these maternity-derived '
                                                            'records: millennium-maternity.',
                                           'source_table': 'Fully qualified configured bronze source table.',
                                           'source_update_timestamp': 'Greatest of '
                                                                      'map_mat_birth.BirthSourceRecordUpdatedDateTime, '
                                                                      'PregnancySourceRecordUpdatedDateTime '
                                                                      'and NNUSourceLastUpdate for the '
                                                                      'contributing birth row. Null inputs '
                                                                      'are ignored; all null gives null. No '
                                                                      'separate pregnancy or neonatal row is '
                                                                      'joined to add a clock, and the '
                                                                      'current Silver refresh time is not '
                                                                      'substituted.',
                                           'target_person_id': 'Resolved target-side person identifier when '
                                                               'available.',
                                           'target_subject_key': 'Always-populated subject key for the '
                                                                 'relationship target person.',
                                           'valid_from': 'Relationship validity start from source evidence.',
                                           'valid_to': 'Relationship validity end when source evidence '
                                                       'supplies one.'},
                               'quantity_override': False,
                               'table': 'One source-backed relationship between two people; maternity '
                                        'currently supplies mother-to-baby links. Gold QC twin of the silver '
                                        'product: 0 columns are repaired or nulled, 1 check(s) are advisory. '
                                        'Each rule states its reason in the pipeline notebook, and Lakeflow '
                                        'expectation metrics report what every rule matched on each update.'}}
