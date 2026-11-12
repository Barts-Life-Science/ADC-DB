"""Gold spine.journey_participant: 2 dataset(s).

- datasets: _qc_spine_journey_participant, spine_journey_participant
- moved verbatim from: 1_gold_spine_and_reference:1199, 1_gold_spine_and_reference:1221, 1_gold_spine_and_reference:1259, 1_gold_spine_and_reference:1264, 1_gold_spine_and_reference:1268, 1_gold_spine_and_reference:1288

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_spine.journey_participant ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_JOURNEY_PARTICIPANT_SELECT = [
    '`journey_participant_key` AS `journey_participant_key`',
    '`journey_key` AS `journey_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`role_code` AS `role_code`',
    '`role_display` AS `role_display`',
    '`valid_from` AS `valid_from`',
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN `valid_from` IS NOT NULL AND `valid_to` IS NOT NULL AND `valid_from` > `valid_to` THEN NULL ELSE CASE WHEN (CAST(`valid_to` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`valid_to` AS DATE)) < 9999) THEN NULL ELSE `valid_to` END END AS `valid_to`",
    '`construction_rule` AS `construction_rule`',
    '`construction_version` AS `construction_version`',
    '`source_system` AS `source_system`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`record_status` AS `record_status`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _spine_journey_participant_metadata; source history stays on the main research table.
SPINE_JOURNEY_PARTICIPANT_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 7,368 of 1,010,584
    # rows (0.729%) when profiled on 2026-08-24.
    "gold.spine.journey_participant.valid_from.future_owner":
        "NOT COALESCE((CAST(`valid_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 25,297 of
    # 1,010,584 rows (2.5%) when profiled on 2026-08-24.
    "gold.spine.journey_participant.valid_to.future_owner":
        "NOT COALESCE((CAST(`valid_to` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
SPINE_JOURNEY_PARTICIPANT_PERSON_SCOPE_RULES = {
    "gold.spine.journey_participant.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
SPINE_JOURNEY_PARTICIPANT_ORPHAN_RULES = {
    "gold.spine.journey_participant.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._spine_journey_participant"),
    refresh_policy="auto",
)
@dp.expect_all(SPINE_JOURNEY_PARTICIPANT_ADVISORY_RULES)
@dp.expect_all_or_drop(SPINE_JOURNEY_PARTICIPANT_PERSON_SCOPE_RULES)
@dp.expect_all(SPINE_JOURNEY_PARTICIPANT_ORPHAN_RULES)
def _gold_qc_spine_journey_participant():
    """Quality-controlled twin of journey_spine.journey_participant."""
    # 106 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "spine_journey_participant",
        SPINE_JOURNEY_PARTICIPANT_SELECT,
        fk_columns=["person_id"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_spine.journey_participant"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_spine_journey_participant():
    """Contract-v2 public twin of spine_journey_participant; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_journey_participant")).selectExpr(
        '`journey_participant_key` AS `journey_participant_key`',
        '`journey_key` AS `journey_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`role_code` AS `role_code`',
        '`role_display` AS `role_display`',
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
    'spine_journey_participant': GoldRules(
        table='spine_journey_participant',
        select=SPINE_JOURNEY_PARTICIPANT_SELECT,
        advisory=SPINE_JOURNEY_PARTICIPANT_ADVISORY_RULES,
        person_scope=SPINE_JOURNEY_PARTICIPANT_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['journey_participant_key'],
         'support_columns': ['record_status', 'load_batch_id', 'source_update_timestamp', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_spine_journey_participant': {'columns': {'construction_rule': 'Governed participant-construction rule.',
                                               'construction_version': 'Governed participant-construction '
                                                                       'version.',
                                               'journey_key': 'Deterministic SHA-256 journey key.',
                                               'journey_participant_key': 'Deterministic SHA-256 '
                                                                          'journey-participant key.',
                                               'loaded_at': 'Mother participants inherit the '
                                                            'pregnancy/trimester journey load clock: '
                                                            'greatest of selected map_mat_pregnancy.ADC_UPDT '
                                                            'and maximum map_mat_birth.ADC_UPDT for the '
                                                            'Pregnancy_ID group. Baby participants use only '
                                                            'their own map_mat_birth.ADC_UPDT. The two arms '
                                                            'are unioned without a mother/baby clock maximum '
                                                            'or substitution of clinical validity times or '
                                                            'the current Silver refresh time.',
                                               'person_id': 'Millennium PERSON_ID; native participant '
                                                            'foreign key when resolved.',
                                               'record_status': 'Mother rows are superseded only when their '
                                                                'constructed pregnancy/trimester journey '
                                                                'status_code is superseded; completed and '
                                                                'all other journey statuses map to active. '
                                                                'Baby rows are superseded when '
                                                                'map_mat_birth.PregnancySource_DELETE_IND '
                                                                'cast to LONG is nonzero, otherwise active; '
                                                                'null defaults zero. Neither arm emits '
                                                                'retracted, and the union does not reconcile '
                                                                'statuses between mother and baby.',
                                               'role_code': 'Controlled role in the journey.',
                                               'role_display': 'Human-readable participant role.',
                                               'source_row_id': 'Stable source-row identifier supporting the '
                                                                'role.',
                                               'source_system': 'Source-system label for these '
                                                                'maternity-derived records: '
                                                                'millennium-maternity.',
                                               'source_table': 'Fully qualified configured bronze source '
                                                               'table.',
                                               'source_update_timestamp': 'Mother participants inherit the '
                                                                          'pregnancy/trimester journey '
                                                                          'clock: greatest of the selected '
                                                                          "pregnancy row's "
                                                                          'MAT_RECORD_UPDATED_DT, '
                                                                          'MSDS_RECORD_UPDATED_DT and '
                                                                          'BIRTH_ADC_UPDT, plus maximum '
                                                                          'BirthSourceRecordUpdatedDateTime '
                                                                          'across its map_mat_birth '
                                                                          'Pregnancy_ID group. Baby '
                                                                          'participants use greatest of '
                                                                          'their own map_mat_birth '
                                                                          'BirthSourceRecordUpdatedDateTime, '
                                                                          'PregnancySourceRecordUpdatedDateTime '
                                                                          'and NNUSourceLastUpdate. These '
                                                                          'clocks are carried per arm '
                                                                          'without a cross-arm maximum.',
                                               'subject_id_system': 'Identifier system used for participant '
                                                                    'subject_key.',
                                               'subject_key': 'Deterministic SHA-256 over the strongest '
                                                              'source identifier. Not salted, not secret; a '
                                                              'stable join key across feeds where person_id '
                                                              'is unresolved.',
                                               'valid_from': 'Role validity start.',
                                               'valid_to': 'Role validity end.'},
                                   'quantity_override': True,
                                   'table': 'Internal quality-controlled twin of spine_journey_participant: '
                                            'the Gold repairs, nulls and expectations are applied here and '
                                            'the published Gold MV reads this object. Materialized rather '
                                            'than temporary so the published MV can refresh incrementally.'},
 'spine_journey_participant': {'columns': {'construction_rule': 'Governed participant-construction rule.',
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
                               'quantity_override': False,
                               'table': 'One journey-to-person role interval, retaining unresolved '
                                        'participants through subject_key. Gold QC twin of the silver '
                                        'product: 2 columns are repaired or nulled, 2 check(s) are advisory. '
                                        'Each rule states its reason in the pipeline notebook, and Lakeflow '
                                        'expectation metrics report what every rule matched on each update.'}}
