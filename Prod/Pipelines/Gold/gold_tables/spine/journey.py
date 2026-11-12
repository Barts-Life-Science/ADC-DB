"""Gold spine.journey: 2 dataset(s).

- datasets: _qc_spine_journey, spine_journey
- moved verbatim from: 1_gold_spine_and_reference:1002, 1_gold_spine_and_reference:1006, 1_gold_spine_and_reference:1026, 1_gold_spine_and_reference:921, 1_gold_spine_and_reference:951, 1_gold_spine_and_reference:997

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_spine.journey ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_JOURNEY_SELECT = [
    '`journey_key` AS `journey_key`',
    '`parent_journey_key` AS `parent_journey_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`journey_type_code` AS `journey_type_code`',
    '`journey_type_display` AS `journey_type_display`',
    '`period_start` AS `period_start`',
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN `period_start` IS NOT NULL AND `period_end` IS NOT NULL AND `period_start` > `period_end` THEN NULL ELSE CASE WHEN (CAST(`period_end` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`period_end` AS DATE)) < 9999) THEN NULL ELSE `period_end` END END AS `period_end`",
    '`period_start_basis` AS `period_start_basis`',  # SILVER_RELEASE_20261025 item 5
    '`period_end_basis` AS `period_end_basis`',  # SILVER_RELEASE_20261025 item 5
    '`status_code` AS `status_code`',
    '`defining_coding_system` AS `defining_coding_system`',
    '`defining_code` AS `defining_code`',
    '`defining_display` AS `defining_display`',
    '`outcome_code` AS `outcome_code`',
    '`outcome_display` AS `outcome_display`',
    '`source_journey_identifier` AS `source_journey_identifier`',
    '`construction_rule` AS `construction_rule`',
    '`construction_version` AS `construction_version`',
    '`source_system` AS `source_system`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _spine_journey_metadata; source history stays on the main research table.
SPINE_JOURNEY_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 25,297 of 824,072
    # rows (3.07%) when profiled on 2026-08-24.
    "gold.spine.journey.period_end.future_owner":
        "NOT COALESCE((CAST(`period_end` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 6,633 of 824,072
    # rows (0.805%) when profiled on 2026-08-24.
    "gold.spine.journey.period_start.future_owner":
        "NOT COALESCE((CAST(`period_start` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
SPINE_JOURNEY_PERSON_SCOPE_RULES = {
    "gold.spine.journey.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
SPINE_JOURNEY_ORPHAN_RULES = {
    "gold.spine.journey.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._spine_journey"),
    refresh_policy="auto",
)
@dp.expect_all(SPINE_JOURNEY_ADVISORY_RULES)
@dp.expect_all_or_drop(SPINE_JOURNEY_PERSON_SCOPE_RULES)
@dp.expect_all(SPINE_JOURNEY_ORPHAN_RULES)
def _gold_qc_spine_journey():
    """Quality-controlled twin of journey_spine.journey."""
    # 4 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "spine_journey",
        SPINE_JOURNEY_SELECT,
        fk_columns=["person_id"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_spine.journey"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_spine_journey():
    """Contract-v2 public twin of spine_journey; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_journey")).selectExpr(
        '`journey_key` AS `journey_key`',
        '`parent_journey_key` AS `parent_journey_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`journey_type_code` AS `journey_type_code`',
        '`journey_type_display` AS `journey_type_display`',
        '`period_start` AS `period_start`',
        '`period_end` AS `period_end`',
        '`period_start_basis` AS `period_start_basis`',  # SILVER_RELEASE_20261025 item 5
        '`period_end_basis` AS `period_end_basis`',  # SILVER_RELEASE_20261025 item 5
        '`status_code` AS `status_code`',
        '`defining_coding_system` AS `defining_coding_system`',
        '`defining_code` AS `defining_code`',
        '`defining_display` AS `defining_display`',
        '`outcome_code` AS `outcome_code`',
        '`outcome_display` AS `outcome_display`',
        '`source_journey_identifier` AS `source_journey_identifier`',
        '`construction_rule` AS `construction_rule`',
        '`construction_version` AS `construction_version`',
        '`source_system` AS `source_system`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'spine_journey': GoldRules(
        table='spine_journey',
        select=SPINE_JOURNEY_SELECT,
        advisory=SPINE_JOURNEY_ADVISORY_RULES,
        person_scope=SPINE_JOURNEY_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['journey_key'],
         'support_columns': ['load_batch_id', 'source_update_timestamp', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_spine_journey': {'columns': {'construction_rule': 'Governed constructor name.',
                                   'construction_version': 'Governed constructor version.',
                                   'defining_code': 'Constructor-defining concept code.',
                                   'defining_coding_system': 'Coding system for the constructor-defining '
                                                             'concept.',
                                   'defining_display': 'Constructor-defining concept display.',
                                   'journey_key': 'Deterministic SHA-256 journey key.',
                                   'journey_type_code': 'Controlled journey type.',
                                   'journey_type_display': 'Human-readable journey type.',
                                   'loaded_at': 'Greatest of ADC_UPDT from the selected map_mat_pregnancy '
                                                'row and maximum map_mat_birth.ADC_UPDT across all rows in '
                                                'the Pregnancy_ID group. The selected pregnancy row, not a '
                                                'maximum over all pregnancy versions, contributes its clock. '
                                                'Pregnancy and trimester rows carry the same value; clinical '
                                                'period boundaries and the current Silver refresh time are '
                                                'not used.',
                                   'outcome_code': 'Source pregnancy outcome code where supplied.',
                                   'outcome_display': 'Source pregnancy outcome display where supplied.',
                                   'parent_journey_key': 'Deterministic SHA-256 parent journey key for '
                                                         'governed nesting.',
                                   'period_end': 'Journey end. Pregnancy (PREGNANCY_DATES_V2): the last '
                                                 'birth; else maternity service discharge, else the expected '
                                                 'delivery date, each only within 0-308 days after '
                                                 'period_start. If the span still exceeds 308 days (or is '
                                                 'negative) the less-trusted side is NULLed, never both. See '
                                                 'period_end_basis.',
                                   'period_end_basis': 'Rule that set period_end (PREGNANCY_DATES_V2). '
                                                       'Pregnancy rows: last_birth, '
                                                       'maternity_service_discharge (only within 0-308 days '
                                                       'after the start), expected_delivery_date (only '
                                                       'within 0-308 days after the start), or '
                                                       'nulled_span_guard. Trimester rows: trimester_nominal '
                                                       'for the fixed 98-day trimester ends, otherwise their '
                                                       "pregnancy's end basis. NULL when no source date "
                                                       'exists or none passed its rule.',
                                   'period_start': 'Journey start. Pregnancy (PREGNANCY_DATES_V2): last '
                                                   'menstrual period, else expected delivery date - 280 '
                                                   'days; the latest birth minus its plausible recorded '
                                                   'gestation replaces either when missing or more than 14 '
                                                   'days apart; first contact / first antenatal appointment '
                                                   'only as a last resort within 308 days before period_end. '
                                                   'See period_start_basis.',
                                   'period_start_basis': 'Rule that set period_start (PREGNANCY_DATES_V2). '
                                                         'Pregnancy rows: birth_minus_gestation (latest '
                                                         'birth with a plausible recorded gestation of '
                                                         '140-308 days, used when LMP/EDD dating is missing '
                                                         'or differs from it by more than 14 days), '
                                                         'last_menstrual_period, '
                                                         'expected_delivery_date_minus_280, first_contact or '
                                                         'first_antenatal_appointment (last resort, only '
                                                         'within 308 days before period_end), or '
                                                         'nulled_span_guard (the span still exceeded 308 '
                                                         'days or was negative and this was the less-trusted '
                                                         "side). Trimester rows carry their pregnancy's "
                                                         'start basis. NULL when no source date exists.',
                                   'person_id': 'Millennium PERSON_ID; native primary-subject foreign key '
                                                'when resolved.',
                                   'source_journey_identifier': 'Verbatim source pregnancy identifier.',
                                   'source_row_id': 'Stable constructor source-row identifier.',
                                   'source_system': 'Source-system label for these maternity-derived '
                                                    'records: millennium-maternity.',
                                   'source_table': 'Fully qualified configured pregnancy source table.',
                                   'source_update_timestamp': 'Greatest of the selected map_mat_pregnancy '
                                                              "row's MAT_RECORD_UPDATED_DT, "
                                                              'MSDS_RECORD_UPDATED_DT and BIRTH_ADC_UPDT, '
                                                              'and the maximum '
                                                              'map_mat_birth.BirthSourceRecordUpdatedDateTime '
                                                              'across its Pregnancy_ID group. Pregnancy rows '
                                                              'are selected by the existing '
                                                              'live/load/person/hash rank before this join. '
                                                              'The same value is carried to the pregnancy '
                                                              'and all trimester rows; it is not just the '
                                                              'birth timestamp or the current Silver refresh '
                                                              'time.',
                                   'status_code': 'Normalized journey lifecycle state.',
                                   'subject_id_system': 'Identifier system used for subject_key.',
                                   'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                  'identifier. Not salted, not secret; a stable join key '
                                                  'across feeds where person_id is unresolved.'},
                       'quantity_override': True,
                       'table': 'Internal quality-controlled twin of spine_journey: the Gold repairs, nulls '
                                'and expectations are applied here and the published Gold MV reads this '
                                'object. Materialized rather than temporary so the published MV can refresh '
                                'incrementally.'},
 'spine_journey': {'columns': {'construction_rule': 'Governed constructor name.',
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
                   'quantity_override': False,
                   'table': 'One deterministic longitudinal episode of care, including nested structural '
                            'child journeys. Gold QC twin of the silver product: 2 columns are repaired or '
                            'nulled, 2 check(s) are advisory. Each rule states its reason in the pipeline '
                            'notebook, and Lakeflow expectation metrics report what every rule matched on '
                            'each update.'}}
