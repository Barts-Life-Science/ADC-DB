"""Gold spine.person_gp_registration: 2 dataset(s).

- datasets: _qc_reference_person_gp_registration, reference_person_gp_registration
- moved verbatim from: 1_gold_spine_and_reference:5140, 1_gold_spine_and_reference:5143, 1_gold_spine_and_reference:5161, 1_gold_spine_and_reference:5166, 1_gold_spine_and_reference:5170, 1_gold_spine_and_reference:5191, 1_gold_spine_and_reference:5196, 1_gold_spine_and_reference:5202

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.person_gp_registration ==== PMS_P1_T5_GOLD_V1
REFERENCE_PERSON_GP_REGISTRATION_COLUMNS = ["person_gp_registration_key", "person_id", "person_org_reltn_id", "organization_id",
    "practice_ods_code", "practice_name", "registration_start", "registration_end", "active_ind", "current_ind",
    "free_text_ind", "loaded_at", "_source_system", "_source_table", "_source_row_id"]


REFERENCE_PERSON_GP_REGISTRATION_SELECT = [  # INSR_W3 W3.06: expanded from REFERENCE_PERSON_GP_REGISTRATION_COLUMNS so each column's transform is visible
    '`person_gp_registration_key` AS `person_gp_registration_key`',
    '`person_id` AS `person_id`',
    '`person_org_reltn_id` AS `person_org_reltn_id`',
    '`organization_id` AS `organization_id`',
    '`practice_ods_code` AS `practice_ods_code`',
    '`practice_name` AS `practice_name`',
    # INSR_W3 W3.06: Gold: NULL where exactly 1948-07-04 23:00:00 UTC, the NHS-founding placeholder (P3g: n=211, x11.7).
    "CASE WHEN unix_seconds(CAST(`registration_start` AS TIMESTAMP)) = -678243600 THEN NULL ELSE `registration_start` END AS `registration_start`",
    '`registration_end` AS `registration_end`',
    '`active_ind` AS `active_ind`',
    '`current_ind` AS `current_ind`',
    '`free_text_ind` AS `free_text_ind`',
    '`loaded_at` AS `loaded_at`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
]


REFERENCE_PERSON_GP_REGISTRATION_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`registration_end_derived` AS `registration_end_derived`',
    '`registration_end_basis` AS `registration_end_basis`',
    '`practice_code_ods_valid` AS `practice_code_ods_valid`',
]


REFERENCE_PERSON_GP_REGISTRATION_MANDATORY_RULES = {
    # A registration that belongs to no spine person cannot be used; _qc nulls person_id for non-spine parents.
    "gold.reference.person_gp_registration.person_id.fk_containment": "`person_id` IS NOT NULL",
}


REFERENCE_PERSON_GP_REGISTRATION_ADVISORY_RULES = {
    # INSR_W4 W4.06 string_discipline (P9_L5172): GP practice codes are ODS A99999; legacy PCT codes (5C4, 5C5 ...) fail the format (L5172).
    'gold.reference.person_gp_registration.practice_ods_code.insr_f18_ods_format':
        "NOT COALESCE((`practice_ods_code` IS NOT NULL AND NOT `practice_ods_code` RLIKE '^[A-Z][0-9]{5}$'), FALSE)",
    # INSR_W4 W4.06 value_set (P9_L5172): V8199x are pseudo-practices (unregistered / not known), not practices (L5172).
    'gold.reference.person_gp_registration.practice_ods_code.insr_f18_ods_pseudo':
        "NOT COALESCE((`practice_ods_code` RLIKE '^V8199'), FALSE)",
    # A malformed practice code is reported, not dropped: the registration itself is still true.
    "gold.reference.person_gp_registration.ods_code_wellformed": "`practice_ods_code` IS NULL OR `practice_ods_code` RLIKE '^[A-Z][0-9]{5}$'",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
REFERENCE_PERSON_GP_REGISTRATION_PERSON_SCOPE_RULES = {
    "gold.reference.person_gp_registration.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.06: warn_row rules (J4); rows are kept.
REFERENCE_PERSON_GP_REGISTRATION_W3_06_ADVISORY_RULES = {
    "gold.reference.person_gp_registration.registration_start.pre_nhs_1901_1948": "NOT COALESCE(CAST(`registration_start` AS DATE) BETWEEN DATE'1901-01-01' AND DATE'1948-12-31', FALSE)",
    "gold.reference.person_gp_registration.registration_start.cutover_effective_date": "NOT COALESCE(CAST(`registration_start` AS DATE) IN (DATE'2014-05-08', DATE'2017-09-01'), FALSE)",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
REFERENCE_PERSON_GP_REGISTRATION_ORPHAN_RULES = {
    "gold.reference.person_gp_registration.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(name=_n("gold_qc._reference_person_gp_registration"), refresh_policy="auto")
@dp.expect_all_or_drop(REFERENCE_PERSON_GP_REGISTRATION_MANDATORY_RULES)
@dp.expect_all(REFERENCE_PERSON_GP_REGISTRATION_ADVISORY_RULES)
@dp.expect_all_or_drop(REFERENCE_PERSON_GP_REGISTRATION_PERSON_SCOPE_RULES)
@dp.expect_all(REFERENCE_PERSON_GP_REGISTRATION_W3_06_ADVISORY_RULES)
@dp.expect_all(REFERENCE_PERSON_GP_REGISTRATION_ORPHAN_RULES)
def _gold_qc_reference_person_gp_registration():
    """Quality-controlled twin of journey_reference.person_gp_registration."""
    df = _qc("reference_person_gp_registration", REFERENCE_PERSON_GP_REGISTRATION_SELECT, fk_columns=["person_id"])
    return df


@dp.materialized_view(name=_n("gold_reference.person_gp_registration"),
    table_properties={"quality": "gold"}, refresh_policy="auto")
def gold_reference_person_gp_registration():
    """Contract-v2 public twin of reference_person_gp_registration."""
    df = spark.read.table(_n("gold_qc._reference_person_gp_registration")).selectExpr(
        *[f"`{c}` AS `{c}`" for c in REFERENCE_PERSON_GP_REGISTRATION_COLUMNS])  # INSR_W3 W3.06: transforms live in the twin
    return df


RULES = {
    'reference_person_gp_registration': GoldRules(
        table='reference_person_gp_registration',
        select=REFERENCE_PERSON_GP_REGISTRATION_SELECT,
        mandatory=REFERENCE_PERSON_GP_REGISTRATION_MANDATORY_RULES,
        advisory=REFERENCE_PERSON_GP_REGISTRATION_ADVISORY_RULES,
        person_scope=REFERENCE_PERSON_GP_REGISTRATION_PERSON_SCOPE_RULES),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_person_gp_registration': {'columns': {'_source_row_id': 'PERSON_ORG_RELTN_ID as string.',
                                                      '_source_system': 'Always millennium.',
                                                      '_source_table': 'Bronze source table.',
                                                      'active_ind': 'Source ACTIVE_IND = 1.',
                                                      'current_ind': 'True on the latest-starting active, '
                                                                     'open registration per person as of the '
                                                                     'bronze build.',
                                                      'free_text_ind': 'Practice was recorded as free text, '
                                                                       'not a coded organisation.',
                                                      'loaded_at': 'map_person_gp_registration.ADC_UPDT '
                                                                   'carried unchanged.',
                                                      'organization_id': 'Millennium ORGANIZATION_ID of the '
                                                                         'registered practice; null when '
                                                                         'free-text only.',
                                                      'person_gp_registration_key': 'Deterministic key for '
                                                                                    'the Millennium '
                                                                                    'registered-practice '
                                                                                    'relationship row.',
                                                      'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                                      'person_org_reltn_id': 'Millennium '
                                                                             'PERSON_ORG_RELTN_ID; one '
                                                                             'registration row.',
                                                      'practice_code_ods_valid': 'Practice code class: valid '
                                                                                 '(England/Wales/NI letter + '
                                                                                 '5 digits), pseudo_v81 (V81 '
                                                                                 "'no registered GP' pseudo "
                                                                                 'codes), legacy_pct (5xx '
                                                                                 'primary care trust codes), '
                                                                                 'malformed, or NULL. '
                                                                                 'INSR_W2 (L5172).',
                                                      'practice_name': 'Practice name from ORGANIZATION, '
                                                                       'else the free-text name on the '
                                                                       'relationship.',
                                                      'practice_ods_code': 'NHS ODS code of the practice '
                                                                           '(ORGANIZATION_ALIAS pool '
                                                                           '6031508); null when the '
                                                                           'organisation carries none.',
                                                      'registration_end': 'End of the registration; null '
                                                                          'while open (2100 sentinel removed '
                                                                          'in bronze).',
                                                      'registration_end_basis': 'source_end or '
                                                                                'next_registration_start; '
                                                                                'NULL when the registration '
                                                                                'is still open. INSR_W2 '
                                                                                'W2.06.',
                                                      'registration_end_derived': 'registration_end, else '
                                                                                  'the start of the same '
                                                                                  "person's next "
                                                                                  'registration (an '
                                                                                  'open-ended row followed '
                                                                                  'by a later registration '
                                                                                  'has ended). INSR_W2 '
                                                                                  'W2.06.',
                                                      'registration_start': 'Start of the registration '
                                                                            '(BEG_EFFECTIVE_DT_TM, UTC).'},
                                          'quantity_override': True,
                                          'table': 'Internal quality-controlled twin of '
                                                   'reference_person_gp_registration.'},
 'reference_person_gp_registration': {'columns': {'_source_row_id': 'PERSON_ORG_RELTN_ID as string.',
                                                  '_source_system': 'Always millennium.',
                                                  '_source_table': 'Bronze source table.',
                                                  'active_ind': 'Source ACTIVE_IND = 1.',
                                                  'current_ind': 'True on the latest-starting active, open '
                                                                 'registration per person as of the bronze '
                                                                 'build.',
                                                  'free_text_ind': 'Practice was recorded as free text, not '
                                                                   'a coded organisation.',
                                                  'loaded_at': 'map_person_gp_registration.ADC_UPDT carried '
                                                               'unchanged.',
                                                  'organization_id': 'Millennium ORGANIZATION_ID of the '
                                                                     'registered practice; null when '
                                                                     'free-text only.',
                                                  'person_gp_registration_key': 'Deterministic key for the '
                                                                                'Millennium '
                                                                                'registered-practice '
                                                                                'relationship row.',
                                                  'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                                  'person_org_reltn_id': 'Millennium PERSON_ORG_RELTN_ID; '
                                                                         'one registration row.',
                                                  'practice_code_ods_valid': 'Practice code class: valid '
                                                                             '(England/Wales/NI letter + 5 '
                                                                             "digits), pseudo_v81 (V81 'no "
                                                                             "registered GP' pseudo codes), "
                                                                             'legacy_pct (5xx primary care '
                                                                             'trust codes), malformed, or '
                                                                             'NULL. INSR_W2 (L5172).',
                                                  'practice_name': 'Practice name from ORGANIZATION, else '
                                                                   'the free-text name on the relationship.',
                                                  'practice_ods_code': 'NHS ODS code of the practice '
                                                                       '(ORGANIZATION_ALIAS pool 6031508); '
                                                                       'null when the organisation carries '
                                                                       'none.',
                                                  'registration_end': 'End of the registration; null while '
                                                                      'open (2100 sentinel removed in '
                                                                      'bronze).',
                                                  'registration_end_basis': 'source_end or '
                                                                            'next_registration_start; NULL '
                                                                            'when the registration is still '
                                                                            'open. INSR_W2 W2.06.',
                                                  'registration_end_derived': 'registration_end, else the '
                                                                              "start of the same person's "
                                                                              'next registration (an '
                                                                              'open-ended row followed by a '
                                                                              'later registration has '
                                                                              'ended). INSR_W2 W2.06.',
                                                  'registration_start': 'Start of the registration '
                                                                        '(BEG_EFFECTIVE_DT_TM, UTC).'},
                                      'quantity_override': False,
                                      'table': 'Registered GP practice history per person. Gold QC twin: 1 '
                                               'rule drops rows (no spine person), 1 advisory. Coverage: '
                                               'Registration starts heap on 2014-05-08 and 2017-09-01 '
                                               '(system cut-overs, about 350 and 270 times the usual daily '
                                               "count): those dates mean 'registered on or before'; "
                                               'complete-through month per release: '
                                               '6_mgmt.data_quality.release_finding (finding_code '
                                               'F2_complete_through).'}}
