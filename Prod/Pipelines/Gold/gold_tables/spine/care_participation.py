"""Gold spine.care_participation: 2 dataset(s).

- datasets: _qc_spine_care_participation, spine_care_participation
- moved verbatim from: 1_gold_spine_and_reference:106, 1_gold_spine_and_reference:128, 1_gold_spine_and_reference:158, 1_gold_spine_and_reference:163, 1_gold_spine_and_reference:167, 1_gold_spine_and_reference:187

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_spine.care_participation ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_CARE_PARTICIPATION_SELECT = [
    '`care_participation_key` AS `care_participation_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`journey_key` AS `journey_key`',
    '`service_id` AS `service_id`',
    # INSR_W3 W3.07: Gold: NULL where practitioner 0 = no practitioner (F7; silver W2.01 already NULLs it).
    "CASE WHEN CAST(`practitioner_id` AS BIGINT) = 0 THEN NULL ELSE `practitioner_id` END AS `practitioner_id`",
    '`role` AS `role`',
    "CASE WHEN CAST(`valid_from` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`valid_from` AS DATE)) < 9999 OR CAST(`valid_from` AS DATE) = DATE'1900-01-01' OR CAST(`valid_from` AS DATE) < DATE'1901-01-01' AND CAST(`valid_from` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `valid_from` END AS `valid_from`",
    # INSR_W3 W3.07: Gold: NULL where an open-end marker in [2099-12-31, 9999) (L3892: 94.6% of non-null valid_to).
    "CASE WHEN (CAST(`valid_to` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`valid_to` AS DATE)) < 9999) THEN NULL ELSE `valid_to` END AS `valid_to`",
    '`construction_rule` AS `construction_rule`',
    '`construction_version` AS `construction_version`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _spine_care_participation_metadata; source history stays on the main research table.
SPINE_CARE_PARTICIPATION_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 3 of 144,668,217
    # rows (2.07e-06%) when profiled on 2026-08-24.
    "gold.spine.care_participation.valid_from.future_owner":
        "NOT COALESCE((CAST(`valid_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
SPINE_CARE_PARTICIPATION_PERSON_SCOPE_RULES = {
    "gold.spine.care_participation.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
SPINE_CARE_PARTICIPATION_ORPHAN_RULES = {
    "gold.spine.care_participation.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._spine_care_participation"),
    refresh_policy="auto",
)
@dp.expect_all(SPINE_CARE_PARTICIPATION_ADVISORY_RULES)
@dp.expect_all_or_drop(SPINE_CARE_PARTICIPATION_PERSON_SCOPE_RULES)
@dp.expect_all(SPINE_CARE_PARTICIPATION_ORPHAN_RULES)
def _gold_qc_spine_care_participation():
    """Quality-controlled twin of journey_spine.care_participation."""
    # 54 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "spine_care_participation",
        SPINE_CARE_PARTICIPATION_SELECT,
        fk_columns=["person_id"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_spine.care_participation"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_spine_care_participation():
    """Contract-v2 public twin of spine_care_participation; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_care_participation")).selectExpr(
        '`care_participation_key` AS `care_participation_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`journey_key` AS `journey_key`',
        '`service_id` AS `service_id`',
        '`practitioner_id` AS `practitioner_id`',
        '`role` AS `role`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`construction_rule` AS `construction_rule`',
        '`construction_version` AS `construction_version`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'spine_care_participation': GoldRules(
        table='spine_care_participation',
        select=SPINE_CARE_PARTICIPATION_SELECT,
        advisory=SPINE_CARE_PARTICIPATION_ADVISORY_RULES,
        person_scope=SPINE_CARE_PARTICIPATION_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['care_participation_key'], 'support_columns': ['load_batch_id', 'loaded_at']},
        value_rules={'gold.spine.care_participation.practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                           'column': 'practitioner_id',
                                                                           'companions': [],
                                                                           'match': '`practitioner_id` = 0',
                                                                           'table': 'spine_care_participation'}}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_spine_care_participation': {'columns': {'care_participation_key': 'Deterministic SHA-256 participation '
                                                                        'key.',
                                              'construction_rule': 'Governed derivation rule.',
                                              'construction_version': 'Governed derivation-rule version.',
                                              'encounter_id': 'Millennium ENCNTR_ID as BIGINT.',
                                              'journey_key': 'Deterministic journey key when available.',
                                              'loaded_at': 'map_encounter.ADC_UPDT inherited unchanged '
                                                           'through the canonical encounter row for each '
                                                           'registrar, recorder or discharger participation. '
                                                           'This is encounter ingestion provenance, not a '
                                                           'practitioner update, role-validity timestamp or '
                                                           'Silver refresh time; a missing source timestamp '
                                                           'remains null.',
                                              'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                              'practitioner_id': 'Millennium personnel PERSON_ID as BIGINT.',
                                              'role': 'Source-field-derived participation role.',
                                              'service_id': 'Service-registration context when available.',
                                              'source_row_id': 'Source encounter row identity.',
                                              'source_table': 'Fully qualified bronze source table.',
                                              'subject_id_system': 'Identifier system used for subject_key.',
                                              'subject_key': 'Deterministic SHA-256 over the strongest '
                                                             'source identifier. Not salted, not secret; a '
                                                             'stable join key across feeds where person_id '
                                                             'is unresolved.',
                                              'valid_from': 'Participation validity start.',
                                              'valid_to': 'Participation validity end.'},
                                  'quantity_override': True,
                                  'table': 'Internal quality-controlled twin of spine_care_participation: '
                                           'the Gold repairs, nulls and expectations are applied here and '
                                           'the published Gold MV reads this object. Materialized rather '
                                           'than temporary so the published MV can refresh incrementally.'},
 'spine_care_participation': {'columns': {'care_participation_key': 'Deterministic SHA-256 participation '
                                                                    'key.',
                                          'construction_rule': 'Governed derivation rule.',
                                          'construction_version': 'Governed derivation-rule version.',
                                          'encounter_id': 'Millennium ENCNTR_ID as BIGINT.',
                                          'journey_key': 'Deterministic journey key when available.',
                                          'loaded_at': 'map_encounter.ADC_UPDT inherited unchanged through '
                                                       'the canonical encounter row for each registrar, '
                                                       'recorder or discharger participation. This is '
                                                       'encounter ingestion provenance, not a practitioner '
                                                       'update, role-validity timestamp or Silver refresh '
                                                       'time; a missing source timestamp remains null.',
                                          'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                          'practitioner_id': 'Millennium personnel PERSON_ID as BIGINT.',
                                          'role': 'Source-field-derived participation role.',
                                          'service_id': 'Service-registration context when available.',
                                          'source_row_id': 'Source encounter row identity.',
                                          'source_table': 'Fully qualified bronze source table.',
                                          'subject_id_system': 'Identifier system used for subject_key.',
                                          'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                         'identifier. Not salted, not secret; a stable join '
                                                         'key across feeds where person_id is unresolved.',
                                          'valid_from': 'Participation validity start.',
                                          'valid_to': 'Participation validity end.'},
                              'quantity_override': False,
                              'table': 'One practitioner role attribution to a subject in encounter context. '
                                       'Gold QC twin of the silver product: 2 columns are repaired or '
                                       'nulled, 1 check(s) are advisory. Each rule states its reason in the '
                                       'pipeline notebook, and Lakeflow expectation metrics report what '
                                       'every rule matched on each update.'}}
