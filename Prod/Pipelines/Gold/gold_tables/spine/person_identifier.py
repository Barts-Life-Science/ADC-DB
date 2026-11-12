"""Gold spine.person_identifier: 2 dataset(s).

- datasets: _qc_spine_person_identifier, spine_person_identifier
- moved verbatim from: 1_gold_spine_and_reference:1745, 1_gold_spine_and_reference:1766, 1_gold_spine_and_reference:1780, 1_gold_spine_and_reference:1823, 1_gold_spine_and_reference:1828, 1_gold_spine_and_reference:1832, 1_gold_spine_and_reference:1850

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_spine.person_identifier ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_PERSON_IDENTIFIER_SELECT = [
    '`person_identifier_key` AS `person_identifier_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`identifier_system` AS `identifier_system`',
    "CASE WHEN TRIM(CAST(`identifier_value` AS STRING)) = '' THEN NULL ELSE `identifier_value` END AS `identifier_value`",
    '`identifier_type_code` AS `identifier_type_code`',
    '`status` AS `status`',
    "CASE WHEN CAST(`valid_from` AS DATE) = DATE'1900-01-01' OR CAST(`valid_from` AS DATE) < DATE'1901-01-01' AND CAST(`valid_from` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `valid_from` END AS `valid_from`",
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN `valid_from` IS NOT NULL AND `valid_to` IS NOT NULL AND `valid_from` > `valid_to` THEN NULL ELSE CASE WHEN (CAST(`valid_to` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`valid_to` AS DATE)) < 9999) OR CAST(`valid_to` AS DATE) = DATE'1900-01-01' OR CAST(`valid_to` AS DATE) < DATE'1901-01-01' AND CAST(`valid_to` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `valid_to` END END AS `valid_to`",
    '`source_system` AS `source_system`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`source_object` AS `source_object`',
    '`source_identifier_id` AS `source_identifier_id`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _spine_person_identifier_metadata; source history stays on the main research table.
SPINE_PERSON_IDENTIFIER_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 27,018,061 rows of
    # 27,522,731 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",

    # The flow nulls person_id when it names a parent the spine does not have, and this rule
    # then drops the row, because an identifier belonging to no person identifies nothing a
    # researcher can join to. That was 1,383 orphaned rows plus 504,671 that already had no
    # person_id, out of 27,522,731.
    "gold.spine.person_identifier.person_id.fk_containment": "`person_id` IS NOT NULL",
}


SPINE_PERSON_IDENTIFIER_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 504,670 of 27,522,731 at the profile.
    "gold.spine.person_identifier.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 2 of 27,522,731
    # rows (7.27e-06%) when profiled on 2026-08-24.
    "gold.spine.person_identifier.valid_from.future_owner":
        "NOT COALESCE((CAST(`valid_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 38 of 27,522,731
    # rows (0.000138%) when profiled on 2026-08-24.
    "gold.spine.person_identifier.valid_to.future_owner":
        "NOT COALESCE((CAST(`valid_to` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
SPINE_PERSON_IDENTIFIER_PERSON_SCOPE_RULES = {
    "gold.spine.person_identifier.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
SPINE_PERSON_IDENTIFIER_ORPHAN_RULES = {
    "gold.spine.person_identifier.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._spine_person_identifier"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(SPINE_PERSON_IDENTIFIER_MANDATORY_RULES)
@dp.expect_all(SPINE_PERSON_IDENTIFIER_ADVISORY_RULES)
@dp.expect_all_or_drop(SPINE_PERSON_IDENTIFIER_PERSON_SCOPE_RULES)
@dp.expect_all(SPINE_PERSON_IDENTIFIER_ORPHAN_RULES)
def _gold_qc_spine_person_identifier():
    """Quality-controlled twin of journey_spine.person_identifier."""
    df = _qc(
        "spine_person_identifier",
        SPINE_PERSON_IDENTIFIER_SELECT,
        fk_columns=["person_id"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_spine.person_identifier"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_spine_person_identifier():
    """Contract-v2 public twin of spine_person_identifier; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_person_identifier")).selectExpr(
        '`person_identifier_key` AS `person_identifier_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`identifier_system` AS `identifier_system`',
        '`identifier_value` AS `identifier_value`',
        '`identifier_type_code` AS `identifier_type_code`',
        '`status` AS `status`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`source_system` AS `source_system`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`source_object` AS `source_object`',
        '`source_identifier_id` AS `source_identifier_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'spine_person_identifier': GoldRules(
        table='spine_person_identifier',
        select=SPINE_PERSON_IDENTIFIER_SELECT,
        mandatory=SPINE_PERSON_IDENTIFIER_MANDATORY_RULES,
        advisory=SPINE_PERSON_IDENTIFIER_ADVISORY_RULES,
        person_scope=SPINE_PERSON_IDENTIFIER_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['person_identifier_key'],
         'support_columns': ['identity_status', 'load_batch_id', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_spine_person_identifier': {'columns': {'identifier_system': 'Namespace for the source identifier.',
                                             'identifier_type_code': 'Text label of the alias type derived '
                                                                     'from the alias type code, '
                                                                     'distinguishing medical record number '
                                                                     'from NHS number rows.',
                                             'identifier_value': 'Source identifier value.',
                                             'loaded_at': 'Arm-specific clock: map_person.ADC_UPDT for '
                                                          'Millennium person IDs; always null for both '
                                                          'community key and community number assignments; '
                                                          'map_pacs_patient_link.ADC_UPDT for PACS; '
                                                          'map_patient_identifier.PIPELINE_UPDT_DT_TM for '
                                                          'Millennium aliases; map_endobase_patient.ADC_UPDT '
                                                          'for EndoBase; map_cancer_ptl_linkage.ADC_UPDT for '
                                                          'both Pathfinder MRN and NHS assignments. The '
                                                          'eight arms are unioned without a cross-arm '
                                                          'maximum or Silver refresh timestamp.',
                                             'person_id': 'Resolved Millennium person identifier when '
                                                          'available.',
                                             'person_identifier_key': 'B3 bronze field ROW_ID; source value '
                                                                      'retained unless documented as '
                                                                      'derived.',
                                             'source_identifier_id': 'B3 bronze field ROW_ID; source value '
                                                                     'retained unless documented as derived.',
                                             'source_object': 'Value describing source object for the person '
                                                              'identifier record. It is produced by the '
                                                              'silver transformation and has no direct '
                                                              'bronze-column lineage entry. Whitespace and '
                                                              'source sentinel text are retained unless the '
                                                              'pipeline explicitly normalizes them; null '
                                                              'means no value was supplied.',
                                             'source_row_id': 'Stable source row identifier.',
                                             'source_system': 'Source system name.',
                                             'source_table': 'Fully qualified bronze source table.',
                                             'status': 'Source assignment or matching status.',
                                             'subject_id_system': 'Identifier system used to derive '
                                                                  'subject_key.',
                                             'subject_key': 'deterministic join key over the strongest '
                                                            'available subject evidence.',
                                             'valid_from': 'Identifier validity start.',
                                             'valid_to': 'Identifier validity end.'},
                                 'quantity_override': True,
                                 'table': 'Internal quality-controlled twin of spine_person_identifier: the '
                                          'Gold repairs, nulls and expectations are applied here and the '
                                          'published Gold MV reads this object. Materialized rather than '
                                          'temporary so the published MV can refresh incrementally.'},
 'spine_person_identifier': {'columns': {'identifier_system': 'Namespace for the source identifier.',
                                         'identifier_type_code': 'Text label of the alias type derived from '
                                                                 'the alias type code, distinguishing '
                                                                 'medical record number from NHS number '
                                                                 'rows.',
                                         'identifier_value': 'Source identifier value.',
                                         'loaded_at': 'Arm-specific clock: map_person.ADC_UPDT for '
                                                      'Millennium person IDs; always null for both community '
                                                      'key and community number assignments; '
                                                      'map_pacs_patient_link.ADC_UPDT for PACS; '
                                                      'map_patient_identifier.PIPELINE_UPDT_DT_TM for '
                                                      'Millennium aliases; map_endobase_patient.ADC_UPDT for '
                                                      'EndoBase; map_cancer_ptl_linkage.ADC_UPDT for both '
                                                      'Pathfinder MRN and NHS assignments. The eight arms '
                                                      'are unioned without a cross-arm maximum or Silver '
                                                      'refresh timestamp.',
                                         'person_id': 'Resolved Millennium person identifier when available.',
                                         'person_identifier_key': 'Deterministic SHA-256 key of the '
                                                                  'identifier row, hashed from a namespace '
                                                                  'and the source identifier (Millennium '
                                                                  'person id, community patient key, or '
                                                                  'community database plus patient number). '
                                                                  'Not a source row id.',
                                         'source_identifier_id': 'B3 bronze field ROW_ID; source value '
                                                                 'retained unless documented as derived.',
                                         'source_object': 'Value describing source object for the person '
                                                          'identifier record. It is produced by the silver '
                                                          'transformation and has no direct bronze-column '
                                                          'lineage entry. Whitespace and source sentinel '
                                                          'text are retained unless the pipeline explicitly '
                                                          'normalizes them; null means no value was '
                                                          'supplied.',
                                         'source_row_id': 'Stable source row identifier.',
                                         'source_system': 'Source system name.',
                                         'source_table': 'Fully qualified bronze source table.',
                                         'status': 'Source assignment or matching status.',
                                         'subject_id_system': 'Identifier system used to derive subject_key.',
                                         'subject_key': 'deterministic join key over the strongest available '
                                                        'subject evidence.',
                                         'valid_from': 'Identifier validity start.',
                                         'valid_to': 'Identifier validity end.'},
                             'quantity_override': False,
                             'table': 'One source identifier assignment with an always-populated subject '
                                      'key. Gold QC twin of the silver product: 4 columns are repaired or '
                                      'nulled, 2 rule(s) drop rows, 3 check(s) are advisory. Each rule '
                                      'states its reason in the pipeline notebook, and Lakeflow expectation '
                                      'metrics report what every rule matched on each update.'}}
