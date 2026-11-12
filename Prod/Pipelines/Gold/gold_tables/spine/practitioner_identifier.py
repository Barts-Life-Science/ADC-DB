"""Gold spine.practitioner_identifier: 2 dataset(s).

- datasets: _qc_reference_practitioner_identifier, reference_practitioner_identifier
- moved verbatim from: 1_gold_spine_and_reference:4069, 1_gold_spine_and_reference:4091, 1_gold_spine_and_reference:4098, 1_gold_spine_and_reference:4140, 1_gold_spine_and_reference:4152

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.practitioner_identifier ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_PRACTITIONER_IDENTIFIER_SELECT = [
    '`practitioner_identifier_key` AS `practitioner_identifier_key`',
    '`practitioner_id` AS `practitioner_id`',
    '`practitioner_person_id` AS `practitioner_person_id`',
    '`source_alias_id` AS `source_alias_id`',
    '`alias_type_meaning` AS `alias_type_meaning`',
    '`alias_type_display` AS `alias_type_display`',
    '`alias` AS `alias`',
    '`active_ind` AS `active_ind`',
    '`effective_now_ind` AS `effective_now_ind`',
    '`beg_effective` AS `beg_effective`',
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN (CAST(`end_effective` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`end_effective` AS DATE)) < 9999) OR CAST(`end_effective` AS DATE) = DATE'2100-01-01' THEN NULL ELSE `end_effective` END AS `end_effective`",
    "CASE WHEN UPPER(TRIM(CAST(`contributor_system_code` AS STRING))) = '0' THEN NULL ELSE `contributor_system_code` END AS `contributor_system_code`",
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_PRACTITIONER_IDENTIFIER_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 519,155 rows of 519,155 that
    # are current and attributable. Superseded versions and rows whose identity was never
    # resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(record_status = 'active')",
}


REFERENCE_PRACTITIONER_IDENTIFIER_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 2 of 519,155 rows
    # (0.000385%) when profiled on 2026-08-24.
    "gold.reference.practitioner_identifier.beg_effective.future_owner":
        "NOT COALESCE((CAST(`beg_effective` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 2 of 519,155 rows
    # (0.000385%) when profiled on 2026-08-24.
    "gold.reference.practitioner_identifier.end_effective.future_owner":
        "NOT COALESCE((CAST(`end_effective` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 519,155 at the profile.
    "gold.reference.practitioner_identifier.record_status.default_view_active":
        "record_status = 'active'",
}


@dp.materialized_view(
    name=_n("gold_qc._reference_practitioner_identifier"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(REFERENCE_PRACTITIONER_IDENTIFIER_MANDATORY_RULES)
@dp.expect_all(REFERENCE_PRACTITIONER_IDENTIFIER_ADVISORY_RULES)
def _gold_qc_reference_practitioner_identifier():
    """Quality-controlled twin of journey_reference.practitioner_identifier."""
    df = _qc("reference_practitioner_identifier", REFERENCE_PRACTITIONER_IDENTIFIER_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_reference.practitioner_identifier"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_practitioner_identifier():
    """Contract-v2 public twin of reference_practitioner_identifier; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_practitioner_identifier")).selectExpr(
        '`practitioner_identifier_key` AS `practitioner_identifier_key`',
        '`practitioner_id` AS `practitioner_id`',
        '`practitioner_person_id` AS `practitioner_person_id`',
        '`source_alias_id` AS `source_alias_id`',
        '`alias_type_meaning` AS `alias_type_meaning`',
        '`alias_type_display` AS `alias_type_display`',
        '`alias` AS `alias`',
        '`active_ind` AS `active_ind`',
        '`effective_now_ind` AS `effective_now_ind`',
        '`beg_effective` AS `beg_effective`',
        '`end_effective` AS `end_effective`',
        '`contributor_system_code` AS `contributor_system_code`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'reference_practitioner_identifier': GoldRules(
        table='reference_practitioner_identifier',
        select=REFERENCE_PRACTITIONER_IDENTIFIER_SELECT,
        mandatory=REFERENCE_PRACTITIONER_IDENTIFIER_MANDATORY_RULES,
        advisory=REFERENCE_PRACTITIONER_IDENTIFIER_ADVISORY_RULES,
        lifecycle={'join_keys': ['practitioner_identifier_key'], 'support_columns': ['record_status', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_practitioner_identifier': {'columns': {'_source_row_id': 'Native personnel-alias record '
                                                                         'identifier from `PRSNL_ALIAS_ID`, '
                                                                         'retained as a string.',
                                                       '_source_system': 'Value describing source system for '
                                                                         'the practitioner identifier '
                                                                         'record. It is produced by the '
                                                                         'silver transformation and has no '
                                                                         'direct bronze-column lineage '
                                                                         'entry. Whitespace and source '
                                                                         'sentinel text are retained unless '
                                                                         'the pipeline explicitly normalizes '
                                                                         'them; null means no value was '
                                                                         'supplied.',
                                                       '_source_table': 'Value describing source table for '
                                                                        'the practitioner identifier record. '
                                                                        'It is produced by the silver '
                                                                        'transformation and has no direct '
                                                                        'bronze-column lineage entry. '
                                                                        'Whitespace and source sentinel text '
                                                                        'are retained unless the pipeline '
                                                                        'explicitly normalizes them; null '
                                                                        'means no value was supplied.',
                                                       'active_ind': 'Source numeric active indicator for '
                                                                     'the personnel alias, carried from '
                                                                     '`ACTIVE_IND` without boolean '
                                                                     'conversion.',
                                                       'alias': 'Raw personnel alias value.',
                                                       'alias_type_display': 'Display for the alias type.',
                                                       'alias_type_meaning': 'CDF meaning for the alias '
                                                                             'type.',
                                                       'beg_effective': 'Alias begin-effective timestamp.',
                                                       'contributor_system_code': 'Alias contributor-system '
                                                                                  'code.',
                                                       'effective_now_ind': 'Whether the alias is effective '
                                                                            'at pipeline time.',
                                                       'end_effective': 'Alias end-effective timestamp.',
                                                       'loaded_at': 'map_medical_personnel_alias.ADC_UPDT '
                                                                    'carried unchanged for the alias row. '
                                                                    'This is bronze load provenance, not the '
                                                                    'alias begin/end-effective timestamp or '
                                                                    'Silver refresh time.',
                                                       'practitioner_id': 'Native Millennium personnel '
                                                                          'PERSON_ID as BIGINT; joins to '
                                                                          'reference_practitioner.practitioner_id.',
                                                       'practitioner_identifier_key': 'Deterministic SHA-256 '
                                                                                      'key for the '
                                                                                      'practitioner alias '
                                                                                      'row.',
                                                       'practitioner_person_id': 'Source-system identifier '
                                                                                 'for the practitioner '
                                                                                 'person associated with '
                                                                                 'each practitioner '
                                                                                 'identifier record. It is '
                                                                                 'derived from bronze field '
                                                                                 '`PERSON_ID` in '
                                                                                 '`4_prod.bronze.map_medical_personnel_alias`. '
                                                                                 'Its uniqueness is limited '
                                                                                 'to the originating source '
                                                                                 'namespace; null means no '
                                                                                 'identifier was supplied.',
                                                       'record_status': 'Retracted when '
                                                                        'map_medical_personnel_alias.SOURCE_PRESENT_IND '
                                                                        'is false, otherwise active; a '
                                                                        'missing presence flag defaults '
                                                                        'true. ACTIVE_IND, EFFECTIVE_NOW_IND '
                                                                        'and alias validity dates do not '
                                                                        'determine this label, and no '
                                                                        'superseded status is emitted.',
                                                       'source_alias_id': 'Native personnel-alias record '
                                                                          'identifier from `PRSNL_ALIAS_ID`, '
                                                                          'retained as a string.'},
                                           'quantity_override': True,
                                           'table': 'Internal quality-controlled twin of '
                                                    'reference_practitioner_identifier: the Gold repairs, '
                                                    'nulls and expectations are applied here and the '
                                                    'published Gold MV reads this object. Materialized '
                                                    'rather than temporary so the published MV can refresh '
                                                    'incrementally.'},
 'reference_practitioner_identifier': {'columns': {'_source_row_id': 'Native personnel-alias record '
                                                                     'identifier from `PRSNL_ALIAS_ID`, '
                                                                     'retained as a string.',
                                                   '_source_system': 'Value describing source system for the '
                                                                     'practitioner identifier record. It is '
                                                                     'produced by the silver transformation '
                                                                     'and has no direct bronze-column '
                                                                     'lineage entry. Whitespace and source '
                                                                     'sentinel text are retained unless the '
                                                                     'pipeline explicitly normalizes them; '
                                                                     'null means no value was supplied.',
                                                   '_source_table': 'Value describing source table for the '
                                                                    'practitioner identifier record. It is '
                                                                    'produced by the silver transformation '
                                                                    'and has no direct bronze-column lineage '
                                                                    'entry. Whitespace and source sentinel '
                                                                    'text are retained unless the pipeline '
                                                                    'explicitly normalizes them; null means '
                                                                    'no value was supplied.',
                                                   'active_ind': 'Source numeric active indicator for the '
                                                                 'personnel alias, carried from `ACTIVE_IND` '
                                                                 'without boolean conversion.',
                                                   'alias': 'Raw personnel alias value.',
                                                   'alias_type_display': 'Display for the alias type.',
                                                   'alias_type_meaning': 'CDF meaning for the alias type.',
                                                   'beg_effective': 'Alias begin-effective timestamp.',
                                                   'contributor_system_code': 'Alias contributor-system '
                                                                              'code.',
                                                   'effective_now_ind': 'Whether the alias is effective at '
                                                                        'pipeline time.',
                                                   'end_effective': 'Alias end-effective timestamp.',
                                                   'loaded_at': 'map_medical_personnel_alias.ADC_UPDT '
                                                                'carried unchanged for the alias row. This '
                                                                'is bronze load provenance, not the alias '
                                                                'begin/end-effective timestamp or Silver '
                                                                'refresh time.',
                                                   'practitioner_id': 'Native Millennium personnel PERSON_ID '
                                                                      'as BIGINT; joins to '
                                                                      'reference_practitioner.practitioner_id.',
                                                   'practitioner_identifier_key': 'Deterministic SHA-256 key '
                                                                                  'for the practitioner '
                                                                                  'alias row.',
                                                   'practitioner_person_id': 'Source-system identifier for '
                                                                             'the practitioner person '
                                                                             'associated with each '
                                                                             'practitioner identifier '
                                                                             'record. It is derived from '
                                                                             'bronze field `PERSON_ID` in '
                                                                             '`4_prod.bronze.map_medical_personnel_alias`. '
                                                                             'Its uniqueness is limited to '
                                                                             'the originating source '
                                                                             'namespace; null means no '
                                                                             'identifier was supplied.',
                                                   'record_status': 'Retracted when '
                                                                    'map_medical_personnel_alias.SOURCE_PRESENT_IND '
                                                                    'is false, otherwise active; a missing '
                                                                    'presence flag defaults true. '
                                                                    'ACTIVE_IND, EFFECTIVE_NOW_IND and alias '
                                                                    'validity dates do not determine this '
                                                                    'label, and no superseded status is '
                                                                    'emitted.',
                                                   'source_alias_id': 'Native personnel-alias record '
                                                                      'identifier from `PRSNL_ALIAS_ID`, '
                                                                      'retained as a string.'},
                                       'quantity_override': False,
                                       'table': 'One staff alias assignment; alias values can include '
                                                'IG-sensitive staff postcodes. Gold QC twin of the silver '
                                                'product: 2 columns are repaired or nulled, 1 rule(s) drop '
                                                'rows, 3 check(s) are advisory. Each rule states its reason '
                                                'in the pipeline notebook, and Lakeflow expectation metrics '
                                                'report what every rule matched on each update.'}}
