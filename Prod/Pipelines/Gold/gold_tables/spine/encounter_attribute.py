"""Gold spine.encounter_attribute: 2 dataset(s).

- datasets: _qc_reference_encounter_attribute, reference_encounter_attribute
- moved verbatim from: 1_gold_spine_and_reference:2545, 1_gold_spine_and_reference:2569, 1_gold_spine_and_reference:2602, 1_gold_spine_and_reference:2606, 1_gold_spine_and_reference:2622

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.encounter_attribute ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_ENCOUNTER_ATTRIBUTE_SELECT = [
    '`encounter_attribute_key` AS `encounter_attribute_key`',
    '`encounter_id` AS `encounter_id`',
    '`source_encounter_id` AS `source_encounter_id`',
    '`attribute_name` AS `attribute_name`',
    '`value_kind` AS `value_kind`',
    "CASE WHEN UPPER(TRIM(CAST(`value_code` AS STRING))) = '0' THEN NULL ELSE `value_code` END AS `value_code`",
    '`value_display` AS `value_display`',
    '`value_code_set` AS `value_code_set`',
    '`value_datetime` AS `value_datetime`',
    '`value_numeric` AS `value_numeric`',
    '`answered_ind` AS `answered_ind`',
    '`active_ind` AS `active_ind`',
    '`current_ind` AS `current_ind`',
    '`beg_effective` AS `beg_effective`',
    '`end_effective` AS `end_effective`',
    '`link_status` AS `link_status`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_ENCOUNTER_ATTRIBUTE_MANDATORY_RULES = {
    # The flow nulls encounter_id when it names a parent the spine does not have, and this
    # rule then drops the row, because the row is only an attribute of an encounter, so with
    # no encounter it describes nothing. That was 203,581 orphaned rows plus 0 that already
    # had no encounter_id, out of 87,111,763.
    "gold.reference.encounter_attribute.encounter_id.fk_containment":
        "`encounter_id` IS NOT NULL",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
REFERENCE_ENCOUNTER_ATTRIBUTE_ORPHAN_RULES = {
    "gold.reference.encounter_attribute.encounter_id.orphan_nulled": "NOT COALESCE(`_qc_encounter_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._reference_encounter_attribute"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(REFERENCE_ENCOUNTER_ATTRIBUTE_MANDATORY_RULES)
@dp.expect_all(REFERENCE_ENCOUNTER_ATTRIBUTE_ORPHAN_RULES)
def _gold_qc_reference_encounter_attribute():
    """Quality-controlled twin of journey_reference.encounter_attribute."""
    df = _qc(
        "reference_encounter_attribute",
        REFERENCE_ENCOUNTER_ATTRIBUTE_SELECT,
        fk_columns=["encounter_id"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_reference.encounter_attribute"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_encounter_attribute():
    """Contract-v2 public twin of reference_encounter_attribute; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_encounter_attribute")).selectExpr(
        '`encounter_attribute_key` AS `encounter_attribute_key`',
        '`encounter_id` AS `encounter_id`',
        '`source_encounter_id` AS `source_encounter_id`',
        '`attribute_name` AS `attribute_name`',
        '`value_kind` AS `value_kind`',
        '`value_code` AS `value_code`',
        '`value_display` AS `value_display`',
        '`value_code_set` AS `value_code_set`',
        '`value_datetime` AS `value_datetime`',
        '`value_numeric` AS `value_numeric`',
        '`answered_ind` AS `answered_ind`',
        '`active_ind` AS `active_ind`',
        '`current_ind` AS `current_ind`',
        '`beg_effective` AS `beg_effective`',
        '`end_effective` AS `end_effective`',
        '`link_status` AS `link_status`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'reference_encounter_attribute': GoldRules(
        table='reference_encounter_attribute',
        select=REFERENCE_ENCOUNTER_ATTRIBUTE_SELECT,
        mandatory=REFERENCE_ENCOUNTER_ATTRIBUTE_MANDATORY_RULES,
        lifecycle={'join_keys': ['encounter_attribute_key'], 'support_columns': ['loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_encounter_attribute': {'columns': {'_source_row_id': 'Primary key of the source '
                                                                     'mill_encntr_info row (ENCNTR_INFO_ID), '
                                                                     'giving one row per allowlisted '
                                                                     'encounter information record.',
                                                   '_source_system': 'Value describing source system for the '
                                                                     'encounter attribute record. It is '
                                                                     'produced by the silver transformation '
                                                                     'and has no direct bronze-column '
                                                                     'lineage entry. Whitespace and source '
                                                                     'sentinel text are retained unless the '
                                                                     'pipeline explicitly normalizes them; '
                                                                     'null means no value was supplied.',
                                                   '_source_table': 'Value describing source table for the '
                                                                    'encounter attribute record. It is '
                                                                    'produced by the silver transformation '
                                                                    'and has no direct bronze-column lineage '
                                                                    'entry. Whitespace and source sentinel '
                                                                    'text are retained unless the pipeline '
                                                                    'explicitly normalizes them; null means '
                                                                    'no value was supplied.',
                                                   'active_ind': 'Source indicator of whether the row is '
                                                                 'active or in an inactive state such as '
                                                                 'logically deleted or combined away.',
                                                   'answered_ind': 'Flag indicating whether the row carries '
                                                                   'an answered coded value rather than the '
                                                                   'unanswered zero code.',
                                                   'attribute_name': 'Standardised curated label for the '
                                                                     'encounter attribute, derived from the '
                                                                     'source information sub-type code.',
                                                   'beg_effective': 'BEG_EFFECTIVE_DT_TM with future-dated '
                                                                    'and sentinel values excluded, leaving '
                                                                    'only usable effective-from date and '
                                                                    'time values.',
                                                   'current_ind': 'Flag marking the latest active row for '
                                                                  'each encounter and information subtype '
                                                                  'within the retained history.',
                                                   'encounter_attribute_key': 'Deterministic SHA-256 key for '
                                                                              'the encounter attribute row.',
                                                   'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT; '
                                                                   'joins to spine_encounter.encounter_id.',
                                                   'end_effective': 'END_EFFECTIVE_DT_TM with future-dated '
                                                                    'and sentinel values excluded, leaving '
                                                                    'only usable effective-to date and time '
                                                                    'values.',
                                                   'link_status': "Indicator of whether the row's encounter "
                                                                  'identifier resolves to an encounter in '
                                                                  'the curated encounter foundation.',
                                                   'loaded_at': 'map_encounter_attribute.PIPELINE_UPDT_DT_TM '
                                                                'carried unchanged through the shared '
                                                                'attribute projection. This is the '
                                                                'contributing bronze write clock, not the '
                                                                "attribute's effective dates, clinical value "
                                                                'time or Silver refresh time.',
                                                   'source_encounter_id': 'Internal encounter identifier '
                                                                          'from the source encounter '
                                                                          'information row (ENCNTR_ID), '
                                                                          'linking the attribute to its '
                                                                          'encounter.',
                                                   'value_code': 'Coded value of the encounter information '
                                                                 'row, drawn from the code set applicable to '
                                                                 'the attribute, where zero denotes an '
                                                                 'unanswered value.',
                                                   'value_code_set': 'Code set to which VALUE_CD actually '
                                                                     'belongs, resolved from the code value '
                                                                     'lookup.',
                                                   'value_datetime': 'VALUE_DT_TM with future-dated and '
                                                                     'sentinel values excluded, leaving only '
                                                                     'usable date and time values.',
                                                   'value_display': 'The display string for the code_value',
                                                   'value_kind': 'Indicator of which value representation '
                                                                 'applies to the row, such as a coded, '
                                                                 'date/time or numeric value.',
                                                   'value_numeric': 'The numeric (non-codified) value of the '
                                                                    'encounter information.  If the value of '
                                                                    'the row necessitates storing a numeric '
                                                                    '(non-codified) value, it is placed in '
                                                                    'this column.'},
                                       'quantity_override': True,
                                       'table': 'Internal quality-controlled twin of '
                                                'reference_encounter_attribute: the Gold repairs, nulls and '
                                                'expectations are applied here and the published Gold MV '
                                                'reads this object. Materialized rather than temporary so '
                                                'the published MV can refresh incrementally.'},
 'reference_encounter_attribute': {'columns': {'_source_row_id': 'Primary key of the source mill_encntr_info '
                                                                 'row (ENCNTR_INFO_ID), giving one row per '
                                                                 'allowlisted encounter information record.',
                                               '_source_system': 'Value describing source system for the '
                                                                 'encounter attribute record. It is produced '
                                                                 'by the silver transformation and has no '
                                                                 'direct bronze-column lineage entry. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                               '_source_table': 'Value describing source table for the '
                                                                'encounter attribute record. It is produced '
                                                                'by the silver transformation and has no '
                                                                'direct bronze-column lineage entry. '
                                                                'Whitespace and source sentinel text are '
                                                                'retained unless the pipeline explicitly '
                                                                'normalizes them; null means no value was '
                                                                'supplied.',
                                               'active_ind': 'Source indicator of whether the row is active '
                                                             'or in an inactive state such as logically '
                                                             'deleted or combined away.',
                                               'answered_ind': 'Flag indicating whether the row carries an '
                                                               'answered coded value rather than the '
                                                               'unanswered zero code.',
                                               'attribute_name': 'Standardised curated label for the '
                                                                 'encounter attribute, derived from the '
                                                                 'source information sub-type code.',
                                               'beg_effective': 'BEG_EFFECTIVE_DT_TM with future-dated and '
                                                                'sentinel values excluded, leaving only '
                                                                'usable effective-from date and time values.',
                                               'current_ind': 'Flag marking the latest active row for each '
                                                              'encounter and information subtype within the '
                                                              'retained history.',
                                               'encounter_attribute_key': 'Deterministic SHA-256 key for the '
                                                                          'encounter attribute row.',
                                               'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT; joins '
                                                               'to spine_encounter.encounter_id.',
                                               'end_effective': 'END_EFFECTIVE_DT_TM with future-dated and '
                                                                'sentinel values excluded, leaving only '
                                                                'usable effective-to date and time values.',
                                               'link_status': "Indicator of whether the row's encounter "
                                                              'identifier resolves to an encounter in the '
                                                              'curated encounter foundation.',
                                               'loaded_at': 'map_encounter_attribute.PIPELINE_UPDT_DT_TM '
                                                            'carried unchanged through the shared attribute '
                                                            'projection. This is the contributing bronze '
                                                            "write clock, not the attribute's effective "
                                                            'dates, clinical value time or Silver refresh '
                                                            'time.',
                                               'source_encounter_id': 'Internal encounter identifier from '
                                                                      'the source encounter information row '
                                                                      '(ENCNTR_ID), linking the attribute to '
                                                                      'its encounter.',
                                               'value_code': 'Coded value of the encounter information row, '
                                                             'drawn from the code set applicable to the '
                                                             'attribute, where zero denotes an unanswered '
                                                             'value.',
                                               'value_code_set': 'Code set to which VALUE_CD actually '
                                                                 'belongs, resolved from the code value '
                                                                 'lookup.',
                                               'value_datetime': 'VALUE_DT_TM with future-dated and sentinel '
                                                                 'values excluded, leaving only usable date '
                                                                 'and time values.',
                                               'value_display': 'The display string for the code_value',
                                               'value_kind': 'Indicator of which value representation '
                                                             'applies to the row, such as a coded, date/time '
                                                             'or numeric value.',
                                               'value_numeric': 'The numeric (non-codified) value of the '
                                                                'encounter information.  If the value of the '
                                                                'row necessitates storing a numeric '
                                                                '(non-codified) value, it is placed in this '
                                                                'column.'},
                                   'quantity_override': False,
                                   'table': 'One allowlisted encounter attribute with current and effective '
                                            'evidence. Gold QC twin of the silver product: 2 columns are '
                                            'repaired or nulled, 1 rule(s) drop rows, 0 check(s) are '
                                            'advisory. Each rule states its reason in the pipeline notebook, '
                                            'and Lakeflow expectation metrics report what every rule matched '
                                            'on each update.'}}
