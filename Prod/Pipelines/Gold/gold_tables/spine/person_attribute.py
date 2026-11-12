"""Gold spine.person_attribute: 2 dataset(s).

- datasets: _qc_reference_person_attribute, reference_person_attribute
- moved verbatim from: 1_gold_spine_and_reference:3546, 1_gold_spine_and_reference:3570, 1_gold_spine_and_reference:3603, 1_gold_spine_and_reference:3608, 1_gold_spine_and_reference:3612, 1_gold_spine_and_reference:3629

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.person_attribute ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_PERSON_ATTRIBUTE_SELECT = [
    '`person_attribute_key` AS `person_attribute_key`',
    '`person_id` AS `person_id`',
    '`source_person_id` AS `source_person_id`',
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
REFERENCE_PERSON_ATTRIBUTE_MANDATORY_RULES = {
    # The flow nulls person_id when it names a parent the spine does not have, and this rule
    # then drops the row, because the row is only an attribute of a person, so with no
    # person it describes nothing. That was 331 orphaned rows plus 0 that already had no
    # person_id, out of 9,775,723.
    "gold.reference.person_attribute.person_id.fk_containment": "`person_id` IS NOT NULL",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
REFERENCE_PERSON_ATTRIBUTE_PERSON_SCOPE_RULES = {
    "gold.reference.person_attribute.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
REFERENCE_PERSON_ATTRIBUTE_ORPHAN_RULES = {
    "gold.reference.person_attribute.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._reference_person_attribute"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(REFERENCE_PERSON_ATTRIBUTE_MANDATORY_RULES)
@dp.expect_all_or_drop(REFERENCE_PERSON_ATTRIBUTE_PERSON_SCOPE_RULES)
@dp.expect_all(REFERENCE_PERSON_ATTRIBUTE_ORPHAN_RULES)
def _gold_qc_reference_person_attribute():
    """Quality-controlled twin of journey_reference.person_attribute."""
    df = _qc(
        "reference_person_attribute",
        REFERENCE_PERSON_ATTRIBUTE_SELECT,
        fk_columns=["person_id"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_reference.person_attribute"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_person_attribute():
    """Contract-v2 public twin of reference_person_attribute; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_person_attribute")).selectExpr(
        '`person_attribute_key` AS `person_attribute_key`',
        '`person_id` AS `person_id`',
        '`source_person_id` AS `source_person_id`',
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
    'reference_person_attribute': GoldRules(
        table='reference_person_attribute',
        select=REFERENCE_PERSON_ATTRIBUTE_SELECT,
        mandatory=REFERENCE_PERSON_ATTRIBUTE_MANDATORY_RULES,
        person_scope=REFERENCE_PERSON_ATTRIBUTE_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['person_attribute_key'], 'support_columns': ['loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_person_attribute': {'columns': {'_source_row_id': 'Unique source primary key of the '
                                                                  'originating mill_person_info row, carried '
                                                                  'through as the row grain of this table.',
                                                '_source_system': 'Value describing source system for the '
                                                                  'person attribute record. It is produced '
                                                                  'by the silver transformation and has no '
                                                                  'direct bronze-column lineage entry. '
                                                                  'Whitespace and source sentinel text are '
                                                                  'retained unless the pipeline explicitly '
                                                                  'normalizes them; null means no value was '
                                                                  'supplied.',
                                                '_source_table': 'Value describing source table for the '
                                                                 'person attribute record. It is produced by '
                                                                 'the silver transformation and has no '
                                                                 'direct bronze-column lineage entry. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                                'active_ind': 'Indicator of whether the source row is in an '
                                                              'active state or has been made inactive, such '
                                                              'as logically deleted or combined away.',
                                                'answered_ind': 'Flag indicating whether a coded value was '
                                                                'actually recorded for the attribute rather '
                                                                'than left unanswered.',
                                                'attribute_name': 'Standardised label for the person '
                                                                  'attribute represented by the row, derived '
                                                                  'from the source information sub-type '
                                                                  'code.',
                                                'beg_effective': 'Row begin effective date/time after '
                                                                 'removal of sentinel and implausible '
                                                                 'values, leaving null where no usable value '
                                                                 'remains.',
                                                'current_ind': 'Flag marking the latest active row for each '
                                                               'person and attribute sub-type within the '
                                                               'retained history.',
                                                'end_effective': 'Row end effective date/time after removal '
                                                                 'of sentinel and open-ended placeholder '
                                                                 'values, leaving null where no usable value '
                                                                 'remains.',
                                                'link_status': "Indicator of whether the attribute row's "
                                                               'person identifier matched a person in the '
                                                               'curated person foundation table.',
                                                'loaded_at': 'map_person_attribute.PIPELINE_UPDT_DT_TM '
                                                             'carried unchanged through the shared attribute '
                                                             'projection. This is the contributing bronze '
                                                             "write clock, not the attribute's effective "
                                                             'dates, clinical value time or Silver refresh '
                                                             'time.',
                                                'person_attribute_key': 'Deterministic SHA-256 key for the '
                                                                        'person attribute row.',
                                                'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                                'source_person_id': 'Internal system-assigned person '
                                                                    'identifier from the source person '
                                                                    'table, linking the attribute row to a '
                                                                    'person.',
                                                'value_code': 'Coded value recorded for the attribute, where '
                                                              'zero indicates the attribute was not answered '
                                                              'and is never decoded.',
                                                'value_code_set': 'Code set of the decoded coded value as '
                                                                  'held in the reference code value lookup.',
                                                'value_datetime': 'Attribute date/time after removal of '
                                                                  'sentinel and implausible values, leaving '
                                                                  'null where no usable value remains.',
                                                'value_display': 'The display string for the code_value',
                                                'value_kind': 'Indicator of the value representation '
                                                              'expected for the attribute, such as a coded '
                                                              'value, date or numeric value.',
                                                'value_numeric': 'If the comment is of a numeric type, it is '
                                                                 'stored in this attribute'},
                                    'quantity_override': True,
                                    'table': 'Internal quality-controlled twin of '
                                             'reference_person_attribute: the Gold repairs, nulls and '
                                             'expectations are applied here and the published Gold MV reads '
                                             'this object. Materialized rather than temporary so the '
                                             'published MV can refresh incrementally.'},
 'reference_person_attribute': {'columns': {'_source_row_id': 'Unique source primary key of the originating '
                                                              'mill_person_info row, carried through as the '
                                                              'row grain of this table.',
                                            '_source_system': 'Value describing source system for the person '
                                                              'attribute record. It is produced by the '
                                                              'silver transformation and has no direct '
                                                              'bronze-column lineage entry. Whitespace and '
                                                              'source sentinel text are retained unless the '
                                                              'pipeline explicitly normalizes them; null '
                                                              'means no value was supplied.',
                                            '_source_table': 'Value describing source table for the person '
                                                             'attribute record. It is produced by the silver '
                                                             'transformation and has no direct bronze-column '
                                                             'lineage entry. Whitespace and source sentinel '
                                                             'text are retained unless the pipeline '
                                                             'explicitly normalizes them; null means no '
                                                             'value was supplied.',
                                            'active_ind': 'Indicator of whether the source row is in an '
                                                          'active state or has been made inactive, such as '
                                                          'logically deleted or combined away.',
                                            'answered_ind': 'Flag indicating whether a coded value was '
                                                            'actually recorded for the attribute rather than '
                                                            'left unanswered.',
                                            'attribute_name': 'Standardised label for the person attribute '
                                                              'represented by the row, derived from the '
                                                              'source information sub-type code.',
                                            'beg_effective': 'Row begin effective date/time after removal of '
                                                             'sentinel and implausible values, leaving null '
                                                             'where no usable value remains.',
                                            'current_ind': 'Flag marking the latest active row for each '
                                                           'person and attribute sub-type within the '
                                                           'retained history.',
                                            'end_effective': 'Row end effective date/time after removal of '
                                                             'sentinel and open-ended placeholder values, '
                                                             'leaving null where no usable value remains.',
                                            'link_status': "Indicator of whether the attribute row's person "
                                                           'identifier matched a person in the curated '
                                                           'person foundation table.',
                                            'loaded_at': 'map_person_attribute.PIPELINE_UPDT_DT_TM carried '
                                                         'unchanged through the shared attribute projection. '
                                                         'This is the contributing bronze write clock, not '
                                                         "the attribute's effective dates, clinical value "
                                                         'time or Silver refresh time.',
                                            'person_attribute_key': 'Deterministic SHA-256 key for the '
                                                                    'person attribute row.',
                                            'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                            'source_person_id': 'Internal system-assigned person identifier '
                                                                'from the source person table, linking the '
                                                                'attribute row to a person.',
                                            'value_code': 'Coded value recorded for the attribute, where '
                                                          'zero indicates the attribute was not answered and '
                                                          'is never decoded.',
                                            'value_code_set': 'Code set of the decoded coded value as held '
                                                              'in the reference code value lookup.',
                                            'value_datetime': 'Attribute date/time after removal of sentinel '
                                                              'and implausible values, leaving null where no '
                                                              'usable value remains.',
                                            'value_display': 'The display string for the code_value',
                                            'value_kind': 'Indicator of the value representation expected '
                                                          'for the attribute, such as a coded value, date or '
                                                          'numeric value.',
                                            'value_numeric': 'If the comment is of a numeric type, it is '
                                                             'stored in this attribute'},
                                'quantity_override': False,
                                'table': 'One allowlisted person attribute; no-fixed-abode and public-care '
                                         'flags are IG-sensitive. Gold QC twin of the silver product: 2 '
                                         'columns are repaired or nulled, 1 rule(s) drop rows, 0 check(s) '
                                         'are advisory. Each rule states its reason in the pipeline '
                                         'notebook, and Lakeflow expectation metrics report what every rule '
                                         'matched on each update.'}}
