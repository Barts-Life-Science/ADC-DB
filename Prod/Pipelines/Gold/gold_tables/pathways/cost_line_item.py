"""Gold pathways.cost_line_item: 2 dataset(s).

- datasets: _qc_reference_cost_line_item, reference_cost_line_item
- moved verbatim from: 4_gold_care_pathways:4005, 4_gold_care_pathways:4023, 4_gold_care_pathways:4028, 4_gold_care_pathways:4036, 4_gold_care_pathways:4079, 4_gold_care_pathways:4091

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.cost_line_item ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_COST_LINE_ITEM_SELECT = [
    '`cost_line_item_key` AS `cost_line_item_key`',
    '`costed_activity_key` AS `costed_activity_key`',
    '`extract_cd` AS `extract_cd`',
    '`activity_record_id` AS `activity_record_id`',
    '`line_hash` AS `line_hash`',
    '`activity_cost_item_cd` AS `activity_cost_item_cd`',
    '`resource_cost_item_cd` AS `resource_cost_item_cd`',
    '`activity_count` AS `activity_count`',
    '`unbundled_subtype_cd` AS `unbundled_subtype_cd`',
    '`unbundled_currency_cd` AS `unbundled_currency_cd`',
    '`unbundled_currency_datetime` AS `unbundled_currency_datetime`',
    '`total_cost` AS `total_cost`',
    '`total_o_cost` AS `total_o_cost`',
    '`source_duplicate_count` AS `source_duplicate_count`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]


REFERENCE_COST_LINE_ITEM_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`activity_count_decimal` AS `activity_count_decimal`',
]


# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_COST_LINE_ITEM_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 181,295,498 rows of
    # 181,295,498 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(record_status = 'active')",
}


REFERENCE_COST_LINE_ITEM_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 181,295,498 at the profile.
    "gold.reference.cost_line_item.record_status.default_view_active":
        "record_status = 'active'",

    # Negative values here are signed by design -- credit lines in the costing feed and
    # below-zero readings on a calibrated scale -- so they are counted, not removed. Seen on
    # 11,112,901 of 181,295,498 rows (6.13%) when profiled on 2026-08-24.
    "gold.reference.cost_line_item.total_cost.negative_mass_scale_owner":
        "NOT COALESCE((`total_cost` < 0), FALSE)",

    # Negative values here are signed by design -- credit lines in the costing feed and
    # below-zero readings on a calibrated scale -- so they are counted, not removed. Seen on
    # 16,457,918 of 181,295,498 rows (9.08%) when profiled on 2026-08-24.
    "gold.reference.cost_line_item.total_o_cost.negative_mass_scale_owner":
        "NOT COALESCE((`total_o_cost` < 0), FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._reference_cost_line_item"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(REFERENCE_COST_LINE_ITEM_MANDATORY_RULES)
@dp.expect_all(REFERENCE_COST_LINE_ITEM_ADVISORY_RULES)
def _gold_qc_reference_cost_line_item():
    """Quality-controlled twin of journey_reference.cost_line_item."""
    df = _qc("reference_cost_line_item", REFERENCE_COST_LINE_ITEM_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_reference.cost_line_item"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_cost_line_item():
    """Contract-v2 public twin of reference_cost_line_item; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_cost_line_item")).selectExpr(
        '`cost_line_item_key` AS `cost_line_item_key`',
        '`costed_activity_key` AS `costed_activity_key`',
        '`extract_cd` AS `extract_cd`',
        '`activity_record_id` AS `activity_record_id`',
        '`line_hash` AS `line_hash`',
        '`activity_cost_item_cd` AS `activity_cost_item_cd`',
        '`resource_cost_item_cd` AS `resource_cost_item_cd`',
        '`activity_count` AS `activity_count`',
        '`unbundled_subtype_cd` AS `unbundled_subtype_cd`',
        '`unbundled_currency_cd` AS `unbundled_currency_cd`',
        '`unbundled_currency_datetime` AS `unbundled_currency_datetime`',
        '`total_cost` AS `total_cost`',
        '`total_o_cost` AS `total_o_cost`',
        '`source_duplicate_count` AS `source_duplicate_count`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
        '`activity_count_decimal` AS `activity_count_decimal`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'reference_cost_line_item': GoldRules(
        table='reference_cost_line_item',
        select=REFERENCE_COST_LINE_ITEM_SELECT,
        mandatory=REFERENCE_COST_LINE_ITEM_MANDATORY_RULES,
        advisory=REFERENCE_COST_LINE_ITEM_ADVISORY_RULES,
        lifecycle={'join_keys': ['cost_line_item_key'], 'support_columns': ['record_status', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_cost_line_item': {'columns': {'activity_cost_item_cd': 'National Cost Collection activity '
                                                                       'cost item code for the cost line, '
                                                                       'preserved as supplied by the source '
                                                                       'extract without mapping.',
                                              'activity_count': 'Source-reported activity count for the cost '
                                                                'line, retained as text as supplied by the '
                                                                'extract.',
                                              'activity_count_decimal': 'activity_count as DECIMAL(18,4); '
                                                                        'NULL when the source text is not a '
                                                                        'number. INSR_W2 W2.11 (L5028).',
                                              'activity_record_id': 'SLAM ACTIVITY_RECORD_ID as BIGINT; '
                                                                    'joins with extract_cd to the parent '
                                                                    'native key.',
                                              'cost_line_item_key': 'Deterministic SHA-256 key for this SLAM '
                                                                    'cost line.',
                                              'costed_activity_key': 'Deterministic SHA-256 key for the '
                                                                     'parent costed activity.',
                                              'extract_cd': 'Source costing extract discriminator; required '
                                                            'in every cost-line join.',
                                              'line_hash': 'Content-hash key derived from the source line '
                                                           'attributes, used to identify a distinct cost '
                                                           'line after collapsing exact duplicates.',
                                              'loaded_at': 'map_slam_cost_line_item.ADC_UPDT carried '
                                                           'unchanged for the cost-line row. The parent '
                                                           'activity key is derived without a parent join or '
                                                           'added parent clock; this is bronze load '
                                                           'provenance, not the currency date/time or Silver '
                                                           'refresh time.',
                                              'record_status': 'Retracted when '
                                                               'map_slam_cost_line_item.SOURCE_PRESENT_IND '
                                                               'is false, otherwise active; a missing '
                                                               'presence flag defaults true. No superseded '
                                                               'status is emitted, and the value is a '
                                                               'derived STRING label rather than the '
                                                               'original Boolean presence flag.',
                                              'resource_cost_item_cd': 'National Cost Collection resource '
                                                                       'cost item code for the cost line, '
                                                                       'preserved as supplied by the source '
                                                                       'extract without mapping.',
                                              'source_duplicate_count': 'Exact source rows represented by '
                                                                        'this collapsed line.',
                                              'total_cost': 'Total cost reported by the source extract for '
                                                            'the cost line, retained at exact decimal(36,18) '
                                                            'precision.',
                                              'total_o_cost': 'Component of the line total cost reported by '
                                                              'the source extract, retained at exact '
                                                              'decimal(36,18) precision.',
                                              'unbundled_currency_cd': 'Unbundled currency code recorded '
                                                                       'against the cost line by the source '
                                                                       'extract, null on ordinary bundled '
                                                                       'lines.',
                                              'unbundled_currency_datetime': 'Date and time recorded against '
                                                                             'the unbundled currency for the '
                                                                             'cost line, as reported by the '
                                                                             'source extract.',
                                              'unbundled_subtype_cd': 'Observed values: 1 high-cost drug, 2 '
                                                                      'device, 3 diagnostic/other, 0 '
                                                                      'default; null on ordinary lines.'},
                                  'quantity_override': True,
                                  'table': 'Internal quality-controlled twin of reference_cost_line_item: '
                                           'the Gold repairs, nulls and expectations are applied here and '
                                           'the published Gold MV reads this object. Materialized rather '
                                           'than temporary so the published MV can refresh incrementally.'},
 'reference_cost_line_item': {'columns': {'activity_cost_item_cd': 'National Cost Collection activity cost '
                                                                   'item code for the cost line, preserved '
                                                                   'as supplied by the source extract '
                                                                   'without mapping.',
                                          'activity_count': 'Source-reported activity count for the cost '
                                                            'line, retained as text as supplied by the '
                                                            'extract.',
                                          'activity_count_decimal': 'activity_count as DECIMAL(18,4); NULL '
                                                                    'when the source text is not a number. '
                                                                    'INSR_W2 W2.11 (L5028).',
                                          'activity_record_id': 'SLAM ACTIVITY_RECORD_ID as BIGINT; joins '
                                                                'with extract_cd to the parent native key.',
                                          'cost_line_item_key': 'Deterministic SHA-256 key for this SLAM '
                                                                'cost line.',
                                          'costed_activity_key': 'Deterministic SHA-256 key for the parent '
                                                                 'costed activity.',
                                          'extract_cd': 'Source costing extract discriminator; required in '
                                                        'every cost-line join.',
                                          'line_hash': 'Content-hash key derived from the source line '
                                                       'attributes, used to identify a distinct cost line '
                                                       'after collapsing exact duplicates.',
                                          'loaded_at': 'map_slam_cost_line_item.ADC_UPDT carried unchanged '
                                                       'for the cost-line row. The parent activity key is '
                                                       'derived without a parent join or added parent clock; '
                                                       'this is bronze load provenance, not the currency '
                                                       'date/time or Silver refresh time.',
                                          'record_status': 'Retracted when '
                                                           'map_slam_cost_line_item.SOURCE_PRESENT_IND is '
                                                           'false, otherwise active; a missing presence flag '
                                                           'defaults true. No superseded status is emitted, '
                                                           'and the value is a derived STRING label rather '
                                                           'than the original Boolean presence flag.',
                                          'resource_cost_item_cd': 'National Cost Collection resource cost '
                                                                   'item code for the cost line, preserved '
                                                                   'as supplied by the source extract '
                                                                   'without mapping.',
                                          'source_duplicate_count': 'Exact source rows represented by this '
                                                                    'collapsed line.',
                                          'total_cost': 'Total cost reported by the source extract for the '
                                                        'cost line, retained at exact decimal(36,18) '
                                                        'precision.',
                                          'total_o_cost': 'Component of the line total cost reported by the '
                                                          'source extract, retained at exact decimal(36,18) '
                                                          'precision.',
                                          'unbundled_currency_cd': 'Unbundled currency code recorded against '
                                                                   'the cost line by the source extract, '
                                                                   'null on ordinary bundled lines.',
                                          'unbundled_currency_datetime': 'Date and time recorded against the '
                                                                         'unbundled currency for the cost '
                                                                         'line, as reported by the source '
                                                                         'extract.',
                                          'unbundled_subtype_cd': 'Observed values: 1 high-cost drug, 2 '
                                                                  'device, 3 diagnostic/other, 0 default; '
                                                                  'null on ordinary lines.'},
                              'quantity_override': False,
                              'table': 'One frozen PLICS cost line; excluded from the event plane. Gold QC '
                                       'twin of the silver product: 0 columns are repaired or nulled, 1 '
                                       'rule(s) drop rows, 3 check(s) are advisory. Each rule states its '
                                       'reason in the pipeline notebook, and Lakeflow expectation metrics '
                                       'report what every rule matched on each update. Coverage: Frozen '
                                       'feeds with no rows in the last 12 months: condition stage to '
                                       '2023-10, costed activity and cost line items to 2022-03, device to '
                                       '2020-01; complete-through month per release: '
                                       '6_mgmt.data_quality.release_finding (finding_code '
                                       'F2_complete_through).'}}
