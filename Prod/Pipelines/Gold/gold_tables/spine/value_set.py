"""Gold spine.value_set: 2 dataset(s).

- datasets: _qc_reference_value_set, reference_value_set
- moved verbatim from: 1_gold_spine_and_reference:5072, 1_gold_spine_and_reference:5098, 1_gold_spine_and_reference:5108

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.value_set ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_VALUE_SET_SELECT = [
    '`value_set_key` AS `value_set_key`',
    '`canonical_url` AS `canonical_url`',
    '`version` AS `version`',
    '`member_system` AS `member_system`',
    "CASE WHEN UPPER(TRIM(CAST(`member_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `member_code` END AS `member_code`",
    '`member_display` AS `member_display`',
    '`status` AS `status`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`loaded_at` AS `loaded_at`',
]


@dp.materialized_view(
    name=_n("gold_qc._reference_value_set"),
    refresh_policy="auto",
)
def _gold_qc_reference_value_set():
    """Quality-controlled twin of journey_reference.value_set."""
    df = _qc("reference_value_set", REFERENCE_VALUE_SET_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_reference.value_set"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_value_set():
    """Contract-v2 public twin of reference_value_set; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_value_set")).selectExpr(
        '`value_set_key` AS `value_set_key`',
        '`canonical_url` AS `canonical_url`',
        '`version` AS `version`',
        '`member_system` AS `member_system`',
        '`member_code` AS `member_code`',
        '`member_display` AS `member_display`',
        '`status` AS `status`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'reference_value_set': GoldRules(
        table='reference_value_set',
        select=REFERENCE_VALUE_SET_SELECT,
        lifecycle={'join_keys': ['value_set_key'], 'support_columns': ['load_batch_id', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_value_set': {'columns': {'canonical_url': 'Canonical value-set URL.',
                                         'loaded_at': '3_lookup.omop.value_set_release.updated_at carried '
                                                      'unchanged for the value-set membership row. This is '
                                                      "the lookup's update timestamp, not a bronze ADC_UPDT "
                                                      'clock, release identifier or Silver refresh time; a '
                                                      'missing value remains null.',
                                         'member_code': 'Source-system code representing member for the '
                                                        'value set record. It is produced by the silver '
                                                        'transformation and has no direct bronze-column '
                                                        'lineage entry. Source code meanings and sentinel '
                                                        'values are retained unless the pipeline explicitly '
                                                        'maps them; null means no code was supplied.',
                                         'member_display': 'Human-readable member label for the value set '
                                                           'record. It is produced by the silver '
                                                           'transformation and has no direct bronze-column '
                                                           'lineage entry. Whitespace and source sentinel '
                                                           'text are retained unless the pipeline explicitly '
                                                           'normalizes them; null means no value was '
                                                           'supplied.',
                                         'member_system': 'Member coding system.',
                                         'source_row_id': 'Stable source membership identity.',
                                         'source_table': 'Governed bronze value-set source.',
                                         'status': 'Value-set membership status.',
                                         'value_set_key': 'Deterministic SHA-256 value-set membership key.',
                                         'version': 'Pinned value-set version.'},
                             'quantity_override': True,
                             'table': 'Internal quality-controlled twin of reference_value_set: the Gold '
                                      'repairs, nulls and expectations are applied here and the published '
                                      'Gold MV reads this object. Materialized rather than temporary so the '
                                      'published MV can refresh incrementally.'},
 'reference_value_set': {'columns': {'canonical_url': 'Canonical value-set URL.',
                                     'loaded_at': '3_lookup.omop.value_set_release.updated_at carried '
                                                  'unchanged for the value-set membership row. This is the '
                                                  "lookup's update timestamp, not a bronze ADC_UPDT clock, "
                                                  'release identifier or Silver refresh time; a missing '
                                                  'value remains null.',
                                     'member_code': 'Source-system code representing member for the value '
                                                    'set record. It is produced by the silver transformation '
                                                    'and has no direct bronze-column lineage entry. Source '
                                                    'code meanings and sentinel values are retained unless '
                                                    'the pipeline explicitly maps them; null means no code '
                                                    'was supplied.',
                                     'member_display': 'Human-readable member label for the value set '
                                                       'record. It is produced by the silver transformation '
                                                       'and has no direct bronze-column lineage entry. '
                                                       'Whitespace and source sentinel text are retained '
                                                       'unless the pipeline explicitly normalizes them; null '
                                                       'means no value was supplied.',
                                     'member_system': 'Member coding system.',
                                     'source_row_id': 'Stable source membership identity.',
                                     'source_table': 'Governed bronze value-set source.',
                                     'status': 'Value-set membership status.',
                                     'value_set_key': 'Deterministic SHA-256 value-set membership key.',
                                     'version': 'Pinned value-set version.'},
                         'quantity_override': False,
                         'table': 'Pinned value-set membership materialized from the governed '
                                  'value_set_release lookup asset (FHIR R4 4.0.1 core sets, UK Core ethnic '
                                  'category, NHS RTT period status); v3-ServiceDeliveryLocationRoleType is a '
                                  'recorded v1 exclusion. Gold QC twin of the silver product: 1 columns are '
                                  'repaired or nulled, 0 check(s) are advisory. Each rule states its reason '
                                  'in the pipeline notebook, and Lakeflow expectation metrics report what '
                                  'every rule matched on each update.'}}
