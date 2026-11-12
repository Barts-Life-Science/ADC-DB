"""Gold spine.organization: 2 dataset(s).

- datasets: _qc_reference_organization, reference_organization
- moved verbatim from: 1_gold_spine_and_reference:3008, 1_gold_spine_and_reference:3023, 1_gold_spine_and_reference:3046, 1_gold_spine_and_reference:3056

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.organization ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_ORGANIZATION_SELECT = [
    '`organization_key` AS `organization_key`',
    '`organization_id` AS `organization_id`',
    '`source_organization_id` AS `source_organization_id`',
    '`name` AS `name`',
    '`status` AS `status`',
    '`valid_from` AS `valid_from`',
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN `valid_from` IS NOT NULL AND `valid_to` IS NOT NULL AND `valid_from` > `valid_to` THEN NULL ELSE CASE WHEN (CAST(`valid_to` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`valid_to` AS DATE)) < 9999) THEN NULL ELSE `valid_to` END END AS `valid_to`",
    '`address_city` AS `address_city`',
    '`address_postcode_masked` AS `address_postcode_masked`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`loaded_at` AS `loaded_at`',
]


REFERENCE_ORGANIZATION_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`member_source` AS `member_source`',
]


@dp.materialized_view(
    name=_n("gold_qc._reference_organization"),
    refresh_policy="auto",
)
def _gold_qc_reference_organization():
    """Quality-controlled twin of journey_reference.organization."""
    df = _qc("reference_organization", REFERENCE_ORGANIZATION_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_reference.organization"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_organization():
    """Contract-v2 public twin of reference_organization; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_organization")).selectExpr(
        '`organization_key` AS `organization_key`',
        '`organization_id` AS `organization_id`',
        '`source_organization_id` AS `source_organization_id`',
        '`name` AS `name`',
        '`status` AS `status`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`address_city` AS `address_city`',
        '`address_postcode_masked` AS `address_postcode_masked`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`loaded_at` AS `loaded_at`',
        '`member_source` AS `member_source`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'reference_organization': GoldRules(
        table='reference_organization',
        select=REFERENCE_ORGANIZATION_SELECT,
        lifecycle={'join_keys': ['organization_key'], 'support_columns': ['load_batch_id', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_organization': {'columns': {'address_city': 'Source organization address city.',
                                            'address_postcode_masked': 'Privacy-aware source postcode.',
                                            'loaded_at': 'Maximum map_care_site.ADC_UPDT over source rows '
                                                         'sharing the non-null ORGANIZATION_ID. The maximum '
                                                         'is not restricted to active source rows or the row '
                                                         'supplying the selected name/address; it records '
                                                         'contributing bronze ingestion, not organization '
                                                         'validity time or Silver refresh time.',
                                            'member_source': 'map_care_site for organisations with a '
                                                             'care-site row; in_use_fk for organisation ids '
                                                             'that encounters, appointments, medication '
                                                             'orders or ED tracking reference but '
                                                             'map_care_site lacks (name and address unknown, '
                                                             'status referenced_only). INSR_W2 W2.11 '
                                                             '(L4164).',
                                            'name': 'Source organization name.',
                                            'organization_id': 'Millennium ORGANIZATION_ID; primary key for '
                                                               'the Millennium arm.',
                                            'organization_key': 'Deterministic SHA-256 organization key.',
                                            'source_organization_id': 'Millennium organization identifier.',
                                            'source_row_id': 'Stable source organization identifier.',
                                            'source_table': 'Fully qualified bronze source table.',
                                            'status': 'Source-derived organization status.',
                                            'valid_from': 'Source validity start.',
                                            'valid_to': 'Source validity end.'},
                                'quantity_override': True,
                                'table': 'Internal quality-controlled twin of reference_organization: the '
                                         'Gold repairs, nulls and expectations are applied here and the '
                                         'published Gold MV reads this object. Materialized rather than '
                                         'temporary so the published MV can refresh incrementally.'},
 'reference_organization': {'columns': {'address_city': 'Source organization address city.',
                                        'address_postcode_masked': 'Privacy-aware source postcode.',
                                        'loaded_at': 'Maximum map_care_site.ADC_UPDT over source rows '
                                                     'sharing the non-null ORGANIZATION_ID. The maximum is '
                                                     'not restricted to active source rows or the row '
                                                     'supplying the selected name/address; it records '
                                                     'contributing bronze ingestion, not organization '
                                                     'validity time or Silver refresh time.',
                                        'member_source': 'map_care_site for organisations with a care-site '
                                                         'row; in_use_fk for organisation ids that '
                                                         'encounters, appointments, medication orders or ED '
                                                         'tracking reference but map_care_site lacks (name '
                                                         'and address unknown, status referenced_only). '
                                                         'INSR_W2 W2.11 (L4164).',
                                        'name': 'Source organization name.',
                                        'organization_id': 'Millennium ORGANIZATION_ID; primary key for the '
                                                           'Millennium arm.',
                                        'organization_key': 'Deterministic SHA-256 organization key.',
                                        'source_organization_id': 'Millennium organization identifier.',
                                        'source_row_id': 'Stable source organization identifier.',
                                        'source_table': 'Fully qualified bronze source table.',
                                        'status': 'Source-derived organization status.',
                                        'valid_from': 'Source validity start.',
                                        'valid_to': 'Source validity end.'},
                            'quantity_override': False,
                            'table': 'Thin v1 provider organization dimension from care-site bronze. Gold QC '
                                     'twin of the silver product: 1 columns are repaired or nulled, 0 '
                                     'check(s) are advisory. Each rule states its reason in the pipeline '
                                     'notebook, and Lakeflow expectation metrics report what every rule '
                                     'matched on each update.'}}
