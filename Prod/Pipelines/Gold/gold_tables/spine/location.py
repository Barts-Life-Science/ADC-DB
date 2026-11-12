"""Gold spine.location: 2 dataset(s).

- datasets: _qc_reference_location, reference_location
- moved verbatim from: 1_gold_spine_and_reference:2900, 1_gold_spine_and_reference:2921, 1_gold_spine_and_reference:2955, 1_gold_spine_and_reference:2965

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.location ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_LOCATION_SELECT = [
    '`location_key` AS `location_key`',
    '`location_code` AS `location_code`',
    '`parent_location_key` AS `parent_location_key`',
    '`location_level` AS `location_level`',
    '`source_location_code` AS `source_location_code`',
    '`name` AS `name`',
    '`status` AS `status`',
    '`organization_id` AS `organization_id`',
    '`physical_type_code` AS `physical_type_code`',
    '`valid_from` AS `valid_from`',
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN `valid_from` IS NOT NULL AND `valid_to` IS NOT NULL AND `valid_from` > `valid_to` THEN NULL ELSE CASE WHEN (CAST(`valid_to` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`valid_to` AS DATE)) < 9999) THEN NULL ELSE `valid_to` END END AS `valid_to`",
    '`latitude` AS `latitude`',
    '`longitude` AS `longitude`',
    '`address_city` AS `address_city`',
    '`address_postcode_masked` AS `address_postcode_masked`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`loaded_at` AS `loaded_at`',
]


REFERENCE_LOCATION_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`organization_id_basis` AS `organization_id_basis`',
    '`coordinate_source` AS `coordinate_source`',
    '`coordinate_status` AS `coordinate_status`',
]


@dp.materialized_view(
    name=_n("gold_qc._reference_location"),
    refresh_policy="auto",
)
def _gold_qc_reference_location():
    """Quality-controlled twin of journey_reference.location."""
    df = _qc("reference_location", REFERENCE_LOCATION_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_reference.location"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_location():
    """Contract-v2 public twin of reference_location; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_location")).selectExpr(
        '`location_key` AS `location_key`',
        '`location_code` AS `location_code`',
        '`parent_location_key` AS `parent_location_key`',
        '`location_level` AS `location_level`',
        '`source_location_code` AS `source_location_code`',
        '`name` AS `name`',
        '`status` AS `status`',
        '`organization_id` AS `organization_id`',
        '`physical_type_code` AS `physical_type_code`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`latitude` AS `latitude`',
        '`longitude` AS `longitude`',
        '`address_city` AS `address_city`',
        '`address_postcode_masked` AS `address_postcode_masked`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`loaded_at` AS `loaded_at`',
        '`organization_id_basis` AS `organization_id_basis`',  # INSR_W3 W3.12
        '`coordinate_source` AS `coordinate_source`',  # INSR_W3 W3.12
        '`coordinate_status` AS `coordinate_status`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'reference_location': GoldRules(
        table='reference_location',
        select=REFERENCE_LOCATION_SELECT,
        lifecycle={'join_keys': ['location_key'], 'support_columns': ['load_batch_id', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_location': {'columns': {'address_city': 'Source address city.',
                                        'address_postcode_masked': 'Privacy-aware source postcode.',
                                        'coordinate_source': 'Where latitude/longitude come from: source '
                                                             '(the Millennium address record) or '
                                                             'onspd_postcode_centroid (ONS Postcode '
                                                             'Directory centroid of the site postcode, used '
                                                             'when the source has none); null when the '
                                                             'location has no coordinates (INSR W7.08, J15 '
                                                             'ruling 2026-10-29).',
                                        'coordinate_status': 'postcode_outside_trust_areas when the '
                                                             "location's postcode area is not one of the "
                                                             "Trust's own site postcode areas (areas of its "
                                                             'active facility-level care sites); coordinates '
                                                             'and coordinate_source are then NULL. NULL '
                                                             'otherwise (INSR W7.08, W7-S1 ruling '
                                                             '2026-10-31).',
                                        'latitude': 'Latitude of the location: the Millennium address '
                                                    "record's, or for a Trust care site with none the ONS "
                                                    'Postcode Directory centroid of its postcode (see '
                                                    'coordinate_source). Trust sites only (J15).',
                                        'loaded_at': 'From map_care_site: maximum ADC_UPDT per facility_cd '
                                                     'for facility rows, maximum ADC_UPDT per building_cd '
                                                     "for building rows, and the individual source row's "
                                                     'ADC_UPDT for nurse-unit rows. The hierarchy union adds '
                                                     'no parent clock or cross-level aggregation; this is '
                                                     'bronze ingestion provenance, not location validity '
                                                     'time or Silver refresh time. map_location_unit rows '
                                                     "carry that row's ADC_UPDT.",
                                        'location_code': 'Native Millennium location code; primary key '
                                                         'together with location_level.',
                                        'location_key': 'Deterministic SHA-256 key across location levels.',
                                        'location_level': 'Hierarchy level. map_care_site rows: facility, '
                                                          'building or nurse_unit. map_location_unit rows: '
                                                          'its LOCATION_LEVEL -- nurse_unit, room, bed, or '
                                                          'the lower-cased Millennium location type (e.g. '
                                                          'ancilsurg, rad, ambulatory, waitroom, facility, '
                                                          'building); unclassified when the type is unknown.',
                                        'longitude': 'Longitude of the location; same source rule as '
                                                     'latitude.',
                                        'name': 'Source location name.',
                                        'organization_id': 'Millennium ORGANIZATION_ID as BIGINT when '
                                                           'available.',
                                        'organization_id_basis': 'care_site_source when map_care_site '
                                                                 'carries the organisation; parent_care_site '
                                                                 '/ grandparent_care_site / '
                                                                 'great_grandparent_care_site when a '
                                                                 'map_location_unit row inherits it through '
                                                                 'PARENT_LOCATION_CD; NULL when no care-site '
                                                                 'ancestor within three hops has one. '
                                                                 'INSR_W2 W2.11 (L4164).',
                                        'parent_location_key': 'Deterministic SHA-256 key of the parent '
                                                               'location: the facility or building for '
                                                               'care-site rows; for map_location_unit rows '
                                                               "the key of PARENT_LOCATION_CD (a bed's room, "
                                                               "a room's nurse unit, a nurse unit's "
                                                               'building, otherwise the Millennium '
                                                               'location-group parent). Null when the parent '
                                                               'is not published.',
                                        'physical_type_code': 'FHIR physical location type code: si '
                                                              'facility, bu building, wa nurse unit, ro '
                                                              'room, bd bed; null for other Millennium '
                                                              'location types.',
                                        'source_location_code': 'Source Millennium location code.',
                                        'source_row_id': 'Stable source row identifier.',
                                        'source_table': 'Fully qualified bronze source table.',
                                        'status': 'Source-derived location status.',
                                        'valid_from': 'Source validity start.',
                                        'valid_to': 'Source validity end.'},
                            'quantity_override': True,
                            'table': 'Internal quality-controlled twin of reference_location: the Gold '
                                     'repairs, nulls and expectations are applied here and the published '
                                     'Gold MV reads this object. Materialized rather than temporary so the '
                                     'published MV can refresh incrementally.'},
 'reference_location': {'columns': {'address_city': 'Source address city.',
                                    'address_postcode_masked': 'Privacy-aware source postcode.',
                                    'coordinate_source': 'source (Millennium address record) or '
                                                         'onspd_postcode_centroid (ONS Postcode Directory '
                                                         'centroid of the site postcode); null when the '
                                                         'location has no coordinates (INSR W7.08).',
                                    'coordinate_status': "postcode_outside_trust_areas when the location's "
                                                         "postcode area is not one of the Trust's own site "
                                                         'postcode areas (areas of its active facility-level '
                                                         'care sites); coordinates and coordinate_source are '
                                                         'then NULL. NULL otherwise (INSR W7.08, W7-S1 '
                                                         'ruling 2026-10-31).',
                                    'latitude': "Latitude of the location: the Millennium address record's, "
                                                'or for a Trust care site with none the ONS Postcode '
                                                'Directory centroid of its postcode (see coordinate_source). '
                                                'Trust sites only (J15).',
                                    'loaded_at': 'From map_care_site: maximum ADC_UPDT per facility_cd for '
                                                 'facility rows, maximum ADC_UPDT per building_cd for '
                                                 "building rows, and the individual source row's ADC_UPDT "
                                                 'for nurse-unit rows. The hierarchy union adds no parent '
                                                 'clock or cross-level aggregation; this is bronze ingestion '
                                                 'provenance, not location validity time or Silver refresh '
                                                 "time. map_location_unit rows carry that row's ADC_UPDT.",
                                    'location_code': 'Native Millennium location code; primary key together '
                                                     'with location_level.',
                                    'location_key': 'Deterministic SHA-256 key across location levels.',
                                    'location_level': 'Hierarchy level. map_care_site rows: facility, '
                                                      'building or nurse_unit. map_location_unit rows: its '
                                                      'LOCATION_LEVEL -- nurse_unit, room, bed, or the '
                                                      'lower-cased Millennium location type (e.g. ancilsurg, '
                                                      'rad, ambulatory, waitroom, facility, building); '
                                                      'unclassified when the type is unknown.',
                                    'longitude': 'Longitude of the location; same source rule as latitude.',
                                    'name': 'Source location name.',
                                    'organization_id': 'Millennium ORGANIZATION_ID as BIGINT when available.',
                                    'organization_id_basis': 'care_site_source when map_care_site carries '
                                                             'the organisation; parent_care_site / '
                                                             'grandparent_care_site / '
                                                             'great_grandparent_care_site when a '
                                                             'map_location_unit row inherits it through '
                                                             'PARENT_LOCATION_CD; NULL when no care-site '
                                                             'ancestor within three hops has one. INSR_W2 '
                                                             'W2.11 (L4164).',
                                    'parent_location_key': 'Deterministic SHA-256 key of the parent '
                                                           'location: the facility or building for care-site '
                                                           'rows; for map_location_unit rows the key of '
                                                           "PARENT_LOCATION_CD (a bed's room, a room's nurse "
                                                           "unit, a nurse unit's building, otherwise the "
                                                           'Millennium location-group parent). Null when the '
                                                           'parent is not published.',
                                    'physical_type_code': 'FHIR physical location type code: si facility, bu '
                                                          'building, wa nurse unit, ro room, bd bed; null '
                                                          'for other Millennium location types.',
                                    'source_location_code': 'Source Millennium location code.',
                                    'source_row_id': 'Stable source row identifier.',
                                    'source_table': 'Fully qualified bronze source table.',
                                    'status': 'Source-derived location status.',
                                    'valid_from': 'Source validity start.',
                                    'valid_to': 'Source validity end.'},
                        'quantity_override': False,
                        'table': 'Effective-dated facility, building, and nurse-unit hierarchy. Gold QC twin '
                                 'of the silver product: 1 columns are repaired or nulled, 0 check(s) are '
                                 'advisory. Each rule states its reason in the pipeline notebook, and '
                                 'Lakeflow expectation metrics report what every rule matched on each '
                                 'update.'}}
