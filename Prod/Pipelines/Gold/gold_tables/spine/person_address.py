"""Gold spine.person_address: 2 dataset(s).

- datasets: _qc_reference_person_address, reference_person_address
- moved verbatim from: 1_gold_spine_and_reference:3091, 1_gold_spine_and_reference:3180, 1_gold_spine_and_reference:3191, 1_gold_spine_and_reference:3295, 1_gold_spine_and_reference:3300, 1_gold_spine_and_reference:3304, 1_gold_spine_and_reference:3322

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.person_address ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_PERSON_ADDRESS_SELECT = [
    '`person_address_key` AS `person_address_key`',
    '`source_address_id` AS `source_address_id`',
    '`parent_entity` AS `parent_entity`',
    '`person_id` AS `person_id`',
    '`organization_id` AS `organization_id`',
    "CASE WHEN UPPER(TRIM(CAST(`address_type_code` AS STRING))) = '0' THEN NULL ELSE `address_type_code` END AS `address_type_code`",
    '`active_ind` AS `active_ind`',
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN CAST(`beg_effective` AS DATE) = DATE'1900-01-01' OR CAST(`beg_effective` AS DATE) = DATE'2100-01-01' OR YEAR(CAST(`beg_effective` AS DATE)) >= 2100 OR CAST(`beg_effective` AS DATE) < DATE'1901-01-01' AND CAST(`beg_effective` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `beg_effective` END AS `beg_effective`",
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN (CAST(`end_effective` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`end_effective` AS DATE)) < 9999) OR CAST(`end_effective` AS DATE) = DATE'1900-01-01' OR CAST(`end_effective` AS DATE) = DATE'2100-01-01' OR CAST(`end_effective` AS DATE) < DATE'1901-01-01' AND CAST(`end_effective` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `end_effective` END AS `end_effective`",
    '`open_ended_ind` AS `open_ended_ind`',
    '`street_address` AS `street_address`',
    '`city` AS `city`',
    '`postcode` AS `postcode`',
    '`postcode_masked` AS `postcode_masked`',
    '`postcode_outward` AS `postcode_outward`',
    '`uprn` AS `uprn`',
    '`lsoa` AS `lsoa`',
    '`msoa` AS `msoa`',
    '`local_authority_code` AS `local_authority_code`',
    '`imd_decile` AS `imd_decile`',
    '`imd_quintile` AS `imd_quintile`',
    '`latitude` AS `latitude`',
    '`longitude` AS `longitude`',
    '`uprn_match_quality` AS `uprn_match_quality`',
    '`epc_current_energy_rating` AS `epc_current_energy_rating`',
    '`epc_potential_energy_rating` AS `epc_potential_energy_rating`',
    '`epc_property_type` AS `epc_property_type`',
    '`epc_built_form` AS `epc_built_form`',
    '`epc_construction_age_band` AS `epc_construction_age_band`',
    '`epc_tenure` AS `epc_tenure`',
    '`epc_mains_gas_flag` AS `epc_mains_gas_flag`',
    '`epc_total_floor_area` AS `epc_total_floor_area`',
    '`epc_inspection_date` AS `epc_inspection_date`',
    '`epc_lodgement_date` AS `epc_lodgement_date`',
    '`epc_fuel_poverty_risk` AS `epc_fuel_poverty_risk`',
    '`epc_cold_hazard_proxy` AS `epc_cold_hazard_proxy`',
    '`epc_spatial_heating_poverty` AS `epc_spatial_heating_poverty`',
    '`epc_off_gas_grid` AS `epc_off_gas_grid`',
    '`output_area` AS `output_area`',
    '`imd2025_score` AS `imd2025_score`',
    '`imd2025_rank` AS `imd2025_rank`',
    '`imd2025_decile` AS `imd2025_decile`',
    '`imd2025_quintile` AS `imd2025_quintile`',
    '`imd2025_income_decile` AS `imd2025_income_decile`',
    '`imd2025_employment_decile` AS `imd2025_employment_decile`',
    '`imd2025_education_decile` AS `imd2025_education_decile`',
    '`imd2025_health_decile` AS `imd2025_health_decile`',
    '`imd2025_crime_decile` AS `imd2025_crime_decile`',
    '`imd2025_barriers_decile` AS `imd2025_barriers_decile`',
    '`imd2025_living_environment_decile` AS `imd2025_living_environment_decile`',
    '`imd2025_idaci_decile` AS `imd2025_idaci_decile`',
    '`imd2025_idaopi_decile` AS `imd2025_idaopi_decile`',
    '`radon_class_max` AS `radon_class_max`',
    '`radon_potential_band` AS `radon_potential_band`',
    '`radon_pct_lower` AS `radon_pct_lower`',
    '`radon_pct_upper` AS `radon_pct_upper`',
    '`radon_tile_ref` AS `radon_tile_ref`',
    '`radon_location_method` AS `radon_location_method`',
    '`census_usual_residents` AS `census_usual_residents`',
    '`census_persons_per_sq_km` AS `census_persons_per_sq_km`',
    '`census_pct_hh_deprived_any` AS `census_pct_hh_deprived_any`',
    '`census_pct_hh_deprived_3plus` AS `census_pct_hh_deprived_3plus`',
    '`census_pct_hh_overcrowded` AS `census_pct_hh_overcrowded`',
    '`census_pct_hh_social_rented` AS `census_pct_hh_social_rented`',
    '`census_pct_hh_private_rented` AS `census_pct_hh_private_rented`',
    '`census_pct_hh_no_car` AS `census_pct_hh_no_car`',
    '`census_pct_hh_no_central_heating` AS `census_pct_hh_no_central_heating`',
    '`census_pct_bad_health` AS `census_pct_bad_health`',
    '`census_pct_disabled` AS `census_pct_disabled`',
    '`census_pct_no_qualifications` AS `census_pct_no_qualifications`',
    '`census_pct_unemployed` AS `census_pct_unemployed`',
    '`census_pct_long_term_sick` AS `census_pct_long_term_sick`',
    '`census_pct_limited_english` AS `census_pct_limited_english`',
    '`census_pct_ethnic_white` AS `census_pct_ethnic_white`',
    '`census_pct_ethnic_asian` AS `census_pct_ethnic_asian`',
    '`census_pct_ethnic_black` AS `census_pct_ethnic_black`',
    '`census_pct_ethnic_mixed` AS `census_pct_ethnic_mixed`',
    '`census_pct_ethnic_other` AS `census_pct_ethnic_other`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`address_id` AS `address_id`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_PERSON_ADDRESS_MANDATORY_RULES = {
    # The flow nulls person_id when it names a parent the spine does not have, and this rule
    # then drops the row unless organization_id names the parent instead, because an address
    # belonging to nobody at all cannot be used, and the orphans are a wider population than
    # the spine covers rather than a join defect. That is the 3,803,862 orphaned rows out of
    # 17,057,866. The 42,490 rows with no person_id at all are not orphans: they belong to
    # something other than a person, and they are kept whenever organization_id says what.
    "gold.reference.person_address.person_id.fk_containment":
        "`person_id` IS NOT NULL OR `organization_id` IS NOT NULL",
}


REFERENCE_PERSON_ADDRESS_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 122 of 17,057,866
    # rows (0.000715%) when profiled on 2026-08-24.
    "gold.reference.person_address.beg_effective.future_owner":
        "NOT COALESCE((CAST(`beg_effective` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 1,861 of
    # 17,057,866 rows (0.0109%) when profiled on 2026-08-24.
    "gold.reference.person_address.end_effective.future_owner":
        "NOT COALESCE((CAST(`end_effective` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
REFERENCE_PERSON_ADDRESS_PERSON_SCOPE_RULES = {
    "gold.reference.person_address.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
REFERENCE_PERSON_ADDRESS_ORPHAN_RULES = {
    "gold.reference.person_address.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._reference_person_address"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(REFERENCE_PERSON_ADDRESS_MANDATORY_RULES)
@dp.expect_all(REFERENCE_PERSON_ADDRESS_ADVISORY_RULES)
@dp.expect_all_or_drop(REFERENCE_PERSON_ADDRESS_PERSON_SCOPE_RULES)
@dp.expect_all(REFERENCE_PERSON_ADDRESS_ORPHAN_RULES)
def _gold_qc_reference_person_address():
    """Quality-controlled twin of journey_reference.person_address."""
    df = _qc(
        "reference_person_address",
        REFERENCE_PERSON_ADDRESS_SELECT,
        fk_columns=["person_id"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_reference.person_address"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_person_address():
    """Contract-v2 public twin of reference_person_address; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_person_address")).selectExpr(
        '`person_address_key` AS `person_address_key`',
        '`source_address_id` AS `source_address_id`',
        '`parent_entity` AS `parent_entity`',
        '`person_id` AS `person_id`',
        '`organization_id` AS `organization_id`',
        '`address_type_code` AS `address_type_code`',
        '`active_ind` AS `active_ind`',
        '`beg_effective` AS `beg_effective`',
        '`end_effective` AS `end_effective`',
        '`open_ended_ind` AS `open_ended_ind`',
        '`street_address` AS `street_address`',
        '`city` AS `city`',
        '`postcode` AS `postcode`',
        '`postcode_masked` AS `postcode_masked`',
        '`postcode_outward` AS `postcode_outward`',
        '`uprn` AS `uprn`',
        '`lsoa` AS `lsoa`',
        '`msoa` AS `msoa`',
        '`local_authority_code` AS `local_authority_code`',
        '`imd_decile` AS `imd_decile`',
        '`imd_quintile` AS `imd_quintile`',
        '`latitude` AS `latitude`',
        '`longitude` AS `longitude`',
        '`uprn_match_quality` AS `uprn_match_quality`',
        '`epc_current_energy_rating` AS `epc_current_energy_rating`',
        '`epc_potential_energy_rating` AS `epc_potential_energy_rating`',
        '`epc_property_type` AS `epc_property_type`',
        '`epc_built_form` AS `epc_built_form`',
        '`epc_construction_age_band` AS `epc_construction_age_band`',
        '`epc_tenure` AS `epc_tenure`',
        '`epc_mains_gas_flag` AS `epc_mains_gas_flag`',
        '`epc_total_floor_area` AS `epc_total_floor_area`',
        '`epc_inspection_date` AS `epc_inspection_date`',
        '`epc_lodgement_date` AS `epc_lodgement_date`',
        '`epc_fuel_poverty_risk` AS `epc_fuel_poverty_risk`',
        '`epc_cold_hazard_proxy` AS `epc_cold_hazard_proxy`',
        '`epc_spatial_heating_poverty` AS `epc_spatial_heating_poverty`',
        '`epc_off_gas_grid` AS `epc_off_gas_grid`',
        '`output_area` AS `output_area`',
        '`imd2025_score` AS `imd2025_score`',
        '`imd2025_rank` AS `imd2025_rank`',
        '`imd2025_decile` AS `imd2025_decile`',
        '`imd2025_quintile` AS `imd2025_quintile`',
        '`imd2025_income_decile` AS `imd2025_income_decile`',
        '`imd2025_employment_decile` AS `imd2025_employment_decile`',
        '`imd2025_education_decile` AS `imd2025_education_decile`',
        '`imd2025_health_decile` AS `imd2025_health_decile`',
        '`imd2025_crime_decile` AS `imd2025_crime_decile`',
        '`imd2025_barriers_decile` AS `imd2025_barriers_decile`',
        '`imd2025_living_environment_decile` AS `imd2025_living_environment_decile`',
        '`imd2025_idaci_decile` AS `imd2025_idaci_decile`',
        '`imd2025_idaopi_decile` AS `imd2025_idaopi_decile`',
        '`radon_class_max` AS `radon_class_max`',
        '`radon_potential_band` AS `radon_potential_band`',
        '`radon_pct_lower` AS `radon_pct_lower`',
        '`radon_pct_upper` AS `radon_pct_upper`',
        '`radon_tile_ref` AS `radon_tile_ref`',
        '`radon_location_method` AS `radon_location_method`',
        '`census_usual_residents` AS `census_usual_residents`',
        '`census_persons_per_sq_km` AS `census_persons_per_sq_km`',
        '`census_pct_hh_deprived_any` AS `census_pct_hh_deprived_any`',
        '`census_pct_hh_deprived_3plus` AS `census_pct_hh_deprived_3plus`',
        '`census_pct_hh_overcrowded` AS `census_pct_hh_overcrowded`',
        '`census_pct_hh_social_rented` AS `census_pct_hh_social_rented`',
        '`census_pct_hh_private_rented` AS `census_pct_hh_private_rented`',
        '`census_pct_hh_no_car` AS `census_pct_hh_no_car`',
        '`census_pct_hh_no_central_heating` AS `census_pct_hh_no_central_heating`',
        '`census_pct_bad_health` AS `census_pct_bad_health`',
        '`census_pct_disabled` AS `census_pct_disabled`',
        '`census_pct_no_qualifications` AS `census_pct_no_qualifications`',
        '`census_pct_unemployed` AS `census_pct_unemployed`',
        '`census_pct_long_term_sick` AS `census_pct_long_term_sick`',
        '`census_pct_limited_english` AS `census_pct_limited_english`',
        '`census_pct_ethnic_white` AS `census_pct_ethnic_white`',
        '`census_pct_ethnic_asian` AS `census_pct_ethnic_asian`',
        '`census_pct_ethnic_black` AS `census_pct_ethnic_black`',
        '`census_pct_ethnic_mixed` AS `census_pct_ethnic_mixed`',
        '`census_pct_ethnic_other` AS `census_pct_ethnic_other`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`address_id` AS `address_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'reference_person_address': GoldRules(
        table='reference_person_address',
        select=REFERENCE_PERSON_ADDRESS_SELECT,
        mandatory=REFERENCE_PERSON_ADDRESS_MANDATORY_RULES,
        advisory=REFERENCE_PERSON_ADDRESS_ADVISORY_RULES,
        person_scope=REFERENCE_PERSON_ADDRESS_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['person_address_key'], 'support_columns': ['loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_person_address': {'columns': {'_source_row_id': 'The address ID is the primary key of the '
                                                                'address table.',
                                              '_source_system': 'Value describing source system for the '
                                                                'person address record. It is produced by '
                                                                'the silver transformation and has no direct '
                                                                'bronze-column lineage entry. Whitespace and '
                                                                'source sentinel text are retained unless '
                                                                'the pipeline explicitly normalizes them; '
                                                                'null means no value was supplied.',
                                              '_source_table': 'Value describing source table for the person '
                                                               'address record. It is produced by the silver '
                                                               'transformation and has no direct '
                                                               'bronze-column lineage entry. Whitespace and '
                                                               'source sentinel text are retained unless the '
                                                               'pipeline explicitly normalizes them; null '
                                                               'means no value was supplied.',
                                              'active_ind': 'The table row is active or inactive. A row is '
                                                            'generally active unless it is in an inactive '
                                                            'state such as logically deleted, combined away, '
                                                            'pending purge, etc.',
                                              'address_id': 'Millennium ADDRESS_ID as BIGINT; primary key of '
                                                            'this table.',
                                              'address_type_code': 'Code value identifying the type of '
                                                                   'address recorded on the source address '
                                                                   'row.',
                                              'beg_effective': 'The date and time for which this table row '
                                                               'becomes effective. Normally, this will be '
                                                               'the date and time the row is added, but '
                                                               'could be a past or future date and time.',
                                              'census_pct_bad_health': 'Percent of output-area residents '
                                                                       'reporting bad or very bad health '
                                                                       '(TS037).',
                                              'census_pct_disabled': 'Percent of output-area residents '
                                                                     'disabled under the Equality Act '
                                                                     '(TS038).',
                                              'census_pct_ethnic_asian': 'Percent of output-area residents '
                                                                         'in the Asian, Asian British or '
                                                                         'Asian Welsh ethnic group (TS021).',
                                              'census_pct_ethnic_black': 'Percent of output-area residents '
                                                                         'in the Black, Black British, Black '
                                                                         'Welsh, Caribbean or African ethnic '
                                                                         'group (TS021).',
                                              'census_pct_ethnic_mixed': 'Percent of output-area residents '
                                                                         'in the Mixed or Multiple ethnic '
                                                                         'group (TS021).',
                                              'census_pct_ethnic_other': 'Percent of output-area residents '
                                                                         'in the Other ethnic group (TS021).',
                                              'census_pct_ethnic_white': 'Percent of output-area residents '
                                                                         'in the White ethnic group (TS021).',
                                              'census_pct_hh_deprived_3plus': 'Percent of households in the '
                                                                              'output area deprived in three '
                                                                              'or four dimensions (TS011).',
                                              'census_pct_hh_deprived_any': 'Percent of households in the '
                                                                            'output area deprived in at '
                                                                            'least one Census 2021 household '
                                                                            'deprivation dimension (TS011).',
                                              'census_pct_hh_no_car': 'Percent of households in the output '
                                                                      'area with no car or van (TS045).',
                                              'census_pct_hh_no_central_heating': 'Percent of households in '
                                                                                  'the output area with no '
                                                                                  'central heating (TS046).',
                                              'census_pct_hh_overcrowded': 'Percent of households in the '
                                                                           'output area with a bedroom '
                                                                           'occupancy rating of -1 or less '
                                                                           '(TS052).',
                                              'census_pct_hh_private_rented': 'Percent of households in the '
                                                                              'output area renting privately '
                                                                              '(TS054).',
                                              'census_pct_hh_social_rented': 'Percent of households in the '
                                                                             'output area renting socially '
                                                                             '(TS054).',
                                              'census_pct_limited_english': 'Percent of output-area '
                                                                            'residents aged 3+ who cannot '
                                                                            'speak English well or at all '
                                                                            '(TS029).',
                                              'census_pct_long_term_sick': 'Percent of output-area residents '
                                                                           'aged 16+ inactive through '
                                                                           'long-term sickness or disability '
                                                                           '(TS066).',
                                              'census_pct_no_qualifications': 'Percent of output-area '
                                                                              'residents aged 16+ with no '
                                                                              'qualifications (TS067).',
                                              'census_pct_unemployed': 'Unemployed as a percent of '
                                                                       'economically active output-area '
                                                                       'residents aged 16+ (TS066).',
                                              'census_persons_per_sq_km': 'Census 2021 population density of '
                                                                          'the output area, persons per '
                                                                          'square kilometre (TS006).',
                                              'census_usual_residents': 'Census 2021 usual residents of the '
                                                                        'address output area (TS001).',
                                              'city': 'The city field is the text name of the city '
                                                      'associated with the address row.',
                                              'end_effective': 'The date/time after which the row is no '
                                                               'longer valid as active current data.  This '
                                                               'may be valued with the date that the row '
                                                               'became inactive.',
                                              'epc_built_form': 'Detached/Semi/Terrace etc. Affects heat '
                                                                'loss via surface-area-to-volume ratio.',
                                              'epc_cold_hazard_proxy': 'True if EPC band F or G. '
                                                                       'Approximates HHSRS Category 1 '
                                                                       'excessive cold hazard.',
                                              'epc_construction_age_band': 'Building age band. Strongest '
                                                                           'single predictor of fabric '
                                                                           'quality. Pre-1919 homes avg SAP '
                                                                           '~45.',
                                              'epc_current_energy_rating': 'EPC band A-G. Core fuel poverty '
                                                                           'indicator; bands D-G flag risk '
                                                                           'under LILEE definition.',
                                              'epc_fuel_poverty_risk': 'True if EPC band D-G. Flags fuel '
                                                                       'poverty risk under LILEE definition.',
                                              'epc_inspection_date': 'Date the EPC assessment was carried '
                                                                     'out.',
                                              'epc_lodgement_date': 'Date the EPC was lodged on the '
                                                                    'register.',
                                              'epc_mains_gas_flag': 'Whether mains gas is available. '
                                                                    'Off-gas-grid homes face higher fuel '
                                                                    'costs.',
                                              'epc_off_gas_grid': 'True if no mains gas connection. Off-grid '
                                                                  'homes face higher fuel costs.',
                                              'epc_potential_energy_rating': 'Achievable EPC band after '
                                                                             'recommended improvements.',
                                              'epc_property_type': 'Property type recorded in the linked '
                                                                   'Energy Performance Certificate, such as '
                                                                   'house, flat, bungalow, maisonette or '
                                                                   'park home; carried from `PROPERTY_TYPE`.',
                                              'epc_spatial_heating_poverty': 'True if heated rooms < '
                                                                             'habitable rooms, indicating '
                                                                             'partial home heating.',
                                              'epc_tenure': 'Tenure at time of EPC: Owner-occupied / Rented '
                                                            '(private) / Rented (social).',
                                              'epc_total_floor_area': 'Total floor area in m². Overcrowding '
                                                                      'proxy when linked to household size.',
                                              'imd2025_barriers_decile': 'IoD 2025 barriers domain decile of '
                                                                         'the address LSOA (1 = most '
                                                                         'deprived).',
                                              'imd2025_crime_decile': 'IoD 2025 crime domain decile of the '
                                                                      'address LSOA (1 = most deprived).',
                                              'imd2025_decile': 'IMD 2025 decile of the address LSOA (1 = '
                                                                'most deprived 10%). imd_decile remains IMD '
                                                                '2019.',
                                              'imd2025_education_decile': 'IoD 2025 education domain decile '
                                                                          'of the address LSOA (1 = most '
                                                                          'deprived).',
                                              'imd2025_employment_decile': 'IoD 2025 employment domain '
                                                                           'decile of the address LSOA (1 = '
                                                                           'most deprived).',
                                              'imd2025_health_decile': 'IoD 2025 health domain decile of the '
                                                                       'address LSOA (1 = most deprived).',
                                              'imd2025_idaci_decile': 'IoD 2025 idaci domain decile of the '
                                                                      'address LSOA (1 = most deprived).',
                                              'imd2025_idaopi_decile': 'IoD 2025 idaopi domain decile of the '
                                                                       'address LSOA (1 = most deprived).',
                                              'imd2025_income_decile': 'IoD 2025 income domain decile of the '
                                                                       'address LSOA (1 = most deprived).',
                                              'imd2025_living_environment_decile': 'IoD 2025 living '
                                                                                   'environment domain '
                                                                                   'decile of the address '
                                                                                   'LSOA (1 = most '
                                                                                   'deprived).',
                                              'imd2025_quintile': 'IMD 2025 quintile of the address LSOA (1 '
                                                                  '= most deprived). imd_quintile remains '
                                                                  'IMD 2019.',
                                              'imd2025_rank': 'IMD 2025 rank of the address LSOA (1 = most '
                                                              'deprived in England).',
                                              'imd2025_score': 'English Index of Multiple Deprivation 2025 '
                                                               'score of the address LSOA '
                                                               '(map_address_area).',
                                              'imd_decile': 'IMD_Decile is used to store the Index of '
                                                            'Multiple Deprivation (IMD) decile value.',
                                              'imd_quintile': 'IMD_Quintile is used to store the Index of '
                                                              'Multiple Deprivation (IMD) quintile value.',
                                              'latitude': 'Latitude coordinate of the matched address '
                                                          'property from the reference address data.',
                                              'loaded_at': 'map_address.ADC_UPDT carried unchanged for the '
                                                           'address row. The EPC enrichment join contributes '
                                                           'no load timestamp, so this field does not track '
                                                           'changes to the EPC payload or its '
                                                           'inspection/lodgement dates. It is address-source '
                                                           'bronze provenance, not Silver refresh time.',
                                              'local_authority_code': 'Local Authority District code for the '
                                                                      'address, derived from the postcode '
                                                                      'geography lookup.',
                                              'longitude': 'Longitude coordinate of the matched address '
                                                           'property from the reference address data.',
                                              'lsoa': 'LSOA stands for Lower Layer Super Output Area, which '
                                                      'is a geographic area used for small area statistics '
                                                      'in the UK.',
                                              'msoa': '2021 Census Middle Layer Super Output Area code '
                                                      'derived from the address postcode geography lookup.',
                                              'open_ended_ind': 'The date/time after which the row is no '
                                                                'longer valid as active current data.  This '
                                                                'may be valued with the date that the row '
                                                                'became inactive.',
                                              'organization_id': 'Native Millennium ORGANIZATION_ID as '
                                                                 'BIGINT for organization-owned addresses.',
                                              'output_area': '2021 Output Area code of the address '
                                                             '(map_address.OA21CD); joins to '
                                                             'reference_area_census2021_oa.output_area.',
                                              'parent_entity': 'The upper case name of the table to which '
                                                               'this address row is related (i.e., PERSON, '
                                                               'PRSNL, ORGANIZATION, etc.)',
                                              'person_address_key': 'Deterministic SHA-256 key for the '
                                                                    'Millennium address row.',
                                              'person_id': 'Native Millennium PERSON_ID as BIGINT for '
                                                           'person-owned addresses.',
                                              'postcode': 'Postcode value as held in the source address '
                                                          'record, retained for provenance.',
                                              'postcode_masked': 'Partially masked version of the postcode '
                                                                 'for privacy protection.',
                                              'postcode_outward': 'Outward (first) part of the postcode '
                                                                  'derived from the source address postcode '
                                                                  'for privacy-aware geographic analysis.',
                                              'radon_class_max': 'Indicative radon class (1-6) of the '
                                                                 "address's 1 km grid square (UKHSA/BGS "
                                                                 'atlas v3).',
                                              'radon_location_method': 'UPRN_POINT (AddressBase coordinates '
                                                                       'of the accepted UPRN) or OA_CENTROID '
                                                                       '(output-area centroid).',
                                              'radon_pct_lower': 'Lower bound of the radon potential band, '
                                                                 'percent of homes.',
                                              'radon_pct_upper': 'Upper bound of the radon potential band, '
                                                                 'percent of homes.',
                                              'radon_potential_band': 'Estimated percent of homes in the '
                                                                      'square above the radon Action Level, '
                                                                      'as a band.',
                                              'radon_tile_ref': 'OS 1 km grid square the radon class comes '
                                                                'from.',
                                              'source_address_id': 'The address ID is the primary key of the '
                                                                   'address table.',
                                              'street_address': 'Concatenated street address.',
                                              'uprn': 'Unique Property Reference Number - unique identifier '
                                                      'for every spatial address in Great Britain '
                                                      '(1-999999999999).',
                                              'uprn_match_quality': 'It provides a descriptive label '
                                                                    'indicating the type or quality of the '
                                                                    'address match, based on the matching '
                                                                    'algorithm used to link the address to '
                                                                    'the reference data.'},
                                  'quantity_override': True,
                                  'table': 'Internal quality-controlled twin of reference_person_address: '
                                           'the Gold repairs, nulls and expectations are applied here and '
                                           'the published Gold MV reads this object. Materialized rather '
                                           'than temporary so the published MV can refresh incrementally.'},
 'reference_person_address': {'columns': {'_source_row_id': 'The address ID is the primary key of the '
                                                            'address table.',
                                          '_source_system': 'Value describing source system for the person '
                                                            'address record. It is produced by the silver '
                                                            'transformation and has no direct bronze-column '
                                                            'lineage entry. Whitespace and source sentinel '
                                                            'text are retained unless the pipeline '
                                                            'explicitly normalizes them; null means no value '
                                                            'was supplied.',
                                          '_source_table': 'Value describing source table for the person '
                                                           'address record. It is produced by the silver '
                                                           'transformation and has no direct bronze-column '
                                                           'lineage entry. Whitespace and source sentinel '
                                                           'text are retained unless the pipeline explicitly '
                                                           'normalizes them; null means no value was '
                                                           'supplied.',
                                          'active_ind': 'The table row is active or inactive. A row is '
                                                        'generally active unless it is in an inactive state '
                                                        'such as logically deleted, combined away, pending '
                                                        'purge, etc.',
                                          'address_id': 'Millennium ADDRESS_ID as BIGINT; primary key of '
                                                        'this table.',
                                          'address_type_code': 'Code value identifying the type of address '
                                                               'recorded on the source address row.',
                                          'beg_effective': 'The date and time for which this table row '
                                                           'becomes effective. Normally, this will be the '
                                                           'date and time the row is added, but could be a '
                                                           'past or future date and time.',
                                          'census_pct_bad_health': 'Percent of output-area residents '
                                                                   'reporting bad or very bad health '
                                                                   '(TS037).',
                                          'census_pct_disabled': 'Percent of output-area residents disabled '
                                                                 'under the Equality Act (TS038).',
                                          'census_pct_ethnic_asian': 'Percent of output-area residents in '
                                                                     'the Asian, Asian British or Asian '
                                                                     'Welsh ethnic group (TS021).',
                                          'census_pct_ethnic_black': 'Percent of output-area residents in '
                                                                     'the Black, Black British, Black Welsh, '
                                                                     'Caribbean or African ethnic group '
                                                                     '(TS021).',
                                          'census_pct_ethnic_mixed': 'Percent of output-area residents in '
                                                                     'the Mixed or Multiple ethnic group '
                                                                     '(TS021).',
                                          'census_pct_ethnic_other': 'Percent of output-area residents in '
                                                                     'the Other ethnic group (TS021).',
                                          'census_pct_ethnic_white': 'Percent of output-area residents in '
                                                                     'the White ethnic group (TS021).',
                                          'census_pct_hh_deprived_3plus': 'Percent of households in the '
                                                                          'output area deprived in three or '
                                                                          'four dimensions (TS011).',
                                          'census_pct_hh_deprived_any': 'Percent of households in the output '
                                                                        'area deprived in at least one '
                                                                        'Census 2021 household deprivation '
                                                                        'dimension (TS011).',
                                          'census_pct_hh_no_car': 'Percent of households in the output area '
                                                                  'with no car or van (TS045).',
                                          'census_pct_hh_no_central_heating': 'Percent of households in the '
                                                                              'output area with no central '
                                                                              'heating (TS046).',
                                          'census_pct_hh_overcrowded': 'Percent of households in the output '
                                                                       'area with a bedroom occupancy rating '
                                                                       'of -1 or less (TS052).',
                                          'census_pct_hh_private_rented': 'Percent of households in the '
                                                                          'output area renting privately '
                                                                          '(TS054).',
                                          'census_pct_hh_social_rented': 'Percent of households in the '
                                                                         'output area renting socially '
                                                                         '(TS054).',
                                          'census_pct_limited_english': 'Percent of output-area residents '
                                                                        'aged 3+ who cannot speak English '
                                                                        'well or at all (TS029).',
                                          'census_pct_long_term_sick': 'Percent of output-area residents '
                                                                       'aged 16+ inactive through long-term '
                                                                       'sickness or disability (TS066).',
                                          'census_pct_no_qualifications': 'Percent of output-area residents '
                                                                          'aged 16+ with no qualifications '
                                                                          '(TS067).',
                                          'census_pct_unemployed': 'Unemployed as a percent of economically '
                                                                   'active output-area residents aged 16+ '
                                                                   '(TS066).',
                                          'census_persons_per_sq_km': 'Census 2021 population density of the '
                                                                      'output area, persons per square '
                                                                      'kilometre (TS006).',
                                          'census_usual_residents': 'Census 2021 usual residents of the '
                                                                    'address output area (TS001).',
                                          'city': 'The city field is the text name of the city associated '
                                                  'with the address row.',
                                          'end_effective': 'The date/time after which the row is no longer '
                                                           'valid as active current data.  This may be '
                                                           'valued with the date that the row became '
                                                           'inactive.',
                                          'epc_built_form': 'Detached/Semi/Terrace etc. Affects heat loss '
                                                            'via surface-area-to-volume ratio.',
                                          'epc_cold_hazard_proxy': 'True if EPC band F or G. Approximates '
                                                                   'HHSRS Category 1 excessive cold hazard.',
                                          'epc_construction_age_band': 'Building age band. Strongest single '
                                                                       'predictor of fabric quality. '
                                                                       'Pre-1919 homes avg SAP ~45.',
                                          'epc_current_energy_rating': 'EPC band A-G. Core fuel poverty '
                                                                       'indicator; bands D-G flag risk under '
                                                                       'LILEE definition.',
                                          'epc_fuel_poverty_risk': 'True if EPC band D-G. Flags fuel poverty '
                                                                   'risk under LILEE definition.',
                                          'epc_inspection_date': 'Date the EPC assessment was carried out.',
                                          'epc_lodgement_date': 'Date the EPC was lodged on the register.',
                                          'epc_mains_gas_flag': 'Whether mains gas is available. '
                                                                'Off-gas-grid homes face higher fuel costs.',
                                          'epc_off_gas_grid': 'True if no mains gas connection. Off-grid '
                                                              'homes face higher fuel costs.',
                                          'epc_potential_energy_rating': 'Achievable EPC band after '
                                                                         'recommended improvements.',
                                          'epc_property_type': 'Property type recorded in the linked Energy '
                                                               'Performance Certificate, such as house, '
                                                               'flat, bungalow, maisonette or park home; '
                                                               'carried from `PROPERTY_TYPE`.',
                                          'epc_spatial_heating_poverty': 'True if heated rooms < habitable '
                                                                         'rooms, indicating partial home '
                                                                         'heating.',
                                          'epc_tenure': 'Tenure at time of EPC: Owner-occupied / Rented '
                                                        '(private) / Rented (social).',
                                          'epc_total_floor_area': 'Total floor area in m². Overcrowding '
                                                                  'proxy when linked to household size.',
                                          'imd2025_barriers_decile': 'IoD 2025 barriers domain decile of the '
                                                                     'address LSOA (1 = most deprived).',
                                          'imd2025_crime_decile': 'IoD 2025 crime domain decile of the '
                                                                  'address LSOA (1 = most deprived).',
                                          'imd2025_decile': 'IMD 2025 decile of the address LSOA (1 = most '
                                                            'deprived 10%). imd_decile remains IMD 2019.',
                                          'imd2025_education_decile': 'IoD 2025 education domain decile of '
                                                                      'the address LSOA (1 = most deprived).',
                                          'imd2025_employment_decile': 'IoD 2025 employment domain decile of '
                                                                       'the address LSOA (1 = most '
                                                                       'deprived).',
                                          'imd2025_health_decile': 'IoD 2025 health domain decile of the '
                                                                   'address LSOA (1 = most deprived).',
                                          'imd2025_idaci_decile': 'IoD 2025 idaci domain decile of the '
                                                                  'address LSOA (1 = most deprived).',
                                          'imd2025_idaopi_decile': 'IoD 2025 idaopi domain decile of the '
                                                                   'address LSOA (1 = most deprived).',
                                          'imd2025_income_decile': 'IoD 2025 income domain decile of the '
                                                                   'address LSOA (1 = most deprived).',
                                          'imd2025_living_environment_decile': 'IoD 2025 living environment '
                                                                               'domain decile of the address '
                                                                               'LSOA (1 = most deprived).',
                                          'imd2025_quintile': 'IMD 2025 quintile of the address LSOA (1 = '
                                                              'most deprived). imd_quintile remains IMD '
                                                              '2019.',
                                          'imd2025_rank': 'IMD 2025 rank of the address LSOA (1 = most '
                                                          'deprived in England).',
                                          'imd2025_score': 'English Index of Multiple Deprivation 2025 score '
                                                           'of the address LSOA (map_address_area).',
                                          'imd_decile': 'IMD_Decile is used to store the Index of Multiple '
                                                        'Deprivation (IMD) decile value.',
                                          'imd_quintile': 'IMD_Quintile is used to store the Index of '
                                                          'Multiple Deprivation (IMD) quintile value.',
                                          'latitude': 'Latitude coordinate of the matched address property '
                                                      'from the reference address data.',
                                          'loaded_at': 'map_address.ADC_UPDT carried unchanged for the '
                                                       'address row. The EPC enrichment join contributes no '
                                                       'load timestamp, so this field does not track changes '
                                                       'to the EPC payload or its inspection/lodgement '
                                                       'dates. It is address-source bronze provenance, not '
                                                       'Silver refresh time.',
                                          'local_authority_code': 'Local Authority District code for the '
                                                                  'address, derived from the postcode '
                                                                  'geography lookup.',
                                          'longitude': 'Longitude coordinate of the matched address property '
                                                       'from the reference address data.',
                                          'lsoa': 'LSOA stands for Lower Layer Super Output Area, which is a '
                                                  'geographic area used for small area statistics in the UK.',
                                          'msoa': '2021 Census Middle Layer Super Output Area code derived '
                                                  'from the address postcode geography lookup.',
                                          'open_ended_ind': 'The date/time after which the row is no longer '
                                                            'valid as active current data.  This may be '
                                                            'valued with the date that the row became '
                                                            'inactive.',
                                          'organization_id': 'Native Millennium ORGANIZATION_ID as BIGINT '
                                                             'for organization-owned addresses.',
                                          'output_area': '2021 Output Area code of the address '
                                                         '(map_address.OA21CD); joins to '
                                                         'reference_area_census2021_oa.output_area.',
                                          'parent_entity': 'The upper case name of the table to which this '
                                                           'address row is related (i.e., PERSON, PRSNL, '
                                                           'ORGANIZATION, etc.)',
                                          'person_address_key': 'Deterministic SHA-256 key for the '
                                                                'Millennium address row.',
                                          'person_id': 'Native Millennium PERSON_ID as BIGINT for '
                                                       'person-owned addresses.',
                                          'postcode': 'Postcode value as held in the source address record, '
                                                      'retained for provenance.',
                                          'postcode_masked': 'Partially masked version of the postcode for '
                                                             'privacy protection.',
                                          'postcode_outward': 'Outward (first) part of the postcode derived '
                                                              'from the source address postcode for '
                                                              'privacy-aware geographic analysis.',
                                          'radon_class_max': "Indicative radon class (1-6) of the address's "
                                                             '1 km grid square (UKHSA/BGS atlas v3).',
                                          'radon_location_method': 'UPRN_POINT (AddressBase coordinates of '
                                                                   'the accepted UPRN) or OA_CENTROID '
                                                                   '(output-area centroid).',
                                          'radon_pct_lower': 'Lower bound of the radon potential band, '
                                                             'percent of homes.',
                                          'radon_pct_upper': 'Upper bound of the radon potential band, '
                                                             'percent of homes.',
                                          'radon_potential_band': 'Estimated percent of homes in the square '
                                                                  'above the radon Action Level, as a band.',
                                          'radon_tile_ref': 'OS 1 km grid square the radon class comes from.',
                                          'source_address_id': 'The address ID is the primary key of the '
                                                               'address table.',
                                          'street_address': 'Concatenated street address.',
                                          'uprn': 'Unique Property Reference Number - unique identifier for '
                                                  'every spatial address in Great Britain (1-999999999999).',
                                          'uprn_match_quality': 'It provides a descriptive label indicating '
                                                                'the type or quality of the address match, '
                                                                'based on the matching algorithm used to '
                                                                'link the address to the reference data.'},
                              'quantity_override': False,
                              'table': 'One person or organization address assignment; direct address, '
                                       'postcode and UPRN fields are IG-sensitive. Gold QC twin of the '
                                       'silver product: 4 columns are repaired or nulled, 1 rule(s) drop '
                                       'rows, 2 check(s) are advisory. Each rule states its reason in the '
                                       'pipeline notebook, and Lakeflow expectation metrics report what '
                                       'every rule matched on each update.'}}
