"""Silver spine.person_address: 1 dataset(s).

- datasets: reference_person_address
- moved verbatim from: 1_silver_spine_and_reference:5765, 1_silver_spine_and_reference:5767, 1_silver_spine_and_reference:5769, 1_silver_spine_and_reference:5772, 1_silver_spine_and_reference:5860, 1_silver_spine_and_reference:5863, 1_silver_spine_and_reference:5868, 1_silver_spine_and_reference:5931

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Reference / person address
SRC_ADDRESS = "4_prod.bronze.map_address"


SRC_ADDRESS_EPC = "4_prod.bronze.map_address_epc"


SRC_ADDRESS_AREA = "4_prod.bronze.map_address_area"  # GEO_AREA_ENRICHMENT_V1


# GEO_AREA_ENRICHMENT_V1: area enrichment columns (bronze map_address_area column, published column).
PERSON_ADDRESS_AREA_COLUMNS = [('IMD2025_SCORE', 'imd2025_score'), ('IMD2025_RANK', 'imd2025_rank'), ('IMD2025_DECILE', 'imd2025_decile'), ('IMD2025_QUINTILE', 'imd2025_quintile'), ('IMD2025_INCOME_DECILE', 'imd2025_income_decile'), ('IMD2025_EMPLOYMENT_DECILE', 'imd2025_employment_decile'), ('IMD2025_EDUCATION_DECILE', 'imd2025_education_decile'), ('IMD2025_HEALTH_DECILE', 'imd2025_health_decile'), ('IMD2025_CRIME_DECILE', 'imd2025_crime_decile'), ('IMD2025_BARRIERS_DECILE', 'imd2025_barriers_decile'), ('IMD2025_LIVING_ENVIRONMENT_DECILE', 'imd2025_living_environment_decile'), ('IMD2025_IDACI_DECILE', 'imd2025_idaci_decile'), ('IMD2025_IDAOPI_DECILE', 'imd2025_idaopi_decile'), ('RADON_CLASS_MAX', 'radon_class_max'), ('RADON_POTENTIAL_BAND', 'radon_potential_band'), ('RADON_PCT_LOWER', 'radon_pct_lower'), ('RADON_PCT_UPPER', 'radon_pct_upper'), ('RADON_TILE_REF', 'radon_tile_ref'), ('RADON_LOCATION_METHOD', 'radon_location_method'), ('CENSUS_USUAL_RESIDENTS', 'census_usual_residents'), ('CENSUS_PERSONS_PER_SQ_KM', 'census_persons_per_sq_km'), ('CENSUS_PCT_HH_DEPRIVED_ANY', 'census_pct_hh_deprived_any'), ('CENSUS_PCT_HH_DEPRIVED_3PLUS', 'census_pct_hh_deprived_3plus'), ('CENSUS_PCT_HH_OVERCROWDED', 'census_pct_hh_overcrowded'), ('CENSUS_PCT_HH_SOCIAL_RENTED', 'census_pct_hh_social_rented'), ('CENSUS_PCT_HH_PRIVATE_RENTED', 'census_pct_hh_private_rented'), ('CENSUS_PCT_HH_NO_CAR', 'census_pct_hh_no_car'), ('CENSUS_PCT_HH_NO_CENTRAL_HEATING', 'census_pct_hh_no_central_heating'), ('CENSUS_PCT_BAD_HEALTH', 'census_pct_bad_health'), ('CENSUS_PCT_DISABLED', 'census_pct_disabled'), ('CENSUS_PCT_NO_QUALIFICATIONS', 'census_pct_no_qualifications'), ('CENSUS_PCT_UNEMPLOYED', 'census_pct_unemployed'), ('CENSUS_PCT_LONG_TERM_SICK', 'census_pct_long_term_sick'), ('CENSUS_PCT_LIMITED_ENGLISH', 'census_pct_limited_english'), ('CENSUS_PCT_ETHNIC_WHITE', 'census_pct_ethnic_white'), ('CENSUS_PCT_ETHNIC_ASIAN', 'census_pct_ethnic_asian'), ('CENSUS_PCT_ETHNIC_BLACK', 'census_pct_ethnic_black'), ('CENSUS_PCT_ETHNIC_MIXED', 'census_pct_ethnic_mixed'), ('CENSUS_PCT_ETHNIC_OTHER', 'census_pct_ethnic_other')]


PERSON_ADDRESS_LIFECYCLE_FIELDS = [
]


PERSON_ADDRESS_RETIRED_COLUMNS = [

]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_person_address():
    # Assemble person address rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    a = read_source(SRC_ADDRESS).alias("a")
    e = read_source(SRC_ADDRESS_EPC).select(
        F.col("ADDRESS_ID").alias("_epc_address_id"),
        F.col("CURRENT_ENERGY_RATING").alias("epc_current_energy_rating"),
        F.col("POTENTIAL_ENERGY_RATING").alias("epc_potential_energy_rating"),
        F.col("PROPERTY_TYPE").alias("epc_property_type"),
        F.col("BUILT_FORM").alias("epc_built_form"),
        F.col("CONSTRUCTION_AGE_BAND").alias("epc_construction_age_band"),
        F.col("TENURE").alias("epc_tenure"),
        F.col("MAINS_GAS_FLAG").alias("epc_mains_gas_flag"),
        F.col("TOTAL_FLOOR_AREA").alias("epc_total_floor_area"),
        F.col("EPC_INSPECTION_DATE").alias("epc_inspection_date"),
        F.col("EPC_LODGEMENT_DATE").alias("epc_lodgement_date"),
        F.col("fuel_poverty_risk").alias("epc_fuel_poverty_risk"),
        F.col("hhsrs_cold_hazard_proxy").alias("epc_cold_hazard_proxy"),
        F.col("spatial_heating_poverty").alias("epc_spatial_heating_poverty"),
        F.col("off_gas_grid").alias("epc_off_gas_grid"),
    ).alias("e")
    d = a.join(e, a.ADDRESS_ID == e._epc_address_id, "left")
    # GEO_AREA_ENRICHMENT_V1: IoD 2025, radon and Census 2021 output-area measures for the address.
    g = read_source(SRC_ADDRESS_AREA).select(
        F.col("ADDRESS_ID").alias("_area_address_id"),
        *[F.col(b).alias(s) for b, s in PERSON_ADDRESS_AREA_COLUMNS],
    ).alias("g")
    d = d.join(g, a.ADDRESS_ID == g._area_address_id, "left")
    # contract v2: rename the address hash as a key and publish native address, person, and organization ids
    return d.select(
        stable_id("person_address:mill", a.ADDRESS_ID).alias("person_address_key"),
        a.ADDRESS_ID.cast("bigint").alias("address_id"),
        a.ADDRESS_ID.cast("string").alias("source_address_id"),
        a.PARENT_ENTITY_NAME.alias("parent_entity"),
        F.when(a.PARENT_ENTITY_NAME == "PERSON", a.PARENT_ENTITY_ID.cast("bigint"))
         .alias("person_id"),
        F.when(a.PARENT_ENTITY_NAME == "ORGANIZATION", a.PARENT_ENTITY_ID.cast("bigint"))
         .alias("organization_id"),
        a.ADDRESS_TYPE_CODE.cast("string").alias("address_type_code"),
        a.ACTIVE_IND.alias("active_ind"), a.BEG_EFFECTIVE_DT_TM.alias("beg_effective"),
        a.END_EFFECTIVE_DT_TM.alias("end_effective"),
        (a.END_EFFECTIVE_DT_TM >= F.lit("2100-01-01").cast("timestamp"))
         .alias("open_ended_ind"),
        a.full_street_address.alias("street_address"), a.CITY.alias("city"),
        a.SOURCE_ZIPCODE.alias("postcode"), a.masked_zipcode.alias("postcode_masked"),
        a.POSTCODE_OUTWARD.alias("postcode_outward"), a.UPRN.cast("string").alias("uprn"),
        a.LSOA.alias("lsoa"), a.MSOA21CD.alias("msoa"),
        a.LADCD.alias("local_authority_code"), a.IMD_Decile.alias("imd_decile"),
        a.IMD_Quintile.alias("imd_quintile"), a.LATITUDE.alias("latitude"),
        a.LONGITUDE.alias("longitude"), a.match_quality.alias("uprn_match_quality"),
        F.col("epc_current_energy_rating"), F.col("epc_potential_energy_rating"),
        F.col("epc_property_type"), F.col("epc_built_form"),
        F.col("epc_construction_age_band"), F.col("epc_tenure"),
        F.col("epc_mains_gas_flag"), F.col("epc_total_floor_area"),
        F.col("epc_inspection_date"), F.col("epc_lodgement_date"),
        F.col("epc_fuel_poverty_risk"), F.col("epc_cold_hazard_proxy"),
        F.col("epc_spatial_heating_poverty"), F.col("epc_off_gas_grid"),
        a.OA21CD.alias("output_area"), *[F.col(s) for _, s in PERSON_ADDRESS_AREA_COLUMNS],
        a.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_ADDRESS).alias("_source_table"),
        a.ADDRESS_ID.cast("string").alias("_source_row_id"),
    )


@materialized_view(
    name=_n("journey_reference.person_address"),
    cluster_by=["person_id"], refresh_policy="auto",
)
def person_address():
    # Build the declared dataset: Address assignments with EPC extension; direct
    # address/postcode/UPRN fields are IG-sensitive.
    return _lifecycle_source_person_address().drop(*PERSON_ADDRESS_LIFECYCLE_FIELDS, *PERSON_ADDRESS_RETIRED_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_person_address': {'columns': {'_source_row_id': 'The address ID is the primary key of the '
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
                              'table': 'Address assignments with EPC extension; direct address/postcode/UPRN '
                                       'fields are IG-sensitive.'}}
