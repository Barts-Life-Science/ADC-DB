"""Silver spine.area_iod2025_lsoa: 1 dataset(s).

- datasets: reference_area_iod2025_lsoa
- moved verbatim from: 1_silver_spine_and_reference:5953, 1_silver_spine_and_reference:5959, 1_silver_spine_and_reference:5976

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


SRC_IOD2025_LSOA = "3_lookup.geo.iod_2025_lsoa"


AREA_IOD2025_LSOA_COLUMNS = ['lsoa21_name', 'lad24_code', 'lad24_name', 'imd_score', 'imd_rank', 'imd_decile', 'income_score', 'income_rank', 'income_decile', 'employment_score', 'employment_rank', 'employment_decile', 'education_score', 'education_rank', 'education_decile', 'health_score', 'health_rank', 'health_decile', 'crime_score', 'crime_rank', 'crime_decile', 'barriers_score', 'barriers_rank', 'barriers_decile', 'living_environment_score', 'living_environment_rank', 'living_environment_decile', 'idaci_score', 'idaci_rank', 'idaci_decile', 'idaopi_score', 'idaopi_rank', 'idaopi_decile', 'children_young_people_score', 'children_young_people_rank', 'children_young_people_decile', 'adult_skills_score', 'adult_skills_rank', 'adult_skills_decile', 'geographical_barriers_score', 'geographical_barriers_rank', 'geographical_barriers_decile', 'wider_barriers_score', 'wider_barriers_rank', 'wider_barriers_decile', 'indoors_score', 'indoors_rank', 'indoors_decile', 'outdoors_score', 'outdoors_rank', 'outdoors_decile', 'population_total_mid2022', 'population_children_0_15_mid2022', 'population_older_60_plus_mid2022', 'population_working_age_18_66_mid2022', 'imd_quintile', 'iod_release']


@materialized_view(
    name=_n("journey_reference.area_iod2025_lsoa"),
    cluster_by=["lsoa"], refresh_policy="auto",
)
def area_iod2025_lsoa():
    # Publish the IoD 2025 lookup as a reference table keyed by LSOA.
    return read_source(SRC_IOD2025_LSOA).select(
        F.col("lsoa21_code").alias("lsoa"), *[F.col(c) for c in AREA_IOD2025_LSOA_COLUMNS],
        F.col("ADC_UPDT").alias("loaded_at"),
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_area_iod2025_lsoa': {'columns': {'adult_skills_decile': 'IoD 2025 File 7 column: Adult Skills '
                                                                    'Sub-domain Decile (where 1 is most '
                                                                    'deprived 10% of LSOAs).',
                                             'adult_skills_rank': 'IoD 2025 File 7 column: Adult Skills '
                                                                  'Sub-domain Rank (where 1 is most '
                                                                  'deprived).',
                                             'adult_skills_score': 'IoD 2025 File 7 column: Adult Skills '
                                                                   'Sub-domain Score.',
                                             'barriers_decile': 'IoD 2025 File 7 column: Barriers to Housing '
                                                                'and Services Decile (where 1 is most '
                                                                'deprived 10% of LSOAs).',
                                             'barriers_rank': 'IoD 2025 File 7 column: Barriers to Housing '
                                                              'and Services Rank (where 1 is most deprived).',
                                             'barriers_score': 'IoD 2025 File 7 column: Barriers to Housing '
                                                               'and Services Score.',
                                             'children_young_people_decile': 'IoD 2025 File 7 column: '
                                                                             'Children and Young People '
                                                                             'Sub-domain Decile (where 1 is '
                                                                             'most deprived 10% of LSO.',
                                             'children_young_people_rank': 'IoD 2025 File 7 column: Children '
                                                                           'and Young People Sub-domain Rank '
                                                                           '(where 1 is most deprived).',
                                             'children_young_people_score': 'IoD 2025 File 7 column: '
                                                                            'Children and Young People '
                                                                            'Sub-domain Score.',
                                             'crime_decile': 'IoD 2025 File 7 column: Crime Decile (where 1 '
                                                             'is most deprived 10% of LSOAs).',
                                             'crime_rank': 'IoD 2025 File 7 column: Crime Rank (where 1 is '
                                                           'most deprived).',
                                             'crime_score': 'IoD 2025 File 7 column: Crime Score.',
                                             'education_decile': 'IoD 2025 File 7 column: Education, Skills '
                                                                 'and Training Decile (where 1 is most '
                                                                 'deprived 10% of LSOAs).',
                                             'education_rank': 'IoD 2025 File 7 column: Education, Skills '
                                                               'and Training Rank (where 1 is most '
                                                               'deprived).',
                                             'education_score': 'IoD 2025 File 7 column: Education, Skills '
                                                                'and Training Score.',
                                             'employment_decile': 'IoD 2025 File 7 column: Employment Decile '
                                                                  '(where 1 is most deprived 10% of LSOAs).',
                                             'employment_rank': 'IoD 2025 File 7 column: Employment Rank '
                                                                '(where 1 is most deprived).',
                                             'employment_score': 'IoD 2025 File 7 column: Employment Score '
                                                                 '(rate).',
                                             'geographical_barriers_decile': 'IoD 2025 File 7 column: '
                                                                             'Geographical Barriers '
                                                                             'Sub-domain Decile (where 1 is '
                                                                             'most deprived 10% of LSOAs).',
                                             'geographical_barriers_rank': 'IoD 2025 File 7 column: '
                                                                           'Geographical Barriers Sub-domain '
                                                                           'Rank (where 1 is most deprived).',
                                             'geographical_barriers_score': 'IoD 2025 File 7 column: '
                                                                            'Geographical Barriers '
                                                                            'Sub-domain Score.',
                                             'health_decile': 'IoD 2025 File 7 column: Health Deprivation '
                                                              'and Disability Decile (where 1 is most '
                                                              'deprived 10% of LSOAs).',
                                             'health_rank': 'IoD 2025 File 7 column: Health Deprivation and '
                                                            'Disability Rank (where 1 is most deprived).',
                                             'health_score': 'IoD 2025 File 7 column: Health Deprivation and '
                                                             'Disability Score.',
                                             'idaci_decile': 'IoD 2025 File 7 column: Income Deprivation '
                                                             'Affecting Children Index (IDACI) Decile (where '
                                                             '1 is most deprived 10% of LSOAs).',
                                             'idaci_rank': 'IoD 2025 File 7 column: Income Deprivation '
                                                           'Affecting Children Index (IDACI) Rank (where 1 '
                                                           'is most deprived).',
                                             'idaci_score': 'IoD 2025 File 7 column: Income Deprivation '
                                                            'Affecting Children Index (IDACI) Score (rate).',
                                             'idaopi_decile': 'IoD 2025 File 7 column: Income Deprivation '
                                                              'Affecting Older People (IDAOPI) Decile (where '
                                                              '1 is most deprived 10% of LSOAs).',
                                             'idaopi_rank': 'IoD 2025 File 7 column: Income Deprivation '
                                                            'Affecting Older People (IDAOPI) Rank (where 1 '
                                                            'is most deprived).',
                                             'idaopi_score': 'IoD 2025 File 7 column: Income Deprivation '
                                                             'Affecting Older People (IDAOPI) Score (rate).',
                                             'imd_decile': 'IoD 2025 File 7 column: Index of Multiple '
                                                           'Deprivation (IMD) Decile (where 1 is most '
                                                           'deprived 10% of LSOAs).',
                                             'imd_quintile': 'Derived: IMD quintile = ceil(imd_decile / 2), '
                                                             'where 1 is the most deprived 20% of LSOAs in '
                                                             'England.',
                                             'imd_rank': 'IoD 2025 File 7 column: Index of Multiple '
                                                         'Deprivation (IMD) Rank (where 1 is most deprived).',
                                             'imd_score': 'IoD 2025 File 7 column: Index of Multiple '
                                                          'Deprivation (IMD) Score.',
                                             'income_decile': 'IoD 2025 File 7 column: Income Decile (where '
                                                              '1 is most deprived 10% of LSOAs).',
                                             'income_rank': 'IoD 2025 File 7 column: Income Rank (where 1 is '
                                                            'most deprived).',
                                             'income_score': 'IoD 2025 File 7 column: Income Score (rate).',
                                             'indoors_decile': 'IoD 2025 File 7 column: Indoors Sub-domain '
                                                               'Decile (where 1 is most deprived 10% of '
                                                               'LSOAs).',
                                             'indoors_rank': 'IoD 2025 File 7 column: Indoors Sub-domain '
                                                             'Rank (where 1 is most deprived).',
                                             'indoors_score': 'IoD 2025 File 7 column: Indoors Sub-domain '
                                                              'Score.',
                                             'iod_release': 'Indices of Deprivation release this row comes '
                                                            'from (IoD2025).',
                                             'lad24_code': 'IoD 2025 File 7 column: Local Authority District '
                                                           'code (2024).',
                                             'lad24_name': 'IoD 2025 File 7 column: Local Authority District '
                                                           'name (2024).',
                                             'living_environment_decile': 'IoD 2025 File 7 column: Living '
                                                                          'Environment Decile (where 1 is '
                                                                          'most deprived 10% of LSOAs).',
                                             'living_environment_rank': 'IoD 2025 File 7 column: Living '
                                                                        'Environment Rank (where 1 is most '
                                                                        'deprived).',
                                             'living_environment_score': 'IoD 2025 File 7 column: Living '
                                                                         'Environment Score.',
                                             'loaded_at': '3_lookup.geo.iod_2025_lsoa.ADC_UPDT carried '
                                                          'unchanged (lookup load time, not a Silver refresh '
                                                          'time).',
                                             'lsoa': '2021 LSOA code (England); primary key. Joins to '
                                                     'reference_person_address.lsoa.',
                                             'lsoa21_name': 'IoD 2025 File 7 column: LSOA name (2021).',
                                             'outdoors_decile': 'IoD 2025 File 7 column: Outdoors Sub-domain '
                                                                'Decile (where 1 is most deprived 10% of '
                                                                'LSOAs).',
                                             'outdoors_rank': 'IoD 2025 File 7 column: Outdoors Sub-domain '
                                                              'Rank (where 1 is most deprived).',
                                             'outdoors_score': 'IoD 2025 File 7 column: Outdoors Sub-domain '
                                                               'Score.',
                                             'population_children_0_15_mid2022': 'IoD 2025 File 7 column: '
                                                                                 'Dependent Children aged '
                                                                                 '0-15: mid 2022.',
                                             'population_older_60_plus_mid2022': 'IoD 2025 File 7 column: '
                                                                                 'Older population aged 60 '
                                                                                 'and over: mid 2022.',
                                             'population_total_mid2022': 'IoD 2025 File 7 column: Total '
                                                                         'population: mid 2022.',
                                             'population_working_age_18_66_mid2022': 'IoD 2025 File 7 '
                                                                                     'column: Working age '
                                                                                     'population 18-66 (for '
                                                                                     'use with Employment '
                                                                                     'Deprivation Domain): '
                                                                                     'mid 2022.',
                                             'wider_barriers_decile': 'IoD 2025 File 7 column: Wider '
                                                                      'Barriers Sub-domain Decile (where 1 '
                                                                      'is most deprived 10% of LSOAs).',
                                             'wider_barriers_rank': 'IoD 2025 File 7 column: Wider Barriers '
                                                                    'Sub-domain Rank (where 1 is most '
                                                                    'deprived).',
                                             'wider_barriers_score': 'IoD 2025 File 7 column: Wider Barriers '
                                                                     'Sub-domain Score.'},
                                 'table': 'English Indices of Deprivation 2025 for every England LSOA (2021 '
                                          'boundaries): IMD and all domain and sub-domain scores, ranks and '
                                          'deciles, with mid-2022 populations. One row per lsoa.'}}
