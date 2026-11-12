"""Gold spine.area_iod2025_lsoa: 2 dataset(s).

- datasets: _qc_reference_area_iod2025_lsoa, reference_area_iod2025_lsoa
- moved verbatim from: 1_gold_spine_and_reference:3517, 1_gold_spine_and_reference:3521, 1_gold_spine_and_reference:3530

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.area_iod2025_lsoa (GEO_AREA_ENRICHMENT_V1) ====
REFERENCE_AREA_IOD2025_LSOA_SELECT = ['`lsoa` AS `lsoa`', '`lsoa21_name` AS `lsoa21_name`', '`lad24_code` AS `lad24_code`', '`lad24_name` AS `lad24_name`', '`imd_score` AS `imd_score`', '`imd_rank` AS `imd_rank`', '`imd_decile` AS `imd_decile`', '`income_score` AS `income_score`', '`income_rank` AS `income_rank`', '`income_decile` AS `income_decile`', '`employment_score` AS `employment_score`', '`employment_rank` AS `employment_rank`', '`employment_decile` AS `employment_decile`', '`education_score` AS `education_score`', '`education_rank` AS `education_rank`', '`education_decile` AS `education_decile`', '`health_score` AS `health_score`', '`health_rank` AS `health_rank`', '`health_decile` AS `health_decile`', '`crime_score` AS `crime_score`', '`crime_rank` AS `crime_rank`', '`crime_decile` AS `crime_decile`', '`barriers_score` AS `barriers_score`', '`barriers_rank` AS `barriers_rank`', '`barriers_decile` AS `barriers_decile`', '`living_environment_score` AS `living_environment_score`', '`living_environment_rank` AS `living_environment_rank`', '`living_environment_decile` AS `living_environment_decile`', '`idaci_score` AS `idaci_score`', '`idaci_rank` AS `idaci_rank`', '`idaci_decile` AS `idaci_decile`', '`idaopi_score` AS `idaopi_score`', '`idaopi_rank` AS `idaopi_rank`', '`idaopi_decile` AS `idaopi_decile`', '`children_young_people_score` AS `children_young_people_score`', '`children_young_people_rank` AS `children_young_people_rank`', '`children_young_people_decile` AS `children_young_people_decile`', '`adult_skills_score` AS `adult_skills_score`', '`adult_skills_rank` AS `adult_skills_rank`', '`adult_skills_decile` AS `adult_skills_decile`', '`geographical_barriers_score` AS `geographical_barriers_score`', '`geographical_barriers_rank` AS `geographical_barriers_rank`', '`geographical_barriers_decile` AS `geographical_barriers_decile`', '`wider_barriers_score` AS `wider_barriers_score`', '`wider_barriers_rank` AS `wider_barriers_rank`', '`wider_barriers_decile` AS `wider_barriers_decile`', '`indoors_score` AS `indoors_score`', '`indoors_rank` AS `indoors_rank`', '`indoors_decile` AS `indoors_decile`', '`outdoors_score` AS `outdoors_score`', '`outdoors_rank` AS `outdoors_rank`', '`outdoors_decile` AS `outdoors_decile`', '`population_total_mid2022` AS `population_total_mid2022`', '`population_children_0_15_mid2022` AS `population_children_0_15_mid2022`', '`population_older_60_plus_mid2022` AS `population_older_60_plus_mid2022`', '`population_working_age_18_66_mid2022` AS `population_working_age_18_66_mid2022`', '`imd_quintile` AS `imd_quintile`', '`iod_release` AS `iod_release`', '`loaded_at` AS `loaded_at`']


@dp.materialized_view(
    name=_n("gold_qc._reference_area_iod2025_lsoa"),
    refresh_policy="auto",
)
def _gold_qc_reference_area_iod2025_lsoa():
    """Quality-controlled twin of journey_reference.area_iod2025_lsoa."""
    return _qc("reference_area_iod2025_lsoa", REFERENCE_AREA_IOD2025_LSOA_SELECT)


@dp.materialized_view(
    name=_n("gold_reference.area_iod2025_lsoa"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_area_iod2025_lsoa():
    """Public twin of reference_area_iod2025_lsoa."""
    df = spark.read.table(_n("gold_qc._reference_area_iod2025_lsoa")).selectExpr(*REFERENCE_AREA_IOD2025_LSOA_SELECT)
    return df


RULES = {
    'reference_area_iod2025_lsoa': GoldRules(
        table='reference_area_iod2025_lsoa',
        select=REFERENCE_AREA_IOD2025_LSOA_SELECT),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_area_iod2025_lsoa': {'columns': {'adult_skills_decile': 'IoD 2025 File 7 column: Adult '
                                                                        'Skills Sub-domain Decile (where 1 '
                                                                        'is most deprived 10% of LSOAs).',
                                                 'adult_skills_rank': 'IoD 2025 File 7 column: Adult Skills '
                                                                      'Sub-domain Rank (where 1 is most '
                                                                      'deprived).',
                                                 'adult_skills_score': 'IoD 2025 File 7 column: Adult Skills '
                                                                       'Sub-domain Score.',
                                                 'barriers_decile': 'IoD 2025 File 7 column: Barriers to '
                                                                    'Housing and Services Decile (where 1 is '
                                                                    'most deprived 10% of LSOAs).',
                                                 'barriers_rank': 'IoD 2025 File 7 column: Barriers to '
                                                                  'Housing and Services Rank (where 1 is '
                                                                  'most deprived).',
                                                 'barriers_score': 'IoD 2025 File 7 column: Barriers to '
                                                                   'Housing and Services Score.',
                                                 'children_young_people_decile': 'IoD 2025 File 7 column: '
                                                                                 'Children and Young People '
                                                                                 'Sub-domain Decile (where 1 '
                                                                                 'is most deprived 10% of '
                                                                                 'LSO.',
                                                 'children_young_people_rank': 'IoD 2025 File 7 column: '
                                                                               'Children and Young People '
                                                                               'Sub-domain Rank (where 1 is '
                                                                               'most deprived).',
                                                 'children_young_people_score': 'IoD 2025 File 7 column: '
                                                                                'Children and Young People '
                                                                                'Sub-domain Score.',
                                                 'crime_decile': 'IoD 2025 File 7 column: Crime Decile '
                                                                 '(where 1 is most deprived 10% of LSOAs).',
                                                 'crime_rank': 'IoD 2025 File 7 column: Crime Rank (where 1 '
                                                               'is most deprived).',
                                                 'crime_score': 'IoD 2025 File 7 column: Crime Score.',
                                                 'education_decile': 'IoD 2025 File 7 column: Education, '
                                                                     'Skills and Training Decile (where 1 is '
                                                                     'most deprived 10% of LSOAs).',
                                                 'education_rank': 'IoD 2025 File 7 column: Education, '
                                                                   'Skills and Training Rank (where 1 is '
                                                                   'most deprived).',
                                                 'education_score': 'IoD 2025 File 7 column: Education, '
                                                                    'Skills and Training Score.',
                                                 'employment_decile': 'IoD 2025 File 7 column: Employment '
                                                                      'Decile (where 1 is most deprived 10% '
                                                                      'of LSOAs).',
                                                 'employment_rank': 'IoD 2025 File 7 column: Employment Rank '
                                                                    '(where 1 is most deprived).',
                                                 'employment_score': 'IoD 2025 File 7 column: Employment '
                                                                     'Score (rate).',
                                                 'geographical_barriers_decile': 'IoD 2025 File 7 column: '
                                                                                 'Geographical Barriers '
                                                                                 'Sub-domain Decile (where 1 '
                                                                                 'is most deprived 10% of '
                                                                                 'LSOAs).',
                                                 'geographical_barriers_rank': 'IoD 2025 File 7 column: '
                                                                               'Geographical Barriers '
                                                                               'Sub-domain Rank (where 1 is '
                                                                               'most deprived).',
                                                 'geographical_barriers_score': 'IoD 2025 File 7 column: '
                                                                                'Geographical Barriers '
                                                                                'Sub-domain Score.',
                                                 'health_decile': 'IoD 2025 File 7 column: Health '
                                                                  'Deprivation and Disability Decile (where '
                                                                  '1 is most deprived 10% of LSOAs).',
                                                 'health_rank': 'IoD 2025 File 7 column: Health Deprivation '
                                                                'and Disability Rank (where 1 is most '
                                                                'deprived).',
                                                 'health_score': 'IoD 2025 File 7 column: Health Deprivation '
                                                                 'and Disability Score.',
                                                 'idaci_decile': 'IoD 2025 File 7 column: Income Deprivation '
                                                                 'Affecting Children Index (IDACI) Decile '
                                                                 '(where 1 is most deprived 10% of LSOAs).',
                                                 'idaci_rank': 'IoD 2025 File 7 column: Income Deprivation '
                                                               'Affecting Children Index (IDACI) Rank (where '
                                                               '1 is most deprived).',
                                                 'idaci_score': 'IoD 2025 File 7 column: Income Deprivation '
                                                                'Affecting Children Index (IDACI) Score '
                                                                '(rate).',
                                                 'idaopi_decile': 'IoD 2025 File 7 column: Income '
                                                                  'Deprivation Affecting Older People '
                                                                  '(IDAOPI) Decile (where 1 is most deprived '
                                                                  '10% of LSOAs).',
                                                 'idaopi_rank': 'IoD 2025 File 7 column: Income Deprivation '
                                                                'Affecting Older People (IDAOPI) Rank (where '
                                                                '1 is most deprived).',
                                                 'idaopi_score': 'IoD 2025 File 7 column: Income Deprivation '
                                                                 'Affecting Older People (IDAOPI) Score '
                                                                 '(rate).',
                                                 'imd_decile': 'IoD 2025 File 7 column: Index of Multiple '
                                                               'Deprivation (IMD) Decile (where 1 is most '
                                                               'deprived 10% of LSOAs).',
                                                 'imd_quintile': 'Derived: IMD quintile = ceil(imd_decile / '
                                                                 '2), where 1 is the most deprived 20% of '
                                                                 'LSOAs in England.',
                                                 'imd_rank': 'IoD 2025 File 7 column: Index of Multiple '
                                                             'Deprivation (IMD) Rank (where 1 is most '
                                                             'deprived).',
                                                 'imd_score': 'IoD 2025 File 7 column: Index of Multiple '
                                                              'Deprivation (IMD) Score.',
                                                 'income_decile': 'IoD 2025 File 7 column: Income Decile '
                                                                  '(where 1 is most deprived 10% of LSOAs).',
                                                 'income_rank': 'IoD 2025 File 7 column: Income Rank (where '
                                                                '1 is most deprived).',
                                                 'income_score': 'IoD 2025 File 7 column: Income Score '
                                                                 '(rate).',
                                                 'indoors_decile': 'IoD 2025 File 7 column: Indoors '
                                                                   'Sub-domain Decile (where 1 is most '
                                                                   'deprived 10% of LSOAs).',
                                                 'indoors_rank': 'IoD 2025 File 7 column: Indoors Sub-domain '
                                                                 'Rank (where 1 is most deprived).',
                                                 'indoors_score': 'IoD 2025 File 7 column: Indoors '
                                                                  'Sub-domain Score.',
                                                 'iod_release': 'Indices of Deprivation release this row '
                                                                'comes from (IoD2025).',
                                                 'lad24_code': 'IoD 2025 File 7 column: Local Authority '
                                                               'District code (2024).',
                                                 'lad24_name': 'IoD 2025 File 7 column: Local Authority '
                                                               'District name (2024).',
                                                 'living_environment_decile': 'IoD 2025 File 7 column: '
                                                                              'Living Environment Decile '
                                                                              '(where 1 is most deprived 10% '
                                                                              'of LSOAs).',
                                                 'living_environment_rank': 'IoD 2025 File 7 column: Living '
                                                                            'Environment Rank (where 1 is '
                                                                            'most deprived).',
                                                 'living_environment_score': 'IoD 2025 File 7 column: Living '
                                                                             'Environment Score.',
                                                 'loaded_at': '3_lookup.geo.iod_2025_lsoa.ADC_UPDT carried '
                                                              'unchanged (lookup load time, not a Silver '
                                                              'refresh time).',
                                                 'lsoa': '2021 LSOA code (England); primary key. Joins to '
                                                         'reference_person_address.lsoa.',
                                                 'lsoa21_name': 'IoD 2025 File 7 column: LSOA name (2021).',
                                                 'outdoors_decile': 'IoD 2025 File 7 column: Outdoors '
                                                                    'Sub-domain Decile (where 1 is most '
                                                                    'deprived 10% of LSOAs).',
                                                 'outdoors_rank': 'IoD 2025 File 7 column: Outdoors '
                                                                  'Sub-domain Rank (where 1 is most '
                                                                  'deprived).',
                                                 'outdoors_score': 'IoD 2025 File 7 column: Outdoors '
                                                                   'Sub-domain Score.',
                                                 'population_children_0_15_mid2022': 'IoD 2025 File 7 '
                                                                                     'column: Dependent '
                                                                                     'Children aged 0-15: '
                                                                                     'mid 2022.',
                                                 'population_older_60_plus_mid2022': 'IoD 2025 File 7 '
                                                                                     'column: Older '
                                                                                     'population aged 60 and '
                                                                                     'over: mid 2022.',
                                                 'population_total_mid2022': 'IoD 2025 File 7 column: Total '
                                                                             'population: mid 2022.',
                                                 'population_working_age_18_66_mid2022': 'IoD 2025 File 7 '
                                                                                         'column: Working '
                                                                                         'age population '
                                                                                         '18-66 (for use '
                                                                                         'with Employment '
                                                                                         'Deprivation '
                                                                                         'Domain): mid 2022.',
                                                 'wider_barriers_decile': 'IoD 2025 File 7 column: Wider '
                                                                          'Barriers Sub-domain Decile (where '
                                                                          '1 is most deprived 10% of LSOAs).',
                                                 'wider_barriers_rank': 'IoD 2025 File 7 column: Wider '
                                                                        'Barriers Sub-domain Rank (where 1 '
                                                                        'is most deprived).',
                                                 'wider_barriers_score': 'IoD 2025 File 7 column: Wider '
                                                                         'Barriers Sub-domain Score.'},
                                     'quantity_override': True,
                                     'table': 'Internal quality-controlled twin of '
                                              'reference_area_iod2025_lsoa. No repairs or rules: English '
                                              'Indices of Deprivation 2025 LSOA values are published lookup '
                                              'values.'},
 'reference_area_iod2025_lsoa': {'columns': {'adult_skills_decile': 'IoD 2025 File 7 column: Adult Skills '
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
                                 'quantity_override': False,
                                 'table': 'English indices of deprivation 2025 lsoa values, one row per '
                                          'lsoa. Gold twin of the silver reference table; no rows dropped or '
                                          'repaired.'}}
