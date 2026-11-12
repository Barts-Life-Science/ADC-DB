"""Gold spine.person_air_quality_annual: 1 dataset(s).

- datasets: reference_person_air_quality_annual
- moved verbatim from: 1_gold_spine_and_reference:3461, 1_gold_spine_and_reference:3466

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from gold_core.gold_core import _n, _src, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== gold_reference.person_air_quality_annual (INSR_W7_AQ_ANNUAL_V1, W7.07b) ====
REFERENCE_PERSON_AIR_QUALITY_ANNUAL_COLUMNS = ['person_air_quality_year_key', 'person_id', 'year_start', 'no2_annual_mean', 'no2_nearest_2km_annual_mean', 'no2_nearest_site_distance_km_mean', 'no2_address_months', 'pm2_5_annual_mean', 'pm2_5_nearest_2km_annual_mean', 'pm2_5_nearest_site_distance_km_mean', 'pm2_5_address_months', 'pm10_annual_mean', 'pm10_nearest_2km_annual_mean', 'pm10_nearest_site_distance_km_mean', 'pm10_address_months', 'o3_annual_mean', 'o3_nearest_2km_annual_mean', 'o3_nearest_site_distance_km_mean', 'o3_address_months', 'min_data_capture_pct']  # INSR_W7 W7.07b


@dp.materialized_view(
    name=_n("gold_reference.person_air_quality_annual"),
    table_properties={"quality": "gold"},
    cluster_by=["person_id"],
    refresh_policy="auto",
)
def gold_reference_person_air_quality_annual():
    """INSR_W7 W7.07b: public twin of reference_person_air_quality_annual, contained like the monthly twin."""
    kept = (spark.read.table(_n("gold_reference.person_address"))  # GOLD_MV_SPLIT_V1: the published (kept) rows
            .where(F.col("person_id").isNotNull())
            .select(F.col("person_id").alias("_gold_person_id"))
            .distinct())
    df = (spark.read.table(_src("reference_person_air_quality_annual"))
          .join(kept, F.col("person_id") == F.col("_gold_person_id"), "inner")
          .select(*REFERENCE_PERSON_AIR_QUALITY_ANNUAL_COLUMNS))
    return df


finalise()


COMMENTS = {'reference_person_air_quality_annual': {'columns': {'min_data_capture_pct': 'Lowest source data-capture '
                                                                             'percentage among the sites '
                                                                             'behind any monthly estimate in '
                                                                             'the year; a completeness '
                                                                             'caveat, not a quality gate.',
                                                     'no2_address_months': 'Address-months in the year with '
                                                                           'a NO2 estimate (12 per address '
                                                                           'for a full year; two addresses '
                                                                           'in one month count twice).',
                                                     'no2_annual_mean': "Mean NO2 over the person's "
                                                                        'address-months in the year, from '
                                                                        'the monthly DAR059 '
                                                                        'tri_weighted_25km_value estimates '
                                                                        '(reference_person_address_air_quality).',
                                                     'no2_nearest_2km_annual_mean': 'Mean NO2 over the '
                                                                                    'address-months that had '
                                                                                    'a reporting site within '
                                                                                    '2 km (DAR059 '
                                                                                    'nearest_within_2km_value); '
                                                                                    'NULL when none did.',
                                                     'no2_nearest_site_distance_km_mean': 'Mean distance in '
                                                                                          'km from the '
                                                                                          "person's "
                                                                                          'addresses to the '
                                                                                          'nearest site '
                                                                                          'reporting NO2 in '
                                                                                          'each month.',
                                                     'o3_address_months': 'Address-months in the year with a '
                                                                          'O3 estimate (12 per address for a '
                                                                          'full year; two addresses in one '
                                                                          'month count twice).',
                                                     'o3_annual_mean': "Mean O3 over the person's "
                                                                       'address-months in the year, from the '
                                                                       'monthly DAR059 '
                                                                       'tri_weighted_25km_value estimates '
                                                                       '(reference_person_address_air_quality).',
                                                     'o3_nearest_2km_annual_mean': 'Mean O3 over the '
                                                                                   'address-months that had '
                                                                                   'a reporting site within '
                                                                                   '2 km (DAR059 '
                                                                                   'nearest_within_2km_value); '
                                                                                   'NULL when none did.',
                                                     'o3_nearest_site_distance_km_mean': 'Mean distance in '
                                                                                         'km from the '
                                                                                         "person's addresses "
                                                                                         'to the nearest '
                                                                                         'site reporting O3 '
                                                                                         'in each month.',
                                                     'person_air_quality_year_key': 'Deterministic key of '
                                                                                    'the person and calendar '
                                                                                    'year.',
                                                     'person_id': 'Native Millennium PERSON_ID of the person '
                                                                  'whose address estimates are averaged.',
                                                     'pm10_address_months': 'Address-months in the year with '
                                                                            'a PM10 estimate (12 per address '
                                                                            'for a full year; two addresses '
                                                                            'in one month count twice).',
                                                     'pm10_annual_mean': "Mean PM10 over the person's "
                                                                         'address-months in the year, from '
                                                                         'the monthly DAR059 '
                                                                         'tri_weighted_25km_value estimates '
                                                                         '(reference_person_address_air_quality).',
                                                     'pm10_nearest_2km_annual_mean': 'Mean PM10 over the '
                                                                                     'address-months that '
                                                                                     'had a reporting site '
                                                                                     'within 2 km (DAR059 '
                                                                                     'nearest_within_2km_value); '
                                                                                     'NULL when none did.',
                                                     'pm10_nearest_site_distance_km_mean': 'Mean distance in '
                                                                                           'km from the '
                                                                                           "person's "
                                                                                           'addresses to the '
                                                                                           'nearest site '
                                                                                           'reporting PM10 '
                                                                                           'in each month.',
                                                     'pm2_5_address_months': 'Address-months in the year '
                                                                             'with a PM2.5 estimate (12 per '
                                                                             'address for a full year; two '
                                                                             'addresses in one month count '
                                                                             'twice).',
                                                     'pm2_5_annual_mean': "Mean PM2.5 over the person's "
                                                                          'address-months in the year, from '
                                                                          'the monthly DAR059 '
                                                                          'tri_weighted_25km_value estimates '
                                                                          '(reference_person_address_air_quality).',
                                                     'pm2_5_nearest_2km_annual_mean': 'Mean PM2.5 over the '
                                                                                      'address-months that '
                                                                                      'had a reporting site '
                                                                                      'within 2 km (DAR059 '
                                                                                      'nearest_within_2km_value); '
                                                                                      'NULL when none did.',
                                                     'pm2_5_nearest_site_distance_km_mean': 'Mean distance '
                                                                                            'in km from the '
                                                                                            "person's "
                                                                                            'addresses to '
                                                                                            'the nearest '
                                                                                            'site reporting '
                                                                                            'PM2.5 in each '
                                                                                            'month.',
                                                     'year_start': 'First day of the calendar year the means '
                                                                   'cover. The current year is partial.'},
                                         'quantity_override': False,
                                         'table': 'Annual air-quality means per person and calendar year '
                                                  '(NO2, PM2.5, PM10, O3). Gold twin of the silver product, '
                                                  'restricted to people with an address assignment published '
                                                  'in gold reference_person_address.'}}
