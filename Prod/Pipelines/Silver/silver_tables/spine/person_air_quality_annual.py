"""Silver spine.person_air_quality_annual: 1 dataset(s).

- datasets: reference_person_air_quality_annual
- moved verbatim from: 1_silver_spine_and_reference:6208, 1_silver_spine_and_reference:6213, 1_silver_spine_and_reference:6222, 1_silver_spine_and_reference:6238, 1_silver_spine_and_reference:6250

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, spark, stable_id


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# ==== journey_reference.person_air_quality_annual (INSR_W7_AQ_ANNUAL_V1, W7.07b; J15 ruling 2026-10-29) ====

# The pollutants the browser offers; the others stay in the monthly product.  # INSR_W7 W7.07b
AQ_ANNUAL_POLLUTANTS = ['no2', 'pm2_5', 'pm10', 'o3']


def _aq_pollutant_mean(pollutant, value_column):
    """Mean of one monthly value over a pollutant's address-months: summed as DECIMAL(38,10), because an Enzyme
    refresh refuses SUM/AVG over DOUBLE at run time, then divided by the count and cast back to DOUBLE."""
    hit = F.col("pollutant") == pollutant
    total = F.sum(F.when(hit, F.col(value_column).cast("decimal(38,10)")))
    months = F.count(F.when(hit, F.col(value_column)))
    return (total / months).cast("double")


def _aq_annual_columns():
    """Four aggregates per pollutant (tri-weighted mean, nearest-2 km mean, nearest-site distance, months) and the
    year's lowest data capture."""
    columns = []
    for pollutant in AQ_ANNUAL_POLLUTANTS:
        hit = F.col("pollutant") == pollutant
        columns += [
            _aq_pollutant_mean(pollutant, "tri_weighted_25km_value").alias(f"{pollutant}_annual_mean"),
            _aq_pollutant_mean(pollutant, "nearest_within_2km_value").alias(f"{pollutant}_nearest_2km_annual_mean"),
            _aq_pollutant_mean(pollutant, "nearest_site_distance_km").alias(
                f"{pollutant}_nearest_site_distance_km_mean"),
            F.count(F.when(hit, F.col("tri_weighted_25km_value"))).alias(f"{pollutant}_address_months"),
        ]
    return columns + [F.min("min_data_capture_pct").alias("min_data_capture_pct")]


def aq_annual(monthly):
    """One row per person and calendar year from the monthly address estimates. Pure (the W7 fixture notebook runs it on
    hand-built rows): a filter, one GROUP BY and a deterministic key, with no window, literal date or join."""
    return (monthly
            .where(F.col("pollutant").isin(AQ_ANNUAL_POLLUTANTS) & F.col("person_id").isNotNull())
            .groupBy("person_id", F.trunc(F.col("month"), "year").alias("year_start"))
            .agg(*_aq_annual_columns())
            .withColumn("person_air_quality_year_key",
                        stable_id("person_air_quality_year:aq", F.col("person_id"), F.col("year_start")))
            .select(*['person_air_quality_year_key', 'person_id', 'year_start', 'no2_annual_mean', 'no2_nearest_2km_annual_mean', 'no2_nearest_site_distance_km_mean', 'no2_address_months', 'pm2_5_annual_mean', 'pm2_5_nearest_2km_annual_mean', 'pm2_5_nearest_site_distance_km_mean', 'pm2_5_address_months', 'pm10_annual_mean', 'pm10_nearest_2km_annual_mean', 'pm10_nearest_site_distance_km_mean', 'pm10_address_months', 'o3_annual_mean', 'o3_nearest_2km_annual_mean', 'o3_nearest_site_distance_km_mean', 'o3_address_months', 'min_data_capture_pct']))


@materialized_view(
    name=_n("journey_reference.person_air_quality_annual"),
    # INSR_W7 review: "auto", the estate policy since EIR_20261021. "incremental" is not strict (it never fails a
    # refresh); it skips the cost model, so a full rewrite of the 14.65bn-row monthly source would run ROW_BASED
    # instead of recomputing (the 10-19/10-20 release-day cascade).
    refresh_policy="auto",
)
def person_air_quality_annual():
    """INSR_W7 W7.07b: the browser's air-quality criteria read this table, not the monthly product."""
    return aq_annual(spark.read.table(_n("journey_reference.person_address_air_quality")))


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


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
                                         'table': 'Annual air-quality means per person and calendar year, '
                                                  'from the monthly DAR059 address estimates in '
                                                  'person_address_air_quality (NO2, PM2.5, PM10, O3). '
                                                  'Derived values only: no address, site or area code (J15 '
                                                  'ruling 2026-10-29).'}}
