"""Silver spine.air_quality_estimates: 1 dataset(s).

- datasets: _reference_air_quality_estimates
- moved verbatim from: 1_silver_spine_and_reference:6097, 1_silver_spine_and_reference:6162

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, spark
from silver_tables.spine._shared import (
    AQ_LOCATION,
    AQ_SITE_LOCATION,
    SRC_AIR_QUALITY_SITE_MONTH,
    aq_observations,
)


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


def aq_location_estimates_join(addresses, geometry, observations):
    """Native relational comparator: reuse geometry before monthly expansion.

    Unlike the array prototype, all joins/aggregates are Photon-native. Person
    identities and their repetition are absent until the final residency join.
    """
    scopes = addresses.groupBy(*AQ_LOCATION).agg(
        F.min("_residency_start_month").alias("_first_month"),
        F.max("_residency_end_month").alias("_last_month"),
    )
    pairs = geometry.join(scopes, AQ_LOCATION, "inner")
    candidates = pairs.join(observations, AQ_SITE_LOCATION, "inner").where(
        (F.col("month_start") >= F.col("_first_month")) & (F.col("month_start") <= F.col("_last_month")))
    ranked = candidates.groupBy(*AQ_LOCATION, "month_start", "pollutant").agg(
        F.slice(F.array_sort(F.collect_list(F.struct("distance_km", "site_id", "measurement_value", "data_capture_pct"))), 1, 3).alias("nearest3"))
    weight = lambda site: F.lit(1.0) / F.pow(F.greatest(site["distance_km"], F.lit(0.1)), F.lit(2.0))
    return ranked.select(
        *AQ_LOCATION, F.col("month_start").alias("month"), "pollutant",
        F.when(F.col("nearest3")[0]["distance_km"] <= 2.0, F.col("nearest3")[0]["measurement_value"]).alias("nearest_within_2km_value"),
        F.col("nearest3")[0]["site_id"].alias("nearest_site_id"),
        F.col("nearest3")[0]["distance_km"].alias("nearest_site_distance_km"),
        (F.aggregate("nearest3", F.lit(0.0), lambda total, site: total + site["measurement_value"] * weight(site))
         / F.aggregate("nearest3", F.lit(0.0), lambda total, site: total + weight(site))).alias("tri_weighted_25km_value"),
        F.size("nearest3").alias("tri_weighted_site_count"),
        F.array_min(F.transform("nearest3", lambda site: site["data_capture_pct"])).alias("min_data_capture_pct"),
    )


@materialized_view(
    name=_n("journey_reference._air_quality_estimates"), private=True,
    refresh_policy="auto",
)
def _air_quality_estimates():
    # Build the declared dataset: Internal location/month/pollutant estimates, using the nearest
    # three reporting observations and parity-proven inverse-square weights; exact person
    # residency is attached by the research product.
    return aq_location_estimates_join(
        spark.read.table(_n("journey_reference._air_quality_assignments")),
        spark.read.table(_n("journey_reference._air_quality_geometry")),
        aq_observations(read_source(SRC_AIR_QUALITY_SITE_MONTH)),
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_reference_air_quality_estimates': {'columns': {},
                                      'table': 'Internal location/month/pollutant estimates, using the '
                                               'nearest three reporting observations and parity-proven '
                                               'inverse-square weights; exact person residency is attached '
                                               'by the research product.'}}
