"""Silver spine.air_quality_geometry: 1 dataset(s).

- datasets: _reference_air_quality_geometry
- moved verbatim from: 1_silver_spine_and_reference:6062, 1_silver_spine_and_reference:6072, 1_silver_spine_and_reference:6148

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


def aq_distance():
    # Operation order and constants intentionally match the parity-proven query.
    # Calculate great-circle distance in kilometres between address and monitoring-site
    # coordinates.
    return F.lit(2.0) * F.lit(6371.0088) * F.asin(F.sqrt(
        F.pow(F.sin(F.radians(F.col("site_latitude") - F.col("address_latitude")) / F.lit(2.0)), F.lit(2.0))
        + F.cos(F.radians("address_latitude")) * F.cos(F.radians("site_latitude"))
        * F.pow(F.sin(F.radians(F.col("site_longitude") - F.col("address_longitude")) / F.lit(2.0)), F.lit(2.0))
    ))


def aq_geometry(addresses, observations):
    # Generate nearby address/site candidates using neighbouring spatial cells and calculate
    # their distances.
    locations = addresses.select(*AQ_LOCATION).distinct()
    sites = observations.select(*AQ_SITE_LOCATION).distinct()
    # The compact geometry relation is independent of month and person. Retain all
    # sites within the original 5x5 neighbourhood and 25km radius, not a static top 3.
    # Expand the small station-geometry side (currently 1,339 x 25), not millions
    # of assignments. Equality keys avoid a location x all-sites nested-loop join.
    site_neighbours = sites.withColumn("_cell", F.explode(F.array(*[
        F.struct((F.col("site_lat_cell") + dx).alias("address_lat_cell"),
                 (F.col("site_lon_cell") + dy).alias("address_lon_cell"))
        for dx in (-2, -1, 0, 1, 2) for dy in (-2, -1, 0, 1, 2)
    ]))).select(*AQ_SITE_LOCATION, "_cell.*")
    nearby = locations.join(site_neighbours, ["address_lat_cell", "address_lon_cell"], "inner").withColumn("distance_km", aq_distance()).where(F.col("distance_km") <= 25.0)
    return nearby.select(*AQ_LOCATION, *AQ_SITE_LOCATION, "distance_km")


@materialized_view(
    name=_n("journey_reference._air_quality_geometry"), private=True,
    refresh_policy="auto",
)
def _air_quality_geometry():
    # Build the declared dataset: Internal great-circle distances for distinct address locations
    # and monitoring-site positions within the original 5x5 cells and 25 km; retains all
    # eligible sites.
    return aq_geometry(
        spark.read.table(_n("journey_reference._air_quality_assignments")),
        aq_observations(read_source(SRC_AIR_QUALITY_SITE_MONTH)),
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_reference_air_quality_geometry': {'columns': {},
                                     'table': 'Internal great-circle distances for distinct address '
                                              'locations and monitoring-site positions within the original '
                                              '5x5 cells and 25 km; retains all eligible sites.'}}
