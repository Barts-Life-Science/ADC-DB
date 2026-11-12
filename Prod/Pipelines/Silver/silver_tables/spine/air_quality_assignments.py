"""Silver spine.air_quality_assignments: 1 dataset(s).

- datasets: _reference_air_quality_assignments
- moved verbatim from: 1_silver_spine_and_reference:5998, 1_silver_spine_and_reference:6028, 1_silver_spine_and_reference:6135

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from functools import reduce
import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, spark
from silver_tables.spine._shared import AQ_LOCATION


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Journey_reference.person_address_air_quality

# ==== journey_reference.person_address_air_quality ====
SRC_ADDRESS_AIR_QUALITY_CELL = "4_prod.bronze.map_address_air_quality_cell"


def aq_addresses(addresses, cells):
    # Select usable patient addresses and express their residency periods as inclusive start and
    # end months.
    a = addresses.where(
        F.col("person_address_key").isNotNull() & F.col("address_id").isNotNull()
        & F.col("person_id").isNotNull() & F.col("beg_effective").isNotNull()
    ).select(
        "person_address_key", F.col("address_id").cast("bigint").alias("address_id"),
        F.col("person_id").cast("bigint").alias("person_id"),
        F.trunc(F.to_date("beg_effective"), "month").alias("_residency_start_month"),
        F.trunc(F.to_date(F.coalesce("end_effective", F.lit("2100-01-01").cast("timestamp"))), "month").alias("_residency_end_month"),
    )
    c = cells.select(
        F.col("ADDRESS_ID").cast("bigint").alias("address_id"),
        F.col("LATITUDE").cast("double").alias("address_latitude"),
        F.col("LONGITUDE").cast("double").alias("address_longitude"),
        F.col("lat_cell").alias("address_lat_cell"), F.col("lon_cell").alias("address_lon_cell"),
    ).where(reduce(lambda x, y: x & y, [F.col(c).isNotNull() for c in AQ_LOCATION]))
    return a.join(c, "address_id", "inner")


# Reference /  air quality assignments

# Reusable implementation tables live only in the configured internal schema.
# Public lineage (native assignment/person IDs and chosen monitoring site) stays
# on reference_person_address_air_quality; no companion research view is added.
@materialized_view(
    name=_n("journey_reference._air_quality_assignments"), private=True,
    refresh_policy="auto",
)
def _air_quality_assignments():
    # Build the declared dataset: Internal eligible address assignments with exact residency
    # months and air-quality coordinates; supports location-level estimate reuse.
    return aq_addresses(
        spark.read.table(_n("journey_reference.person_address")),
        read_source(SRC_ADDRESS_AIR_QUALITY_CELL),
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_reference_air_quality_assignments': {'columns': {},
                                        'table': 'Internal eligible address assignments with exact residency '
                                                 'months and air-quality coordinates; supports '
                                                 'location-level estimate reuse.'}}
