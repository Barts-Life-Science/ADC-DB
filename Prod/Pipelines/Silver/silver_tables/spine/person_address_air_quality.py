"""Silver spine.person_address_air_quality: 1 dataset(s).

- datasets: reference_person_address_air_quality
- moved verbatim from: 1_silver_spine_and_reference:6022, 1_silver_spine_and_reference:6089, 1_silver_spine_and_reference:6184

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, spark
from silver_tables.spine._shared import AQ_LOCATION


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


AQ_PUBLIC = [
    "person_address_key", "address_id", "person_id", "month", "pollutant",
    "nearest_within_2km_value", "nearest_site_id", "nearest_site_distance_km",
    "tri_weighted_25km_value", "tri_weighted_site_count", "min_data_capture_pct",
]


def aq_attach_residency(addresses, estimates):
    # Join location estimates to patient addresses and retain only months within each residency
    # period.
    return addresses.join(estimates, AQ_LOCATION, "inner").where(
        (F.col("month") >= F.col("_residency_start_month"))
        & (F.col("month") <= F.col("_residency_end_month"))
    ).select(*AQ_PUBLIC)


# Reference / person address air quality
@materialized_view(
    name=_n("journey_reference.person_address_air_quality"),
    refresh_policy="auto",
)
def person_address_air_quality():
    # Geometry and location estimates are reused internally. Exact original
    # person-address residency bounds are applied here, including month endpoints.
    # Build the declared dataset: Monthly ambient air-quality estimates for the months a person
    # lived at an address, using sites that reported each pollutant-month. Publishes nearest-
    # site within 2 km and inverse-square-distance weighted nearest-three within 25 km DAR059
    # methods.
    return aq_attach_residency(
        spark.read.table(_n("journey_reference._air_quality_assignments")),
        spark.read.table(_n("journey_reference._air_quality_estimates")),
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_person_address_air_quality': {'columns': {'address_id': 'Native Millennium ADDRESS_ID for the '
                                                                    'address assignment.',
                                                      'min_data_capture_pct': 'Lowest source data-capture '
                                                                              'percentage among the '
                                                                              'contributing sites; a '
                                                                              'completeness caveat rather '
                                                                              'than a quality gate.',
                                                      'month': 'First day of the calendar month the estimate '
                                                               'covers; the person lived at the address '
                                                               'during this month.',
                                                      'nearest_site_distance_km': 'Great-circle distance '
                                                                                  'from the address to '
                                                                                  'nearest_site_id, in '
                                                                                  'kilometres.',
                                                      'nearest_site_id': 'Site identifier of the nearest '
                                                                         'site that reported this pollutant '
                                                                         'in this month within 25 km.',
                                                      'nearest_within_2km_value': 'Monthly mean at the '
                                                                                  'nearest reporting site '
                                                                                  'when that site is within '
                                                                                  '2 km; NULL otherwise. '
                                                                                  'DAR059 method '
                                                                                  'nearest_within_2km.',
                                                      'person_address_key': 'Deterministic key of the '
                                                                            'address assignment this '
                                                                            'estimate belongs to; joins to '
                                                                            'reference_person_address.person_address_key.',
                                                      'person_id': 'Native Millennium PERSON_ID of the '
                                                                   'address holder.',
                                                      'pollutant': 'Pollutant measure reported by bronze '
                                                                   'map_air_quality_site_month, such as NO2 '
                                                                   'or PM2.5.',
                                                      'tri_weighted_25km_value': 'Inverse-square-distance-weighted '
                                                                                 'mean of up to the three '
                                                                                 'nearest reporting sites '
                                                                                 'within 25 km, using weight '
                                                                                 '1/max(distance_km, 0.1)^2. '
                                                                                 'DAR059 method '
                                                                                 'tri_weighted_25km.',
                                                      'tri_weighted_site_count': 'Number of reporting sites, '
                                                                                 'from one to three, that '
                                                                                 'contributed to '
                                                                                 'tri_weighted_25km_value.'},
                                          'table': 'Monthly ambient air-quality estimates for the months a '
                                                   'person lived at an address, using sites that reported '
                                                   'each pollutant-month. Publishes nearest-site within 2 km '
                                                   'and inverse-square-distance weighted nearest-three '
                                                   'within 25 km DAR059 methods.'}}

# INSR_R4_SILVER_V1 (aq): generated by r4/release/gen_silver_components.py; edit the generator, not this.
from silver_core.parent_inputs import with_cluster_by  # noqa: E402
SPECS = with_cluster_by(SPECS, 'reference_person_address_air_quality', ('person_id',))
