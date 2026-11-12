"""Gold spine.person_address_air_quality: 1 dataset(s).

- datasets: reference_person_address_air_quality
- moved verbatim from: 1_gold_spine_and_reference:3430, 1_gold_spine_and_reference:3434

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from gold_core.gold_core import _n, _src, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.person_address_air_quality (GEO_AREA_ENRICHMENT_V1) ====

# 14.65B rows (2026-10-13), so one published MV and no separate QC twin. Containment matches reference_person_address:
# a row is kept only when its address assignment survives that table's QC twin with a person the spine holds (the
# silver product also covers 2.04M people outside the spine). Both join inputs are MVs, so the refresh stays incremental.
REFERENCE_PERSON_ADDRESS_AIR_QUALITY_COLUMNS = ['person_address_key', 'address_id', 'person_id', 'month', 'pollutant', 'nearest_within_2km_value', 'nearest_site_id', 'nearest_site_distance_km', 'tri_weighted_25km_value', 'tri_weighted_site_count', 'min_data_capture_pct']


@dp.materialized_view(
    name=_n("gold_reference.person_address_air_quality"),
    table_properties={"quality": "gold"},
    cluster_by=["person_id"],
    refresh_policy="auto",
)
def gold_reference_person_address_air_quality():
    """Public twin of reference_person_address_air_quality, contained by the gold address assignments."""
    # GOLD_MV_SPLIT_V1: the published (kept) address rows - person_address is a single copy, so its twin is never
    # declared; the R4 view joined the person_address view, i.e. the same rows.
    kept = (spark.read.table(_n("gold_reference.person_address"))
            .where(F.col("person_id").isNotNull())
            .select(F.col("person_address_key").alias("_gold_person_address_key")))
    df = (spark.read.table(_src("reference_person_address_air_quality"))
          .join(kept, F.col("person_address_key") == F.col("_gold_person_address_key"), "inner")
          .select(*REFERENCE_PERSON_ADDRESS_AIR_QUALITY_COLUMNS))
    return df


finalise()


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
                                          'quantity_override': False,
                                          'table': 'Monthly ambient air-quality estimates for each person '
                                                   'address assignment and pollutant (DAR059 methods: '
                                                   'nearest site within 2 km, inverse-distance weighting of '
                                                   'up to three sites within 25 km). Gold twin of the silver '
                                                   'product, restricted to address assignments published in '
                                                   'gold reference_person_address. Coverage: Gravimetric '
                                                   'PM10 and PM2.5 stop in July 2026 at the reporting '
                                                   'networks (address rows 0.89 of June; other pollutants '
                                                   'unchanged); complete-through month per release: '
                                                   '6_mgmt.data_quality.release_finding (finding_code '
                                                   'F2_complete_through).'}}
