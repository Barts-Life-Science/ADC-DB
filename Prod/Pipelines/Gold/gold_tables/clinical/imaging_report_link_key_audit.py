"""Gold clinical.imaging_report_link_key_audit: 1 dataset(s).

- datasets: _qc_clinical_imaging_report_link_key_audit
- moved verbatim from: 2_gold_clinical_events:3050

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from gold_core.gold_core import _n, _src, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# SILVER_RELEASE_20261025 item 2: link_key is the table's primary key. Silver once repeated 3,721 keys (duplicate raw
# blob rows fanned out its anonymised-text join); this audit holds every key that occurs more than once and fails the
# Gold update when it holds any, so a repeat cannot reach Gold silently. Normally empty; incrementally maintained.
@dp.materialized_view(
    name=_n("gold_qc._clinical_imaging_report_link_key_audit"),
    refresh_policy="auto",
)
@dp.expect_or_fail("gold.clinical.imaging_report_link.link_key_unique", "link_rows = 1")
def _gold_qc_clinical_imaging_report_link_key_audit():
    """Repeated link_keys of the silver product, with how often each occurs."""
    return (spark.read.table(_src("clinical_imaging_report_link"))
            .groupBy("link_key").agg(F.count(F.lit(1)).alias("link_rows"))
            .where(F.col("link_rows") > 1))


finalise()


COMMENTS = {'_qc_clinical_imaging_report_link_key_audit': {'columns': {},
                                                'quantity_override': False,
                                                'table': 'Internal audit: link_key values occurring more '
                                                         'than once in silver clinical_imaging_report_link. '
                                                         'Must stay empty; any row fails the Gold update '
                                                         '(expect_or_fail).'}}
