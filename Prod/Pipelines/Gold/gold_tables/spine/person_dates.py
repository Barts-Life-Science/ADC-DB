"""Gold spine.person_dates: 1 dataset(s).

- datasets: _gold_person_dates
- moved verbatim from: 1_gold_spine_and_reference:60

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from gold_core.gold_core import _n, _src, spark  # INSR_R4_GOLD_V1: _n names the MV
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# INSR_R4_GOLD_V1: an MV (was a temporary view): the gold views join it, so it needs row tracking, CDF and DV.
@dp.materialized_view(name=_n("gold_qc._person_dates"), refresh_policy="auto")
def _gold_person_dates():
    """Prepare repaired person dates so event checks agree with Gold's person table."""
    # The two dates are read through the same expressions gold_spine.person
    # publishes them with, rather than raw from silver. A flag measured against a
    # value gold has removed would contradict the person row a researcher joins
    # it to: a death recorded before its own birth is not in the person table, so
    # nothing may be flagged as happening after it, and a birth held at the
    # source's "date of birth unknown" marker is not a birth to compare against.
    return (
        spark.read.table(_src("spine_person"))
        .select(
            F.col("person_id").alias("_qc_date_person_id"),
            F.expr("CASE WHEN CAST(`birth_datetime` AS DATE) IN (DATE'1901-01-01', DATE'1902-01-01', DATE'1910-01-01') THEN NULL ELSE CASE WHEN CAST(`birth_datetime` AS DATE) = DATE'1899-12-30' OR CAST(`birth_datetime` AS DATE) = DATE'1900-01-01' OR YEAR(CAST(`birth_datetime` AS DATE)) >= 2100 OR CAST(`birth_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`birth_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `birth_datetime` END END").alias("_qc_birth_datetime"),  # INSR_W3 W3.12: = spine_person
            F.expr("CASE WHEN `deceased_datetime` < `birth_datetime` THEN NULL ELSE `deceased_datetime` END").alias("_qc_deceased_datetime"),
        )
        .where(F.col("_qc_date_person_id").isNotNull())
        # INSR_R4_GOLD_V1: one row per person, chosen deterministically (dropDuplicates plans First, which
        # blocks incremental refresh and would rewrite every reader on each update).
        .groupBy("_qc_date_person_id")
        .agg(F.max(F.struct("_qc_birth_datetime", "_qc_deceased_datetime")).alias("d"))
        .select("_qc_date_person_id", "d.*")
    )


finalise()


COMMENTS = {'_gold_person_dates': {'columns': {}, 'quantity_override': False, 'table': None}}
