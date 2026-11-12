"""Gold spine.scope: 1 dataset(s).

- datasets: _qc_person_scope
- moved verbatim from: 1_gold_spine_and_reference:83

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from gold_core.gold_core import _n, _src, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# INSR_W3 W3.02 (C0 ruling 1, 2026-10-29): the gold person scope that _qc and _tdx_product read.
@dp.materialized_view(
    name=_n("gold_qc._person_scope"),
    refresh_policy="auto",
)
def _gold_qc_person_scope():
    """Project silver person_scope to the two columns gold joins on; one row per spine person."""
    return (
        spark.read.table(_src("reference_person_scope"))
        .where(F.col("person_id").isNotNull())
        .select(
            F.col("person_id").cast("bigint").alias("_qc_scope_person_id"),
            F.col("in_scope").alias("_qc_scope_in_scope"),
        )
    )


finalise()


COMMENTS = {'_qc_person_scope': {'columns': {},
                      'quantity_override': False,
                      'table': 'Internal: one row per silver spine person with the gold scope flag (in_scope '
                               '= has_encounter OR has_clinical_data, from silver reference_person_scope). '
                               'Only the key and the flag, so a change to a person descriptor does not reach '
                               'the person twins. A single-source projection, so it refreshes '
                               'incrementally.'}}
