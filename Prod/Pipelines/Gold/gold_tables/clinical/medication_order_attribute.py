"""Gold clinical.medication_order_attribute: 2 dataset(s).

- datasets: _qc_clinical_medication_order_attribute, clinical_medication_order_attribute
- moved verbatim from: 2_gold_clinical_events:8025, 2_gold_clinical_events:8028, 2_gold_clinical_events:8035, 2_gold_clinical_events:8045

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.medication_order_attribute ==== PMS_P1_T6_GOLD_V1
CLINICAL_MEDICATION_ORDER_ATTRIBUTE_MANDATORY_RULES = {
    # An attribute row whose order is not admitted to gold has no parent to describe.
    "gold.clinical_medication_order_attribute.parent_admitted": "COALESCE(__gold_parent_present, FALSE)"}


CLINICAL_MEDICATION_ORDER_ATTRIBUTE_ADVISORY_RULES = {
    # INSR_W4 W4.06 numeric_plausibility (INSR_20261029_S1 F9_heavy_tail): Above 10x the p99.9 of the INSR S1 sample (F9 heavy tail, L4529/J25).
    'gold.clinical.medication_order_attribute.volumedose_value.insr_f9_medication_cap':
        'NOT COALESCE((`volumedose_value` > 2.4e+07), FALSE)',
    # Implausible weights are reported, not dropped; the other attributes on the order stay valid.
    "gold.clinical_medication_order_attribute.weight_plausible": "weight_value IS NULL OR weight_value BETWEEN 0.2 AND 400"}


@dp.materialized_view(name=_n("gold_qc._clinical_medication_order_attribute"),
    refresh_policy="auto")
@dp.expect_all_or_drop(CLINICAL_MEDICATION_ORDER_ATTRIBUTE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_MEDICATION_ORDER_ATTRIBUTE_ADVISORY_RULES)
def _gold_qc_clinical_medication_order_attribute():
    parent = spark.read.table(_n("gold_clinical.medication_order")).select('order_id').dropDuplicates(['order_id'])
    child = spark.read.table(_src("clinical_medication_order_attribute"))
    return _with_parent_status(child, parent, ['order_id'])


@dp.materialized_view(name=_n("gold_clinical.medication_order_attribute"),
    table_properties={"quality": "gold"}, refresh_policy="auto")
def gold_clinical_medication_order_attribute():
    return spark.read.table(_n("gold_qc._clinical_medication_order_attribute")).drop("__gold_parent_present")


finalise()


COMMENTS = {'_qc_clinical_medication_order_attribute': {'columns': {},
                                             'quantity_override': False,
                                             'table': 'Internal QC twin of '
                                                      'clinical_medication_order_attribute; drops orders not '
                                                      'admitted to gold medication_order.'},
 'clinical_medication_order_attribute': {'columns': {},
                                         'quantity_override': False,
                                         'table': 'One row per admitted medication order with its latest '
                                                  'detail attributes. Join key: order_id. One row per '
                                                  'medication order and order-detail meaning (latest '
                                                  'action); most meanings are sparse at source, and gold '
                                                  'carries every value silver has.'}}
