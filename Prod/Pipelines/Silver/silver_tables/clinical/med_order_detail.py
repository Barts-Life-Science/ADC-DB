"""Silver clinical.med_order_detail: 1 dataset(s).

- datasets: _clinical_med_order_detail_grouped
- moved verbatim from: 2_silver_clinical_events:5095, 2_silver_clinical_events:5121

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source
from silver_tables.clinical._shared import SRC_MEDICATION_ORDER_DETAIL


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


def _med_order_detail_grouped_query():
    # Group medication-order detail fields with their sequence, text and numeric values.
    s = read_source(SRC_MEDICATION_ORDER_DETAIL)
    detail = F.struct(
        F.coalesce(s.ACTION_SEQUENCE, F.lit(0)).cast("long").alias("action_sequence"),
        F.coalesce(s.DETAIL_SEQUENCE, F.lit(0)).cast("long").alias("detail_sequence"),
        s.OE_FIELD_ID.cast("string").alias("field_id"),
        s.OE_FIELD_MEANING.alias("field_meaning"),
        # INSR_W2 W2.05 (L856): Millennium pads some detail values with trailing CR/LF.
        F.regexp_replace(F.coalesce(s.OE_FIELD_DISPLAY_VALUE_EXTEND, s.OE_FIELD_DISPLAY_VALUE),
                         r"[\r\n]+$", "").alias("value_text"),
        s.OE_FIELD_VALUE.cast("decimal(38,10)").alias("value_number"),
        s.OE_FIELD_DT_TM_VALUE.alias("value_datetime"),
        s.DETAIL_HISTORY_CONTRACT.alias("history_contract"),
        s.PARENT_ACTION_SEQUENCE.cast("long").alias("parent_action_sequence"),
        s.LAST_ACTION_SEQUENCE.cast("long").alias("last_action_sequence"),
        s.SOURCE_PRESENT_IND.alias("source_present_ind"),
    )
    return s.groupBy("ORDER_ID").agg(
        F.to_json(F.sort_array(F.collect_list(detail))).alias("order_details_json"),
        F.count(F.lit(1)).cast("long").alias("order_detail_count"),
        F.max(F.greatest(s.SOURCE_ADC_UPDT, s.ORDER_SOURCE_ADC_UPDT, s.ADC_UPDT))
         .alias("detail_source_update_timestamp"),
        F.max(s.ADC_UPDT).alias("detail_loaded_at"),
    )


@materialized_view(
    name=_n("journey_clinical._med_order_detail_grouped"),
    private=True,
    refresh_policy="auto",
)
def _med_order_detail_grouped():
    # Build the declared dataset: Internal ordered latest-action-detail aggregate for medication
    # orders.
    return _med_order_detail_grouped_query()


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_med_order_detail_grouped': {'columns': {},
                                        'table': 'Internal ordered latest-action-detail aggregate for '
                                                 'medication orders.'}}
