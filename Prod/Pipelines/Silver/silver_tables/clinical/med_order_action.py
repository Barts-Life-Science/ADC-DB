"""Silver clinical.med_order_action: 1 dataset(s).

- datasets: _clinical_med_order_action_grouped
- moved verbatim from: 2_silver_clinical_events:4991, 2_silver_clinical_events:5019

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id
from silver_tables._common.journey import SRC_MEDICATION_ORDER_ACTION


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical /  med order action grouped
def _med_order_action_grouped_query():
    # Group medication-order actions with their status and effective-time evidence.
    s = read_source(SRC_MEDICATION_ORDER_ACTION)
    action = F.struct(
        F.coalesce(s.ACTION_SEQUENCE, F.lit(0)).cast("long").alias("sequence"),
        F.coalesce(s.ACTION_TYPE_DESCRIPTION, s.ACTION_TYPE_CD.cast("string")).alias("status_kind"),
        s.ORDER_STATUS_CD.cast("string").alias("status_code"),
        F.coalesce(s.ORDER_STATUS_DESCRIPTION, s.ACTION_TYPE_DESCRIPTION).alias("status_display"),
        F.coalesce(s.ACTION_DT_TM, s.EFFECTIVE_DT_TM, s.ORDER_DT_TM).alias("effective_datetime"),
        F.when(s.ACTION_PERSONNEL_ID.isNotNull(),
               stable_id("practitioner:mill", s.ACTION_PERSONNEL_ID)).alias("practitioner_id"),
        s.ACTION_SEQUENCE.cast("long").alias("source_action_sequence"),
        s.ACTION_TYPE_CD.cast("string").alias("action_type_code"),
        s.ACTION_TYPE_DESCRIPTION.alias("action_type_display"),
        s.ACTION_QUALIFIER_CD.cast("string").alias("action_qualifier_code"),
        s.ACTION_QUALIFIER_DESCRIPTION.alias("action_qualifier_display"),
        (s.ACTION_REJECTED_IND == 1).alias("action_rejected_ind"),
        s.HISTORICAL_FEED_IND.alias("historical_feed_ind"),
        s.SOURCE_PRESENT_IND.alias("source_present_ind"),
    )
    return s.groupBy("ORDER_ID").agg(
        F.to_json(F.sort_array(F.collect_list(action))).alias("status_history_json"),
        F.count(F.lit(1)).cast("long").alias("status_history_count"),
        F.max(F.greatest(s.SOURCE_ADC_UPDT, s.ORDER_SOURCE_ADC_UPDT, s.ADC_UPDT))
         .alias("action_source_update_timestamp"),
        F.max(s.ADC_UPDT).alias("action_loaded_at"),
    )


@materialized_view(
    name=_n("journey_clinical._med_order_action_grouped"),
    private=True,
    refresh_policy="auto",
)
def _med_order_action_grouped():
    # Build the declared dataset: Internal ordered status-history aggregate for medication
    # orders.
    return _med_order_action_grouped_query()


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_med_order_action_grouped': {'columns': {},
                                        'table': 'Internal ordered status-history aggregate for medication '
                                                 'orders.'}}
