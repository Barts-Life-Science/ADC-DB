"""Silver clinical.medication_order_status: 1 dataset(s).

- datasets: clinical_medication_order_status
- moved verbatim from: 2_silver_clinical_events:6009, 2_silver_clinical_events:6036

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


# Clinical / medication order status

# ==== journey_clinical.medication_order_status ====
MEDICATION_ORDER_STATUS_PUBLIC_COLUMNS = [
    "order_id", "sequence", "status_kind", "status_code", "status_display",
    "effective_datetime", "practitioner_key", "source_action_sequence",
    "action_type_code", "action_type_display", "action_qualifier_code",
    "action_qualifier_display", "action_rejected_ind", "historical_feed_ind",
    "source_present_ind", "loaded_at",
]


@materialized_view(
    name=_n("journey_clinical.medication_order_status"),
    refresh_policy="auto",
)
def medication_order_status():
    # Build the declared dataset: One ordered source action or status row per medication order.
    s = read_source(SRC_MEDICATION_ORDER_ACTION)
    return s.select(
        s.ORDER_ID.cast("bigint").alias("order_id"),
        F.coalesce(s.ACTION_SEQUENCE, F.lit(0)).cast("long").alias("sequence"),
        F.coalesce(s.ACTION_TYPE_DESCRIPTION, s.ACTION_TYPE_CD.cast("string"))
         .alias("status_kind"),
        s.ORDER_STATUS_CD.cast("string").alias("status_code"),
        F.coalesce(s.ORDER_STATUS_DESCRIPTION, s.ACTION_TYPE_DESCRIPTION)
         .alias("status_display"),
        F.coalesce(s.ACTION_DT_TM, s.EFFECTIVE_DT_TM, s.ORDER_DT_TM)
         .alias("effective_datetime"),
        F.when(
            s.ACTION_PERSONNEL_ID.isNotNull(),
            stable_id("practitioner:mill", s.ACTION_PERSONNEL_ID),
        ).alias("practitioner_key"),
        s.ACTION_SEQUENCE.cast("long").alias("source_action_sequence"),
        s.ACTION_TYPE_CD.cast("string").alias("action_type_code"),
        s.ACTION_TYPE_DESCRIPTION.alias("action_type_display"),
        s.ACTION_QUALIFIER_CD.cast("string").alias("action_qualifier_code"),
        s.ACTION_QUALIFIER_DESCRIPTION.alias("action_qualifier_display"),
        (s.ACTION_REJECTED_IND == 1).alias("action_rejected_ind"),
        s.HISTORICAL_FEED_IND.alias("historical_feed_ind"),
        s.SOURCE_PRESENT_IND.alias("source_present_ind"),
        s.ADC_UPDT.alias("loaded_at"),
    ).select(*MEDICATION_ORDER_STATUS_PUBLIC_COLUMNS)


VALUE_POLICIES = {
    'clinical_medication_order_status': ValuePolicy(table='clinical_medication_order_status', enabled=True, zero_as_null=('action_qualifier_code',)),
}


WRAP_POLICY = {'clinical_medication_order_status'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_medication_order_status': {'columns': {'action_qualifier_code': 'Source action-qualifier code.',
                                                  'action_qualifier_display': 'Source action-qualifier '
                                                                              'display.',
                                                  'action_rejected_ind': 'Whether the source marks the '
                                                                         'action rejected.',
                                                  'action_type_code': 'Source action-type code.',
                                                  'action_type_display': 'Source action-type display.',
                                                  'effective_datetime': 'Action, effective, or order '
                                                                        'timestamp; first available.',
                                                  'historical_feed_ind': 'Whether the action came from the '
                                                                         'historical feed.',
                                                  'loaded_at': 'Bronze load timestamp of the action row.',
                                                  'order_id': 'Millennium ORDER_ID of the parent medication '
                                                              'order.',
                                                  'practitioner_key': 'Deterministic practitioner key '
                                                                      'derived from ACTION_PERSONNEL_ID.',
                                                  'sequence': 'Deterministic action sequence, defaulted to '
                                                              'zero when absent.',
                                                  'source_action_sequence': 'Verbatim source action '
                                                                            'sequence.',
                                                  'source_present_ind': 'Whether the source row remains '
                                                                        'present.',
                                                  'status_code': 'Source order-status code at this action.',
                                                  'status_display': 'Source order-status display, falling '
                                                                    'back to action type.',
                                                  'status_kind': 'Source action type display, falling back '
                                                                 'to its code.'},
                                      'table': 'One ordered source action or status row per medication '
                                               'order.'}}

# INSR_R4_SILVER_V1 (child): generated by r4/release/gen_silver_components.py; edit the generator, not this.
from silver_core.parent_inputs import ParentInputs, with_parent_inputs  # noqa: E402
R4_PARENT_INPUTS = ParentInputs(parent='clinical_medication_order', dataset='_clinical_medication_order_parent_inputs', keys=('order_id',), inputs=('identity_status', 'person_id', 'record_status'), admit_sql="coalesce(((identity_status = 'resolved') AND (record_status = 'active')), false)", name=_n('journey_clinical._medication_order_parent_inputs'), parent_name=_n('journey_clinical.medication_order'))
SPECS = with_parent_inputs(SPECS, 'clinical_medication_order_status', R4_PARENT_INPUTS)
