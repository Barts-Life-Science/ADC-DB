"""Silver clinical.medication_order_detail: 1 dataset(s).

- datasets: clinical_medication_order_detail
- moved verbatim from: 2_silver_clinical_events:6079, 2_silver_clinical_events:6103

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


# Clinical / medication order detail

# ==== journey_clinical.medication_order_detail ====
MEDICATION_ORDER_DETAIL_PUBLIC_COLUMNS = [
    "order_id", "action_sequence", "detail_sequence", "field_id",
    "field_meaning", "value_text", "value_number", "value_datetime",
    "history_contract", "parent_action_sequence", "last_action_sequence",
    "source_present_ind", "loaded_at",
    "detail_action_basis",  # INSR_W2 W2.20
]


@materialized_view(
    name=_n("journey_clinical.medication_order_detail"),
    refresh_policy="auto",
)
def medication_order_detail():
    # Build the declared dataset: One latest-action detail row per medication order field.
    s = read_source(SRC_MEDICATION_ORDER_DETAIL)
    return s.select(
        s.ORDER_ID.cast("bigint").alias("order_id"),
        F.coalesce(s.ACTION_SEQUENCE, F.lit(0)).cast("long").alias("action_sequence"),
        F.coalesce(s.DETAIL_SEQUENCE, F.lit(0)).cast("long").alias("detail_sequence"),
        s.OE_FIELD_ID.cast("string").alias("field_id"),
        s.OE_FIELD_MEANING.alias("field_meaning"),
        # INSR_W2 W2.05 (L856): trailing CR/LF stripped (also feeds medication_order_attribute FREQ etc.).
        F.regexp_replace(F.coalesce(s.OE_FIELD_DISPLAY_VALUE_EXTEND, s.OE_FIELD_DISPLAY_VALUE),
                         r"[\r\n]+$", "").alias("value_text"),
        s.OE_FIELD_VALUE.cast("decimal(38,10)").alias("value_number"),
        s.OE_FIELD_DT_TM_VALUE.alias("value_datetime"),
        s.DETAIL_HISTORY_CONTRACT.alias("history_contract"),
        s.PARENT_ACTION_SEQUENCE.cast("long").alias("parent_action_sequence"),
        s.LAST_ACTION_SEQUENCE.cast("long").alias("last_action_sequence"),
        s.DETAIL_ACTION_BASIS.alias("detail_action_basis"),  # INSR_W2 W2.20
        s.SOURCE_PRESENT_IND.alias("source_present_ind"),
        s.ADC_UPDT.alias("loaded_at"),
    ).select(*MEDICATION_ORDER_DETAIL_PUBLIC_COLUMNS)


VALUE_POLICIES = {
    'clinical_medication_order_detail': ValuePolicy(table='clinical_medication_order_detail', enabled=True),
}


WRAP_POLICY = {'clinical_medication_order_detail'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_medication_order_detail': {'columns': {'action_sequence': 'Order action sequence the detail '
                                                                     'belongs to; zero when absent.',
                                                  'detail_action_basis': 'LATEST_ACTION or CARRIED_FORWARD '
                                                                         '(INSR_W1_MED_DETAIL_EFFECTIVE_V1). '
                                                                         'INSR_W2 W2.20.',
                                                  'detail_sequence': 'Detail sequence within the action; '
                                                                     'zero when absent.',
                                                  'field_id': 'Millennium order-entry field identifier.',
                                                  'field_meaning': 'Source order-entry field meaning.',
                                                  'history_contract': 'Source detail-history contract '
                                                                      'marker.',
                                                  'last_action_sequence': 'Source last action sequence.',
                                                  'loaded_at': 'Bronze load timestamp of the order-detail '
                                                               'row.',
                                                  'order_id': 'Millennium ORDER_ID of the parent medication '
                                                              'order.',
                                                  'parent_action_sequence': 'Source parent action sequence.',
                                                  'source_present_ind': 'Whether the source row remains '
                                                                        'present.',
                                                  'value_datetime': 'Datetime order-detail value when '
                                                                    'supplied.',
                                                  'value_number': 'Numeric order-detail value when supplied.',
                                                  'value_text': 'Extended display value, falling back to '
                                                                'display value.'},
                                      'table': 'One latest-action detail row per medication order field.'}}

# INSR_R4_SILVER_V1 (child): generated by r4/release/gen_silver_components.py; edit the generator, not this.
from silver_core.parent_inputs import ParentInputs, with_parent_inputs  # noqa: E402
R4_PARENT_INPUTS = ParentInputs(parent='clinical_medication_order', dataset='_clinical_medication_order_parent_inputs', keys=('order_id',), inputs=('identity_status', 'person_id', 'record_status'), admit_sql="coalesce(((identity_status = 'resolved') AND (record_status = 'active')), false)", name=_n('journey_clinical._medication_order_parent_inputs'), parent_name=_n('journey_clinical.medication_order'))
SPECS = with_parent_inputs(SPECS, 'clinical_medication_order_detail', R4_PARENT_INPUTS)
