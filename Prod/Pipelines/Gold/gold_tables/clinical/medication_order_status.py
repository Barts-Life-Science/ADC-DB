"""Gold clinical.medication_order_status: 2 dataset(s).

- datasets: _qc_clinical_medication_order_status, clinical_medication_order_status
- moved verbatim from: 2_gold_clinical_events:7645, 2_gold_clinical_events:7683, 2_gold_clinical_events:7689, 2_gold_clinical_events:7705

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.medication_order_status ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_medication_order applies the parent Gold drop rule.
CLINICAL_MEDICATION_ORDER_STATUS_SELECT = [
    '`order_id` AS `order_id`',
    '`sequence` AS `sequence`',
    '`status_kind` AS `status_kind`',
    '`status_code` AS `status_code`',
    '`status_display` AS `status_display`',
    '`effective_datetime` AS `effective_datetime`',
    '`practitioner_key` AS `practitioner_key`',
    '`source_action_sequence` AS `source_action_sequence`',
    '`action_type_code` AS `action_type_code`',
    '`action_type_display` AS `action_type_display`',
    '`action_qualifier_code` AS `action_qualifier_code`',
    '`action_qualifier_display` AS `action_qualifier_display`',
    '`action_rejected_ind` AS `action_rejected_ind`',
    '`historical_feed_ind` AS `historical_feed_ind`',
    '`source_present_ind` AS `source_present_ind`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_MEDICATION_ORDER_STATUS_SELECT += [  # GOLD_MV_SPLIT_V1: parent drop-rule inputs silver carries; the R4 view published them
    '`parent_identity_status` AS `parent_identity_status`',
    '`parent_person_id` AS `parent_person_id`',
    '`parent_record_status` AS `parent_record_status`',
    '`parent_row_count` AS `parent_row_count`',
]


CLINICAL_MEDICATION_ORDER_STATUS_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_medication_order_status.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_medication_order_status"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_ORDER_STATUS_MANDATORY_RULES)
def _gold_qc_clinical_medication_order_status():
    """Task 3 Gold child of clinical_medication_order; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.medication_order")).select(
        'order_id',
    ).dropDuplicates(['order_id'])
    child = spark.read.table(_src("clinical_medication_order_status"))
    df = _with_parent_status(child, parent, ['order_id']).selectExpr(*CLINICAL_MEDICATION_ORDER_STATUS_SELECT, "__gold_parent_present AS __gold_parent_present")
    df = insr_w4_apply_value_rules('clinical_medication_order_status', df)  # INSR_W4
    return df


@dp.materialized_view(
    name=_n("gold_clinical.medication_order_status"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_medication_order_status():
    """Publish clinical_medication_order_status without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_medication_order_status")).drop("__gold_parent_present")


RULES = {
    'clinical_medication_order_status': GoldRules(
        table='clinical_medication_order_status',
        select=CLINICAL_MEDICATION_ORDER_STATUS_SELECT,
        mandatory=CLINICAL_MEDICATION_ORDER_STATUS_MANDATORY_RULES,
        value_rules={'gold.clinical.medication_order_status.action_qualifier_code.insr_f7_zero_code': {'action': 'normalize',
                                                                                           'column': 'action_qualifier_code',
                                                                                           'companions': [],
                                                                                           'match': 'TRIM(`action_qualifier_code`) '
                                                                                                    "= '0' "
                                                                                                    'AND '
                                                                                                    '`action_qualifier_display` '
                                                                                                    'IS '
                                                                                                    'NULL',
                                                                                           'table': 'clinical_medication_order_status'}}),
}
_rules = QcBinding(RULES)
insr_w4_apply_value_rules = _rules.insr_w4_apply_value_rules


finalise()


COMMENTS = {'_qc_clinical_medication_order_status': {'columns': {'action_qualifier_code': 'Source action-qualifier '
                                                                               'code.',
                                                      'action_qualifier_display': 'Source action-qualifier '
                                                                                  'display.',
                                                      'action_rejected_ind': 'Whether the source marks the '
                                                                             'action rejected.',
                                                      'action_type_code': 'Source action-type code.',
                                                      'action_type_display': 'Source action-type display.',
                                                      'effective_datetime': 'Action, effective, or order '
                                                                            'timestamp; first available.',
                                                      'historical_feed_ind': 'Whether the action came from '
                                                                             'the historical feed.',
                                                      'loaded_at': 'Bronze load timestamp of the action row.',
                                                      'order_id': 'Millennium ORDER_ID of the parent '
                                                                  'medication order.',
                                                      'practitioner_key': 'Deterministic practitioner key '
                                                                          'derived from ACTION_PERSONNEL_ID.',
                                                      'sequence': 'Deterministic action sequence, defaulted '
                                                                  'to zero when absent.',
                                                      'source_action_sequence': 'Verbatim source action '
                                                                                'sequence.',
                                                      'source_present_ind': 'Whether the source row remains '
                                                                            'present.',
                                                      'status_code': 'Source order-status code at this '
                                                                     'action.',
                                                      'status_display': 'Source order-status display, '
                                                                        'falling back to action type.',
                                                      'status_kind': 'Source action type display, falling '
                                                                     'back to its code.'},
                                          'quantity_override': True,
                                          'table': 'Internal QC of clinical_medication_order_status; counts '
                                                   'rows whose Gold parent is absent.'},
 'clinical_medication_order_status': {'columns': {},
                                      'quantity_override': False,
                                      'table': 'One ordered source action or status row per medication '
                                               'order.'}}
