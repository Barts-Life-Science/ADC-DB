"""Gold clinical.medication_order_detail: 2 dataset(s).

- datasets: _qc_clinical_medication_order_detail, clinical_medication_order_detail
- moved verbatim from: 2_gold_clinical_events:7429, 2_gold_clinical_events:7444, 2_gold_clinical_events:7468, 2_gold_clinical_events:7474, 2_gold_clinical_events:7489

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.medication_order_detail ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_medication_order applies the parent Gold drop rule.
CLINICAL_MEDICATION_ORDER_DETAIL_SELECT = [
    '`order_id` AS `order_id`',
    '`action_sequence` AS `action_sequence`',
    '`detail_sequence` AS `detail_sequence`',
    '`field_id` AS `field_id`',
    '`field_meaning` AS `field_meaning`',
    '`value_text` AS `value_text`',
    '`value_number` AS `value_number`',
    '`value_datetime` AS `value_datetime`',
    '`history_contract` AS `history_contract`',
    '`parent_action_sequence` AS `parent_action_sequence`',
    '`last_action_sequence` AS `last_action_sequence`',
    '`source_present_ind` AS `source_present_ind`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_MEDICATION_ORDER_DETAIL_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`detail_action_basis` AS `detail_action_basis`',
]


CLINICAL_MEDICATION_ORDER_DETAIL_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_medication_order_detail.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_medication_order_detail"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_ORDER_DETAIL_MANDATORY_RULES)
def _gold_qc_clinical_medication_order_detail():
    """Task 3 Gold child of clinical_medication_order; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.medication_order")).select(
        'order_id',
    ).dropDuplicates(['order_id'])
    child = spark.read.table(_src("clinical_medication_order_detail"))
    df = _with_parent_status(child, parent, ['order_id']).selectExpr(*CLINICAL_MEDICATION_ORDER_DETAIL_SELECT, "__gold_parent_present AS __gold_parent_present")
    return df


@dp.materialized_view(
    name=_n("gold_clinical.medication_order_detail"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_medication_order_detail():
    """Publish clinical_medication_order_detail without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_medication_order_detail")).drop("__gold_parent_present")


finalise()


COMMENTS = {'_qc_clinical_medication_order_detail': {'columns': {'action_sequence': 'Order action sequence the detail '
                                                                         'belongs to; zero when absent.',
                                                      'detail_action_basis': 'LATEST_ACTION or '
                                                                             'CARRIED_FORWARD '
                                                                             '(INSR_W1_MED_DETAIL_EFFECTIVE_V1). '
                                                                             'INSR_W2 W2.20.',
                                                      'detail_sequence': 'Detail sequence within the action; '
                                                                         'zero when absent.',
                                                      'field_id': 'Millennium order-entry field identifier.',
                                                      'field_meaning': 'Source order-entry field meaning.',
                                                      'history_contract': 'Source detail-history contract '
                                                                          'marker.',
                                                      'last_action_sequence': 'Source last action sequence.',
                                                      'loaded_at': 'Bronze load timestamp of the '
                                                                   'order-detail row.',
                                                      'order_id': 'Millennium ORDER_ID of the parent '
                                                                  'medication order.',
                                                      'parent_action_sequence': 'Source parent action '
                                                                                'sequence.',
                                                      'source_present_ind': 'Whether the source row remains '
                                                                            'present.',
                                                      'value_datetime': 'Datetime order-detail value when '
                                                                        'supplied.',
                                                      'value_number': 'Numeric order-detail value when '
                                                                      'supplied.',
                                                      'value_text': 'Extended display value, falling back to '
                                                                    'display value.'},
                                          'quantity_override': True,
                                          'table': 'Internal QC of clinical_medication_order_detail; counts '
                                                   'rows whose Gold parent is absent.'},
 'clinical_medication_order_detail': {'columns': {},
                                      'quantity_override': False,
                                      'table': 'One latest-action detail row per medication order field.'}}
