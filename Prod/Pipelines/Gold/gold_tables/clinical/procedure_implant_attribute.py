"""Gold clinical.procedure_implant_attribute: 2 dataset(s).

- datasets: _qc_clinical_procedure_implant_attribute, clinical_procedure_implant_attribute
- moved verbatim from: 2_gold_clinical_events:7720, 2_gold_clinical_events:7756, 2_gold_clinical_events:7762, 2_gold_clinical_events:7777

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.procedure_implant_attribute ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_procedure applies the parent Gold drop rule.
CLINICAL_PROCEDURE_IMPLANT_ATTRIBUTE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`implant_event_id` AS `implant_event_id`',
    '`implant_sequence` AS `implant_sequence`',
    '`sequence` AS `sequence`',
    '`clinical_event_id` AS `clinical_event_id`',
    '`attribute_name` AS `attribute_name`',
    '`attribute_value` AS `attribute_value`',
    '`event_tag` AS `event_tag`',
    '`event_title` AS `event_title`',
    '`result_value` AS `result_value`',
    '`value_source` AS `value_source`',
    '`value_conflict_ind` AS `value_conflict_ind`',
    '`performed_datetime` AS `performed_datetime`',
    '`valid_from` AS `valid_from`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_PROCEDURE_IMPLANT_ATTRIBUTE_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_procedure_implant_attribute.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_procedure_implant_attribute"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_PROCEDURE_IMPLANT_ATTRIBUTE_MANDATORY_RULES)
def _gold_qc_clinical_procedure_implant_attribute():
    """Task 3 Gold child of clinical_procedure; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.procedure")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_procedure_implant_attribute"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_PROCEDURE_IMPLANT_ATTRIBUTE_SELECT, "__gold_parent_present AS __gold_parent_present")
    return df


@dp.materialized_view(
    name=_n("gold_clinical.procedure_implant_attribute"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_procedure_implant_attribute():
    """Publish clinical_procedure_implant_attribute without exposing the internal parent-admission marker."""
    return _with_row_key(spark.read.table(_n("gold_qc._clinical_procedure_implant_attribute")).drop("__gold_parent_present"), "clinical_procedure_implant_attribute")  # INSR_W3 W3.09


def _public_steps_clinical_procedure_implant_attribute(df):
    """clinical_procedure_implant_attribute: the public body after its projection (GoldRules.public_select), moved from gold_clinical_procedure_implant_attribute; df is the projected twin frame."""
    return _with_row_key(df, 'clinical_procedure_implant_attribute')


RULES = {
    'clinical_procedure_implant_attribute': GoldRules(
        table='clinical_procedure_implant_attribute',
        select=CLINICAL_PROCEDURE_IMPLANT_ATTRIBUTE_SELECT,
        mandatory=CLINICAL_PROCEDURE_IMPLANT_ATTRIBUTE_MANDATORY_RULES,
        row_key_sql=("sha2(to_json(named_struct('patient_event_key', `patient_event_key`, 'implant_event_id', "
         "`implant_event_id`, 'implant_sequence', `implant_sequence`, 'sequence', `sequence`, "
         "'clinical_event_id', `clinical_event_id`, 'attribute_name', `attribute_name`, 'attribute_value', "
         "`attribute_value`, 'event_tag', `event_tag`, 'event_title', `event_title`, 'result_value', "
         "`result_value`, 'value_source', `value_source`, 'value_conflict_ind', `value_conflict_ind`, "
         "'performed_datetime', unix_micros(`performed_datetime`), 'valid_from', "
         "unix_micros(`valid_from`), 'loaded_at', unix_micros(`loaded_at`)), map('ignoreNullFields', "
         "'false')), 256)"),
        row_key_columns=('patient_event_key, implant_event_id, implant_sequence, sequence, clinical_event_id, '
         'attribute_name, attribute_value, event_tag, event_title, result_value, value_source, '
         'value_conflict_ind, performed_datetime, valid_from, loaded_at'),
        public_select={'all_but': ['__gold_parent_present']},
        public_steps=(_public_steps_clinical_procedure_implant_attribute,)),
}
_rules = QcBinding(RULES)
_with_row_key = _rules._with_row_key


finalise()


COMMENTS = {'_qc_clinical_procedure_implant_attribute': {'columns': {'attribute_name': 'Source implant attribute name.',
                                                          'attribute_value': 'Source implant attribute '
                                                                             'value.',
                                                          'clinical_event_id': 'Source clinical-event '
                                                                               'identifier for the implant '
                                                                               'attribute.',
                                                          'event_tag': 'Source clinical-event tag.',
                                                          'event_title': 'Source clinical-event title.',
                                                          'implant_event_id': 'Millennium implant EVENT_ID; '
                                                                              'first native parent-key '
                                                                              'component.',
                                                          'implant_sequence': 'Millennium IMPLANT_SEQUENCE; '
                                                                              'second native parent-key '
                                                                              'component.',
                                                          'loaded_at': 'Bronze load timestamp of the implant '
                                                                       'attribute row.',
                                                          'patient_event_key': 'Deterministic key of the '
                                                                               'parent implant '
                                                                               'clinical_procedure row.',
                                                          'performed_datetime': 'Source performed timestamp.',
                                                          'result_value': 'Source result value.',
                                                          'sequence': 'Deterministic source ordering value '
                                                                      'derived from performed or event-start '
                                                                      'time.',
                                                          'valid_from': 'Source validity-start timestamp.',
                                                          'value_conflict_ind': 'Whether source evidence for '
                                                                                'the value conflicts.',
                                                          'value_source': 'Source route that supplied the '
                                                                          'retained value.'},
                                              'quantity_override': True,
                                              'table': 'Internal QC of clinical_procedure_implant_attribute; '
                                                       'counts rows whose Gold parent is absent.'},
 'clinical_procedure_implant_attribute': {'columns': {'row_key': 'Row key: SHA-256 of the JSON of '
                                                                 '(patient_event_key, implant_event_id, '
                                                                 'implant_sequence, sequence, '
                                                                 'clinical_event_id, attribute_name, '
                                                                 'attribute_value, event_tag, event_title, '
                                                                 'result_value, value_source, '
                                                                 'value_conflict_ind, performed_datetime, '
                                                                 'valid_from, loaded_at), NULLs written '
                                                                 'explicitly. Unique per row; stable while '
                                                                 'those values are unchanged. INSR_W3 '
                                                                 'W3.09.'},
                                          'quantity_override': False,
                                          'table': 'One source implant attribute per parent implant '
                                                   'clinical_procedure row.'}}
