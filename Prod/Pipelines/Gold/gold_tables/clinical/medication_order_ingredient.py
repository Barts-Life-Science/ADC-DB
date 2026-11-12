"""Gold clinical.medication_order_ingredient: 2 dataset(s).

- datasets: _qc_clinical_medication_order_ingredient, clinical_medication_order_ingredient
- moved verbatim from: 2_gold_clinical_events:7504, 2_gold_clinical_events:7530, 2_gold_clinical_events:7607, 2_gold_clinical_events:7613, 2_gold_clinical_events:7630

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.medication_order_ingredient ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_medication_order applies the parent Gold drop rule.
CLINICAL_MEDICATION_ORDER_INGREDIENT_SELECT = [
    '`order_id` AS `order_id`',
    '`action_sequence` AS `action_sequence`',
    '`component_sequence` AS `component_sequence`',
    '`component_id` AS `component_id`',
    '`coding_system` AS `coding_system`',
    '`coding_code` AS `coding_code`',
    '`coding_display` AS `coding_display`',
    '`strength_value` AS `strength_value`',
    '`strength_unit` AS `strength_unit`',
    '`volume_value` AS `volume_value`',
    '`volume_unit` AS `volume_unit`',
    '`dose_value` AS `dose_value`',
    '`dose_unit` AS `dose_unit`',
    '`rate_value` AS `rate_value`',
    '`rate_unit` AS `rate_unit`',
    '`concentration_value` AS `concentration_value`',
    '`concentration_unit` AS `concentration_unit`',
    '`ingredient_type_code` AS `ingredient_type_code`',
    '`clinically_significant_ind` AS `clinically_significant_ind`',
    '`include_in_total_volume_ind` AS `include_in_total_volume_ind`',
    '`freetext_dose` AS `freetext_dose`',
    '`source_present_ind` AS `source_present_ind`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_MEDICATION_ORDER_INGREDIENT_SELECT += [
    '`ingredient_source_system` AS `ingredient_source_system`',
    '`ingredient_source_code` AS `ingredient_source_code`',
    '`ingredient_source_display` AS `ingredient_source_display`',
    '`ingredient_snomed_code` AS `ingredient_snomed_code`',
    '`ingredient_snomed_display` AS `ingredient_snomed_display`',
    '`ingredient_omop_concept_id` AS `ingredient_omop_concept_id`',
    '`ingredient_map_method` AS `ingredient_map_method`',
    '`ingredient_map_version` AS `ingredient_map_version`',
    '`ingredient_map_confidence` AS `ingredient_map_confidence`',
    '`ingredient_map_rule_id` AS `ingredient_map_rule_id`',
    '`ingredient_map_candidate_count` AS `ingredient_map_candidate_count`',
    '`ingredient_target_system` AS `ingredient_target_system`',
    '`ingredient_target_code` AS `ingredient_target_code`',
    '`ingredient_target_display` AS `ingredient_target_display`',
    '`ingredient_omop_source_concept_id` AS `ingredient_omop_source_concept_id`',
    '`ingredient_map_cosine` AS `ingredient_map_cosine`',
    '`ingredient_map_scoring_model` AS `ingredient_map_scoring_model`',
    '`ingredient_map_status` AS `ingredient_map_status`',
    '`ingredient_map_status_reason` AS `ingredient_map_status_reason`',
    '`ingredient_map_competing_count` AS `ingredient_map_competing_count`',
    '`ingredient_map_candidate_set_id` AS `ingredient_map_candidate_set_id`',
    '`ingredient_map_component_index` AS `ingredient_map_component_index`',
    '`ingredient_map_component_count` AS `ingredient_map_component_count`',
    '`ingredient_map_rollup_levels` AS `ingredient_map_rollup_levels`',
]


CLINICAL_MEDICATION_ORDER_INGREDIENT_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_medication_order_ingredient.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_medication_order_ingredient"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_ORDER_INGREDIENT_MANDATORY_RULES)
def _gold_qc_clinical_medication_order_ingredient():
    """Task 3 Gold child of clinical_medication_order; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.medication_order")).select(
        'order_id',
    ).dropDuplicates(['order_id'])
    child = _qc("clinical_medication_order_ingredient", CLINICAL_MEDICATION_ORDER_INGREDIENT_SELECT)
    df = _with_parent_status(child, parent, ['order_id'])
    public = (_s3_gate_direct_public(df, "clinical_medication_order_ingredient")
              .drop("_ingredient_gate_pass", "_ingredient_policy_id"))
    return public


@dp.materialized_view(
    name=_n("gold_clinical.medication_order_ingredient"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_medication_order_ingredient():
    """Publish clinical_medication_order_ingredient without exposing the internal parent-admission marker."""
    return _with_row_key(spark.read.table(_n("gold_qc._clinical_medication_order_ingredient")).drop("__gold_parent_present"), "clinical_medication_order_ingredient")  # INSR_W3 W3.09


def _public_steps_clinical_medication_order_ingredient(df):
    """clinical_medication_order_ingredient: the public body after its projection (GoldRules.public_select), moved from gold_clinical_medication_order_ingredient; df is the projected twin frame."""
    return _with_row_key(df, 'clinical_medication_order_ingredient')


RULES = {
    'clinical_medication_order_ingredient': GoldRules(
        table='clinical_medication_order_ingredient',
        select=CLINICAL_MEDICATION_ORDER_INGREDIENT_SELECT,
        mandatory=CLINICAL_MEDICATION_ORDER_INGREDIENT_MANDATORY_RULES,
        s3b_direct_axes=(('ingredient', False),),
        row_key_sql=("sha2(to_json(named_struct('order_id', `order_id`, 'action_sequence', `action_sequence`, "
         "'component_sequence', `component_sequence`, 'ingredient_map_component_index', "
         "`ingredient_map_component_index`), map('ignoreNullFields', 'false')), 256)"),
        row_key_columns='order_id, action_sequence, component_sequence, ingredient_map_component_index',
        value_rules={'gold.clinical.medication_order_ingredient.dose_value.insr_f8_zero_without_unit': {'action': 'invalidate',
                                                                                            'column': 'dose_value',
                                                                                            'companions': [],
                                                                                            'match': '`dose_value` '
                                                                                                     '= 0 '
                                                                                                     'AND '
                                                                                                     '`dose_unit` '
                                                                                                     'IS '
                                                                                                     'NULL',
                                                                                            'table': 'clinical_medication_order_ingredient'},
         'gold.clinical.medication_order_ingredient.ingredient_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                                    'column': 'ingredient_snomed_code',
                                                                                                    'companions': ['ingredient_snomed_display',
                                                                                                                   'ingredient_target_code',
                                                                                                                   'ingredient_target_display'],
                                                                                                    'match': '`ingredient_snomed_code` '
                                                                                                             'IS '
                                                                                                             'NOT '
                                                                                                             'NULL '
                                                                                                             'AND '
                                                                                                             '(NOT '
                                                                                                             '`ingredient_snomed_code` '
                                                                                                             'RLIKE '
                                                                                                             "'^[1-9][0-9]{5,17}$' "
                                                                                                             'OR '
                                                                                                             'substring(`ingredient_snomed_code`, '
                                                                                                             'length(`ingredient_snomed_code`) '
                                                                                                             '- '
                                                                                                             '2, '
                                                                                                             '2) '
                                                                                                             'NOT '
                                                                                                             'IN '
                                                                                                             "('00', "
                                                                                                             "'10') "
                                                                                                             'OR '
                                                                                                             '(substring(`ingredient_snomed_code`, '
                                                                                                             'length(`ingredient_snomed_code`) '
                                                                                                             '- '
                                                                                                             '2, '
                                                                                                             '2) '
                                                                                                             '= '
                                                                                                             "'10' "
                                                                                                             'AND '
                                                                                                             'length(`ingredient_snomed_code`) '
                                                                                                             '< '
                                                                                                             '11))',
                                                                                                    'table': 'clinical_medication_order_ingredient'},
         'gold.clinical.medication_order_ingredient.ingredient_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                                    'column': 'ingredient_target_code',
                                                                                                    'companions': ['ingredient_target_display',
                                                                                                                   'ingredient_snomed_code',
                                                                                                                   'ingredient_snomed_display'],
                                                                                                    'match': '`ingredient_target_system` '
                                                                                                             '= '
                                                                                                             "'http://snomed.info/sct' "
                                                                                                             'AND '
                                                                                                             '`ingredient_target_code` '
                                                                                                             'IS '
                                                                                                             'NOT '
                                                                                                             'NULL '
                                                                                                             'AND '
                                                                                                             '(NOT '
                                                                                                             '`ingredient_target_code` '
                                                                                                             'RLIKE '
                                                                                                             "'^[1-9][0-9]{5,17}$' "
                                                                                                             'OR '
                                                                                                             'substring(`ingredient_target_code`, '
                                                                                                             'length(`ingredient_target_code`) '
                                                                                                             '- '
                                                                                                             '2, '
                                                                                                             '2) '
                                                                                                             'NOT '
                                                                                                             'IN '
                                                                                                             "('00', "
                                                                                                             "'10') "
                                                                                                             'OR '
                                                                                                             '(substring(`ingredient_target_code`, '
                                                                                                             'length(`ingredient_target_code`) '
                                                                                                             '- '
                                                                                                             '2, '
                                                                                                             '2) '
                                                                                                             '= '
                                                                                                             "'10' "
                                                                                                             'AND '
                                                                                                             'length(`ingredient_target_code`) '
                                                                                                             '< '
                                                                                                             '11))',
                                                                                                    'table': 'clinical_medication_order_ingredient'},
         'gold.clinical.medication_order_ingredient.rate_value.insr_f8_zero_without_unit': {'action': 'invalidate',
                                                                                            'column': 'rate_value',
                                                                                            'companions': [],
                                                                                            'match': '`rate_value` '
                                                                                                     '= 0 '
                                                                                                     'AND '
                                                                                                     '`rate_unit` '
                                                                                                     'IS '
                                                                                                     'NULL',
                                                                                            'table': 'clinical_medication_order_ingredient'}},
        public_select={'all_but': ['__gold_parent_present']},
        public_steps=(_public_steps_clinical_medication_order_ingredient,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_with_row_key = _rules._with_row_key
_s3_gate_direct_public = _rules._s3_gate_direct_public


finalise()


COMMENTS = {'_qc_clinical_medication_order_ingredient': {'columns': {'action_sequence': 'Order action sequence the '
                                                                             'ingredient row belongs to; '
                                                                             'zero when absent.',
                                                          'clinically_significant_ind': 'Whether the source '
                                                                                        'flags the '
                                                                                        'ingredient as '
                                                                                        'clinically '
                                                                                        'significant.',
                                                          'coding_code': 'Millennium SYNONYM_ID of the '
                                                                         'ingredient.',
                                                          'coding_display': 'Ordered-as, supplied-as, order, '
                                                                            'or HNA mnemonic; first '
                                                                            'available.',
                                                          'coding_system': 'Cerner synonym identifier '
                                                                           'namespace.',
                                                          'component_id': 'Source composite '
                                                                          'ORDER_ID:ACTION_SEQUENCE:COMP_SEQUENCE.',
                                                          'component_sequence': 'Ingredient component '
                                                                                'sequence within the action; '
                                                                                'zero when absent.',
                                                          'concentration_unit': 'Concentration unit '
                                                                                'description.',
                                                          'concentration_value': 'Concentration as ordered.',
                                                          'dose_unit': 'Ordered-dose unit, falling back to '
                                                                       'dose-quantity unit.',
                                                          'dose_value': 'Ordered dose, falling back to dose '
                                                                        'quantity.',
                                                          'freetext_dose': 'Free-text dose when supplied.',
                                                          'include_in_total_volume_ind': 'Whether the '
                                                                                         'ingredient counts '
                                                                                         'toward total '
                                                                                         'volume.',
                                                          'ingredient_map_candidate_count': 'Governed S3c '
                                                                                            'ingredient '
                                                                                            'coding-axis '
                                                                                            'field.',
                                                          'ingredient_map_candidate_set_id': 'Governed S3c '
                                                                                             'ingredient '
                                                                                             'coding-axis '
                                                                                             'field.',
                                                          'ingredient_map_competing_count': 'Governed S3c '
                                                                                            'ingredient '
                                                                                            'coding-axis '
                                                                                            'field.',
                                                          'ingredient_map_component_count': 'Governed S3c '
                                                                                            'ingredient '
                                                                                            'coding-axis '
                                                                                            'field.',
                                                          'ingredient_map_component_index': 'Governed S3c '
                                                                                            'ingredient '
                                                                                            'coding-axis '
                                                                                            'field.',
                                                          'ingredient_map_confidence': 'Governed S3c '
                                                                                       'ingredient '
                                                                                       'coding-axis field.',
                                                          'ingredient_map_cosine': 'Governed S3c ingredient '
                                                                                   'coding-axis field.',
                                                          'ingredient_map_method': 'Governed S3c ingredient '
                                                                                   'coding-axis field.',
                                                          'ingredient_map_rollup_levels': 'Governed S3c '
                                                                                          'ingredient '
                                                                                          'coding-axis '
                                                                                          'field.',
                                                          'ingredient_map_rule_id': 'Governed S3c ingredient '
                                                                                    'coding-axis field.',
                                                          'ingredient_map_scoring_model': 'Governed S3c '
                                                                                          'ingredient '
                                                                                          'coding-axis '
                                                                                          'field.',
                                                          'ingredient_map_status': 'Governed S3c ingredient '
                                                                                   'coding-axis field.',
                                                          'ingredient_map_status_reason': 'Governed S3c '
                                                                                          'ingredient '
                                                                                          'coding-axis '
                                                                                          'field.',
                                                          'ingredient_map_version': 'Governed S3c ingredient '
                                                                                    'coding-axis field.',
                                                          'ingredient_omop_concept_id': 'Governed S3c '
                                                                                        'ingredient '
                                                                                        'coding-axis field.',
                                                          'ingredient_omop_source_concept_id': 'Governed S3c '
                                                                                               'ingredient '
                                                                                               'coding-axis '
                                                                                               'field.',
                                                          'ingredient_snomed_code': 'Governed S3c ingredient '
                                                                                    'coding-axis field.',
                                                          'ingredient_snomed_display': 'Governed S3c '
                                                                                       'ingredient '
                                                                                       'coding-axis field.',
                                                          'ingredient_source_code': 'Governed S3c ingredient '
                                                                                    'coding-axis field.',
                                                          'ingredient_source_display': 'Governed S3c '
                                                                                       'ingredient '
                                                                                       'coding-axis field.',
                                                          'ingredient_source_system': 'Governed S3c '
                                                                                      'ingredient '
                                                                                      'coding-axis field.',
                                                          'ingredient_target_code': 'Governed S3c ingredient '
                                                                                    'coding-axis field.',
                                                          'ingredient_target_display': 'Governed S3c '
                                                                                       'ingredient '
                                                                                       'coding-axis field.',
                                                          'ingredient_target_system': 'Governed S3c '
                                                                                      'ingredient '
                                                                                      'coding-axis field.',
                                                          'ingredient_type_code': 'Millennium ingredient '
                                                                                  'type flag.',
                                                          'loaded_at': 'Bronze load timestamp of the '
                                                                       'ingredient row.',
                                                          'order_id': 'Millennium ORDER_ID of the parent '
                                                                      'medication order.',
                                                          'rate_unit': 'Normalized rate unit description.',
                                                          'rate_value': 'S10 medication_order '
                                                                        'medication/dosage; retained as a '
                                                                        'source-faithful bronze input or '
                                                                        'Journey standard-block field.',
                                                          'source_present_ind': 'Whether the source row '
                                                                                'remains present.',
                                                          'strength_unit': 'Strength unit description.',
                                                          'strength_value': 'Ingredient strength as ordered.',
                                                          'volume_unit': 'Volume unit description.',
                                                          'volume_value': 'Ingredient volume as ordered.'},
                                              'quantity_override': True,
                                              'table': 'Internal QC of clinical_medication_order_ingredient; '
                                                       'counts rows whose Gold parent is absent.'},
 'clinical_medication_order_ingredient': {'columns': {'row_key': 'Row key: SHA-256 of the JSON of (order_id, '
                                                                 'action_sequence, component_sequence, '
                                                                 'ingredient_map_component_index), NULLs '
                                                                 'written explicitly. Unique per row; stable '
                                                                 'while those values are unchanged. INSR_W3 '
                                                                 'W3.09.'},
                                          'quantity_override': False,
                                          'table': 'One ordered ingredient component per medication order, '
                                                   'linked to its parent by the retained event key.'}}
