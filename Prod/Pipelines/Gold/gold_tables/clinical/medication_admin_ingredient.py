"""Gold clinical.medication_admin_ingredient: 2 dataset(s).

- datasets: _qc_clinical_medication_admin_ingredient, clinical_medication_admin_ingredient
- moved verbatim from: 2_gold_clinical_events:7199, 2_gold_clinical_events:7224, 2_gold_clinical_events:7228, 2_gold_clinical_events:7309, 2_gold_clinical_events:7315, 2_gold_clinical_events:7321, 2_gold_clinical_events:7339

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.medication_admin_ingredient ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_medication_admin applies the parent Gold drop rule.
CLINICAL_MEDICATION_ADMIN_INGREDIENT_SELECT = [
    '`event_id` AS `event_id`',
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


CLINICAL_MEDICATION_ADMIN_INGREDIENT_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`all_ingredients_zero_ind` AS `all_ingredients_zero_ind`',
]


CLINICAL_MEDICATION_ADMIN_INGREDIENT_SELECT += [
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


CLINICAL_MEDICATION_ADMIN_INGREDIENT_INSR_W4_ADVISORY_RULES = {  # INSR_W4
    # INSR_W4 W4.06 numeric_sentinel (W2.05 all_ingredients_zero_ind): Zero ingredient doses remain where not every ingredient is zero (J24 residual after W2.05).
    'gold.clinical.medication_admin_ingredient.dose_value.insr_j24_zero_residual':
        'NOT COALESCE((NOT coalesce(`all_ingredients_zero_ind`, FALSE) AND `dose_value` = 0), FALSE)',
}


CLINICAL_MEDICATION_ADMIN_INGREDIENT_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_medication_admin_ingredient.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_medication_admin_ingredient"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_ADMIN_INGREDIENT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_MEDICATION_ADMIN_INGREDIENT_INSR_W4_ADVISORY_RULES)  # INSR_W4
def _gold_qc_clinical_medication_admin_ingredient():
    """Task 3 Gold child of clinical_medication_admin; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.medication_admin")).select(
        'event_id',
    ).dropDuplicates(['event_id'])
    child = _qc("clinical_medication_admin_ingredient", CLINICAL_MEDICATION_ADMIN_INGREDIENT_SELECT)
    df = _with_parent_status(child, parent, ['event_id'])
    public = (_s3_gate_direct_public(df, "clinical_medication_admin_ingredient")
              .drop("_ingredient_gate_pass", "_ingredient_policy_id"))
    return public


@dp.materialized_view(
    name=_n("gold_clinical.medication_admin_ingredient"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_medication_admin_ingredient():
    """Publish clinical_medication_admin_ingredient without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_medication_admin_ingredient")).drop("__gold_parent_present")


RULES = {
    'clinical_medication_admin_ingredient': GoldRules(
        table='clinical_medication_admin_ingredient',
        select=CLINICAL_MEDICATION_ADMIN_INGREDIENT_SELECT,
        mandatory=CLINICAL_MEDICATION_ADMIN_INGREDIENT_MANDATORY_RULES,
        s3b_direct_axes=(('ingredient', False),),
        value_rules={'gold.clinical.medication_admin_ingredient.ingredient_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
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
                                                                                                    'table': 'clinical_medication_admin_ingredient'},
         'gold.clinical.medication_admin_ingredient.ingredient_target_code.insr_sct_concept_only': {'action': 'invalidate',
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
                                                                                                    'table': 'clinical_medication_admin_ingredient'}}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_gate_direct_public = _rules._s3_gate_direct_public


finalise()


COMMENTS = {'_qc_clinical_medication_admin_ingredient': {'columns': {'action_sequence': 'Administration action '
                                                                             'sequence; zero when absent.',
                                                          'all_ingredients_zero_ind': 'True when every '
                                                                                      'ingredient of the '
                                                                                      'parent administration '
                                                                                      'recorded a dose of 0 '
                                                                                      '(J24); dose_value is '
                                                                                      'then NULL. INSR_W2 '
                                                                                      'W2.05.',
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
                                                                          'INGREDIENT_ORDER_ID:ACTION_SEQUENCE:COMP_SEQUENCE.',
                                                          'component_sequence': 'Ingredient component '
                                                                                'sequence within the action; '
                                                                                'zero when absent.',
                                                          'concentration_unit': 'Source concentration-unit '
                                                                                'code.',
                                                          'concentration_value': 'Concentration value of '
                                                                                 'this ingredient component '
                                                                                 'within the order, '
                                                                                 'interpreted with '
                                                                                 'CONCENTRATION_UNIT_CD.',
                                                          'dose_unit': 'Ordered-dose unit code, falling back '
                                                                       'to dose-quantity unit code.',
                                                          'dose_value': 'Ordered dose, falling back to dose '
                                                                        'quantity.',
                                                          'event_id': 'Millennium EVENT_ID of the parent '
                                                                      'medication administration.',
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
                                                          'rate_unit': 'Normalized rate-unit code.',
                                                          'rate_value': 'Administration or infusion rate for '
                                                                        'this ingredient component '
                                                                        'normalised by the source system, '
                                                                        'interpreted with '
                                                                        'NORMALIZED_RATE_UNIT_CD.',
                                                          'source_present_ind': 'True for rows retained by '
                                                                                'the current administration '
                                                                                'ingredient source.',
                                                          'strength_unit': 'Source strength-unit code.',
                                                          'strength_value': 'Numeric strength value of the '
                                                                            'ingredient component, '
                                                                            'interpreted with '
                                                                            'STRENGTH_UNIT_CD.',
                                                          'volume_unit': 'Source volume-unit code.',
                                                          'volume_value': 'Numeric volume value for the '
                                                                          'ingredient component, interpreted '
                                                                          'with VOLUME_UNIT_CD.'},
                                              'quantity_override': True,
                                              'table': 'Internal QC of clinical_medication_admin_ingredient; '
                                                       'counts rows whose Gold parent is absent.'},
 'clinical_medication_admin_ingredient': {'columns': {},
                                          'quantity_override': False,
                                          'table': 'One ingredient component per medication administration, '
                                                   'retaining source component order and its parent event '
                                                   'key. Coverage: Electronic administration recording '
                                                   'starts in August 2021 (raw holds 20 rows from 2016 to '
                                                   'July 2021) apart from a 2015 pilot island of about '
                                                   '19,000 rows; complete-through month per release: '
                                                   '6_mgmt.data_quality.release_finding (finding_code '
                                                   'F2_complete_through).'}}
