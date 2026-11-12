"""Silver clinical.medication_order_ingredient: 1 dataset(s).

- datasets: clinical_medication_order_ingredient
- moved verbatim from: 2_silver_clinical_events:5912, 2_silver_clinical_events:5950

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.cross_qc import _s3_direct_axis
from silver_core.journey_core import _n, axis_columns, read_source
from silver_tables._common.journey import _s3_lookup_axis
from silver_tables.clinical._shared import SRC_MEDICATION_ORDER_INGREDIENT, SRC_S3C_RXNORM_MULTUM_MAP
from silver_core.value_policy import _zero_measure_rule


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical / medication order ingredient

# ==== journey_clinical.medication_order_ingredient ====
MEDICATION_ORDER_INGREDIENT_PUBLIC_COLUMNS = [
    "order_id", "action_sequence", "component_sequence", "component_id",
    "coding_system", "coding_code", "coding_display", "strength_value",
    "strength_unit", "volume_value", "volume_unit", "dose_value", "dose_unit",
    "rate_value", "rate_unit", "concentration_value", "concentration_unit",
    "ingredient_type_code", "clinically_significant_ind",
    "include_in_total_volume_ind", "freetext_dose", "source_present_ind",
    "loaded_at",
] + axis_columns("ingredient")


@materialized_view(
    name=_n("journey_clinical.medication_order_ingredient"),
    refresh_policy="auto",
)
def medication_order_ingredient():
    # Build the declared dataset: One ordered ingredient component per medication order, linked
    # to its parent by the retained event key.
    s = read_source(SRC_MEDICATION_ORDER_INGREDIENT)
    df = s.select(
        s.ORDER_ID.cast("bigint").alias("order_id"),
        F.coalesce(s.ACTION_SEQUENCE, F.lit(0)).cast("long").alias("action_sequence"),
        F.coalesce(s.COMP_SEQUENCE, F.lit(0)).cast("long").alias("component_sequence"),
        F.concat_ws(
            ":", s.ORDER_ID.cast("string"), s.ACTION_SEQUENCE.cast("string"),
            s.COMP_SEQUENCE.cast("string"),
        ).alias("component_id"),
        F.lit("urn:cerner:synonym_id").alias("coding_system"),
        s.SYNONYM_ID.cast("string").alias("coding_code"),
        F.coalesce(
            s.ORDERED_AS_MNEMONIC, s.SUPPLIED_AS_MNEMONIC,
            s.ORDER_MNEMONIC, s.HNA_ORDER_MNEMONIC,
        ).alias("coding_display"),
        s.STRENGTH.cast("decimal(38,10)").alias("strength_value"),
        s.STRENGTH_UNIT_DESCRIPTION.alias("strength_unit"),
        s.VOLUME.cast("decimal(38,10)").alias("volume_value"),
        s.VOLUME_UNIT_DESCRIPTION.alias("volume_unit"),
        F.coalesce(s.ORDERED_DOSE, s.DOSE_QUANTITY)
         .cast("decimal(38,10)").alias("dose_value"),
        F.coalesce(
            s.ORDERED_DOSE_UNIT_DESCRIPTION, s.DOSE_QUANTITY_UNIT_DESCRIPTION,
        ).alias("dose_unit"),
        s.NORMALIZED_RATE.cast("decimal(38,10)").alias("rate_value"),
        s.NORMALIZED_RATE_UNIT_DESCRIPTION.alias("rate_unit"),
        s.CONCENTRATION.cast("decimal(38,10)").alias("concentration_value"),
        s.CONCENTRATION_UNIT_DESCRIPTION.alias("concentration_unit"),
        s.INGREDIENT_TYPE_FLAG.cast("string").alias("ingredient_type_code"),
        (s.CLINICALLY_SIGNIFICANT_FLAG == 1).alias("clinically_significant_ind"),
        (s.INCLUDE_IN_TOTAL_VOLUME_FLAG == 1).alias("include_in_total_volume_ind"),
        s.FREETEXT_DOSE.alias("freetext_dose"),
        s.SOURCE_PRESENT_IND.alias("source_present_ind"),
        s.ADC_UPDT.alias("loaded_at"),
        s.MULTUM_CODE.cast("string").alias("multum_code"),
    )
    df = _s3_direct_axis(df, "ingredient", F.col("coding_system"), F.col("coding_code"), F.col("coding_display"))
    df = _s3_lookup_axis(df, "ingredient", SRC_S3C_RXNORM_MULTUM_MAP, "urn:cerner:multum:ingredient",
                         F.col("multum_code"), model_name="medication_order_ingredient", row_key="component_id")
    return df.select(*MEDICATION_ORDER_INGREDIENT_PUBLIC_COLUMNS)


VALUE_POLICIES = {
    'clinical_medication_order_ingredient': ValuePolicy(table='clinical_medication_order_ingredient', enabled=True),
}


for _value, _unit in [("strength_value", "strength_unit"), ("volume_value", "volume_unit"),
                      ("dose_value", "dose_unit"), ("rate_value", "rate_unit"),
                      ("concentration_value", "concentration_unit")]:
    VALUE_POLICIES["clinical_medication_order_ingredient"].value_rule(_value, [_value, _unit],
                                                                      _zero_measure_rule(_value, _unit))


WRAP_POLICY = {'clinical_medication_order_ingredient'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_medication_order_ingredient': {'columns': {'action_sequence': 'Order action sequence the '
                                                                         'ingredient row belongs to; zero '
                                                                         'when absent.',
                                                      'clinically_significant_ind': 'Whether the source '
                                                                                    'flags the ingredient as '
                                                                                    'clinically significant.',
                                                      'coding_code': 'Millennium SYNONYM_ID of the '
                                                                     'ingredient.',
                                                      'coding_display': 'Ordered-as, supplied-as, order, or '
                                                                        'HNA mnemonic; first available.',
                                                      'coding_system': 'Cerner synonym identifier namespace.',
                                                      'component_id': 'Source composite '
                                                                      'ORDER_ID:ACTION_SEQUENCE:COMP_SEQUENCE.',
                                                      'component_sequence': 'Ingredient component sequence '
                                                                            'within the action; zero when '
                                                                            'absent.',
                                                      'concentration_unit': 'Concentration unit description.',
                                                      'concentration_value': 'Concentration as ordered.',
                                                      'dose_unit': 'Ordered-dose unit, falling back to '
                                                                   'dose-quantity unit.',
                                                      'dose_value': 'Ordered dose, falling back to dose '
                                                                    'quantity.',
                                                      'freetext_dose': 'Free-text dose when supplied.',
                                                      'include_in_total_volume_ind': 'Whether the ingredient '
                                                                                     'counts toward total '
                                                                                     'volume.',
                                                      'ingredient_map_candidate_count': 'Number of candidate '
                                                                                        'targets observed; a '
                                                                                        'mapped parent is '
                                                                                        'unambiguous when '
                                                                                        'this is 1.',
                                                      'ingredient_map_candidate_set_id': 'Stable identifier '
                                                                                         'of the retained '
                                                                                         'candidate set.',
                                                      'ingredient_map_competing_count': 'Candidates within '
                                                                                        '0.05 cosine of the '
                                                                                        'selected candidate.',
                                                      'ingredient_map_component_count': 'Number of '
                                                                                        'source-derived '
                                                                                        'components.',
                                                      'ingredient_map_component_index': 'One-based '
                                                                                        'source-derived '
                                                                                        'component index.',
                                                      'ingredient_map_confidence': 'Mapping confidence in '
                                                                                   '[0,1] where the lane '
                                                                                   'measured one.',
                                                      'ingredient_map_cosine': 'Measured cosine between the '
                                                                               'normalised source and '
                                                                               'selected target; null when '
                                                                               'scoring did not run.',
                                                      'ingredient_map_method': 'Mapping method: '
                                                                               'source_native, rule, '
                                                                               'exact_synonym or '
                                                                               'embedding_proposed.',
                                                      'ingredient_map_rollup_levels': 'Maximum hierarchy '
                                                                                      'distance to a '
                                                                                      'selected common '
                                                                                      'ancestor.',
                                                      'ingredient_map_rule_id': 'Stable rule or lane '
                                                                                'identifier that produced '
                                                                                'the mapping.',
                                                      'ingredient_map_scoring_model': 'Embedding model that '
                                                                                      'produced map_cosine.',
                                                      'ingredient_map_status': 'Explicit mapping outcome '
                                                                               'from the S3b status '
                                                                               'vocabulary.',
                                                      'ingredient_map_status_reason': 'Reason for the '
                                                                                      'mapping outcome or '
                                                                                      'abstention.',
                                                      'ingredient_map_version': 'Version of the source, '
                                                                                'terminology release or '
                                                                                'model used by the mapping.',
                                                      'ingredient_omop_concept_id': 'OMOP concept identifier '
                                                                                    'selected with the '
                                                                                    'medication-order '
                                                                                    'ingredient mapping.',
                                                      'ingredient_omop_source_concept_id': 'Non-standard '
                                                                                           'OMOP source '
                                                                                           'concept '
                                                                                           'identifier, kept '
                                                                                           'separate from a '
                                                                                           'standard target.',
                                                      'ingredient_snomed_code': 'SNOMED CT code selected for '
                                                                                'the medication-order '
                                                                                'ingredient; null when no '
                                                                                'unambiguous mapping is '
                                                                                'available.',
                                                      'ingredient_snomed_display': 'SNOMED CT preferred '
                                                                                   'display selected for the '
                                                                                   'medication-order '
                                                                                   'ingredient.',
                                                      'ingredient_source_code': 'Source code for the '
                                                                                'medication-order '
                                                                                'ingredient, retained '
                                                                                'without de-identification.',
                                                      'ingredient_source_display': 'Source display for the '
                                                                                   'medication-order '
                                                                                   'ingredient.',
                                                      'ingredient_source_system': 'Canonical coding-system '
                                                                                  'URI for the source '
                                                                                  'medication-order '
                                                                                  'ingredient.',
                                                      'ingredient_target_code': 'Code of the selected target '
                                                                                'in its own vocabulary.',
                                                      'ingredient_target_display': 'Display of the selected '
                                                                                   'target in its own '
                                                                                   'vocabulary.',
                                                      'ingredient_target_system': 'Coding-system URI of the '
                                                                                  'selected target.',
                                                      'ingredient_type_code': 'Millennium ingredient type '
                                                                              'flag.',
                                                      'loaded_at': 'Bronze load timestamp of the ingredient '
                                                                   'row.',
                                                      'order_id': 'Millennium ORDER_ID of the parent '
                                                                  'medication order.',
                                                      'rate_unit': 'Normalized rate unit description.',
                                                      'rate_value': 'S10 medication_order medication/dosage; '
                                                                    'retained as a source-faithful bronze '
                                                                    'input or Journey standard-block field.',
                                                      'source_present_ind': 'Whether the source row remains '
                                                                            'present.',
                                                      'strength_unit': 'Strength unit description.',
                                                      'strength_value': 'Ingredient strength as ordered.',
                                                      'volume_unit': 'Volume unit description.',
                                                      'volume_value': 'Ingredient volume as ordered.'},
                                          'table': 'One ordered ingredient component per medication order, '
                                                   'linked to its parent by the retained event key.'}}
