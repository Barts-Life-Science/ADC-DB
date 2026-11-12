"""Silver clinical.medication_admin_ingredient: 1 dataset(s).

- datasets: clinical_medication_admin_ingredient
- moved verbatim from: 2_silver_clinical_events:6140, 2_silver_clinical_events:6178

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.cross_qc import _s3_direct_axis
from silver_core.journey_core import _n, _usable_code, axis_columns, read_source, spark
from silver_tables._common.journey import _s3_lookup_axis
from silver_tables.clinical._shared import SRC_MED_ADMIN_INGREDIENT, SRC_S3C_RXNORM_MULTUM_MAP
from silver_core.value_policy import _zero_measure_rule


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical / medication admin ingredient

# ==== journey_clinical.medication_admin_ingredient ====
MEDICATION_ADMIN_INGREDIENT_PUBLIC_COLUMNS = [
    "event_id", "action_sequence", "component_sequence", "component_id",
    "coding_system", "coding_code", "coding_display", "strength_value",
    "strength_unit", "volume_value", "volume_unit", "dose_value", "dose_unit",
    "rate_value", "rate_unit", "concentration_value", "concentration_unit",
    "ingredient_type_code", "clinically_significant_ind",
    "include_in_total_volume_ind", "freetext_dose", "source_present_ind",
    "loaded_at",
] + axis_columns("ingredient")


@materialized_view(
    name=_n("journey_clinical.medication_admin_ingredient"),
    refresh_policy="auto",
)
def medication_admin_ingredient():
    # Contract v2 parent/child conservation: medication_admin publishes only the
    # administrations whose ORDER_SYNONYM_ID passes Gate B, so this child applies
    # the same gate. The eligible EVENT_IDs come from the parent's own primitive,
    # so the child cannot drift if the parent's rule changes. Without it the child
    # carried 7,435 rows over 2,989 events that no reader could reach through the
    # parent.
    # Build the declared dataset: One ingredient component per medication administration,
    # retaining source component order and its parent event key.
    admitted = (
        spark.read.table(_n("journey_clinical._medication_admin_primitive"))
        .where(_usable_code(F.col("source_code")))
        .select(F.col("event_id").alias("EVENT_ID"))
        .distinct()
    )
    s = read_source(SRC_MED_ADMIN_INGREDIENT).join(admitted, "EVENT_ID", "left_semi")
    # INSR_W2 W2.05 (J24): the per-administration all-zero flag, equi-joined back on EVENT_ID.
    j24 = spark.read.table(_n("journey_clinical._med_admin_ingredient_grouped")).select(
        F.col("EVENT_ID").alias("_j24_event_id"), F.col("all_ingredients_zero_ind").alias("_j24_all_zero"))
    s = s.join(j24, s.EVENT_ID == F.col("_j24_event_id"), "left")
    df = s.select(
        s.EVENT_ID.cast("bigint").alias("event_id"),
        F.coalesce(s.ACTION_SEQUENCE, F.lit(0)).cast("long").alias("action_sequence"),
        F.coalesce(s.COMP_SEQUENCE, F.lit(0)).cast("long").alias("component_sequence"),
        F.concat_ws(
            ":", s.INGREDIENT_ORDER_ID.cast("string"),
            s.ACTION_SEQUENCE.cast("string"), s.COMP_SEQUENCE.cast("string"),
        ).alias("component_id"),
        F.lit("urn:cerner:synonym_id").alias("coding_system"),
        s.SYNONYM_ID.cast("string").alias("coding_code"),
        F.coalesce(
            s.ORDERED_AS_MNEMONIC, s.SUPPLIED_AS_MNEMONIC,
            s.ORDER_MNEMONIC, s.HNA_ORDER_MNEMONIC,
        ).alias("coding_display"),
        s.STRENGTH.cast("decimal(38,10)").alias("strength_value"),
        s.STRENGTH_UNIT_CD.cast("string").alias("strength_unit"),
        s.VOLUME.cast("decimal(38,10)").alias("volume_value"),
        s.VOLUME_UNIT_CD.cast("string").alias("volume_unit"),
        F.coalesce(s.ORDERED_DOSE, s.DOSE_QUANTITY)
         .cast("decimal(38,10)").alias("dose_value"),
        F.coalesce(s.ORDERED_DOSE_UNIT_CD, s.DOSE_QUANTITY_UNIT_CD)
         .cast("string").alias("dose_unit"),
        s.NORMALIZED_RATE.cast("decimal(38,10)").alias("rate_value"),
        s.NORMALIZED_RATE_UNIT_CD.cast("string").alias("rate_unit"),
        s.CONCENTRATION.cast("decimal(38,10)").alias("concentration_value"),
        s.CONCENTRATION_UNIT_CD.cast("string").alias("concentration_unit"),
        s.INGREDIENT_TYPE_FLAG.cast("string").alias("ingredient_type_code"),
        (s.CLINICALLY_SIGNIFICANT_FLAG == 1).alias("clinically_significant_ind"),
        (s.INCLUDE_IN_TOTAL_VOLUME_FLAG == 1).alias("include_in_total_volume_ind"),
        s.FREETEXT_DOSE.alias("freetext_dose"),
        F.lit(True).alias("source_present_ind"),
        s.ADC_UPDT.alias("loaded_at"),
        s.MULTUM_CODE.cast("string").alias("multum_code"),
        F.coalesce(F.col("_j24_all_zero"), F.lit(False)).alias("all_ingredients_zero_ind"),  # INSR_W2 W2.05
    )
    df = _s3_direct_axis(df, "ingredient", F.col("coding_system"), F.col("coding_code"), F.col("coding_display"))
    df = _s3_lookup_axis(df, "ingredient", SRC_S3C_RXNORM_MULTUM_MAP, "urn:cerner:multum:ingredient",
                         F.col("multum_code"), model_name="medication_admin_ingredient", row_key="component_id")
    return df.select(*MEDICATION_ADMIN_INGREDIENT_PUBLIC_COLUMNS, "all_ingredients_zero_ind")


VALUE_POLICIES = {
    'clinical_medication_admin_ingredient': ValuePolicy(table='clinical_medication_admin_ingredient', enabled=True),
}


for _value, _unit in [("strength_value", "strength_unit"), ("volume_value", "volume_unit"),
                      ("dose_value", "dose_unit"), ("rate_value", "rate_unit"),
                      ("concentration_value", "concentration_unit")]:
    VALUE_POLICIES["clinical_medication_admin_ingredient"].value_rule(_value, [_value, _unit],
                                                                      _zero_measure_rule(_value, _unit))


# J24: when every ingredient dose of an administration is zero, the doses record nothing (the flag says so).
VALUE_POLICIES["clinical_medication_admin_ingredient"].value_rule("dose_value", ["dose_value", "all_ingredients_zero_ind"],
           lambda F: F.when(~F.coalesce(F.col("all_ingredients_zero_ind"), F.lit(False)), F.col("dose_value")))


WRAP_POLICY = {'clinical_medication_admin_ingredient'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_medication_admin_ingredient': {'columns': {'action_sequence': 'Administration action sequence; '
                                                                         'zero when absent.',
                                                      'all_ingredients_zero_ind': 'True when every '
                                                                                  'ingredient of the parent '
                                                                                  'administration recorded a '
                                                                                  'dose of 0 (J24); '
                                                                                  'dose_value is then NULL. '
                                                                                  'INSR_W2 W2.05.',
                                                      'clinically_significant_ind': 'Whether the source '
                                                                                    'flags the ingredient as '
                                                                                    'clinically significant.',
                                                      'coding_code': 'Millennium SYNONYM_ID of the '
                                                                     'ingredient.',
                                                      'coding_display': 'Ordered-as, supplied-as, order, or '
                                                                        'HNA mnemonic; first available.',
                                                      'coding_system': 'Cerner synonym identifier namespace.',
                                                      'component_id': 'Source composite '
                                                                      'INGREDIENT_ORDER_ID:ACTION_SEQUENCE:COMP_SEQUENCE.',
                                                      'component_sequence': 'Ingredient component sequence '
                                                                            'within the action; zero when '
                                                                            'absent.',
                                                      'concentration_unit': 'Source concentration-unit code.',
                                                      'concentration_value': 'Concentration value of this '
                                                                             'ingredient component within '
                                                                             'the order, interpreted with '
                                                                             'CONCENTRATION_UNIT_CD.',
                                                      'dose_unit': 'Ordered-dose unit code, falling back to '
                                                                   'dose-quantity unit code.',
                                                      'dose_value': 'Ordered dose, falling back to dose '
                                                                    'quantity.',
                                                      'event_id': 'Millennium EVENT_ID of the parent '
                                                                  'medication administration.',
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
                                                                                    'medication-administration '
                                                                                    'ingredient mapping.',
                                                      'ingredient_omop_source_concept_id': 'Non-standard '
                                                                                           'OMOP source '
                                                                                           'concept '
                                                                                           'identifier, kept '
                                                                                           'separate from a '
                                                                                           'standard target.',
                                                      'ingredient_snomed_code': 'SNOMED CT code selected for '
                                                                                'the '
                                                                                'medication-administration '
                                                                                'ingredient; null when no '
                                                                                'unambiguous mapping is '
                                                                                'available.',
                                                      'ingredient_snomed_display': 'SNOMED CT preferred '
                                                                                   'display selected for the '
                                                                                   'medication-administration '
                                                                                   'ingredient.',
                                                      'ingredient_source_code': 'Source code for the '
                                                                                'medication-administration '
                                                                                'ingredient, retained '
                                                                                'without de-identification.',
                                                      'ingredient_source_display': 'Source display for the '
                                                                                   'medication-administration '
                                                                                   'ingredient.',
                                                      'ingredient_source_system': 'Canonical coding-system '
                                                                                  'URI for the source '
                                                                                  'medication-administration '
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
                                                      'rate_unit': 'Normalized rate-unit code.',
                                                      'rate_value': 'Administration or infusion rate for '
                                                                    'this ingredient component normalised by '
                                                                    'the source system, interpreted with '
                                                                    'NORMALIZED_RATE_UNIT_CD.',
                                                      'source_present_ind': 'True for rows retained by the '
                                                                            'current administration '
                                                                            'ingredient source.',
                                                      'strength_unit': 'Source strength-unit code.',
                                                      'strength_value': 'Numeric strength value of the '
                                                                        'ingredient component, interpreted '
                                                                        'with STRENGTH_UNIT_CD.',
                                                      'volume_unit': 'Source volume-unit code.',
                                                      'volume_value': 'Numeric volume value for the '
                                                                      'ingredient component, interpreted '
                                                                      'with VOLUME_UNIT_CD.'},
                                          'table': 'One ingredient component per medication administration, '
                                                   'retaining source component order and its parent event '
                                                   'key.'}}

# INSR_R4_SILVER_V1 (child): generated by r4/release/gen_silver_components.py; edit the generator, not this.
from silver_core.parent_inputs import ParentInputs, with_parent_inputs  # noqa: E402
R4_PARENT_INPUTS = ParentInputs(parent='clinical_medication_admin', dataset='_clinical_medication_admin_parent_inputs', keys=('event_id',), inputs=('identity_status', 'person_id'), admit_sql="coalesce(((identity_status = 'resolved')), false)", name=_n('journey_clinical._medication_admin_parent_inputs'), parent_name=_n('journey_clinical.medication_admin'))
SPECS = with_parent_inputs(SPECS, 'clinical_medication_admin_ingredient', R4_PARENT_INPUTS)
