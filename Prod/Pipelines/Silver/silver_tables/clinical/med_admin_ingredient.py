"""Silver clinical.med_admin_ingredient: 1 dataset(s).

- datasets: _clinical_med_admin_ingredient_grouped
- moved verbatim from: 2_silver_clinical_events:4923, 2_silver_clinical_events:4973

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source
from silver_tables.clinical._shared import SRC_MED_ADMIN_INGREDIENT


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


def _med_admin_ingredient_grouped_query():
    # Group medication-administration ingredient components in their action and component
    # sequence.
    s = read_source(SRC_MED_ADMIN_INGREDIENT)
    ingredient = F.struct(
        F.coalesce(s.ACTION_SEQUENCE, F.lit(0)).cast("long").alias("action_sequence"),
        F.coalesce(s.COMP_SEQUENCE, F.lit(0)).cast("long").alias("component_sequence"),
        F.concat_ws(":", s.INGREDIENT_ORDER_ID.cast("string"),
                    s.ACTION_SEQUENCE.cast("string"), s.COMP_SEQUENCE.cast("string"))
         .alias("component_id"),
        F.lit("urn:cerner:synonym_id").alias("coding_system"),
        s.SYNONYM_ID.cast("string").alias("coding_code"),
        F.coalesce(s.ORDERED_AS_MNEMONIC, s.SUPPLIED_AS_MNEMONIC,
                   s.ORDER_MNEMONIC, s.HNA_ORDER_MNEMONIC).alias("coding_display"),
        s.STRENGTH.cast("decimal(38,10)").alias("strength_value"),
        s.STRENGTH_UNIT_CD.cast("string").alias("strength_unit"),
        s.VOLUME.cast("decimal(38,10)").alias("volume_value"),
        s.VOLUME_UNIT_CD.cast("string").alias("volume_unit"),
        F.coalesce(s.ORDERED_DOSE, s.DOSE_QUANTITY).cast("decimal(38,10)").alias("dose_value"),
        F.coalesce(s.ORDERED_DOSE_UNIT_CD, s.DOSE_QUANTITY_UNIT_CD).cast("string").alias("dose_unit"),
        s.NORMALIZED_RATE.cast("decimal(38,10)").alias("rate_value"),
        s.NORMALIZED_RATE_UNIT_CD.cast("string").alias("rate_unit"),
        s.CONCENTRATION.cast("decimal(38,10)").alias("concentration_value"),
        s.CONCENTRATION_UNIT_CD.cast("string").alias("concentration_unit"),
        s.INGREDIENT_TYPE_FLAG.cast("string").alias("ingredient_type_code"),
        (s.CLINICALLY_SIGNIFICANT_FLAG == 1).alias("clinically_significant_ind"),
        (s.INCLUDE_IN_TOTAL_VOLUME_FLAG == 1).alias("include_in_total_volume_ind"),
        s.FREETEXT_DOSE.alias("freetext_dose"),
        F.lit(True).alias("source_present_ind"),
    )
    # INSR_W2 W2.05 (J24): an administration whose every ingredient dose is 0 recorded no dose. The flag is a
    # grouped aggregate (no window); when it holds, dose_value is NULLed inside the ingredient JSON.
    dose = F.coalesce(s.ORDERED_DOSE, s.DOSE_QUANTITY).cast("decimal(38,10)")
    grouped = s.groupBy("EVENT_ID").agg(
        F.sort_array(F.collect_list(ingredient)).alias("_ingredients"),
        F.count(F.lit(1)).cast("long").alias("ingredient_count"),
        F.max(F.greatest(s.UPDT_DT_TM, s.ADC_UPDT)).alias("ingredient_source_update_timestamp"),
        F.max(s.ADC_UPDT).alias("ingredient_loaded_at"),
        (F.max((dose == 0).cast("int")) == 1).alias("_any_zero"),
        (F.max((dose != 0).cast("int")) == 1).alias("_any_nonzero"),
    )
    all_zero = F.col("_any_zero") & ~F.coalesce(F.col("_any_nonzero"), F.lit(False))
    nulled = F.transform("_ingredients", lambda x: x.withField("dose_value", F.lit(None).cast("decimal(38,10)")))
    return grouped.select(
        "EVENT_ID",
        F.to_json(F.when(all_zero, nulled).otherwise(F.col("_ingredients"))).alias("ingredients_json"),
        "ingredient_count", "ingredient_source_update_timestamp", "ingredient_loaded_at",
        F.coalesce(all_zero, F.lit(False)).alias("all_ingredients_zero_ind"),
    )


@materialized_view(
    name=_n("journey_clinical._med_admin_ingredient_grouped"),
    private=True,
    refresh_policy="auto",
)
def _med_admin_ingredient_grouped():
    # Build the declared dataset: Internal ordered ingredient aggregate for medication
    # administrations.
    return _med_admin_ingredient_grouped_query()


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_med_admin_ingredient_grouped': {'columns': {},
                                            'table': 'Internal ordered ingredient aggregate for medication '
                                                     'administrations.'}}
