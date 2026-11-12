"""Gold spine.encounter_close_inference: 1 dataset(s).

- datasets: _qc_spine_encounter_close_inference
- moved verbatim from: 1_gold_spine_and_reference:390, 1_gold_spine_and_reference:391, 1_gold_spine_and_reference:395

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== ENCOUNTER_CLOSE_INFERENCE_V1: inferred close for stays the source still holds open ====

# Silver's encounter_close_state is source truth and is published unchanged. For spells and emergency
# visits the source still holds Active with no departure, Gold adds the time by which later evidence
# shows the stay had ended: the person's next spell or emergency visit, or death. Deterministic: no
# current-date rule, so every refresh of the same data gives the same answer. The evidence lives in
# its own small internal MV, joined once by the published MV, so spine_encounter's plan stays shallow.
CLOSE_INFERENCE_LEVELS = ["spell", "emergency_visit"]


CLOSE_INFERENCE_OPEN_STATES = ["no_departure_recorded", "open_ward_interval", "departure_evidence_invalid"]


@dp.materialized_view(
    name=_n("gold_qc._spine_encounter_close_inference"),
    refresh_policy="auto",
)
def _gold_spine_encounter_close_inference():
    """Closure evidence for source-open spells and emergency visits; one row per encounter with evidence."""
    # GOLD_MV_SPLIT_V1: the pre-public rows (spine_encounter is two-copy so its twin exists; its public MV joins
    # this evidence, so reading the public table would be a cycle).
    enc = spark.read.table(_n("gold_qc._spine_encounter"))
    stays = enc.where(
        F.col("encounter_level").isin(CLOSE_INFERENCE_LEVELS)
        & F.col("encounter_close_state").isin(CLOSE_INFERENCE_OPEN_STATES)
        & (F.col("status_code") == "854")
        & F.col("person_id").isNotNull()
        & F.col("period_start").isNotNull()
    ).select(
        F.col("encounter_id").alias("_ci_encounter_id"),
        F.col("person_id").alias("_ci_person_id"),
        # The stay certainly lasted until its own latest evidence.
        F.greatest(F.col("period_start"), F.col("last_activity_datetime")).alias("_ci_anchor"),
    )
    later = enc.where(
        F.col("encounter_level").isin(CLOSE_INFERENCE_LEVELS)
        & F.col("person_id").isNotNull()
        & F.col("period_start").isNotNull()
    ).select(
        F.col("person_id").alias("_ci_later_person_id"),
        F.col("encounter_level").alias("_ci_later_level"),
        F.col("period_start").alias("_ci_later_start"),
    )
    # The person's first spell and first emergency visit after the stay's latest evidence.
    following = (
        stays.join(later, (F.col("_ci_later_person_id") == F.col("_ci_person_id"))
                   & (F.col("_ci_later_start") > F.col("_ci_anchor")), "left")
        .groupBy("_ci_encounter_id", "_ci_person_id", "_ci_anchor")
        .agg(
            F.min(F.when(F.col("_ci_later_level") == "spell", F.col("_ci_later_start"))).alias("_ci_next_spell"),
            F.min(F.when(F.col("_ci_later_level") == "emergency_visit", F.col("_ci_later_start")))
             .alias("_ci_next_emergency_visit"),
        )
    )
    # INSR_R4_GOLD_V1: gold's repaired death time from the person-dates MV (the same expression spine_person
    # publishes; a death before birth is NULL there too).
    deaths = spark.read.table(_n("gold_qc._person_dates")).select(
        F.col("_qc_date_person_id").alias("_ci_death_person_id"),
        F.col("_qc_deceased_datetime").alias("_ci_death"),
    )
    evidence = following.join(deaths, F.col("_ci_death_person_id") == F.col("_ci_person_id"), "left")
    # A death before the stay's own latest evidence contradicts it and closes nothing.
    death = F.when(F.col("_ci_death") >= F.col("_ci_anchor"), F.col("_ci_death"))
    close = F.least(death, F.col("_ci_next_spell"), F.col("_ci_next_emergency_visit"))
    method = (F.when(death == close, F.lit("death"))
               .when(F.col("_ci_next_spell") == close, F.lit("later_spell"))
               .otherwise(F.lit("later_emergency_visit")))
    return evidence.where(close.isNotNull()).select(
        F.col("_ci_encounter_id").alias("encounter_id"),
        close.alias("inferred_close_datetime"),
        method.alias("inferred_close_method"),
    )


finalise()


COMMENTS = {'_qc_spine_encounter_close_inference': {'columns': {},
                                         'quantity_override': False,
                                         'table': 'Internal: one row per spell or emergency visit the source '
                                                  'still holds open (status Active, no departure) for which '
                                                  "later evidence shows it had ended: the person's next "
                                                  'spell or emergency visit, or death. Read once by gold '
                                                  'spine_encounter; kept separate so that MV joins one small '
                                                  'table.'}}
