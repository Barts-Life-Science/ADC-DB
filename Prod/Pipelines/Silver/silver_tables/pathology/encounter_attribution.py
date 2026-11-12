"""Silver pathology.encounter_attribution: 2 dataset(s).

- datasets: clinical_microbiology_isolate_encounter_attribution, clinical_susceptibility_result_encounter_attribution
- moved verbatim from: 2_silver_clinical_events:3710, 2_silver_clinical_events:3713, 2_silver_clinical_events:3749

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from pyspark.sql.window import Window
import pyspark.sql.functions as F
from silver_core.journey_core import _n, spark


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# INSR_W2 W2.12 (J32) =============================================================================================
# Encounter attribution for diagnostic rows that arrive without an encounter: a candidate encounter of the same
# person whose window contains the event (per arm in INSR_W2_ATTRIBUTION_ARMS: micro/susceptibility use the spell
# itself), ranked contained first, then nearest start, then lowest encounter_id (NULLS LAST). Only the arms that
# passed the ambiguity gate (<= 10% of attributed events with >1 candidate) are declared. Published as a companion
# table so no VARIANT-bearing fact MV gains a join; the fact's own encounter_id is untouched.
INSR_W2_ATTRIBUTION_ARMS = {'imaging_exam': ('journey_clinical.imaging_exam', 2, 12, 'band_-2h_+12h'), 'specimen': ('journey_clinical.specimen', 2, 12, 'band_-2h_+12h'), 'microbiology_isolate': ('journey_clinical.microbiology_isolate', 0, 0, 'spell_containment'), 'susceptibility_result': ('journey_clinical.susceptibility_result', 0, 0, 'spell_containment')}


def _declare_encounter_attribution(arm, fact_table, before_h, after_h, method):
    """Declare one companion MV: patient_event_key -> encounter_id_derived, method, candidate count."""
    def attribution():
        """Rank the same-person encounters whose window holds each unlinked event and keep the best one."""
        x = (spark.read.table(_n(fact_table)).where(F.col("encounter_id").isNull() & F.col("person_id").isNotNull()
                                                    & F.col("event_datetime").isNotNull())
             .select("patient_event_key", F.col("person_id").cast("bigint").alias("person_id"), "event_datetime"))
        e = spark.read.table(_n("journey_spine.encounter")).where(F.col("period_start").isNotNull()).select(
            F.col("person_id").cast("bigint").alias("_e_person"), F.col("encounter_id").alias("_e_id"),
            F.col("period_start").alias("_e_start"), F.coalesce("period_end", "period_start").alias("_e_end"))
        lo = F.col("_e_start") - F.expr(f"INTERVAL {before_h} HOURS")
        hi = F.col("_e_end") + F.expr(f"INTERVAL {after_h} HOURS")
        c = x.join(e, x.person_id == F.col("_e_person"), "inner").where(F.col("event_datetime").between(lo, hi))
        contained = F.col("event_datetime").between(F.col("_e_start"), F.col("_e_end"))
        dist = F.abs(F.unix_timestamp("event_datetime") - F.unix_timestamp("_e_start"))
        w = Window.partitionBy("patient_event_key")
        ranked = (c.withColumn("_rn", F.row_number().over(w.orderBy(contained.desc(), dist.asc_nulls_last(),
                                                                    F.col("_e_id").asc_nulls_last())))
                   .withColumn("_n", F.count(F.lit(1)).over(w)))
        return ranked.where("_rn = 1").select(
            "patient_event_key", "person_id", F.col("_e_id").cast("bigint").alias("encounter_id_derived"),
            F.when(contained, F.lit(method)).otherwise(F.lit(method + ":nearest")).alias("encounter_attribution_method"),
            F.col("_n").cast("int").alias("encounter_attribution_candidates"))
    attribution.__name__ = f"{arm}_encounter_attribution"
    materialized_view(
        name=_n(f"journey_clinical.{arm}_encounter_attribution"), refresh_policy="auto", cluster_by=["patient_event_key"])(attribution)


for _arm in ['microbiology_isolate', 'susceptibility_result']:
    _declare_encounter_attribution(_arm, *INSR_W2_ATTRIBUTION_ARMS[_arm])


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_microbiology_isolate_encounter_attribution': {'columns': {'encounter_attribution_candidates': 'Encounters '
                                                                                                         'of '
                                                                                                         'the '
                                                                                                         'person '
                                                                                                         'whose '
                                                                                                         'window '
                                                                                                         'held '
                                                                                                         'the '
                                                                                                         'event; '
                                                                                                         '>1 '
                                                                                                         'means '
                                                                                                         'the '
                                                                                                         'pick '
                                                                                                         'was '
                                                                                                         'ranked.',
                                                                     'encounter_attribution_method': 'spell_containment '
                                                                                                     '(event '
                                                                                                     'inside '
                                                                                                     'the '
                                                                                                     'window '
                                                                                                     'and '
                                                                                                     'the '
                                                                                                     'spell) '
                                                                                                     'or '
                                                                                                     'spell_containment:nearest '
                                                                                                     '(inside '
                                                                                                     'the '
                                                                                                     'window, '
                                                                                                     'outside '
                                                                                                     'the '
                                                                                                     'spell).',
                                                                     'encounter_id_derived': 'Encounter of '
                                                                                             'the same '
                                                                                             'person whose '
                                                                                             'window holds '
                                                                                             'the event; the '
                                                                                             "fact's own "
                                                                                             'encounter_id '
                                                                                             'is unchanged.',
                                                                     'patient_event_key': 'Key of the '
                                                                                          'microbiology_isolate '
                                                                                          'row (join to '
                                                                                          'clinical_microbiology_isolate).',
                                                                     'person_id': 'Native Millennium '
                                                                                  'PERSON_ID of the row and '
                                                                                  'of the encounter.'},
                                                         'table': 'INSR_W2 W2.12: derived encounter for '
                                                                  'microbiology_isolate rows with no source '
                                                                  'encounter (J32).'},
 'clinical_susceptibility_result_encounter_attribution': {'columns': {'encounter_attribution_candidates': 'Encounters '
                                                                                                          'of '
                                                                                                          'the '
                                                                                                          'person '
                                                                                                          'whose '
                                                                                                          'window '
                                                                                                          'held '
                                                                                                          'the '
                                                                                                          'event; '
                                                                                                          '>1 '
                                                                                                          'means '
                                                                                                          'the '
                                                                                                          'pick '
                                                                                                          'was '
                                                                                                          'ranked.',
                                                                      'encounter_attribution_method': 'spell_containment '
                                                                                                      '(event '
                                                                                                      'inside '
                                                                                                      'the '
                                                                                                      'window '
                                                                                                      'and '
                                                                                                      'the '
                                                                                                      'spell) '
                                                                                                      'or '
                                                                                                      'spell_containment:nearest '
                                                                                                      '(inside '
                                                                                                      'the '
                                                                                                      'window, '
                                                                                                      'outside '
                                                                                                      'the '
                                                                                                      'spell).',
                                                                      'encounter_id_derived': 'Encounter of '
                                                                                              'the same '
                                                                                              'person whose '
                                                                                              'window holds '
                                                                                              'the event; '
                                                                                              "the fact's "
                                                                                              'own '
                                                                                              'encounter_id '
                                                                                              'is unchanged.',
                                                                      'patient_event_key': 'Key of the '
                                                                                           'susceptibility_result '
                                                                                           'row (join to '
                                                                                           'clinical_susceptibility_result).',
                                                                      'person_id': 'Native Millennium '
                                                                                   'PERSON_ID of the row and '
                                                                                   'of the encounter.'},
                                                          'table': 'INSR_W2 W2.12: derived encounter for '
                                                                   'susceptibility_result rows with no '
                                                                   'source encounter (J32).'}}
