"""Encounter attribution and PLICS activity linkage for RBI (spec §5, §10.1, §10.2).

Two jobs:
1. Give stream rows with no native encounter an encounter, but only when exactly one
   observed attendance of the strongest tier contains the event time.
2. Link each PLICS/NCC costed activity to the gold encounter (or appointment) its £
   belongs to.
Every link records its method; nothing is guessed when there is more than one candidate.
"""
from pyspark.sql import functions as F

import rbi_rules as R
from rbi_schema import to_stream

FIN_SYSTEM = "urn:cerner:encntr_alias:fin"
MAX_INTERVAL_DAYS = 366  # longer "attendances" are data errors; they would explode into thousands of dates


def fin_to_encounter(identifiers):
    """FIN → encounter_id, keeping only FINs that name exactly one encounter."""
    f = (identifiers.where(F.col("identifier_system") == FIN_SYSTEM)
         .select(F.col("identifier_value").alias("fin"), F.col("encounter_id").cast("bigint").alias("encounter_id"))
         .distinct())
    return (f.groupBy("fin").agg(F.min("encounter_id").alias("encounter_id"), F.countDistinct("encounter_id").alias("_n"))
            .where(F.col("_n") == 1).drop("_n"))


def _attendance_tier(type_col):
    """CASE expression giving each physical-attendance encounter type its tier; others NULL."""
    expr = None
    for type_display, tier in R.ATTENDANCE_TIER.items():
        cond = type_col == F.lit(type_display)
        expr = F.when(cond, F.lit(tier)) if expr is None else expr.when(cond, F.lit(tier))
    return expr


def attendance_intervals(encounters):
    """Observed attendance intervals, one row per (interval, UTC date) for an equi-join on date.

    Observed = arrival and departure confidence HIGH/MEDIUM and close state 'departed'
    (spec §10.1). Exploding to dates lets attribution join on (person, date) instead of
    comparing every event with every interval the person ever had.
    """
    iv = (encounters.withColumn("tier", _attendance_tier(F.col("type_display")))
          .where(F.col("tier").isNotNull()
                 & F.col("arrival_confidence").isin(*R.OBSERVED_CONFIDENCE)
                 & F.col("departure_confidence").isin(*R.OBSERVED_CONFIDENCE)
                 & (F.col("encounter_close_state") == R.OBSERVED_CLOSE_STATE)
                 & F.col("period_start").isNotNull() & F.col("period_end").isNotNull()
                 & (F.col("period_end") >= F.col("period_start"))
                 & (F.datediff(F.col("period_end"), F.col("period_start")) <= MAX_INTERVAL_DAYS))
          .select(F.col("person_id").cast("bigint").alias("person_id"),
                  F.col("encounter_id").cast("bigint").alias("_cand_encounter_id"), "tier",
                  F.col("period_start").alias("_iv_start"), F.col("period_end").alias("_iv_end")))
    return iv.withColumn("_iv_date", F.explode(F.sequence(F.to_date("_iv_start"), F.to_date("_iv_end"))))


def attribution_candidates(events, intervals, skip_streams=("high_cost_drug",)):
    """Encounter candidates for rows without a native encounter: one row per row that has any candidate.

    _n = distinct encounters in the strongest tier containing the event time; _enc = the smallest of
    them. One join and one aggregation; the top-tier selection is done with array functions so this
    step stays shallow enough for Lakeflow to refresh incrementally.
    """
    todo = events.where(F.col("encounter_id").isNull() & ~F.col("stream").isin(*skip_streams))
    probe = todo.select("resource_use_event_key", "person_id", "event_start", F.to_date("event_start").alias("_iv_date"))
    cand = (probe.join(intervals, ["person_id", "_iv_date"])
            .where((F.col("event_start") >= F.col("_iv_start")) & (F.col("event_start") <= F.col("_iv_end"))))
    g = cand.groupBy("resource_use_event_key").agg(
        F.min("tier").alias("_top"),
        F.collect_set(F.struct(F.col("tier").alias("tier"), F.col("_cand_encounter_id").alias("enc"))).alias("_c"))
    top = F.expr("array_distinct(transform(filter(_c, x -> x.tier = _top), x -> x.enc))")
    return g.select("resource_use_event_key", F.size(top).alias("_n"), F.array_min(top).alias("_enc"))


def apply_attribution(events, candidates, skip_streams=("high_cost_drug",)):
    """Set encounter_id and attribution columns from the candidates (spec §5).

    - native encounter: kept, method 'native'.
    - exactly one candidate in the strongest tier: linked, 'interval_unique', score 1.
    - several: stays NULL, 'interval_ambiguous', candidate_count = n (no time-proximity tie-break).
    - none: 'none'. Streams in skip_streams (monthly ledger lines): 'not_applicable'.
    """
    e = events.drop("attribution_method", "attribution_score", "candidate_count").join(candidates, "resource_use_event_key", "left")
    skip = F.col("stream").isin(*skip_streams)
    native = F.col("encounter_id").isNotNull()
    unique = F.coalesce(F.col("_n") == 1, F.lit(False))
    return to_stream(e.withColumns({
        "encounter_id": F.when(skip | native, F.col("encounter_id")).when(unique, F.col("_enc")),
        "attribution_method": (F.when(skip, F.lit("not_applicable")).when(native, F.lit("native"))
                               .when(unique, F.lit("interval_unique")).when(F.col("_n") > 1, F.lit("interval_ambiguous"))
                               .otherwise(F.lit("none"))),
        "attribution_score": F.when(skip, F.lit(None).cast("double")).when(native | unique, F.lit(1.0)),
        "candidate_count": F.when(skip, F.lit(0)).when(native, F.lit(1)).otherwise(F.coalesce(F.col("_n"), F.lit(0))).cast("int"),
    }))


def attribute_encounters(events, intervals, skip_streams=("high_cost_drug",)):
    """Fill NULL encounter_id from observed attendance intervals (candidates, then apply)."""
    return apply_attribution(events, attribution_candidates(events, intervals, skip_streams), skip_streams)


def plics_fin_links(activities, fin_map):
    """Activity linkage stage 1: the native encounter (NCC via ENCNTRID) or the FIN encounter.

    APC/OUT/OUTS hosp_spell_id = FIN; EC attendance_id = FIN (spec §10.2). Kept as its own MV so each
    linkage stage stays shallow enough to refresh incrementally.
    """
    a = activities.select("extract_cd", "activity_record_id", "feed_type",
                          F.col("person_id").cast("bigint").alias("person_id"),
                          F.col("encounter_id").cast("bigint").alias("_native_encounter_id"),
                          F.when(F.col("feed_type").isin("APC", "OUT", "OUTS"), F.col("hosp_spell_id"))
                           .when(F.col("feed_type") == "EC", F.col("attendance_id")).cast("string").alias("fin"))
    return a.join(fin_map.withColumnRenamed("encounter_id", "_fin_encounter_id"), "fin", "left")


def plics_op_candidates(activities, appointment_events):
    """Activity linkage stage 2: OP activities → consumed appointments on the same person and London date.

    One row per OP activity with a candidate: count (_n), and the smallest candidate's key and encounter.
    """
    op = (appointment_events.where((F.col("stream") == "outpatient_appointment") & (F.col("consumption_status") == "consumed"))
          .select("person_id", F.col("event_date_local").alias("_appt_date"),
                  F.col("resource_use_event_key").alias("_appt_key"), F.col("encounter_id").alias("_appt_encounter_id")))
    return (activities.where(F.col("feed_type") == "OP")
            .select("extract_cd", "activity_record_id", F.col("person_id").cast("bigint").alias("person_id"),
                    F.to_date("appointment_date").alias("_appt_date"))
            .join(op, ["person_id", "_appt_date"])
            .groupBy("extract_cd", "activity_record_id")
            .agg(F.count(F.lit(1)).alias("_n"), F.min("_appt_key").alias("_appt_key"),
                 F.min("_appt_encounter_id").alias("_appt_encounter_id")))


def combine_plics_links(fin_links, op_candidates):
    """Activity linkage stage 3: one row per activity with method, entity and score (spec §10.2).

    - native / fin_attendance / fin_spell from stage 1;
    - person_date when exactly one consumed appointment matched (stage 2);
    - person_date_ambiguous / none otherwise (score NULL).
    """
    a = fin_links.join(op_candidates, ["extract_cd", "activity_record_id"], "left")
    method = (F.when(F.col("_native_encounter_id").isNotNull(), F.lit("native"))
              .when(F.col("_fin_encounter_id").isNotNull() & (F.col("feed_type") == "EC"), F.lit("fin_attendance"))
              .when(F.col("_fin_encounter_id").isNotNull(), F.lit("fin_spell"))
              .when(F.col("_n") == 1, F.lit("person_date"))
              .when(F.col("_n") > 1, F.lit("person_date_ambiguous"))
              .otherwise(F.lit("none")))
    return (a.withColumn("link_method", method).select(
        "extract_cd", "activity_record_id", "feed_type", "person_id",
        F.coalesce(F.col("_native_encounter_id"), F.col("_fin_encounter_id"),
                   F.when(F.col("link_method") == "person_date", F.col("_appt_encounter_id"))).alias("encounter_id"),
        F.when(F.col("link_method") == "person_date", F.col("_appt_key")).alias("appointment_key"),
        "link_method",
        F.when(F.col("link_method").isin("native", "fin_spell", "fin_attendance", "person_date"), F.lit(1.0)).alias("link_score"),
        F.coalesce(F.col("_n"), F.lit(0)).cast("int").alias("candidate_count"),
    ))


def plics_activity_links(activities, fin_map, appointment_events):
    """One row per costed activity with the gold entity its £ belongs to (stages 1–3 composed)."""
    return combine_plics_links(plics_fin_links(activities, fin_map), plics_op_candidates(activities, appointment_events))
