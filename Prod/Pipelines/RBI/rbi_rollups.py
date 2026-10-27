"""Published RBI tables (spec §3): the costed fact and its summaries.

Every summary sums the fact, so totals agree with it by construction. The authoritative
£ is cost_total + cost_overhead (spec §10.5).
"""
from pyspark.sql import functions as F

import rbi_rules as R
from rbi_costing import COST_METHOD_VERSION
from rbi_schema import STREAM_COLUMNS, event_key, exact_sum, financial_year, in_window, london  # INCR_FIX_20261018, RBI_REL_20261025

COST_COLUMNS = [
    ("cost_total", "double"), ("cost_overhead", "double"), ("cost_basis", "string"), ("cost_reason", "string"),
    ("cost_modelled_shadow", "double"), ("cost_superseded", "double"), ("cost_stratum_level", "string"),
    ("cost_base_fy", "string"), ("uplift_factor", "double"), ("cost_method_version", "string"),
]
FACT_COLUMNS = STREAM_COLUMNS + COST_COLUMNS + [("spell_encounter_id", "bigint"), ("journey_keys", "array<string>")]
DOMAINS = ("beds", "theatres", "outpatients_ed", "diagnostics_drugs", "costing")
ACTUAL_BASES = ("actual_plics", "actual_ncc_allocated", "drug_expenditure_actual")


def to_fact(df):
    """Project onto FACT_COLUMNS; absent columns become typed NULLs."""
    present = set(df.columns)
    return df.select([(F.col(n) if n in present else F.lit(None)).cast(t).alias(n) for n, t in FACT_COLUMNS])


def unallocated_as_rows(unallocated, activities, links):
    """Ledger £ that reached no atomic row (ledger_unallocated output), as fact rows (stream 'plics_unallocated').

    This keeps Σ(cost_total + cost_overhead) over the fact equal to every £ RBI received.
    Unallocated overhead goes to cost_overhead. Superseded HCD £ stays visible in
    cost_superseded with cost_total 0.
    """
    keys = ["extract_cd", "activity_record_id"]
    a = activities.select(*keys, F.col("person_id").cast("bigint").alias("person_id"), F.col("event_datetime").alias("event_start"))
    x = (unallocated.join(a, keys, "left").join(links.select(*keys, "encounter_id"), keys, "left")
         .withColumn("event_date_local", F.to_date(london(F.col("event_start")))))
    is_oh = F.col("component") == "overhead"
    return to_fact(x.withColumns({
        "resource_use_event_key": event_key(F.col("extract_cd"), F.col("activity_record_id"), F.col("component")),
        "domain": F.lit("costing"),
        "stream": F.lit("plics_unallocated"),
        "unit_type": F.lit("gbp"),
        "consumption_status": F.lit("not_applicable"),
        "consumption_evidence": F.concat(F.lit("component:"), F.col("component")),
        "financial_year": financial_year(F.col("event_date_local")),
        "attribution_method": F.lit("activity_link"),
        "stratum_l1": F.col("component"),
        "source_table": F.lit("gold.clinical_costed_activity"),
        "source_key": F.concat_ws("|", F.col("extract_cd"), F.col("activity_record_id")),
        "rule_version": F.lit(R.RULE_VERSION),
        "cost_total": F.when(is_oh, F.lit(0.0)).otherwise(F.col("cost_total")),
        "cost_overhead": F.when(is_oh, F.col("cost_total")).otherwise(F.lit(0.0)),
        "cost_basis": F.col("cost_basis"),
        "cost_method_version": F.lit(COST_METHOD_VERSION),
    }))


def resource_use_event(costed, unallocated_rows, encounters, journey_links):
    """The published fact: costed stream rows + unallocated rows, with spell and journey keys.

    spell_encounter_id = encounter_id when that encounter is a bed-holding type.
    journey_keys = every spine journey the row's encounter belongs to (sorted).
    """
    spells = (encounters.where(F.col("type_display").isin(*R.BED_ENCOUNTER_TYPES))
              .select(F.col("encounter_id").cast("bigint").alias("encounter_id"),
                      F.col("encounter_id").cast("bigint").alias("spell_encounter_id")))
    journeys = (journey_links.where(F.col("encounter_id").isNotNull())
                .groupBy(F.col("encounter_id").cast("bigint").alias("encounter_id"))
                .agg(F.array_sort(F.collect_set("journey_key")).alias("journey_keys")))
    both = to_fact(costed).unionByName(to_fact(unallocated_rows)).drop("spell_encounter_id", "journey_keys")
    return to_fact(both.join(spells, "encounter_id", "left").join(journeys, "encounter_id", "left"))


def _q(stream, status="consumed"):
    """Σ quantity for a stream's rows with the given consumption status."""
    return exact_sum(F.when((F.col("stream") == stream) & (F.col("consumption_status") == status), F.col("quantity")))


def _n(stream, status="consumed", extra=None):
    """Count of a stream's rows with the given consumption status (and optional extra condition)."""
    cond = (F.col("stream") == stream) & (F.col("consumption_status") == status)
    return F.count(F.when(cond if extra is None else cond & extra, 1))


def _all_cost():
    return F.col("cost_total") + F.col("cost_overhead")


# ---- Observation bound in the rollups (RBI_REL_20261025) ---------------------------------------

def _observed_instant(column):
    """The column's value on rows whose instants were observed (consumed / not_consumed), else NULL.

    Used inside min/max so first/last-event columns never take a booking's future date, an
    'unknown' row's unobserved date, or an unallocated £ row's activity date.
    """
    return F.when(F.col("consumption_status").isin(*R.OBSERVED_STATUSES), F.col(column))


def _within_observation(fact):
    """Drop rows dated beyond observation: 'scheduled' bookings and rows with evidence event_after_observation.

    Those rows carry no quantity and no £, so person-month and service-day totals still agree
    with the fact; they would only add future-dated rows of zeros.
    """
    beyond = ((F.col("consumption_status") == R.SCHEDULED_STATUS)
              | (F.col("consumption_evidence") == R.AFTER_OBSERVATION_EVIDENCE))
    return fact.where(~F.coalesce(beyond, F.lit(False)))


def spell_resource_summary(fact):
    """One row per inpatient spell: resource quantities and £ by basis."""
    s = fact.where(F.col("spell_encounter_id").isNotNull())
    out = s.groupBy("spell_encounter_id").agg(
        F.min("person_id").alias("person_id"),
        F.min(_observed_instant("event_start")).alias("spell_first_event"),  # RBI_REL_20261025
        F.max(_observed_instant("event_end")).alias("spell_last_event"),
        _q("ward_bed_day").alias("bed_days"),
        exact_sum(F.when((F.col("stream") == "critical_care_day") & (F.col("stratum_l1") == "level2"), F.col("quantity"))).alias("cc_level2_days"),
        exact_sum(F.when((F.col("stream") == "critical_care_day") & (F.col("stratum_l1") == "level3"), F.col("quantity"))).alias("cc_level3_days"),
        _q("neonatal_care_day").alias("neonatal_care_days"),
        _q("theatre_case").alias("theatre_minutes"),
        _n("theatre_case").alias("theatre_cases"),
        _n("theatre_case", "not_consumed").alias("cancelled_theatre_cases"),
        _q("pathology_tests").alias("pathology_tests_performed"),
        _n("imaging_exam").alias("imaging_exams"),
        _n("medication_admin").alias("medication_admins"),
        _n("transfusion").alias("transfusion_units"),
        exact_sum("cost_total").alias("cost_total"),
        exact_sum("cost_overhead").alias("cost_overhead"),
        exact_sum(F.when(F.col("cost_basis").isin(*ACTUAL_BASES), F.col("cost_total"))).alias("cost_actual"),
        exact_sum(F.when(F.col("cost_basis") == "modelled_bottom_up", F.col("cost_total"))).alias("cost_modelled"),
        exact_sum(F.when(F.col("stream") == "plics_unallocated", _all_cost())).alias("cost_unallocated"),
    )
    return out.withColumn("financial_year", financial_year(F.to_date(london(F.col("spell_first_event")))))


def _domain_costs():
    """One £ column per domain (cost_total + cost_overhead)."""
    return [exact_sum(F.when(F.col("domain") == d, _all_cost())).alias(f"cost_{d}") for d in DOMAINS]


def journey_resource_summary(fact):
    """One row per spine journey: cross-setting resource use and £ (spec §3)."""
    j = fact.where(F.col("journey_keys").isNotNull()).withColumn("journey_key", F.explode("journey_keys"))
    return j.groupBy("journey_key").agg(
        F.min("person_id").alias("person_id"),
        F.min(_observed_instant("event_start")).alias("first_event"),  # RBI_REL_20261025
        F.max(_observed_instant("event_start")).alias("last_event"),
        F.countDistinct("encounter_id").alias("n_encounters"),
        _q("ward_bed_day").alias("bed_days"),
        _q("theatre_case").alias("theatre_minutes"),
        _n("outpatient_appointment").alias("outpatient_attendances"),
        _n("tracking_attendance", extra=F.col("tracking_class") == "ed").alias("ed_attendances"),
        *_domain_costs(),
        exact_sum(_all_cost()).alias("cost_all"),
    )


def person_month_resource(fact):
    """One row per person × London calendar month: utilisation profile and £ by domain."""
    p = (_within_observation(fact).where(F.col("person_id").isNotNull())  # RBI_REL_20261025
         .withColumn("month", F.trunc(F.col("event_date_local"), "month")))
    return p.groupBy("person_id", "month").agg(
        _q("ward_bed_day").alias("bed_days"),
        _q("theatre_case").alias("theatre_minutes"),
        _n("outpatient_appointment").alias("outpatient_attendances"),
        _n("outpatient_appointment", "not_consumed").alias("outpatient_not_attended"),
        _n("tracking_attendance", extra=F.col("tracking_class") == "ed").alias("ed_attendances"),
        _q("pathology_tests").alias("pathology_tests_performed"),
        _n("imaging_exam").alias("imaging_exams"),
        _n("medication_admin").alias("medication_admins"),
        exact_sum(F.when(F.col("stream") == "high_cost_drug", F.col("cost_total"))).alias("hcd_cost"),
        *_domain_costs(),
        exact_sum(_all_cost()).alias("cost_all"),
    )


def service_day_activity(fact):
    """One row per stream × service unit × site × London day: throughput, consumption and £.

    service_unit = ward, theatre, clinic, else the stream's primary stratum (e.g. tracking group).
    """
    s = (_within_observation(fact).where(F.col("stream") != "plics_unallocated")  # RBI_REL_20261025
         .withColumn("service_unit", F.coalesce("nurse_unit_code", "theatre_code", "clinic_code", "stratum_l1", F.lit("unknown")))
         .withColumn("_minutes", F.when(F.col("event_end").isNotNull(),
                                        (F.unix_timestamp("event_end") - F.unix_timestamp("event_start")) / 60.0)))
    return s.groupBy("stream", "service_unit", "site_code", "event_date_local").agg(
        F.count(F.lit(1)).alias("rows"),
        F.count(F.when(F.col("consumption_status") == "consumed", 1)).alias("consumed_rows"),
        F.count(F.when(F.col("consumption_status") == "not_consumed", 1)).alias("not_consumed_rows"),
        F.count(F.when(F.col("consumption_status") == "unknown", 1)).alias("unknown_rows"),
        exact_sum(F.when(F.col("consumption_status") == "consumed", F.col("quantity"))).alias("quantity_consumed"),
        exact_sum("quantity_scheduled").alias("quantity_scheduled"),
        exact_sum(F.when(F.col("consumption_status") == "not_consumed", F.col("quantity_scheduled"))).alias("quantity_scheduled_not_consumed"),
        exact_sum("cost_total").alias("cost_total"),
        exact_sum("cost_overhead").alias("cost_overhead"),
        F.percentile_approx("_minutes", 0.5).cast("double").alias("median_duration_minutes"),
    )


def costed_activity_unit(hrg, fin_map, fact, window_start):
    """Finance-recognisable view: one row per SLAM APC spell or OP attendance with RBI £ (spec §3).

    APC: hosp_prov_spell_num = FIN → encounter; £ = all fact £ on that encounter.
    OP: person + London date → the unique consumed appointment row; £ = that row's £. Dated OP rows are
    bounded like every stream (RBI_REL_20261025): on or after the window start, and not dated after
    their own loaded_at (an attendance dated after its load was not observed). Undated OP rows stay.
    Gold's own hrg encounter_id is always NULL (spec §2) and is dropped so RBI's link is the only one.
    """
    hrg = hrg.drop("encounter_id")
    enc_cost = (fact.where(F.col("encounter_id").isNotNull()).groupBy("encounter_id")
                .agg(exact_sum(_all_cost()).alias("cost_total"),
                     exact_sum(F.when(F.col("cost_basis").isin(*ACTUAL_BASES), F.col("cost_total"))).alias("cost_actual"),
                     exact_sum(F.when(F.col("cost_basis") == "modelled_bottom_up", F.col("cost_total"))).alias("cost_modelled")))
    apc = (hrg.where(F.col("source_feed") == "slam_apc_hrg")
           .join(fin_map.select(F.col("fin").alias("hosp_prov_spell_num"), "encounter_id"), "hosp_prov_spell_num", "left")
           .groupBy("hosp_prov_spell_num")
           .agg(F.min("encounter_id").alias("encounter_id"), F.min("person_id").alias("person_id"),
                F.max("spell_hrg_cd").alias("hrg_cd"), F.max("spell_hrg_desc").alias("hrg_desc"),
                F.min("treatment_function_cd").alias("treatment_function_cd"),
                F.min("admission_datetime").alias("activity_start"), F.max("discharge_datetime").alias("activity_end"),
                F.count(F.lit(1)).alias("episode_count"))
           .join(enc_cost, "encounter_id", "left")
           .withColumns({"setting": F.lit("admitted"),
                         "link_method": F.when(F.col("encounter_id").isNotNull(), F.lit("fin_spell")).otherwise(F.lit("none")),
                         "appointment_key": F.lit(None).cast("string"),
                         "activity_unit_key": event_key(F.lit("apc"), F.col("hosp_prov_spell_num"))}))
    appts = (fact.where((F.col("stream") == "outpatient_appointment") & (F.col("consumption_status") == "consumed"))
             .select("person_id", F.col("event_date_local").alias("_d"), F.col("resource_use_event_key").alias("_k"),
                     "encounter_id", _all_cost().alias("_c"),
                     F.when(F.col("cost_basis").isin(*ACTUAL_BASES), F.col("cost_total")).alias("_a"),
                     F.when(F.col("cost_basis") == "modelled_bottom_up", F.col("cost_total")).alias("_m")))
    # RBI_REL_20261025: dated OP rows must be in the window and not after their own load. Undated rows
    # (no event_datetime) are kept as before: they cannot be out of bounds and never link to a £.
    dated_in_bounds = in_window(F.col("event_datetime"), window_start) & (F.col("event_datetime") <= F.col("loaded_at"))
    op_rows = (hrg.where(F.col("source_feed") == "slam_op_hrg")
               .where(F.col("event_datetime").isNull() | F.coalesce(dated_in_bounds, F.lit(False)))
               .withColumn("_d", F.to_date(london(F.col("event_datetime")))))
    cand = (op_rows.select("patient_event_key", "person_id", "_d").join(appts, ["person_id", "_d"])
            .groupBy("patient_event_key").agg(F.count(F.lit(1)).alias("_n"), F.first("_k").alias("_k"),
                                              F.first("encounter_id").alias("_enc"), F.first("_c").alias("_c"),
                                              F.first("_a").alias("_a"), F.first("_m").alias("_m")))
    unique = F.col("_n") == 1
    op = (op_rows.join(cand, "patient_event_key", "left").select(
        "person_id", F.when(unique, F.col("_enc")).alias("encounter_id"),
        F.coalesce("fce_hrg_cd", "source_code").alias("hrg_cd"), F.coalesce("fce_hrg_desc", "source_display").alias("hrg_desc"),
        "treatment_function_cd", F.col("event_datetime").alias("activity_start"), F.lit(None).cast("timestamp").alias("activity_end"),
        F.lit(1).cast("long").alias("episode_count"),
        F.when(unique, F.col("_c")).alias("cost_total"), F.when(unique, F.col("_a")).alias("cost_actual"),
        F.when(unique, F.col("_m")).alias("cost_modelled"),
        F.lit("outpatient").alias("setting"),
        F.when(unique, F.lit("person_date")).when(F.col("_n") > 1, F.lit("person_date_ambiguous")).otherwise(F.lit("none")).alias("link_method"),
        F.when(unique, F.col("_k")).alias("appointment_key"),
        event_key(F.lit("op"), F.col("patient_event_key")).alias("activity_unit_key")))
    cols = ["activity_unit_key", "setting", "person_id", "encounter_id", "appointment_key", "link_method", "hrg_cd", "hrg_desc",
            "treatment_function_cd", "activity_start", "activity_end", "episode_count", "cost_total", "cost_actual", "cost_modelled"]
    return apc.select(*cols).unionByName(op.select(*cols))


def attribution_summary(fact):
    """Rows per stream × attribution method, so link quality is visible next to the data."""
    return fact.groupBy("stream", "attribution_method").agg(F.count(F.lit(1)).alias("rows"))


from rbi_schema_names import COST_COLUMN_NAMES as _COST_NAMES  # noqa: E402

assert [n for n, _ in COST_COLUMNS] == _COST_NAMES, "rbi_schema_names.COST_COLUMN_NAMES drifted from COST_COLUMNS"
