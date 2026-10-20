"""Resource-stream builders for RBI (spec §3 and §10).

Each builder takes gold DataFrames (already restricted to the dev sample when sampling is on)
and returns rows in the STREAM_COLUMNS shape. Builders never drop an eligible source key
silently: rows that cannot be measured are kept with consumption_status 'unknown' and the
reason in consumption_evidence.
"""
from pyspark.sql import functions as F

import rbi_rules as R
from rbi_schema import (LONDON, event_key, exact_sum, financial_year, first_match_class, in_window, london,  # INCR_FIX_20261018
                        status_from_map, to_stream, try_bigint)


def _finish(df, *, domain, stream, unit_type, source_table, date_from="event_start"):
    """Stamp the labels every stream row carries, derive the London date and FY, and project.

    `date_from` names the UTC column to derive event_date_local from; pass None when the
    builder has already set event_date_local itself (bed-days).
    """
    if date_from is not None:
        df = df.withColumn("event_date_local", F.to_date(london(F.col(date_from))))
    return to_stream(df.withColumns({
        "domain": F.lit(domain),
        "stream": F.lit(stream),
        "unit_type": F.lit(unit_type),
        "financial_year": financial_year(F.col("event_date_local")),
        "source_table": F.lit(source_table),
        "rule_version": F.lit(R.RULE_VERSION),
    }))


# ---- Ward bed-days ---------------------------------------------------------------------

def ward_bed_days(stays, encounters, window_start):
    """One row per occupied London calendar day of an eligible bed stay (spec §3, §10.8).

    Eligible: the stay's encounter type is a bed-holding type. Measured: only observed
    occupancy. A stay with an observed start but no observed end is capped at the encounter's
    observed discharge. Otherwise it becomes one 'unknown' row with NULL quantity. Ends are
    never imputed and unknown stays are never exploded into days.
    Inputs: stays = gold spine_location_stay incl. occupancy_* columns and loaded_at;
            encounters = gold spine_encounter (encounter_id, type_display, discharge_datetime).
    """
    enc = encounters.select("encounter_id", F.col("type_display").alias("_enc_type"),
                            F.col("discharge_datetime").alias("_discharge"))
    s = stays.join(enc, "encounter_id", "inner").where(F.col("_enc_type").isin(*R.BED_ENCOUNTER_TYPES))

    # An instant later than the row's own loaded_at cannot have been observed (e.g. the 2100-12-31
    # "still open" sentinel that silver keeps); such ends are treated as unobserved, never exploded.
    loaded = F.col("loaded_at")
    start_ok = F.coalesce(F.col("occupancy_start_basis").isin(*R.OCCUPANCY_START_OBSERVED)
                          & F.col("occupancy_start").isNotNull() & (F.col("occupancy_start") <= loaded), F.lit(False))
    end_ok = F.coalesce(F.col("occupancy_end_basis").isin(*R.OCCUPANCY_END_OBSERVED)
                        & F.col("occupancy_end").isNotNull() & (F.col("occupancy_end") <= loaded), F.lit(False))
    cap_ok = F.coalesce((F.col("_discharge") >= F.col("occupancy_start")) & (F.col("_discharge") <= loaded), F.lit(False))
    s = (s.withColumn("_end_rule", F.when(~start_ok, F.lit("no_observed_start"))
                                    .when(end_ok, F.lit("observed"))
                                    .when(cap_ok, F.lit("encounter_discharge_cap"))
                                    .otherwise(F.lit("no_observed_end")))
          .withColumn("_end", F.when(F.col("_end_rule") == "observed", F.col("occupancy_end"))
                               .when(F.col("_end_rule") == "encounter_discharge_cap", F.col("_discharge")))
          .withColumn("_anchor", F.coalesce(F.col("_end"), F.col("occupancy_start"), F.col("period_start")))
          .where(in_window(F.col("_anchor"), window_start))
          .withColumns({
              "person_id": F.col("person_id").cast("bigint"),
              "site_code": F.col("facility_code"),
              "stratum_l1": F.col("nurse_unit_code"),
              "stratum_l2": F.col("service_code"),
              "priced_by_other_stream": F.lit(False),
              "source_key": F.col("location_stay_key"),
              "consumption_evidence": F.col("_end_rule"),
          }))

    measured = s.where(F.col("_end_rule").isin("observed", "encounter_discharge_cap"))
    # London calendar days decide WHERE the stay is split; elapsed time is measured between UTC
    # instants, so a 24-hour stay across a clock change is exactly 1.0 day, and a stay inside the
    # repeated autumn hour keeps its real length (review fix 4). On the clock-change day itself a
    # full day of occupancy is 23/24 or 25/24 of a bed-day, which is the true elapsed time.
    days = (measured
            .withColumn("event_date_local", F.explode(F.sequence(F.to_date(london(F.col("occupancy_start"))),
                                                                 F.to_date(london(F.col("_end"))))))
            .withColumn("_day_start_utc", F.to_utc_timestamp(F.col("event_date_local").cast("timestamp"), LONDON))
            .withColumn("_day_end_utc", F.to_utc_timestamp(F.date_add(F.col("event_date_local"), 1).cast("timestamp"), LONDON))
            .withColumn("_ds", F.greatest(F.col("occupancy_start"), F.col("_day_start_utc")))
            .withColumn("_de", F.least(F.col("_end"), F.col("_day_end_utc")))
            .withColumn("quantity", (F.unix_timestamp("_de") - F.unix_timestamp("_ds")) / F.lit(86400.0))
            # Keep zero-length stays as one zero row so their key is still emitted.
            .where((F.col("quantity") > 0) | (F.col("occupancy_start") == F.col("_end")))
            .where(F.col("event_date_local") >= F.to_date(F.lit(window_start)))
            .withColumns({
                "event_start": F.col("_ds"),
                "event_end": F.col("_de"),
                "consumption_status": F.lit("consumed"),
                "resource_use_event_key": event_key(F.col("location_stay_key"), F.col("event_date_local")),
            }))
    unknown = (s.where(~F.col("_end_rule").isin("observed", "encounter_discharge_cap"))
               .withColumns({
                   "event_start": F.coalesce(F.col("occupancy_start"), F.col("period_start")),
                   "quantity": F.lit(None).cast("double"),
                   "consumption_status": F.lit("unknown"),
                   "resource_use_event_key": event_key(F.col("location_stay_key"), F.lit("unbounded")),
               })
               .withColumn("event_date_local", F.to_date(london(F.col("event_start")))))
    both = to_stream(days).unionByName(to_stream(unknown))
    return _finish(both, domain="beds", stream="ward_bed_day", unit_type="bed_day",
                   source_table="gold.spine_location_stay", date_from=None)


# ---- Critical care and neonatal care (quantity-only in v1, spec §10.6) -----------------

def critical_care_days(periods, window_start):
    """One row per critical-care period × level (L2/L3) with days > 0.

    Quantity-only: these days are priced as ward bed-days on the CC unit, so the rows carry
    priced_by_other_stream = true.
    """
    levels = F.array(
        F.struct(F.lit("level2").alias("level"), F.col("level2_days").cast("double").alias("days")),
        F.struct(F.lit("level3").alias("level"), F.col("level3_days").cast("double").alias("days")),
    )
    x = (periods.where(in_window(F.col("event_datetime"), window_start))
         .withColumn("_lv", F.explode(levels))
         .where(F.col("_lv.days") > 0)
         .withColumns({
             "resource_use_event_key": event_key(F.col("patient_event_key"), F.col("_lv.level")),
             "quantity": F.col("_lv.days"),
             "consumption_status": F.lit("consumed"),
             "consumption_evidence": F.lit("critical_care_period"),
             "event_start": F.col("event_datetime"),
             "event_end": F.col("period_end_datetime"),
             "nurse_unit_code": F.col("unit_id").cast("string"),
             "service_code": F.col("unit_function"),
             "stratum_l1": F.col("_lv.level"),
             "stratum_l2": F.col("unit_function"),
             "priced_by_other_stream": F.lit(True),
             "source_key": F.col("patient_event_key"),
         }))
    return _finish(x, domain="beds", stream="critical_care_day", unit_type="cc_level_day",
                   source_table="gold.clinical_critical_care_period")


def neonatal_care_days(days, window_start):
    """One row per neonatal care day at its care-level code; quantity-only in v1 (spec §10.6)."""
    start = F.coalesce(F.col("event_datetime"), F.col("activity_date"))
    x = (days.withColumn("event_start", start)
         .where(in_window(F.col("event_start"), window_start))
         .withColumns({
             "resource_use_event_key": event_key(F.col("patient_event_key")),
             "quantity": F.lit(1.0),
             "consumption_status": F.lit("consumed"),
             "consumption_evidence": F.lit("neonatal_care_day"),
             "nurse_unit_code": F.col("ward_location"),
             "service_code": F.col("unit_function"),
             "stratum_l1": F.col("source_code"),
             "stratum_l2": F.col("unit_function"),
             "priced_by_other_stream": F.lit(True),
             "source_key": F.col("patient_event_key"),
         }))
    return _finish(x, domain="beds", stream="neonatal_care_day", unit_type="neonatal_care_day",
                   source_table="gold.clinical_neonatal_care_day")


# ---- Theatres ----------------------------------------------------------------------------

def theatre_cases(cases, window_start):
    """One row per theatre case; quantity = actual surgery minutes (spec §3, §10.9).

    quantity_scheduled carries the scheduled minutes on every case, so scheduled vs used vs
    cancelled minutes can be compared (review fix 10). Cancelled cases are not_consumed with a
    NULL quantity and are never priced. A PERFORMED case without a duration is 'unknown'
    (evidence performed_no_duration), because it cannot be measured.
    """
    status = status_from_map(F.col("case_status"), R.THEATRE_CONSUMPTION)
    minutes = F.col("surgery_duration_minutes").cast("double")
    no_duration = (status == "consumed") & ~F.coalesce(minutes > 0, F.lit(False))
    x = (cases.withColumn("event_start", F.coalesce("surgery_start", "scheduled_start", "checkin_datetime"))
         .where(in_window(F.col("event_start"), window_start))
         .withColumn("consumption_status", F.when(no_duration, F.lit("unknown")).otherwise(status))
         .withColumns({
             "resource_use_event_key": event_key(F.col("theatre_case_key")),
             "consumption_evidence": F.when(no_duration, F.lit("performed_no_duration"))
                                      .otherwise(F.concat(F.lit("case_status:"), F.coalesce(F.col("case_status"), F.lit("<null>")))),
             "quantity": F.when(F.col("consumption_status") == "consumed", minutes),
             "quantity_scheduled": F.col("scheduled_duration_minutes").cast("double"),
             "event_end": F.when(minutes > 0, F.timestamp_seconds(F.unix_timestamp("event_start") + minutes * 60)),
             "theatre_code": F.col("operating_location_display"),
             "site_code": F.col("institution_display"),
             "service_code": F.col("surgical_specialty_id").cast("string"),
             "stratum_l1": F.col("surgical_specialty_id").cast("string"),
             "stratum_l2": F.col("department_display"),
             "priced_by_other_stream": F.lit(False),
             "source_key": F.col("theatre_case_key"),
         }))
    return _finish(x, domain="theatres", stream="theatre_case", unit_type="theatre_minute",
                   source_table="gold.reference_theatre_case")


# ---- Outpatients and ED ----------------------------------------------------------------

def outpatient_appointments(appointments, window_start):
    """One row per appointment: attended = consumed, DNA/cancelled = not_consumed, else unknown."""
    x = (appointments.where(in_window(F.col("event_datetime"), window_start))
         .withColumns({
             "resource_use_event_key": event_key(F.col("patient_event_key")),
             "quantity": F.lit(1.0),
             "consumption_status": status_from_map(F.col("status_meaning"), R.APPOINTMENT_CONSUMPTION),
             "consumption_evidence": F.concat(F.lit("status:"), F.coalesce(F.col("status_meaning"), F.lit("<null>"))),
             "event_start": F.col("event_datetime"),
             "event_end": F.col("event_end_datetime"),
             "clinic_code": F.col("clinic_resource_code"),
             "service_code": F.col("service_resource_code"),
             "stratum_l1": F.concat_ws("|", F.col("service_resource_code"), F.col("appointment_type_code")),
             "stratum_l2": F.col("service_resource_code"),
             "priced_by_other_stream": F.lit(False),
             "source_key": F.col("patient_event_key"),
         }))
    return _finish(x, domain="outpatients_ed", stream="outpatient_appointment", unit_type="attendance",
                   source_table="gold.clinical_appointment")


def tracking_attendances(attendances, window_start):
    """One row per tracking-board attendance, classified by tracking group (spec §3).

    Only ed and sdec are priced from this stream. Theatre-board rows are priced by
    theatre_case (priced_by_other_stream). Other classes are throughput evidence only.
    person_id / encounter_id are STRING in this gold table, so they are try_cast to BIGINT.
    """
    label, rule_id = first_match_class(F.col("tracking_group_display"), R.TRACKING_GROUP_RULES)
    x = (attendances.where(in_window(F.col("checkin_dt_tm"), window_start))
         .withColumns({
             "person_id": try_bigint("person_id"),
             "encounter_id": try_bigint("encounter_id"),
             "tracking_class": label,
             "resource_use_event_key": event_key(F.col("tracking_attendance_key")),
             "quantity": F.lit(1.0),
             "consumption_status": F.lit("consumed"),
             "consumption_evidence": F.concat(F.lit("tracking_rule:"), F.coalesce(rule_id, F.lit("none"))),
             "event_start": F.col("checkin_dt_tm"),
             "event_end": F.col("checkout_dt_tm_clean"),
             "site_code": F.col("trust"),
             "stratum_l1": F.col("tracking_group_display"),
             "stratum_l2": label,
             "priced_by_other_stream": label.isin(*R.PRICED_BY_OTHER_TRACKING_CLASSES),
             "source_key": F.col("tracking_attendance_key"),
         }))
    return _finish(x, domain="outpatients_ed", stream="tracking_attendance", unit_type="attendance",
                   source_table="gold.clinical_tracking_attendance")


# ---- Pathology -----------------------------------------------------------------------------

def pathology_result_evidence(results):
    """Distinct (specimen_key, test code) pairs that have at least one gold result row.

    Kept as its own MV so the 1.56B-row result scan is incremental and done once.
    """
    return (results.where(F.col("specimen_key").isNotNull() & F.col("test_source_code").isNotNull())
            .select(F.col("specimen_key").alias("_ev_specimen_key"),
                    F.col("test_source_code").alias("_ev_test_code"))
            .distinct())


def pathology_tests(orders, specimens, result_evidence, window_start):
    """Requested tests collapsed to person × encounter × London day × discipline (spec §3, §10.7).

    Per requested test, the evidence that it was performed:
    - result_present: a gold result exists for the same specimen and test code;
    - accession_reported: no matching result, but the accession has a report time;
    - otherwise unknown.
    `quantity` = performed tests (both kinds). Pass result_evidence=None for accession-only
    evidence (profiling decision, Task 3).
    """
    # Gold clinical_specimen has no specimen_key column: its patient_event_key is the same
    # stable_id("specimen:accession", …) value as the order's specimen_key (verified 22,791/22,791).
    sp = specimens.select(F.col("patient_event_key").alias("specimen_key"), F.col("discipline").alias("_discipline"),
                          F.col("report_datetime").alias("_reported"))
    o = orders.where(in_window(F.col("event_datetime"), window_start)).join(sp, "specimen_key", "left")
    if result_evidence is not None:
        hit = result_evidence.withColumn("_has_result", F.lit(True))
        o = (o.join(hit, (o["specimen_key"] == hit["_ev_specimen_key"]) & (o["source_code"] == hit["_ev_test_code"]), "left")
             .drop("_ev_specimen_key", "_ev_test_code"))
    else:
        o = o.withColumn("_has_result", F.lit(False))
    has_result = F.coalesce(F.col("_has_result"), F.lit(False))
    reported = ~has_result & F.col("_reported").isNotNull()
    o = o.withColumns({
        "_perf_result": has_result.cast("double"),
        "_perf_reported": reported.cast("double"),
        "_unknown": (~has_result & ~reported).cast("double"),
        "event_date_local": F.to_date(london(F.col("event_datetime"))),
        "_discipline": F.coalesce(F.col("_discipline"), F.lit("unknown")),
    })
    g = (o.groupBy(F.col("person_id").cast("bigint").alias("person_id"), "encounter_id", "event_date_local", "_discipline")
         .agg(F.count(F.lit(1)).cast("double").alias("quantity_requested"),
              exact_sum("_perf_result").alias("quantity_performed_result"),
              exact_sum("_perf_reported").alias("quantity_performed_reported"),
              exact_sum("_unknown").alias("quantity_unknown"),
              F.min("event_datetime").alias("event_start"),
              F.max("event_datetime").alias("event_end")))
    key = event_key(F.col("person_id"), F.col("encounter_id"), F.col("event_date_local"), F.col("_discipline"))
    x = g.withColumns({
        "resource_use_event_key": key,
        "quantity": F.col("quantity_performed_result") + F.col("quantity_performed_reported"),
        "consumption_status": F.when(F.col("quantity_performed_result") + F.col("quantity_performed_reported") > 0, F.lit("consumed"))
                               .otherwise(F.lit("unknown")),
        "consumption_evidence": F.when((F.col("quantity_performed_result") > 0) & (F.col("quantity_performed_reported") > 0), F.lit("mixed"))
                                 .when(F.col("quantity_performed_result") > 0, F.lit("result_present"))
                                 .when(F.col("quantity_performed_reported") > 0, F.lit("accession_reported"))
                                 .otherwise(F.lit("no_evidence")),
        "stratum_l1": F.col("_discipline"),
        "stratum_l2": F.lit("pathology"),
        "priced_by_other_stream": F.lit(False),
        "source_key": key,
    })
    return _finish(x, domain="diagnostics_drugs", stream="pathology_tests", unit_type="test",
                   source_table="gold.clinical_pathology_order", date_from=None)


# ---- Imaging, medication, high-cost drugs, transfusion ----------------------------------

def imaging_exams(exams, window_start):
    """One row per imaging exam; consumed when a performed time exists, otherwise unknown."""
    performed = F.col("performed_datetime").isNotNull()
    x = (exams.withColumn("event_start", F.coalesce("performed_datetime", "event_datetime"))
         .where(in_window(F.col("event_start"), window_start))
         .withColumns({
             "resource_use_event_key": event_key(F.col("patient_event_key")),
             "quantity": F.lit(1.0),
             "consumption_status": F.when(performed, F.lit("consumed")).otherwise(F.lit("unknown")),
             "consumption_evidence": F.when(performed, F.lit("performed_datetime_present")).otherwise(F.lit("no_performed_datetime")),
             "stratum_l1": F.coalesce(F.col("modality_code"), F.lit("unknown")),
             "stratum_l2": F.lit("imaging"),
             "priced_by_other_stream": F.lit(False),
             "source_key": F.col("patient_event_key"),
         }))
    return _finish(x, domain="diagnostics_drugs", stream="imaging_exam", unit_type="exam",
                   source_table="gold.clinical_imaging_exam")


def medication_admins(admins, window_start):
    """One row per medication administration; status map decides consumption (spec §10.9)."""
    x = (admins.withColumn("event_start", F.coalesce("performed_datetime", "event_datetime"))
         .where(in_window(F.col("event_start"), window_start))
         .withColumns({
             "resource_use_event_key": event_key(F.col("patient_event_key")),
             "quantity": F.lit(1.0),
             "consumption_status": status_from_map(F.col("administration_status_display"), R.MEDICATION_ADMIN_CONSUMPTION),
             "consumption_evidence": F.concat(F.lit("status:"), F.coalesce(F.col("administration_status_display"), F.lit("<null>"))),
             "drug_concept_id": try_bigint("medication_omop_concept_id"),
             "stratum_l1": F.coalesce(F.col("route_display"), F.lit("unknown")),
             "stratum_l2": F.lit("medication"),
             "priced_by_other_stream": F.lit(False),
             "source_key": F.col("patient_event_key"),
         }))
    return _finish(x, domain="diagnostics_drugs", stream="medication_admin", unit_type="administration",
                   source_table="gold.clinical_medication_admin")


def high_cost_drug_lines(lines, window_start):
    """One row per HCD ledger line (exclusion_flag = 1 removed); the ledger £ is carried as source_amount."""
    x = (lines.where(F.coalesce(F.expr("try_cast(exclusion_flag AS INT)"), F.lit(0)) == 0)
         .where(in_window(F.col("event_datetime"), window_start))
         .withColumns({
             "resource_use_event_key": event_key(F.col("patient_event_key")),
             "quantity": F.expr("try_cast(quantity AS DOUBLE)"),
             "consumption_status": F.lit("consumed"),
             "consumption_evidence": F.lit("hcd_ledger"),
             "event_start": F.col("event_datetime"),
             "drug_concept_id": F.col("drug_standard_concept_id").cast("bigint"),
             "source_amount": F.col("cost").cast("double"),
             "stratum_l1": F.coalesce(F.col("hcdr_category_cd"), F.lit("unknown")),
             "stratum_l2": F.lit("hcd"),
             "priced_by_other_stream": F.lit(False),
             "source_key": F.col("patient_event_key"),
         }))
    return _finish(x, domain="diagnostics_drugs", stream="high_cost_drug", unit_type="ledger_line",
                   source_table="gold.clinical_drug_expenditure")


def transfusions(units, window_start):
    """One row per transfused unit; status map decides consumption (spec §10.9)."""
    x = (units.withColumn("event_start", F.coalesce("begin_datetime", "event_datetime"))
         .where(in_window(F.col("event_start"), window_start))
         .withColumns({
             "resource_use_event_key": event_key(F.col("transfusion_key")),
             "quantity": F.lit(1.0),
             "consumption_status": status_from_map(F.col("transfusion_status"), R.TRANSFUSION_CONSUMPTION),
             "consumption_evidence": F.concat(F.lit("status:"), F.coalesce(F.col("transfusion_status"), F.lit("<null>"))),
             "event_end": F.col("end_datetime"),
             "stratum_l1": F.coalesce(F.col("blood_product_group"), F.lit("unknown")),
             "stratum_l2": F.lit("blood"),
             "priced_by_other_stream": F.lit(False),
             "source_key": F.col("transfusion_key"),
         }))
    return _finish(x, domain="diagnostics_drugs", stream="transfusion", unit_type="unit",
                   source_table="gold.clinical_transfusion")
