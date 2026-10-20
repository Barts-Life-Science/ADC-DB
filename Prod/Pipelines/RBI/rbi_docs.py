"""Table and column documentation plus IG tags for every published RBI table.

Pure Python. The governance notebook applies these after each pipeline update (MV comments
only land on first build), and the local test proves every published column is covered.
IG classification follows the estate doctrine: direct identifiers 4/2; person, encounter and
patient-grain dates/keys 3/1; codes, statuses, quantities and £ 1/0; synthetic hashes and
pipeline provenance 0/0. Identifiers are published and tagged, never removed (CLAUDE.md).
"""
from rbi_schema_names import COST_COLUMN_NAMES, STREAM_COLUMN_NAMES

TABLE_COMMENTS = {
    "resource_use_event": "One row per unit of resource consumed (bed-day, theatre case, attendance, test day, exam, administration, HCD line, transfusion unit) from FY2019/20, with encounter/spell/journey keys and one authoritative £ (cost_total + cost_overhead). Built from Journey gold; see spec 2026-10-16 RBI.",
    "unit_cost_reference": "Barts unit costs: PLICS £ for a cost-code family divided by RBI-observed consumed quantity on the same score-1 linked entity (encounter, or appointment), per stream × stratum level × base FY.",
    "unit_cost_backtest": "Report-only accuracy of modelled pricing: base-FY unit costs applied to a later FY's PLICS activities, MAPE per stream.",
    "costed_activity_unit": "SLAM APC spells (FIN-linked) and OP attendances (person + date) with their RBI £; the finance-recognisable view.",
    "spell_resource_summary": "One row per inpatient spell: bed-days, critical-care days, theatre minutes, tests, exams, administrations and £ by basis.",
    "journey_resource_summary": "One row per spine journey: cross-setting resource use and £ by domain.",
    "person_month_resource": "One row per person × London calendar month: utilisation profile and £ by domain.",
    "service_day_activity": "One row per stream × service unit × site × London day: throughput, consumption status counts, quantity, £ and median duration.",
    "attribution_summary": "Rows per stream × attribution method, showing how each row found (or failed to find) its encounter.",
}

C = {
    # identity and keys
    "resource_use_event_key": "SHA-256 row key over the source key and the row's grain (e.g. stay + day).",
    "activity_unit_key": "SHA-256 key of the SLAM activity unit (setting + spell number or HRG row key).",
    "journey_key": "Spine journey key (joins spine_journey).",
    "journey_keys": "Sorted spine journey keys the row's encounter belongs to.",
    "person_id": "Millennium PERSON_ID.",
    "encounter_id": "Millennium ENCNTR_ID: native, or attributed by a unique observed attendance interval (see attribution_method).",
    "spell_encounter_id": "encounter_id when that encounter is a bed-holding type (inpatient, day case, regular day/night, maternity, newborn).",
    "appointment_key": "resource_use_event_key of the outpatient appointment row a SLAM OP attendance links to.",
    "source_key": "Key of the gold source row (or collapsed group) this row came from.",
    "source_table": "Gold table the row came from.",
    # labels
    "domain": "Resource domain: beds, theatres, outpatients_ed, diagnostics_drugs, costing.",
    "stream": "Resource stream (one per gold source).",
    "unit_type": "Unit of quantity: bed_day, cc_level_day, theatre_minute, attendance, test, exam, administration, unit, ledger_line, gbp.",
    "setting": "Overhead setting: admitted, outpatient, emergency.",
    # quantities and consumption
    "quantity": "Consumed quantity in unit_type (NULL when not consumed or unmeasurable).",
    "quantity_scheduled": "Theatre: scheduled minutes for the case (all statuses), for scheduled vs used vs cancelled comparisons.",
    "quantity_scheduled_not_consumed": "Σ scheduled minutes of not-consumed rows (e.g. cancelled theatre minutes).",
    "quantity_requested": "Pathology: tests requested in the group.",
    "quantity_performed_result": "Pathology: requested tests with a matching gold result.",
    "quantity_performed_reported": "Pathology: requested tests without a matching result whose accession was reported.",
    "quantity_unknown": "Pathology: requested tests with no evidence of being performed.",
    "consumption_status": "consumed, not_consumed, unknown, or not_applicable (unallocated £ rows). Only consumed rows are priced.",
    "consumption_evidence": "Rule or source status that set consumption_status.",
    # time
    "event_start": "Start instant (UTC).",
    "event_end": "End instant (UTC) where known.",
    "event_date_local": "Europe/London calendar date of the row.",
    "financial_year": "NHS financial year (April start) of event_date_local.",
    "month": "Europe/London calendar month (first day).",
    "first_event": "Earliest event_start in the group.",
    "last_event": "Latest event_start in the group.",
    "spell_first_event": "Earliest event_start in the spell.",
    "spell_last_event": "Latest event_end in the spell.",
    "activity_start": "SLAM activity start (admission or attendance).",
    "activity_end": "SLAM activity end (discharge) where known.",
    # attribution
    "attribution_method": "native, interval_unique, interval_ambiguous, none, not_applicable, activity_link.",
    "attribution_score": "1.0 for deterministic or unique links; NULL otherwise.",
    "candidate_count": "Number of top-tier candidate encounters considered.",
    "link_method": "How a SLAM/PLICS activity linked: fin_spell, fin_attendance, person_date, person_date_ambiguous, native, none.",
    # service attribution
    "nurse_unit_code": "Ward / nurse unit code.",
    "site_code": "Site, facility or trust code as carried by the source.",
    "service_code": "Service or specialty code as carried by the source.",
    "service_unit": "Ward, theatre, clinic or the stream's primary stratum.",
    "clinic_code": "Clinic resource code.",
    "theatre_code": "Operating location.",
    "tracking_class": "Tracking-board class from rule table TG01–TG07: ed, sdec, labour, theatre, endoscopy, cath_lab, inpatient_board, unclassified.",
    "stratum_l1": "Most specific calibration stratum value.",
    "stratum_l2": "Fallback calibration stratum value.",
    "priced_by_other_stream": "True when another stream carries this resource's £ (e.g. CC days within ward bed-days).",
    "drug_concept_id": "OMOP standard drug concept.",
    "source_amount": "£ carried by the source itself (HCD ledger cost).",
    "rule_version": "rbi_rules version the row was built under.",
    # cost
    "cost_total": "Authoritative direct £ for this row (one source by precedence). Additive total = cost_total + cost_overhead.",
    "cost_overhead": "Support-cost £: actual SUP share for PLICS rows, modelled £ × setting ratio for modelled rows.",
    "cost_basis": "drug_expenditure_actual, actual_plics, actual_ncc_allocated, covered_by_hcd_ledger, modelled_bottom_up, none.",
    "cost_reason": "Why cost_basis is none: not_consumed, consumption_unknown, priced_by_other_stream, unpriced_tracking_class, zero_quantity, hcd_drug_without_ledger, no_calibration.",
    "cost_modelled_shadow": "Modelled £ kept for comparison when an actual £ won.",
    "cost_superseded": "PLICS £ covered by a higher-precedence source (HCD ledger); not in cost_total.",
    "cost_stratum_level": "Calibration level that priced a modelled row: l1, l2, all.",
    "cost_base_fy": "Financial year of the unit costs used.",
    "uplift_factor": "Cost-inflation index(row FY) ÷ index(base FY).",
    "cost_method_version": "Costing code version.",
    "cost_all": "cost_total + cost_overhead over the group.",
    "cost_actual": "Σ cost_total with an actual basis.",
    "cost_modelled": "Σ cost_total with modelled basis.",
    "cost_unallocated": "£ that reached no atomic row (plics_unallocated rows).",
    "hcd_cost": "High-cost-drug ledger £.",
    # calibration / backtest
    "stratum_level": "l1, l2 or all.",
    "stratum_value": "Stratum value at that level.",
    "base_fy": "Calibration financial year.",
    "test_fy": "Financial year priced in the backtest.",
    "pounds": "Σ allocated PLICS £ in the stratum.",
    "unit_cost": "pounds ÷ quantity.",
    "n_entities": "Distinct linked entities (encounters, or appointments for outpatients) contributing; the calibration floor applies to this.",
    "activity_count_implied_unit_cost": "Diagnostic: £ ÷ PLICS activity_count for the stream (units vary by code; not used for pricing).",
    "mape": "Mean absolute percentage error, modelled vs actual, per linked entity.",
    "total_actual": "Σ actual PLICS £ of the tested activities.",
    "total_modelled": "Σ modelled £ of the tested activities.",
    # summaries
    "rows": "Row count.",
    "consumed_rows": "Rows with consumption_status = consumed.",
    "not_consumed_rows": "Rows with consumption_status = not_consumed.",
    "unknown_rows": "Rows with consumption_status = unknown.",
    "quantity_consumed": "Σ quantity of consumed rows.",
    "median_duration_minutes": "Median event_end − event_start in minutes (rows with an end).",
    "n_encounters": "Distinct encounters.",
    "bed_days": "Σ occupied bed-days.",
    "cc_level2_days": "Critical-care level 2 days.",
    "cc_level3_days": "Critical-care level 3 days.",
    "neonatal_care_days": "Neonatal care days.",
    "theatre_minutes": "Σ actual theatre minutes.",
    "theatre_cases": "Performed theatre cases.",
    "cancelled_theatre_cases": "Cancelled theatre cases.",
    "pathology_tests_performed": "Σ performed pathology tests.",
    "imaging_exams": "Consumed imaging exams.",
    "medication_admins": "Consumed medication administrations.",
    "transfusion_units": "Transfused units.",
    "outpatient_attendances": "Attended outpatient appointments.",
    "outpatient_not_attended": "DNA / cancelled outpatient appointments.",
    "ed_attendances": "ED tracking attendances.",
    "episode_count": "SLAM episodes in the spell (1 for OP).",
    "hrg_cd": "Spell HRG (APC) or attendance HRG (OP).",
    "hrg_desc": "HRG description.",
    "treatment_function_cd": "Treatment function code.",
}
for _d in ("beds", "theatres", "outpatients_ed", "diagnostics_drugs", "costing"):
    C[f"cost_{_d}"] = f"£ (cost_total + cost_overhead) in the {_d} domain."
COLUMN_COMMENTS = C

_FACT = STREAM_COLUMN_NAMES + COST_COLUMN_NAMES + ["spell_encounter_id", "journey_keys"]
PUBLISHED_COLUMNS = {
    "resource_use_event": _FACT,
    "unit_cost_reference": ["stream", "base_fy", "stratum_value", "pounds", "quantity", "n_entities", "stratum_level",
                            "activity_count_implied_unit_cost", "unit_cost", "cost_method_version"],
    "unit_cost_backtest": ["stream", "n_entities", "mape", "total_actual", "total_modelled", "base_fy", "test_fy", "cost_method_version"],
    "costed_activity_unit": ["activity_unit_key", "setting", "person_id", "encounter_id", "appointment_key", "link_method", "hrg_cd",
                             "hrg_desc", "treatment_function_cd", "activity_start", "activity_end", "episode_count", "cost_total",
                             "cost_actual", "cost_modelled"],
    "spell_resource_summary": ["spell_encounter_id", "person_id", "spell_first_event", "spell_last_event", "bed_days", "cc_level2_days",
                               "cc_level3_days", "neonatal_care_days", "theatre_minutes", "theatre_cases", "cancelled_theatre_cases",
                               "pathology_tests_performed", "imaging_exams", "medication_admins", "transfusion_units", "cost_total",
                               "cost_overhead", "cost_actual", "cost_modelled", "cost_unallocated", "financial_year"],
    "journey_resource_summary": ["journey_key", "person_id", "first_event", "last_event", "n_encounters", "bed_days", "theatre_minutes",
                                 "outpatient_attendances", "ed_attendances", "cost_beds", "cost_theatres", "cost_outpatients_ed",
                                 "cost_diagnostics_drugs", "cost_costing", "cost_all"],
    "person_month_resource": ["person_id", "month", "bed_days", "theatre_minutes", "outpatient_attendances", "outpatient_not_attended",
                              "ed_attendances", "pathology_tests_performed", "imaging_exams", "medication_admins", "hcd_cost",
                              "cost_beds", "cost_theatres", "cost_outpatients_ed", "cost_diagnostics_drugs", "cost_costing", "cost_all"],
    "service_day_activity": ["stream", "service_unit", "site_code", "event_date_local", "rows", "consumed_rows", "not_consumed_rows",
                             "unknown_rows", "quantity_consumed", "quantity_scheduled", "quantity_scheduled_not_consumed",
                             "cost_total", "cost_overhead", "median_duration_minutes"],
    "attribution_summary": ["stream", "attribution_method", "rows"],
}

_PATIENT = {"person_id", "encounter_id", "spell_encounter_id", "appointment_key", "journey_key", "journey_keys",
            "event_start", "event_end", "event_date_local", "month", "first_event", "last_event", "spell_first_event",
            "spell_last_event", "activity_start", "activity_end", "source_key"}
_SYNTHETIC = {"resource_use_event_key", "activity_unit_key", "rule_version", "cost_method_version", "source_table"}


def ig_tags(column):
    """(ig_risk, ig_severity) for a published column name."""
    if column in _PATIENT:
        return ("3", "1")
    if column in _SYNTHETIC:
        return ("0", "0")
    return ("1", "0")
TABLE_COMMENTS["rbi_sample_person"] = "Dev only: the hashed 2% person cohort every stream is restricted to. Never present in prod."
TABLE_COMMENTS["cost_allocation_ledger"] = ("Every actual £ RBI received, one row per PLICS/NCC activity × component × receiving fact row "
                                            "(or unallocated / superseded). Σ amount per activity × component = PLICS duplicate-weighted lines.")
TABLE_COMMENTS["plics_activity_link"] = "Each PLICS/NCC costed activity with the gold encounter (or outpatient appointment) its £ belongs to."
PUBLISHED_COLUMNS["rbi_sample_person"] = ["person_id"]
PUBLISHED_COLUMNS["cost_allocation_ledger"] = ["extract_cd", "activity_record_id", "component", "resource_use_event_key",
                                               "amount_kind", "amount", "cost_basis"]
PUBLISHED_COLUMNS["plics_activity_link"] = ["extract_cd", "activity_record_id", "feed_type", "person_id", "encounter_id",
                                            "appointment_key", "link_method", "link_score", "candidate_count"]
COLUMN_COMMENTS.update({
    "extract_cd": "PLICS/NCC extract (e.g. PE2122NCC, NCC2324).",
    "activity_record_id": "PLICS/NCC activity record id (unique only together with extract_cd).",
    "feed_type": "PLICS feed type (APC, OP, EC, …).",
    "component": "Cost component: ward_care, theatre, ed, imaging, pathology, pharmacy_hcd, pharmacy_other, blood, outpatient, overhead, unallocated, episode_total.",
    "amount_kind": "direct, overhead, unallocated (no receiving row) or superseded (covered by a higher-precedence source).",
    "amount": "£ in this ledger row.",
    "link_score": "1.0 for deterministic or unique links; NULL when unlinked.",
})
_PATIENT.add("activity_record_id")
