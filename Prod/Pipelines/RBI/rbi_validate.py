# Databricks notebook source
# INSR_W6_PMS_RBI_V1 (INSR_20261029 W6: PMS Pharma, PMS Device, RBI)
# RBI validation (spec §8): every check writes a row to <target>_meta.validation_result; the notebook fails if any check fails.
# Also writes <target>_meta.stream_coverage. Sampling and bed settings are passed in explicitly. Lakeflow keeps a removed MV
# as an inactive table, so whether rbi_sample_person exists says nothing about the run being validated (review fix 5).
import json
from datetime import datetime, timezone

import rbi_rules as R

for _name, _default in (("target", "8_dev.rbi"), ("gold", "4_prod.gold"), ("window_start", "2019-04-01"), ("sample_mod", "0"),
                        ("enable_ward_bed_day", "false"), ("location_stay_source", "")):
    dbutils.widgets.text(_name, _default)
TARGET, GOLD, W = dbutils.widgets.get("target"), dbutils.widgets.get("gold"), dbutils.widgets.get("window_start")
SAMPLE_MOD = int(dbutils.widgets.get("sample_mod"))
BEDS = dbutils.widgets.get("enable_ward_bed_day") == "true"
STAY_SRC = dbutils.widgets.get("location_stay_source") or f"{GOLD}.spine_location_stay"
SAMPLED = SAMPLE_MOD > 0
assert TARGET.split(".")[0] in ("8_dev", "4_prod")
if SAMPLED:
    assert spark.catalog.tableExists(f"{TARGET}.rbi_sample_person"), "sample_mod > 0 but no rbi_sample_person"
spark.conf.set("spark.sql.session.timeZone", "UTC")
META, FACT, LEDGER, LINK = f"{TARGET}_meta", f"{TARGET}.resource_use_event", f"{TARGET}.cost_allocation_ledger", f"{TARGET}.plics_activity_link"
RUN_TS = datetime.now(timezone.utc).isoformat()
results, cov_rows = [], []


def check(name, passed, detail):
    """Record one check result."""
    results.append((RUN_TS, name, bool(passed), json.dumps(detail, default=str)[:4000]))


def one(sql):
    """First row of a query as a dict."""
    return spark.sql(sql).first().asDict()


def src(table_fq, person_col="person_id"):
    """A source table restricted to the dev sample when (and only when) sample_mod > 0."""
    if not SAMPLED:
        return table_fq
    return (f"(SELECT g.* FROM {table_fq} g LEFT SEMI JOIN {TARGET}.rbi_sample_person s "
            f"ON try_cast(g.{person_col} AS BIGINT) = s.person_id)")


def in_list(values):
    """SQL IN-list of string literals."""
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


def family_component_sql(code):
    """SQL CASE giving the cost component of an activity cost code, mirroring rbi_rules.plics_family."""
    parts = [f"WHEN {code} IN ({in_list(sorted(R.HCD_PHA_CODES))}) THEN 'pharmacy_hcd'"] if R.HCD_PHA_CODES else []
    parts += [f"WHEN substr({code}, 1, 3) = '{p}' THEN '{comp}'" for p, (_, comp) in R.PLICS_FAMILY.items()]
    return "CASE " + " ".join(parts) + " ELSE 'unallocated' END"


ACTS = src(f"{GOLD}.clinical_costed_activity")

# COMMAND ----------

# Source-key coverage for 1:1 streams: every eligible source key in the window is emitted exactly once.
COVERAGE = {
    "theatre_case": ("reference_theatre_case", "theatre_case_key", "coalesce(surgery_start, scheduled_start, checkin_datetime)", "TRUE", 1),
    "outpatient_appointment": ("clinical_appointment", "patient_event_key", "event_datetime", "TRUE", 1),
    "tracking_attendance": ("clinical_tracking_attendance", "tracking_attendance_key", "checkin_dt_tm", "TRUE", 1),
    "imaging_exam": ("clinical_imaging_exam", "patient_event_key", "coalesce(performed_datetime, event_datetime)", "TRUE", 1),
    "medication_admin": ("clinical_medication_admin", "patient_event_key", "coalesce(performed_datetime, event_datetime)", "TRUE", 1),
    "transfusion": ("clinical_transfusion", "transfusion_key", "coalesce(begin_datetime, event_datetime)", "TRUE", 1),
    "high_cost_drug": ("clinical_drug_expenditure", "patient_event_key", "event_datetime", "coalesce(try_cast(exclusion_flag AS INT), 0) = 0", 1),
    "neonatal_care_day": ("clinical_neonatal_care_day", "patient_event_key", "coalesce(event_datetime, activity_date)", "TRUE", 1),
    "critical_care_day": ("clinical_critical_care_period", "patient_event_key", "event_datetime", "(level2_days > 0 OR level3_days > 0)", 2),
}
for stream, (table, key, ts, extra, max_rows) in COVERAGE.items():
    r = one(f"""
      WITH s AS (SELECT DISTINCT {key} AS k FROM {src(f'{GOLD}.{table}')} WHERE {ts} >= TIMESTAMP'{W}' AND {extra}),
           f AS (SELECT source_key AS k, count(*) AS n FROM {FACT} WHERE stream = '{stream}' GROUP BY 1)
      SELECT (SELECT count(*) FROM s) AS source_keys,
             (SELECT count(*) FROM s LEFT ANTI JOIN f USING (k)) AS missing,
             (SELECT count(*) FROM f LEFT ANTI JOIN s USING (k)) AS unexpected,
             (SELECT count(*) FROM f WHERE n > {max_rows}) AS over_emitted""")
    cov_rows.append((RUN_TS, stream, r["source_keys"], r["missing"], r["unexpected"], r["over_emitted"]))
    check(f"coverage:{stream}", r["missing"] == 0 and r["unexpected"] == 0 and r["over_emitted"] == 0, r)

r = one(f"""SELECT (SELECT count(*) FROM {src(f'{GOLD}.clinical_pathology_order')} WHERE event_datetime >= TIMESTAMP'{W}') AS source_rows,
                   (SELECT coalesce(sum(quantity_requested), 0) FROM {FACT} WHERE stream = 'pathology_tests') AS requested""")
check("coverage:pathology_tests requested", r["source_rows"] == r["requested"], r)
cov_rows.append((RUN_TS, "pathology_tests", r["source_rows"], r["source_rows"] - int(r["requested"]), 0, 0))

# COMMAND ----------

# Beds (review fix 7): compared against eligible source stays and their clipped occupancy durations, never against the output itself.
if BEDS:
    start_ok = f"(occupancy_start_basis IN ({in_list(R.OCCUPANCY_START_OBSERVED)}) AND occupancy_start IS NOT NULL AND occupancy_start <= st.loaded_at)"
    end_ok = f"(occupancy_end_basis IN ({in_list(R.OCCUPANCY_END_OBSERVED)}) AND occupancy_end IS NOT NULL AND occupancy_end <= st.loaded_at)"
    r = one(f"""
      WITH s AS (SELECT st.location_stay_key, st.occupancy_start, st.occupancy_end, st.period_start, st.loaded_at,
                        e.discharge_datetime AS d,
                        coalesce({start_ok}, false) AS sok, coalesce({end_ok}, false) AS eok
                 FROM {src(STAY_SRC)} st JOIN {GOLD}.spine_encounter e ON e.encounter_id = st.encounter_id
                 WHERE e.type_display IN ({in_list(R.BED_ENCOUNTER_TYPES)})),
           x AS (SELECT *, CASE WHEN NOT sok THEN NULL WHEN eok THEN occupancy_end
                                WHEN coalesce(d >= occupancy_start AND d <= loaded_at, false) THEN d END AS end_ts
                 FROM s),
           e AS (SELECT * FROM x WHERE coalesce(end_ts, occupancy_start, period_start) >= TIMESTAMP'{W}'),
           f AS (SELECT source_key, sum(quantity) AS q FROM {FACT} WHERE stream = 'ward_bed_day' GROUP BY 1),
           m AS (SELECT e.location_stay_key, f.q,
                        CASE WHEN e.end_ts IS NOT NULL THEN greatest(0D, (unix_timestamp(e.end_ts)
                             - unix_timestamp(greatest(e.occupancy_start, to_utc_timestamp(TIMESTAMP'{W}', 'Europe/London')))) / 86400.0) END AS expected
                 FROM e LEFT JOIN f ON f.source_key = e.location_stay_key)
      SELECT (SELECT count(*) FROM e) AS eligible_stays,
             (SELECT count(*) FROM e LEFT ANTI JOIN f ON f.source_key = e.location_stay_key) AS missing,
             (SELECT count(*) FROM f LEFT ANTI JOIN e ON e.location_stay_key = f.source_key) AS unexpected,
             (SELECT count_if(expected IS NOT NULL AND abs(coalesce(q, 0) - expected) > 1e-6) FROM m) AS duration_mismatch,
             (SELECT count_if(expected IS NULL AND q IS NOT NULL) FROM m) AS unmeasured_with_quantity""")
    check("beds:coverage and clipped duration vs source stays",
          r["eligible_stays"] > 0 and r["missing"] == 0 and r["unexpected"] == 0 and r["duration_mismatch"] == 0
          and r["unmeasured_with_quantity"] == 0, {**r, "source": STAY_SRC})
    cov_rows.append((RUN_TS, "ward_bed_day", r["eligible_stays"], r["missing"], r["unexpected"], 0))
else:
    r = one(f"SELECT count(*) AS rows FROM {FACT} WHERE stream = 'ward_bed_day'")
    check("beds:disabled by config, stream empty", r["rows"] == 0, {**r, "enable_ward_bed_day": False})

# Critical-care level days reconcile to the period.
r = one(f"""
  WITH f AS (SELECT source_key, sum(CASE WHEN stratum_l1 = 'level2' THEN quantity END) AS l2,
                    sum(CASE WHEN stratum_l1 = 'level3' THEN quantity END) AS l3
             FROM {FACT} WHERE stream = 'critical_care_day' GROUP BY 1)
  SELECT count(*) AS bad FROM f JOIN {src(f'{GOLD}.clinical_critical_care_period')} p ON p.patient_event_key = f.source_key
  WHERE coalesce(f.l2, 0) <> coalesce(p.level2_days, 0) OR coalesce(f.l3, 0) <> coalesce(p.level3_days, 0)""")
check("cc:level days reconcile", r["bad"] == 0, r)

# COMMAND ----------

# Money (review fix 6), reconciled at the grains allocation must preserve.
# (a) Ledger vs PLICS lines per activity × component.
r = one(f"""
  WITH g AS (SELECT l.extract_cd, l.activity_record_id, {family_component_sql('l.activity_cost_item_cd')} AS component,
                    sum(l.total_cost * l.source_duplicate_count) AS gbp
             FROM {GOLD}.reference_cost_line_item l
             LEFT SEMI JOIN {ACTS} a ON a.extract_cd = l.extract_cd AND a.activity_record_id = l.activity_record_id
             GROUP BY 1, 2, 3),
       r AS (SELECT extract_cd, activity_record_id, component, sum(amount) AS gbp FROM {LEDGER}
             WHERE cost_basis = 'actual_plics' GROUP BY 1, 2, 3)
  SELECT count(*) AS keys,
         count_if(abs(coalesce(g.gbp, 0) - coalesce(r.gbp, 0)) > 0.01) AS bad,
         count_if(g.gbp IS NULL) AS null_cost_keys_report_only
  FROM g FULL OUTER JOIN r USING (extract_cd, activity_record_id, component)""")
# Lines whose total_cost is NULL carry no money: NULL counts as £0 on both sides (their count is reported).
check("money:ledger = PLICS lines per activity × component", r["bad"] == 0 and r["keys"] > 0, r)

# (b) Ledger per activity = gold activity total (line activities) and = episode total (NCC activities).
r = one(f"""
  WITH r AS (SELECT extract_cd, activity_record_id, cost_basis, sum(amount) AS gbp FROM {LEDGER} GROUP BY 1, 2, 3)
  SELECT count_if(a.cost_line_count > 0 AND (coalesce(r.cost_basis, 'actual_plics') <> 'actual_plics'
                                              OR abs(coalesce(r.gbp, 0) - coalesce(a.total_cost_sum, 0)) > 0.01)) AS plics_bad,
         count_if(a.cost_line_count > 0 AND a.total_cost_sum IS NULL) AS null_cost_activities_report_only,
         count_if(a.cost_line_count = 0 AND a.total_cost_sum IS NOT NULL
                  AND (r.gbp IS NULL OR r.cost_basis <> 'actual_ncc_allocated' OR abs(r.gbp - a.total_cost_sum) > 0.01)) AS ncc_bad,
         count_if(a.cost_line_count > 0) AS plics_activities, count_if(a.cost_line_count = 0) AS ncc_activities
  FROM {ACTS} a LEFT JOIN r USING (extract_cd, activity_record_id)""")
check("money:ledger = activity totals", r["plics_bad"] == 0 and r["ncc_bad"] == 0, r)

# (c) Every ledger slice attached to a row is exactly what that fact row carries, under the right basis.
r = one(f"""
  WITH l AS (SELECT resource_use_event_key,
                    sum(CASE WHEN cost_basis = 'actual_plics' AND amount_kind = 'direct' THEN amount END) AS pd,
                    sum(CASE WHEN cost_basis = 'actual_plics' AND amount_kind = 'overhead' THEN amount END) AS po,
                    sum(CASE WHEN cost_basis = 'actual_ncc_allocated' AND amount_kind = 'direct' THEN amount END) AS nd
             FROM {LEDGER} WHERE resource_use_event_key IS NOT NULL GROUP BY 1)
  SELECT count(*) AS ledger_rows,
         count_if(f.resource_use_event_key IS NULL) AS ledger_key_not_in_fact,
         count_if(l.pd IS NOT NULL AND l.nd IS NOT NULL) AS both_plics_and_ncc,
         count_if(l.pd IS NOT NULL AND (f.cost_basis <> 'actual_plics' OR abs(f.cost_total - l.pd) > 0.005
                                        OR abs(f.cost_overhead - coalesce(l.po, 0)) > 0.005)) AS plics_row_mismatch,
         count_if(l.pd IS NULL AND l.nd IS NOT NULL AND (f.cost_basis <> 'actual_ncc_allocated' OR abs(f.cost_total - l.nd) > 0.005)) AS ncc_row_mismatch
  FROM l LEFT JOIN {FACT} f USING (resource_use_event_key)""")
check("money:fact rows = ledger slices", all(r[k] == 0 for k in ("ledger_key_not_in_fact", "both_plics_and_ncc",
                                                                 "plics_row_mismatch", "ncc_row_mismatch")), r)

# (d) Unallocated fact rows = ledger unallocated + superseded per activity × component.
r = one(f"""
  WITH l AS (SELECT concat_ws('|', extract_cd, activity_record_id) AS k, component, sum(amount) AS gbp FROM {LEDGER}
             WHERE resource_use_event_key IS NULL GROUP BY 1, 2),
       f AS (SELECT source_key AS k, stratum_l1 AS component, sum(cost_total + cost_overhead + coalesce(cost_superseded, 0)) AS gbp
             FROM {FACT} WHERE stream = 'plics_unallocated' GROUP BY 1, 2)
  SELECT count(*) AS keys, count_if(abs(coalesce(l.gbp, 0) - coalesce(f.gbp, 0)) > 0.01) AS bad
  FROM l FULL OUTER JOIN f USING (k, component)""")
check("money:unallocated rows = ledger", r["bad"] == 0, r)

# (e) £ landed on the entity its activity is linked to (encounter, or the appointment for outpatients).
r = one(f"""
  SELECT count(*) AS slices,
         count_if(NOT (CASE WHEN f.stream = 'outpatient_appointment' THEN f.resource_use_event_key = k.appointment_key
                            ELSE f.encounter_id = k.encounter_id END)) AS wrong_destination,
         count_if(f.person_id <> k.person_id) AS person_differs_report_only
  FROM {LEDGER} l JOIN {FACT} f USING (resource_use_event_key) JOIN {LINK} k USING (extract_cd, activity_record_id)
  WHERE l.resource_use_event_key IS NOT NULL""")
check("money:£ lands on the linked entity", r["wrong_destination"] == 0, r)

# (f) HCD ledger £ carried exactly, line by line.
r = one(f"""
  SELECT count(*) AS lines, count_if(f.resource_use_event_key IS NULL OR abs(f.cost_total - d.cost) > 0.005) AS bad
  FROM {src(f'{GOLD}.clinical_drug_expenditure')} d
  LEFT JOIN (SELECT * FROM {FACT} WHERE stream = 'high_cost_drug') f ON f.source_key = d.patient_event_key
  WHERE d.event_datetime >= TIMESTAMP'{W}' AND coalesce(try_cast(d.exclusion_flag AS INT), 0) = 0""")
check("money:HCD ledger carried per line", r["bad"] == 0, r)

# COMMAND ----------

# Row-level cost rules.
r = one(f"""SELECT count_if(cost_total IS NULL OR cost_overhead IS NULL OR cost_basis IS NULL) AS null_cost,
                   count_if(consumption_status IN ('not_consumed', 'unknown', 'scheduled') AND (cost_total <> 0 OR cost_overhead <> 0)) AS priced_unconsumed,
                   count_if(cost_basis = 'modelled_bottom_up' AND uplift_factor IS NULL) AS modelled_without_uplift,
                   count_if(cost_basis = 'none' AND cost_reason IS NULL) AS none_without_reason
            FROM {FACT}""")
check("rows:cost rules", all(v == 0 for v in r.values()), r)

# Key uniqueness on every published table.
KEYS = {
    "resource_use_event": "resource_use_event_key", "unit_cost_reference": "stream, base_fy, stratum_level, stratum_value",
    "unit_cost_backtest": "stream, base_fy, test_fy", "costed_activity_unit": "activity_unit_key",
    "spell_resource_summary": "spell_encounter_id", "journey_resource_summary": "journey_key",
    "person_month_resource": "person_id, month", "service_day_activity": "stream, service_unit, site_code, event_date_local",
    "attribution_summary": "stream, attribution_method", "plics_activity_link": "extract_cd, activity_record_id",
    "cost_allocation_ledger": "cost_allocation_ledger_key",  # RBI_REL_20261025
    "rbi_coverage": "financial_year, measure, dimension",  # INSR_W6 W6.07
}
# INSR W4.10: the key checks as one UNION ALL statement instead of one statement per table.
key_sql = " UNION ALL ".join(
    f"SELECT '{table}' AS t, count(*) AS n FROM (SELECT {key} FROM {TARGET}.{table} GROUP BY {key} HAVING count(*) > 1)"
    for table, key in KEYS.items())
for row in spark.sql(key_sql).collect():
    check(f"keys:{row['t']}", row["n"] == 0, {"duplicate_keys": row["n"], "key": KEYS[row["t"]]})

# RBI_REL_20261025: the ledger's natural grain is unique too, and the key is never NULL (so the key check above is real).
r = one(f"""SELECT count_if(cost_allocation_ledger_key IS NULL) AS null_keys,
                   count(*) - count(DISTINCT extract_cd, activity_record_id, component, coalesce(resource_use_event_key, '~'),
                                    amount_kind, cost_basis) AS duplicate_grain_rows
            FROM {LEDGER}""")
check("keys:cost_allocation_ledger grain", r["null_keys"] == 0 and r["duplicate_grain_rows"] == 0, r)

# COMMAND ----------

# RBI_REL_20261025: observation bound. Every source loaded_at is in the past, so nothing observed can be in the future.
r = one(f"""SELECT
      (SELECT count_if(stream NOT IN ('high_cost_drug', 'plics_unallocated') AND consumption_status IN ('consumed', 'not_consumed', 'unknown')
                       AND coalesce(consumption_evidence, '') <> 'event_after_observation' AND event_start > current_timestamp()) FROM {FACT}) AS observed_rows_in_future,
      (SELECT count_if(spell_first_event > current_timestamp()) FROM {TARGET}.spell_resource_summary) AS spells_in_future,
      (SELECT count_if(first_event > current_timestamp() OR last_event > current_timestamp()) FROM {TARGET}.journey_resource_summary) AS journeys_in_future,
      (SELECT count_if(setting = 'outpatient' AND (activity_start > current_timestamp() OR activity_start < TIMESTAMP'{W}'))
       FROM {TARGET}.costed_activity_unit) AS op_units_out_of_bounds,
      (SELECT count_if(month > current_date()) FROM {TARGET}.person_month_resource) AS person_months_in_future,
      (SELECT count_if(event_date_local > current_date()) FROM {TARGET}.service_day_activity) AS service_days_in_future""")
check("observation:no future-dated observed rows or rollups", all(v == 0 for v in r.values()), r)
# spell_last_event is an event_end: a case or appointment observed in progress may legitimately end after now (report-only).
r = one(f"SELECT count_if(spell_last_event > current_timestamp()) AS spell_last_event_after_now FROM {TARGET}.spell_resource_summary")
check("observation:spell_last_event after now (report-only)", True, r)

# INSR_W6 W6.06 (J25): site_basis is set exactly when site_code is, and only from its vocabulary.
r = one(f"""SELECT count_if(site_code IS NOT NULL AND site_basis IS NULL) AS site_without_basis,
                   count_if(site_code IS NULL AND site_basis IS NOT NULL) AS basis_without_site,
                   count_if(site_basis NOT IN ({in_list(R.SITE_BASES)})) AS unknown_basis
            FROM {FACT} WHERE stream <> 'plics_unallocated'""")
check("site:basis matches site_code", all(v == 0 for v in r.values()), r)

# INSR_W6 W6.07: NCC plan B slices land on fact rows exactly (only when the arm is configured and has rows);
# floor flags agree with n_entities; the APC arm of costed_activity_unit is inside the window.
r = one(f"""
  WITH l AS (SELECT resource_use_event_key, sum(amount) AS nb FROM {LEDGER}
             WHERE cost_basis = '{R.NCC_PLAN_B_BASIS}' AND amount_kind = 'direct' AND resource_use_event_key IS NOT NULL GROUP BY 1)
  SELECT count(*) AS plan_b_rows,
         count_if(f.cost_basis = '{R.NCC_PLAN_B_BASIS}' AND abs(f.cost_total - l.nb) > 0.005) AS plan_b_row_mismatch
  FROM l LEFT JOIN {FACT} f USING (resource_use_event_key)""")
check("money:NCC plan B slices = fact rows", r["plan_b_row_mismatch"] == 0, r)
# The floor the run under test was built with (the pipeline's rbi.min_calibration_activities). Prod passes none and the
# pipeline default holds; the W9 dev smoke builds with 1 so cohort-sized strata price (review 2026-10-31: comparing that
# build with the default 100 reported 1,025 false mismatches).
dbutils.widgets.text("min_calibration_activities", str(int(R.MIN_CALIBRATION_ACTIVITIES)))
MIN_N = int(dbutils.widgets.get("min_calibration_activities"))
r = one(f"""SELECT count_if(meets_calibration_floor <> (n_entities >= {MIN_N})) AS floor_mismatch,
                   count_if(used_for_pricing AND NOT meets_calibration_floor) AS priced_below_floor,
                   count_if(unit_cost_outlier_ind) AS outliers_report_only
            FROM {TARGET}.unit_cost_reference""")
check("costing:floor flags", r["floor_mismatch"] == 0 and r["priced_below_floor"] == 0, r)
r = one(f"""SELECT count_if(setting = 'admitted' AND coalesce(activity_end, activity_start) < TIMESTAMP'{W}') AS apc_before_window
            FROM {TARGET}.costed_activity_unit""")
check("observation:APC units inside the window", r["apc_before_window"] == 0, r)

r = one(f"""SELECT (SELECT count(DISTINCT stream) FROM {FACT}) AS fact_streams,
                   (SELECT count(DISTINCT stream) FROM {TARGET}.attribution_summary) AS summary_streams""")
check("attribution:summary covers streams", r["fact_streams"] == r["summary_streams"], r)
r = one(f"SELECT count(*) AS n FROM {TARGET}.unit_cost_backtest WHERE mape IS NOT NULL")
# On the 2% sample most strata are under the 100-entity floor, so the backtest is only required at full volume.
check("backtest:populated", r["n"] > 0 or SAMPLED, {**r, "sampled": SAMPLED})

# COMMAND ----------

# INSR W4.10 (F6, F9, F12, F23; L1670, L1715, L1542, L1589, L1735). Hard checks first, then report-only ones.
# Hard (F6): no event ends before it starts (W6.06 NULLs inverted ends), summaries are ordered, ledger £ is never NULL.
r = one(f"SELECT count_if(event_end < event_start) AS end_before_start FROM {FACT}")
check("ordering:event_end >= event_start", r["end_before_start"] == 0, r)
r = one(f"""SELECT (SELECT count_if(spell_first_event > spell_last_event) FROM {TARGET}.spell_resource_summary) AS spells,
                   (SELECT count_if(first_event > last_event) FROM {TARGET}.journey_resource_summary) AS journeys""")
check("ordering:summary first <= last", r["spells"] == 0 and r["journeys"] == 0, r)
r = one(f"SELECT count_if(amount IS NULL) AS ledger_null_amount FROM {LEDGER}")
check("money:ledger amount never NULL", r["ledger_null_amount"] == 0, r)
# Report-only (F9): unit costs over 10x their stream/base-year median, and strata priced from fewer than 5 entities.
r = one(f"""WITH m AS (SELECT stream, base_fy, percentile_approx(unit_cost, 0.5) AS med
                      FROM {TARGET}.unit_cost_reference GROUP BY 1, 2)
            SELECT count_if(u.unit_cost > 10 * m.med) AS over_10x_median, count_if(u.n_entities < 5) AS small_denominator
            FROM {TARGET}.unit_cost_reference u JOIN m USING (stream, base_fy)""")
check("costing:unit-cost outliers (report-only, k=10, n<5)", True, r)
# Report-only (F12): PLICS EC/APC activities linked to an encounter or appointment, per extract; flag groups < 95%.
links = spark.sql(f"""SELECT extract_cd, feed_type, count(*) AS n,
                             count_if(encounter_id IS NOT NULL OR appointment_key IS NOT NULL) AS linked
                      FROM {LINK} GROUP BY 1, 2""").collect()
low = [(x.extract_cd, x.feed_type, round(x.linked / x.n, 4)) for x in links
       if x.feed_type in ("EC", "APC") and x.n and x.linked / x.n < 0.95]
check("linkage:PLICS EC/APC link rate >= 95% (report-only)", True, {"below_95": low[:50], "groups": len(links)})
# Report-only (F23): site/theatre code fill per stream and financial year ('2023/24'), drops > 10pp on the prior year.
fill = spark.sql(f"""WITH f AS (SELECT stream, financial_year,
                                       avg(CASE WHEN site_code IS NULL THEN 0.0 ELSE 1.0 END) AS site_fill,
                                       avg(CASE WHEN theatre_code IS NULL THEN 0.0 ELSE 1.0 END) AS theatre_fill
                                FROM {FACT} GROUP BY 1, 2)
                     SELECT a.stream, a.financial_year, a.site_fill, b.site_fill AS prev_site_fill,
                            a.theatre_fill, b.theatre_fill AS prev_theatre_fill
                     FROM f a JOIN f b ON a.stream = b.stream
                      AND CAST(substr(a.financial_year, 1, 4) AS INT) = CAST(substr(b.financial_year, 1, 4) AS INT) + 1
                     WHERE b.site_fill - a.site_fill > 0.10 OR b.theatre_fill - a.theatre_fill > 0.10""").collect()
check("fill:site/theatre code drop > 10pp (report-only)", True, {"drops": [x.asDict() for x in fill][:50]})
# Report-only (ledger NULLs): NULL money/quantity columns in the four rollups, one statement per rollup.
rollup_nulls = {}
for table in ("spell_resource_summary", "journey_resource_summary", "person_month_resource", "service_day_activity"):
    money = [c for c, t in spark.table(f"{TARGET}.{table}").dtypes
             if t == "double" and (c.startswith("cost_") or c in ("hcd_cost", "bed_days", "theatre_minutes")
                                   or c.startswith("quantity") or c.endswith("_days"))]
    if money:
        row = one(f"SELECT {', '.join(f'count_if({c} IS NULL) AS {c}' for c in money)} FROM {TARGET}.{table}")
        rollup_nulls[table] = {c: n for c, n in row.items() if n}
check("money:rollup money/quantity NULLs (report-only)", True, rollup_nulls)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {META}")
spark.createDataFrame(results, "run_ts string, check_name string, passed boolean, detail string") \
    .write.mode("append").saveAsTable(f"{META}.validation_result")
spark.createDataFrame(cov_rows, "run_ts string, stream string, source_keys bigint, missing bigint, unexpected bigint, over_emitted bigint") \
    .write.mode("append").saveAsTable(f"{META}.stream_coverage")
failed = [(n, d) for _, n, p, d in results if not p]
if failed:
    raise AssertionError("RBI validation failed:\n" + "\n".join(f"{n}: {d}" for n, d in failed))
dbutils.notebook.exit(json.dumps({"checks": len(results), "failed": 0, "sampled": SAMPLED, "beds": BEDS}))
