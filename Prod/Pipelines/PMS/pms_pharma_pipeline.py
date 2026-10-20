# Databricks notebook source
# =====================================================================================================
# PMS dataset — post-marketing surveillance extract for configured drugs of interest (Lakeflow, Phase 2)
#
# WHAT: one cohort per drug set in {pms.config_schema}.drug_of_interest (index = first exposure in any Barts
# medication feed), with patient basics, exposures, concomitant medication, comorbidity, configured outcomes,
# ADR signals (stop reasons, allergy records, Datix medication incidents, LFPSE), labs with a Hy's law screen,
# weight/height, NEWS2, encounters and death. Sources are Journey gold (pre-OMOP) only; a new sponsor drug
# is a config row, not a code change.
#
# PRIVACY: person_id is the only key. No MRN, NHS number, names, addresses, postcodes or contact fields are
# selected. Dates are real and LSOA/GP practice are quasi-identifiers: de-identification (date shift, age
# banding, small-cell suppression) is applied at serve time, per estate rule, not here.
#
# FURTHER IMPROVEMENTS (not in this version):
# - ECG / QTc: when ECGs land in the estate, add pms_ecg (QTc, QRS, PR, rhythm) with baseline and
#   on-treatment windows; QT prolongation is a standard PMS safety endpoint for many drug classes.
# - Clinical-text ADR mining: run discharge/clinic letters, RADRPT reports and Datix narrative
#   (text_safety_document) through a drug-event extractor with negation handling (Polgate) and publish
#   mentions with model/version/score provenance. Most ADR evidence is narrative, not coded.
# - LFPSE multi-drug splitting: lfpse_drugs_involved is a free-text list; split it and map each item through
#   the drug-name lexicon instead of regex-matching the whole field.
# - Brand-name tier in the drug-name lexicon (dm+d AMP/VMP -> VTM), to lift Datix/allergy mapping beyond ~70%.
# - dm+d hierarchy expansion of drug sets (VTM -> VMP/AMP via the OMOP vocab) so code matching, not display
#   regex, carries the load.
# - External linkage (needs DARS/IG approval): ONS deaths (cause of death), HES (out-of-Trust admissions),
#   GP records, NHSBSA primary-care dispensing (exposure before/after the hospital episode), NDRS, PDS.
# - ICU infusions from critical-care charting as an exposure source with rates.
# - Pharmacogenomics from clinical_genomic_result (CYP2C19, CYP2D6, TPMT, DPYD, HLA-B*57:01, ...).
# - Patient-reported outcomes from PowerForms instruments where collected.
# - MedDRA coding of outcomes and ADR signals (licence needed), alongside SNOMED/ICD-10.
# - OMOP CDM export of the same cohort for OHDSI network studies (an output format, not a source).
# - MediCUs cause of death once populated (0% today).
# - Exposure episodes: collapse records into continuous treatment episodes (gap/grace-period rules) for
#   incidence-rate denominators.
# =====================================================================================================
from datetime import datetime, timezone
from functools import reduce
from pyspark import pipelines as dp
from pyspark.sql import Window, functions as F

G = spark.conf.get("pms.gold_schema", "4_prod.gold")
G1 = spark.conf.get("pms.phase1_gold_schema", "4_prod.gold")  # dev: 8_dev.pms_gold holds the T5/T6 tables
CFG = spark.conf.get("pms.config_schema")
DATIX_FACTOR = spark.conf.get("pms.datix_factor_table", f"{G}.clinical_safety_incident_factor")
DATIX_PARTICIPANT = spark.conf.get("pms.datix_participant_table", f"{G}.clinical_safety_incident_participant")
DATIX_INCIDENT = spark.conf.get("pms.datix_incident_table", f"{G}.clinical_safety_incident")
DATIX_LFPSE = spark.conf.get("pms.datix_lfpse_table", f"{G}.clinical_safety_incident_lfpse")
LOOKBACK = int(spark.conf.get("pms.lookback_days", "365"))
FOLLOWUP = int(spark.conf.get("pms.followup_days", "365"))
LONDON = "Europe/London"
ICD10 = "http://hl7.org/fhir/sid/icd-10"
ADR_STOP_REGEX = r"adverse|side[ -]?effect|reaction|allerg|intoleran|toxicit|rash|anaphyla"
NEWS2_TOTAL = ["Royal College of Physicians NEWS2 (National Early Warning Score 2) total score", "NEWS Total"]
VITAL_CODES = {"27113001": "body_weight", "50373000": "body_height"}
PERSON_RULE = {"pms.person_present": "person_id IS NOT NULL"}

# INCR_FIX_20261018: "after now" tests without current_timestamp() (which blocks incremental refresh).
# The run instant is a literal only inside the one-row _pms_run MV; rows equi-join the future-day set, and
# only rows dated on the run day are compared with the run instant.
CALENDAR_TABLE = spark.conf.get("pms.calendar_table", "3_lookup.omop.calendar_day")
CALENDAR_LAST_DATE = "2300-12-31"  # last calendar row; later dates are always "after"
RUN_INSTANT = datetime.now(timezone.utc)  # this update's start, the instant current_timestamp() used to give


@dp.materialized_view(name="_pms_run", private=True)
def _pms_run():
    """One row: this update's run instant and its date (session time zone)."""
    run_ts = F.lit(RUN_INSTANT)
    return (spark.read.table(CALENDAR_TABLE)
            .where(F.col("calendar_date") == F.to_date(run_ts))
            .select(F.col("calendar_date").alias("_pms_run_date"), run_ts.alias("_pms_run_ts"),
                    F.lit(True).alias("_pms_run_key")))


@dp.materialized_view(name="_pms_future_day", private=True)
def _pms_future_day():
    """Every calendar date after the run date."""
    run = spark.read.table("_pms_run")
    return (spark.read.table(CALENDAR_TABLE).withColumn("_pms_run_key", F.lit(True))
            .join(run, "_pms_run_key")
            .where(F.col("calendar_date") > F.col("_pms_run_date"))
            .select(F.col("calendar_date").alias("_pms_future_date")))


def _after_run(df, ts, flag):
    """Add boolean `flag` = timestamp expression `ts` is after the run instant (NULL when ts is NULL).

    Same result as `ts > current_timestamp()`: later days match _pms_future_day, run-day rows compare
    with the run instant, earlier days are not after.
    """
    day, fut, rd, rts = f"_{flag}_day", f"_{flag}_fd", f"_{flag}_rd", f"_{flag}_rts"
    future = spark.read.table("_pms_future_day").select(F.col("_pms_future_date").alias(fut))
    run = spark.read.table("_pms_run").select(F.col("_pms_run_date").alias(rd), F.col("_pms_run_ts").alias(rts))
    joined = (df.withColumn(day, F.to_date(ts))
              .join(future, F.col(day) == F.col(fut), "left")
              .join(run, F.col(day) == F.col(rd), "left"))
    after = (F.col(fut).isNotNull() | (F.col(day) > F.lit(CALENDAR_LAST_DATE).cast("date"))
             | F.coalesce(ts > F.col(rts), F.lit(False)))
    return joined.withColumn(flag, F.when(ts.isNotNull(), after)).drop(day, fut, rd, rts)


def _cfg(name):
    return spark.read.table(f"{CFG}.{name}")


def _drug_match(df, code_cols, text_cols):
    """Inner-join rows to every configured drug set they match, by code or by lower-cased display regex."""
    ds = F.broadcast(_cfg("drug_of_interest").select("drug_set_id", "drug_set_name", "match_codes", "name_regex")).alias("d")
    conds = [F.coalesce(F.array_contains(F.col("d.match_codes"), F.col(f"x.{c}")), F.lit(False)) for c in code_cols]
    conds += [F.coalesce(F.expr(f"lower(x.{c}) RLIKE d.name_regex"), F.lit(False)) for c in text_cols]
    return df.alias("x").join(ds, reduce(lambda a, b: a | b, conds), "inner").select("x.*", "d.drug_set_id", "d.drug_set_name")


def _window(df, ts, mode="window"):
    """Attach the person x drug-set index; keep rows in the study window; add days_from_index and period.
    Rows already keyed by drug_set_id join on (person_id, drug_set_id); raw source rows fan out to each of the
    person's drug sets. mode: window = [index-LOOKBACK, last exposure+FOLLOWUP]; history = before index;
    after = [index, last+FOLLOWUP]; all = any dated row."""
    s = spark.read.table("pms_exposure_summary").select("person_id", "drug_set_id", "index_datetime", "last_exposure_datetime")
    t, idx, last = F.col(ts), F.col("index_datetime"), F.col("last_exposure_datetime")
    lo = idx - F.expr(f"INTERVAL {LOOKBACK} DAYS")
    hi = last + F.expr(f"INTERVAL {FOLLOWUP} DAYS")
    keep = {"window": (t >= lo) & (t <= hi), "history": t < idx, "after": (t >= idx) & (t <= hi), "all": t.isNotNull()}[mode]
    period = F.when(t < idx, "baseline").when(t <= last, "on_treatment").otherwise("post_treatment")
    keys = ["person_id", "drug_set_id"] if "drug_set_id" in df.columns else ["person_id"]
    return (df.join(s, keys).where(keep)
              .withColumn("days_from_index", F.datediff(t, idx)).withColumn("period", period)
              .drop("index_datetime", "last_exposure_datetime"))

# COMMAND ----------

EXPOSURE_TYPES = {"person_id": "bigint", "drug_set_id": "string", "drug_set_name": "string", "exposure_source": "string",
    "exposure_record_key": "string", "order_id": "bigint", "encounter_id": "bigint", "drug_code": "string", "drug_display": "string",
    "exposure_start": "timestamp", "exposure_end": "timestamp", "dose_value": "double", "dose_unit": "string",
    "dose_basis": "string", "dose_text": "string", "quantity_value": "double", "quantity_unit": "string", "route": "string",
    "indication": "string", "stop_reason": "string", "status": "string", "exposure_ineligible_reason": "string",
    "exposure_eligible_ind": "boolean", "source_feed": "string"}
_CARRIED = ("person_id", "drug_set_id", "drug_set_name", "encounter_id")
EXPOSURE_SOURCES = ("order", "administration", "dispense", "homecare_supply", "sact")
ORDER_NOT_PRESCRIBED = ["Cancelled", "Voided Without Results", "Voided With Results"]
ADMIN_NOT_GIVEN = ["Not Done", "In Error"]
SUPPLY_NOT_SUPPLIED = ["CANCELLED_OR_ERROR"]
# T6 attribute columns read by _orders (the exposure contract) and pms_medication_detail (ORDER_DETAIL_COLUMNS below).
ORDER_ATTRIBUTE_COLUMNS = ["strengthdose_value", "strengthdose_unit_display", "volumedose_value", "volumedose_unit_display",
    "freetxtdose_display", "rxroute_display", "indication_display", "dcreason_display", "weight_value", "weight_unit_display",
    "freq_display", "freqschedid_display", "duration_value", "duration_unit_display", "dayssupply_value",
    "dispensecategory_display", "drugform_display", "stoptype_display", "prnreason_display", "prninstructions_display",
    "rate_value", "rate_unit_display", "infuseover_value", "infuseover_unit_display", "reqstart_datetime", "stop_datetime"]


def _n_if(cond):
    """Count of rows where cond holds; 0 (not NULL) inside a non-empty group."""
    return F.sum(cond.cast("int"))


def _status_reason(col, bad):
    """'status:<value>' when the record's status says it never reached the patient, else NULL."""
    return F.when(F.col(col).isin(bad), F.concat(F.lit("status:"), F.col(col)))


def _shape(df, **cols):
    """Project a drug-matched source onto the exposure contract; fields a source lacks become typed NULLs.
    exposure_ineligible_reason = the source's own reason, else future_start; exposure_eligible_ind = no reason."""
    df = _after_run(df, cols["exposure_start"], "_pms_future_start")  # INCR_FIX_20261018
    reason = F.coalesce(cols.pop("exposure_ineligible_reason", F.lit(None).cast("string")),
                        F.when(F.col("_pms_future_start"), F.lit("future_start")))
    cols = {**cols, "exposure_ineligible_reason": reason, "exposure_eligible_ind": reason.isNull()}

    def pick(c):
        return cols[c] if c in cols else (F.col(c) if c in _CARRIED else F.lit(None))
    return df.select(*[pick(c).cast(t).alias(c) for c, t in EXPOSURE_TYPES.items()])


def _order_source():
    """Gold orders with their Phase 1 order-grain attributes (T6); shared by the exposure and detail tables."""
    attr = spark.read.table(f"{G1}.clinical_medication_order_attribute").select("order_id", *ORDER_ATTRIBUTE_COLUMNS)
    return spark.read.table(f"{G}.clinical_medication_order").join(attr, "order_id", "left")


def _orders():
    m = _drug_match(_order_source(), ["medication_target_code", "medication_snomed_code"],
                    ["medication_target_display", "medication_source_display"])
    strength = F.col("strengthdose_value").isNotNull()
    return _shape(m, exposure_source=F.lit("order"), exposure_record_key=F.col("patient_event_key"), order_id=F.col("order_id"),
        drug_code=F.coalesce("medication_target_code", "medication_snomed_code", "medication_source_code"),
        drug_display=F.coalesce("medication_target_display", "medication_source_display"),
        exposure_start=F.coalesce("effective_start_datetime", "event_datetime"),
        exposure_end=F.coalesce("discontinued_datetime", "projected_stop_datetime", "event_end_datetime"),
        dose_value=F.when(strength, F.col("strengthdose_value")).otherwise(F.col("volumedose_value")),
        dose_unit=F.when(strength, F.col("strengthdose_unit_display")).otherwise(F.col("volumedose_unit_display")),
        dose_basis=F.lit("prescribed"), dose_text=F.col("freetxtdose_display"), route=F.col("rxroute_display"),
        indication=F.col("indication_display"), stop_reason=F.col("dcreason_display"), status=F.col("order_status_display"),
        exposure_ineligible_reason=_status_reason("order_status_display", ORDER_NOT_PRESCRIBED), source_feed=F.col("source_feed"))


def _administrations():
    m = _drug_match(spark.read.table(f"{G}.clinical_medication_admin"),
                    ["medication_target_code", "medication_snomed_code"], ["medication_target_display", "medication_source_display"])
    return _shape(m, exposure_source=F.lit("administration"), exposure_record_key=F.col("patient_event_key"), order_id=F.col("order_id"),
        drug_code=F.coalesce("medication_target_code", "medication_snomed_code", "medication_source_code"),
        drug_display=F.coalesce("medication_target_display", "medication_source_display"),
        exposure_start=F.coalesce("performed_datetime", "event_datetime"), exposure_end=F.col("event_end_datetime"),
        dose_value=F.col("dose_value"), dose_unit=F.col("dose_unit"), dose_basis=F.lit("administered"), route=F.col("route_display"),
        status=F.col("administration_status_display"),
        exposure_ineligible_reason=_status_reason("administration_status_display", ADMIN_NOT_GIVEN), source_feed=F.col("source_feed"))


def _dispenses():
    m = _drug_match(spark.read.table(f"{G}.clinical_medication_dispense"),
                    ["medication_target_code", "medication_snomed_code"], ["medication_target_display", "medication_source_display"])
    not_issued = (F.coalesce(F.col("issue_category"), F.lit("")) != "ISSUE") | (F.coalesce(F.col("status_code"), F.lit("")) != "completed")
    return _shape(m, exposure_source=F.lit("dispense"), exposure_record_key=F.col("patient_event_key"),
        drug_code=F.coalesce("medication_target_code", "medication_snomed_code", "medication_source_code"),
        drug_display=F.coalesce("medication_target_display", "medication_source_display"),
        exposure_start=F.col("event_datetime"), exposure_end=F.col("event_end_datetime"),
        dose_text=F.col("drug_strength"), quantity_value=F.col("quantity"), quantity_unit=F.col("quantity_unit"),
        status=F.concat_ws("|", "issue_type", "issue_category", "status_code"),
        exposure_ineligible_reason=F.when(not_issued, F.concat(F.lit("status:"), F.concat_ws("|", "issue_category", "status_code"))),
        source_feed=F.col("source_feed"))


def _supplies():
    m = _drug_match(spark.read.table(f"{G}.clinical_medication_supply"), ["dmd_vtm_code"], ["dmd_vtm_name", "item_description"])
    undelivered = F.when(F.coalesce(F.col("quantity_delivered"), F.lit(0)) <= 0, F.lit("not_delivered"))
    return _shape(m, exposure_source=F.lit("homecare_supply"), exposure_record_key=F.col("patient_event_key"),
        drug_code=F.col("dmd_vtm_code"), drug_display=F.coalesce("dmd_vtm_name", "item_description"),
        exposure_start=F.coalesce(F.col("supply_start_date").cast("timestamp"), F.col("event_datetime")),
        exposure_end=F.col("event_end_datetime"), quantity_value=F.col("quantity_delivered"), quantity_unit=F.col("order_unit"),
        dose_text=F.col("label_directions"), indication=F.col("indication"), stop_reason=F.col("nfd_reason"),
        status=F.col("item_status_display"),
        exposure_ineligible_reason=F.coalesce(_status_reason("item_status_display", SUPPLY_NOT_SUPPLIED), undelivered),
        source_feed=F.col("source_feed"))


def _sact():
    # Gold holds planned future SACT rows (203 iQemo rows on 2026-09-29); _shape marks them future_start, not exposure.
    m = _drug_match(spark.read.table(f"{G}.clinical_cancer_treatment"),
                    ["drug_target_code", "drug_snomed_code"], ["drug_target_display", "drug_source_display"])
    return _shape(m, exposure_source=F.lit("sact"), exposure_record_key=F.col("patient_event_key"),
        drug_code=F.coalesce("drug_target_code", "drug_snomed_code", "drug_source_code"),
        drug_display=F.coalesce("drug_target_display", "drug_source_display"),
        exposure_start=F.col("start_date").cast("timestamp"),
        exposure_end=F.coalesce("end_date", "final_treatment_date").cast("timestamp"),
        dose_value=F.col("dose_value"), dose_unit=F.col("dose_unit_code"), dose_basis=F.lit("sact_prescribed"),
        dose_text=F.col("regimen_name"), route=F.col("route_code"), indication=F.col("indication"), status=F.col("record_type"),
        source_feed=F.lit("cancer_treatment"))


@dp.materialized_view(name="pms_exposure", cluster_by=["person_id", "drug_set_id"],
    comment="Every Barts medication record matching a configured drug set: orders (+ order attributes), administrations, "
            "pharmacy dispenses, homecare supply and SACT. One row per source record x drug set. Records that never reached "
            "the patient (not-done/in-error administrations, cancelled/voided orders, undelivered supply, future starts) are kept "
            "with exposure_eligible_ind = false and exposure_ineligible_reason. dose_* = a per-dose amount (dose_basis says "
            "prescribed/administered); quantity_* = dispensed/delivered amount.")
@dp.expect_all_or_drop({**PERSON_RULE, "pms.exposure_dated": "exposure_start IS NOT NULL"})
def pms_exposure():
    return reduce(lambda a, b: a.unionByName(b), [_orders(), _administrations(), _dispenses(), _supplies(), _sact()])


DETAIL_TYPES = {"person_id": "bigint", "drug_set_id": "string", "exposure_source": "string", "exposure_record_key": "string",
    "order_id": "bigint", "weight_value": "double", "weight_unit_display": "string", "freq_display": "string",
    "freqschedid_display": "string", "frequency_id": "string", "duration_value": "double", "duration_unit_display": "string",
    "dayssupply_value": "double", "dispensecategory_display": "string", "drugform_display": "string", "stoptype_display": "string",
    "prn_ind": "boolean", "prnreason_display": "string", "prninstructions_display": "string", "iv_ind": "boolean",
    "rate_value": "double", "rate_unit_display": "string", "infuseover_value": "double", "infuseover_unit_display": "string",
    "reqstart_datetime": "timestamp", "stop_datetime": "timestamp", "initial_dose_value": "double", "initial_dose_unit": "string",
    "dose_in_mg": "double", "dose_in_ml": "double", "infused_volume": "double", "infused_volume_unit": "string",
    "infusion_rate": "double", "infusion_rate_unit": "string", "site_display": "string", "scheduled_datetime": "timestamp",
    "verified_datetime": "timestamp"}
ORDER_DETAIL_COLUMNS = ["weight_value", "weight_unit_display", "freq_display", "freqschedid_display", "frequency_id",
    "duration_value", "duration_unit_display", "dayssupply_value", "dispensecategory_display", "drugform_display",
    "stoptype_display", "prn_ind", "prnreason_display", "prninstructions_display", "iv_ind", "rate_value", "rate_unit_display",
    "infuseover_value", "infuseover_unit_display", "reqstart_datetime", "stop_datetime"]
ADMIN_DETAIL_COLUMNS = ["initial_dose_value", "initial_dose_unit", "dose_in_mg", "dose_in_ml", "infused_volume",
    "infused_volume_unit", "infusion_rate", "infusion_rate_unit", "site_display", "scheduled_datetime", "verified_datetime",
    "prn_ind", "iv_ind"]


def _detail(src, source, columns):
    """Join one source's detail columns onto its pms_exposure rows by the native record key; absent fields are typed NULLs."""
    keys = (spark.read.table("pms_exposure").where(F.col("exposure_source") == source)
            .select("person_id", "drug_set_id", "exposure_source", "exposure_record_key", "order_id"))
    d = src.select(F.col("patient_event_key").alias("exposure_record_key"), *columns)
    x = keys.join(d, "exposure_record_key", "left")
    return x.select(*[(F.col(c) if c in x.columns else F.lit(None)).cast(t).alias(c) for c, t in DETAIL_TYPES.items()])


@dp.materialized_view(name="pms_medication_detail", cluster_by=["person_id", "drug_set_id"],
    comment="Prescribing and administration detail per order/administration exposure record (key = exposure_source + "
            "exposure_record_key + drug_set_id, as in pms_exposure). Orders: dosing weight, frequency, duration, days supply, "
            "dispense category, PRN, rate/infuse-over, form and stop type from the Phase 1 order attributes. Administrations: "
            "initial dose, standardised mg/ml dose, infused volume, infusion rate, site and schedule/verify times. "
            "order_id links an administration to its order.")
@dp.expect_all_or_drop(PERSON_RULE)
def pms_medication_detail():
    admin = spark.read.table(f"{G}.clinical_medication_admin")
    return _detail(_order_source(), "order", ORDER_DETAIL_COLUMNS).unionByName(_detail(admin, "administration", ADMIN_DETAIL_COLUMNS))


@dp.materialized_view(name="pms_exposure_summary", cluster_by=["person_id"],
    comment="PMS cohort: one row per person x drug set with at least one exposure-eligible record. index_datetime = first "
            "eligible exposure in any feed; last_exposure_datetime = last eligible exposure, capped at the build time so "
            "open-ended orders do not run into the future. ineligible_record_count counts the person's excluded records.")
def pms_exposure_summary():
    # INCR_FIX_20261018: cap each record at the run instant before the max (max of capped values equals
    # the capped max); only records ending after the run pick up the run instant, by an equi-join on the flag.
    last = F.coalesce("exposure_end", "exposure_start")
    e = _after_run(spark.read.table("pms_exposure"), last, "_pms_last_future")
    cap = spark.read.table("_pms_run").select(F.col("_pms_run_key").alias("_pms_last_future"), F.col("_pms_run_ts").alias("_pms_cap"))
    e = e.join(cap, "_pms_last_future", "left")
    ok = F.col("exposure_eligible_ind")
    by_src = [_n_if(ok & (F.col("exposure_source") == s)).alias(f"{s}_record_count") for s in EXPOSURE_SOURCES]
    return (e.groupBy("person_id", "drug_set_id", "drug_set_name").agg(
        F.min(F.when(ok, F.col("exposure_start"))).alias("index_datetime"),
        F.max(F.when(ok, F.when(F.col("_pms_last_future"), F.col("_pms_cap")).otherwise(last))).alias("last_exposure_datetime"),
        _n_if(ok).alias("exposure_record_count"),
        _n_if(~ok).alias("ineligible_record_count"),
        F.countDistinct(F.when(ok, F.to_date("exposure_start"))).alias("exposure_day_count"),
        F.sort_array(F.collect_set(F.when(ok, F.col("exposure_source")))).alias("exposure_sources"),
        F.expr("min_by(indication, exposure_start) FILTER (WHERE exposure_eligible_ind AND indication IS NOT NULL)").alias("first_indication"),
        F.expr("min_by(route, exposure_start) FILTER (WHERE exposure_eligible_ind AND route IS NOT NULL)").alias("first_route"),
        *by_src)
        .where(F.col("index_datetime").isNotNull()))

# COMMAND ----------

@dp.materialized_view(name="pms_death", cluster_by=["person_id"],
    comment="Death per cohort person x drug set. Date = death evidence deceased_datetime (London date), else its calculated "
            "death date, else spine deceased_datetime. spine deceased_ind is NOT used (over-counts ~1.87M living people).")
@dp.expect_all_or_drop(PERSON_RULE)
def pms_death():
    s = spark.read.table("pms_exposure_summary").select("person_id", "drug_set_id", "index_datetime", "last_exposure_datetime")
    ev = spark.read.table(f"{G}.reference_person_death_evidence").select(
        "person_id", F.col("deceased_datetime").alias("ev_dt"), "calculated_death_date",
        F.col("source_desc").alias("death_source"), F.col("precision_desc").alias("death_date_precision"), "cause_of_death")
    sp = spark.read.table(f"{G}.spine_person").select("person_id", F.col("deceased_datetime").alias("sp_dt"))
    d = s.join(ev, "person_id", "left").join(sp, "person_id", "left")
    death_date = F.coalesce(F.to_date(F.from_utc_timestamp("ev_dt", LONDON)), F.col("calculated_death_date").cast("date"),
                            F.to_date(F.from_utc_timestamp("sp_dt", LONDON)))
    evidence = F.when(F.col("ev_dt").isNotNull(), "death_evidence_datetime") \
                .when(F.col("calculated_death_date").isNotNull(), "death_evidence_calculated").otherwise("spine_deceased_datetime")
    return (d.withColumn("death_date", death_date).where(F.col("death_date").isNotNull())
             .select("person_id", "drug_set_id", "death_date", evidence.alias("death_date_basis"), "death_source",
                     "death_date_precision", "cause_of_death",
                     F.datediff("death_date", F.to_date("index_datetime")).alias("days_from_index"),
                     (F.col("death_date") <= F.date_add(F.to_date("last_exposure_datetime"), FOLLOWUP)).alias("within_followup_ind"),
                     # evidence dated > 1 day before the first exposure is contradicted by later activity (wrong-patient or
                     # re-used identifier upstream); published as recorded and flagged, never silently dropped
                     (F.datediff("death_date", F.to_date("index_datetime")) < -1).alias("death_precedes_index_ind")))


@dp.materialized_view(name="pms_patient", cluster_by=["person_id"],
    comment="Basic patient details for every cohort person: sex, year of birth, age at first index, ethnicity, "
            "area deprivation of the current address, current registered GP practice (ODS) and death date. No direct identifiers.")
@dp.expect_all_or_drop(PERSON_RULE)
def pms_patient():
    s = spark.read.table("pms_exposure_summary").groupBy("person_id").agg(
        F.min("index_datetime").alias("first_index_datetime"), F.sort_array(F.collect_set("drug_set_id")).alias("drug_set_ids"))
    p = spark.read.table(f"{G}.spine_person").select(
        "person_id", F.col("gender_display").alias("sex"), "birth_date",
        F.coalesce("ethnicity_target_code", "ethnicity_code").alias("ethnicity_code"),
        F.coalesce("ethnicity_target_display", "ethnicity_display").alias("ethnicity_display"), "current_address_id")
    a = spark.read.table(f"{G}.reference_person_address").select(
        F.col("address_id").alias("current_address_id"), "lsoa", "local_authority_code", "imd_decile", "imd_quintile")
    gp = (spark.read.table(f"{G1}.reference_person_gp_registration").where("current_ind")
          .select("person_id", F.col("practice_ods_code").alias("gp_practice_ods_code"),
                  F.col("registration_start").alias("gp_registration_start")))
    death = spark.read.table("pms_death").groupBy("person_id").agg(F.max("death_date").alias("death_date"))
    x = s.join(p, "person_id", "left").join(a, "current_address_id", "left").join(gp, "person_id", "left").join(death, "person_id", "left")
    return x.select("person_id", "sex", F.year("birth_date").alias("year_of_birth"),
                    F.floor(F.months_between("first_index_datetime", "birth_date") / 12).cast("int").alias("age_at_first_index"),
                    "ethnicity_code", "ethnicity_display", "lsoa", "local_authority_code", "imd_decile", "imd_quintile",
                    "gp_practice_ods_code", "gp_registration_start", "first_index_datetime", "drug_set_ids", "death_date")


@dp.materialized_view(name="pms_encounter", cluster_by=["person_id"],
    comment="Attended hospital encounters (Inpatient/Outpatient/Emergency/Recurring, attendance observed or inferred) "
            "of cohort persons within [index - lookback, last exposure + follow-up].")
@dp.expect_all_or_drop(PERSON_RULE)
def pms_encounter():
    # class_display is only 15% populated (7.46M of 48.5M, measured 2026-09-29); type_class_display is 99.8%.
    # Preadmit / Wait List / Results Only rows are placeholders, and CANCELLED / NONE / FUTURE_SCHEDULED never happened.
    e = (spark.read.table(f"{G}.spine_encounter")
         .where(F.col("type_class_display").isin("Inpatient", "Outpatient", "Emergency", "Recurring")
                & F.col("attendance_evidence").isin("OBSERVED", "INFERRED"))
         .select("person_id", "encounter_id", F.col("type_class_display").alias("encounter_class"), "type_display",
                 "attendance_evidence", "status_display", "period_start", "period_end", "length_of_stay_minutes",
                 "admission_source_display", "discharge_destination_display", "specialty_display", "responsible_service_display"))
    return _window(e, "period_start")

# COMMAND ----------

@dp.materialized_view(name="pms_concomitant_medication", cluster_by=["person_id"],
    comment="All medication orders of cohort persons in the study window, one row per order x drug set; "
            "is_drug_of_interest marks orders that are themselves exposures to that set.")
@dp.expect_all_or_drop(PERSON_RULE)
def pms_concomitant_medication():
    o = spark.read.table(f"{G}.clinical_medication_order").select(
        "person_id", "patient_event_key", "encounter_id",
        F.coalesce("medication_target_code", "medication_snomed_code", "medication_source_code").alias("drug_code"),
        F.coalesce("medication_target_display", "medication_source_display").alias("drug_display"),
        F.coalesce("effective_start_datetime", "event_datetime").alias("start_datetime"),
        F.coalesce("discontinued_datetime", "projected_stop_datetime", "event_end_datetime").alias("end_datetime"),
        F.col("order_status_display").alias("status"))
    own = (spark.read.table("pms_exposure").where("exposure_source = 'order'")
           .select(F.col("exposure_record_key").alias("patient_event_key"), "drug_set_id", F.lit(True).alias("is_drug_of_interest")))
    w = _window(o, "start_datetime")
    return (w.join(own, ["patient_event_key", "drug_set_id"], "left")
             .withColumn("is_drug_of_interest", F.coalesce("is_drug_of_interest", F.lit(False))))


def _conditions():
    return spark.read.table(f"{G}.clinical_condition").select(
        "person_id", "patient_event_key", "encounter_id", "condition_source_system", "condition_source_code",
        "condition_source_display", "condition_snomed_code", "condition_snomed_display", "condition_target_code",
        "condition_target_display", "category_display", "clinical_status_display", "verification_status_display",
        F.coalesce("onset_datetime", "event_datetime").alias("condition_datetime"))


def _match_outcome(c):
    oc = F.broadcast(_cfg("outcome_code")).alias("o")
    is_icd = F.col("x.condition_source_system") == ICD10
    by_icd = is_icd & F.coalesce(F.expr("x.condition_source_code RLIKE o.icd10_regex"), F.lit(False))
    by_code = F.coalesce(F.array_contains(F.col("o.snomed_codes"), F.col("x.condition_snomed_code")), F.lit(False))
    by_text = (~is_icd) & F.coalesce(F.expr(
        "lower(coalesce(x.condition_snomed_display, x.condition_target_display, x.condition_source_display)) RLIKE o.snomed_display_regex"), F.lit(False))
    return (c.alias("x").join(oc, by_icd | by_code | by_text, "inner")
             .select("x.*", "o.outcome_id", "o.outcome_name",
                     F.when(by_icd, "icd10_regex").when(by_code, "snomed_code").otherwise("snomed_display_regex").alias("outcome_match_method")))


@dp.materialized_view(name="pms_comorbidity", cluster_by=["person_id"],
    comment="All recorded conditions of cohort persons dated before the drug-set index (full history, not only the lookback).")
@dp.expect_all_or_drop(PERSON_RULE)
def pms_comorbidity():
    return _window(_conditions(), "condition_datetime", mode="history")


@dp.materialized_view(name="pms_outcome_condition", cluster_by=["person_id"],
    comment="Configured outcome conditions dated from index to last exposure + follow-up. prior_history_ind = the same "
            "outcome was also recorded before index (not incident). outcome_match_method records how the row matched.")
@dp.expect_all_or_drop(PERSON_RULE)
def pms_outcome_condition():
    matched = _match_outcome(_conditions())
    prior = (_window(matched, "condition_datetime", mode="history")
             .select("person_id", "drug_set_id", "outcome_id").distinct().withColumn("prior_history_ind", F.lit(True)))
    return (_window(matched, "condition_datetime", mode="after")
            .join(prior, ["person_id", "drug_set_id", "outcome_id"], "left")
            .withColumn("prior_history_ind", F.coalesce("prior_history_ind", F.lit(False))))


ADR_COLUMNS = ["person_id", "drug_set_id", "signal_source", "signal_record_key", "signal_datetime", "drug_display",
               "drug_code", "reaction_text", "severity_text", "status_text"]


def _datix_patients():
    return (spark.read.table(DATIX_PARTICIPANT)
            .where((F.col("participant_class") == "patient") & F.col("person_id").isNotNull() & F.coalesce("source_present_ind", F.lit(True)))
            .select("inc_id", F.col("person_id").cast("bigint").alias("person_id"), "lfpse_physical_harm").dropDuplicates(["inc_id", "person_id"]))


def _datix_incidents():
    return (spark.read.table(DATIX_INCIDENT).where(F.coalesce("source_present_ind", F.lit(True)))
            .select(F.col("incident_id").alias("inc_id"), "incident_dt_tm_clean", "inc_severity_display",
                    F.concat_ws(" / ", "inc_type_display", "inc_category_display", "inc_subcategory_display").alias("incident_class")))


@dp.materialized_view(name="pms_adr_signal", cluster_by=["person_id"],
    comment="Coded/structured adverse-reaction signals for the drug set at any time: order stop reasons indicating an ADR, "
            "allergy/intolerance records for the drug, Datix medication incident factors (lexicon-mapped) and LFPSE drug reactions.")
@dp.expect_all_or_drop(PERSON_RULE)
def pms_adr_signal():
    stops = (spark.read.table("pms_exposure")
             .where((F.col("exposure_source") == "order") & F.lower("stop_reason").rlike(ADR_STOP_REGEX))
             .select("person_id", "drug_set_id", F.lit("order_discontinuation").alias("signal_source"),
                     F.col("exposure_record_key").alias("signal_record_key"),
                     F.coalesce("exposure_end", "exposure_start").alias("signal_datetime"), "drug_display", "drug_code",
                     F.col("stop_reason").alias("reaction_text"), F.lit(None).cast("string").alias("severity_text"),
                     F.col("status").alias("status_text")))
    alg = spark.read.table(f"{G}.clinical_allergy_intolerance").where(~F.coalesce("absence_assertion_ind", F.lit(False)))
    alg = _drug_match(alg, ["substance_target_code", "substance_snomed_code"],
                      ["substance_target_display", "substance_source_display", "source_display"]).select(
        "person_id", "drug_set_id", F.lit("allergy_record").alias("signal_source"), F.col("patient_event_key").alias("signal_record_key"),
        F.col("event_datetime").alias("signal_datetime"),
        F.coalesce("substance_target_display", "substance_source_display", "source_display").alias("drug_display"),
        F.coalesce("substance_target_code", "substance_snomed_code").alias("drug_code"),
        F.concat_ws(" / ", "substance_type_display", "reaction_class_display").alias("reaction_text"),
        F.col("severity_display").alias("severity_text"), F.col("reaction_status_display").alias("status_text"))
    fac = spark.read.table(DATIX_FACTOR).where((F.col("factor_type") == "medication") & F.coalesce("source_present_ind", F.lit(True)))
    fac = (_drug_match(fac, ["factor_target_code"], ["factor_display"]).select("inc_id", "safety_incident_factor_key", "drug_set_id",
               "factor_display", "factor_target_code")
           .join(_datix_patients(), "inc_id").join(_datix_incidents(), "inc_id", "left")
           .select("person_id", "drug_set_id", F.lit("datix_medication_factor").alias("signal_source"),
                   F.col("safety_incident_factor_key").alias("signal_record_key"), F.col("incident_dt_tm_clean").alias("signal_datetime"),
                   F.col("factor_display").alias("drug_display"), F.col("factor_target_code").alias("drug_code"),
                   F.col("incident_class").alias("reaction_text"), F.col("inc_severity_display").alias("severity_text"),
                   F.lit(None).cast("string").alias("status_text")))
    lf = spark.read.table(DATIX_LFPSE).where(F.coalesce("source_present_ind", F.lit(True)))
    lf = (_drug_match(lf, [], ["lfpse_drugs_involved", "lfpse_drug_reaction"]).select("inc_id", "safety_incident_lfpse_key", "drug_set_id",
              "lfpse_drugs_involved", "lfpse_drug_reaction")
          .join(_datix_patients(), "inc_id").join(_datix_incidents(), "inc_id", "left")
          .select("person_id", "drug_set_id", F.lit("lfpse_drug_reaction").alias("signal_source"),
                  F.col("safety_incident_lfpse_key").alias("signal_record_key"), F.col("incident_dt_tm_clean").alias("signal_datetime"),
                  F.col("lfpse_drugs_involved").alias("drug_display"), F.lit(None).cast("string").alias("drug_code"),
                  F.col("lfpse_drug_reaction").alias("reaction_text"), F.col("lfpse_physical_harm").alias("severity_text"),
                  F.lit(None).cast("string").alias("status_text")))
    parts = [d.select(*[F.col(c).cast(t) for c, t in zip(ADR_COLUMNS,
             ["bigint", "string", "string", "string", "timestamp", "string", "string", "string", "string", "string"])])
             for d in (stops, alg, fac, lf)]
    return _window(reduce(lambda a, b: a.unionByName(b), parts), "signal_datetime", mode="all")

# COMMAND ----------

@dp.materialized_view(name="_pms_lab_stage", cluster_by=["person_id"],
    comment="Internal: numeric pathology results of cohort persons in the study window matched to configured analytes.")
@dp.expect_all_or_drop(PERSON_RULE)
def _pms_lab_stage():
    r = (spark.read.table(f"{G}.clinical_pathology_result")
         .where(F.col("value_number").isNotNull() & F.coalesce("preferred_result_ind", F.lit(True)))
         .select("person_id", "patient_event_key", "encounter_id", "event_datetime", "test_snomed_code",
                 F.lower(F.coalesce("test_target_display", "test_source_display")).alias("test_display"),
                 "value_number", "ucum_code", "unit_source_value", "reference_range_low", "reference_range_high", "interpretation_code"))
    w = _window(r, "event_datetime")  # cohort-restrict before the analyte match: 1.9B rows in, cohort-sized out
    an = F.broadcast(_cfg("analyte").select("analyte_id", "test_codes", "name_regex", "hy_law_role")).alias("a")
    by_code = F.coalesce(F.array_contains(F.col("a.test_codes"), F.col("x.test_snomed_code")), F.lit(False))
    by_text = F.coalesce(F.expr("x.test_display RLIKE a.name_regex"), F.lit(False))
    return (w.alias("x").join(an, by_code | by_text, "inner")
             .select("x.*", "a.analyte_id", "a.hy_law_role",
                     F.when(by_code, "test_code").otherwise("display_regex").alias("analyte_match_method")))


@dp.materialized_view(name="pms_lab_reference",
    comment="Derived upper limit of normal per analyte x UCUM unit: the modal reported reference_range_high across the "
            "cohort's results. Used only where a result carries no range of its own (~70% of pathology rows).")
def pms_lab_reference():
    return (spark.read.table("_pms_lab_stage").groupBy("analyte_id", "ucum_code").agg(
        F.expr("mode(reference_range_high)").alias("modal_uln"),
        F.count("*").alias("result_count"),
        F.avg(F.col("reference_range_high").isNotNull().cast("decimal(38,18)")).cast("double").alias("own_range_share")))


@dp.materialized_view(name="pms_lab", cluster_by=["person_id"],
    comment="Configured analyte results for cohort persons in the study window with ULN (own range, else modal) and x_uln.")
@dp.expect_all_or_drop(PERSON_RULE)
def pms_lab():
    s = spark.read.table("_pms_lab_stage")
    ref = spark.read.table("pms_lab_reference").select("analyte_id", F.col("ucum_code").alias("_u"), "modal_uln")
    x = s.join(ref, (s.analyte_id == ref.analyte_id) & s.ucum_code.eqNullSafe(ref._u), "left").drop(ref.analyte_id).drop("_u")
    uln = F.coalesce("reference_range_high", "modal_uln")
    return (x.withColumn("uln", uln)
             .withColumn("uln_source", F.when(F.col("reference_range_high").isNotNull(), "result")
                                        .when(F.col("modal_uln").isNotNull(), "modal").otherwise("none"))
             .withColumn("x_uln", F.when(uln > 0, F.col("value_number") / uln))
             .drop("modal_uln"))


HY_WINDOW_DAYS = 30


@dp.materialized_view(name="pms_hy_law_screen", cluster_by=["person_id"],
    comment="Hy's law screen after index, one row per episode. An episode is a run of ALT >= 3x ULN results with no gap over "
            "30 days, qualifying when a total bilirubin >= 2x ULN falls within +/-30 days of one of its ALT results. Peaks, "
            "result keys and the ALP (max x ULN within +/-30 days of the episode's ALT results) all belong to that episode. "
            "pattern = potential_hys_law (ALP < 2x ULN), cholestatic_pattern (>= 2x) or alt_bili_no_alp (ALP unmeasured).")
@dp.expect_all_or_drop(PERSON_RULE)
def pms_hy_law_screen():
    lab = spark.read.table("pms_lab").where("days_from_index >= 0 AND x_uln IS NOT NULL")
    key = ["person_id", "drug_set_id"]
    alt = lab.where("hy_law_role = 'alt' AND x_uln >= 3").select(*key, F.col("patient_event_key").alias("alt_key"),
        F.col("event_datetime").alias("alt_dt"), F.col("x_uln").alias("alt_x_uln"))
    w = Window.partitionBy(*key).orderBy("alt_dt", "alt_key")
    new_episode = F.coalesce(F.datediff("alt_dt", F.lag("alt_dt").over(w)) > HY_WINDOW_DAYS, F.lit(True))
    alt = alt.withColumn("episode_no", F.sum(new_episode.cast("int")).over(w.rowsBetween(Window.unboundedPreceding, 0)))
    bili = lab.where("hy_law_role = 'bilirubin' AND x_uln >= 2").select(*key, F.col("patient_event_key").alias("bili_key"),
        F.col("event_datetime").alias("bili_dt"), F.col("x_uln").alias("bili_x_uln"))
    alp = lab.where("hy_law_role = 'alp'").select(*key, F.col("patient_event_key").alias("alp_key"),
        F.col("event_datetime").alias("alp_dt"), F.col("x_uln").alias("alp_x_uln"))
    ep = key + ["episode_no"]
    near = lambda dt: F.abs(F.datediff(dt, "alt_dt")) <= HY_WINDOW_DAYS
    alt_g = alt.groupBy(*ep).agg(
        F.min("alt_dt").alias("first_alt_elevation_datetime"), F.max("alt_dt").alias("last_alt_elevation_datetime"),
        F.max("alt_x_uln").alias("peak_alt_x_uln"), F.max_by("alt_key", "alt_x_uln").alias("peak_alt_result_key"),
        F.count("*").alias("alt_elevation_count"))
    bili_g = (alt.join(bili, key).where(near("bili_dt")).groupBy(*ep).agg(
        F.max("bili_x_uln").alias("peak_bilirubin_x_uln"), F.max_by("bili_key", "bili_x_uln").alias("peak_bilirubin_result_key"),
        F.min("bili_dt").alias("first_bilirubin_elevation_datetime")))
    alp_g = (alt.join(alp, key).where(near("alp_dt")).groupBy(*ep).agg(
        F.max("alp_x_uln").alias("max_alp_x_uln"), F.max_by("alp_key", "alp_x_uln").alias("max_alp_result_key")))
    g = bili_g.join(alt_g, ep).join(alp_g, ep, "left")  # inner on bilirubin: only ALT+bilirubin episodes qualify
    return (g.withColumn("episode_key", F.concat_ws(":", "person_id", "drug_set_id", "episode_no"))
             .withColumn("pattern", F.when(F.col("max_alp_x_uln").isNull(), "alt_bili_no_alp")
                                     .when(F.col("max_alp_x_uln") < 2, "potential_hys_law").otherwise("cholestatic_pattern")))


@dp.materialized_view(name="pms_hy_law_summary", cluster_by=["person_id"],
    comment="Person x drug set rollup of pms_hy_law_screen episodes: ever_* flags per pattern, so a potential Hy's law "
            "episode is never hidden by a separate cholestatic one, plus the first potential Hy's law episode date.")
@dp.expect_all_or_drop(PERSON_RULE)
def pms_hy_law_summary():
    s = spark.read.table("pms_hy_law_screen")
    ever = lambda p: F.max((F.col("pattern") == p).cast("int")).cast("boolean")
    return s.groupBy("person_id", "drug_set_id").agg(
        F.count("*").alias("episode_count"),
        ever("potential_hys_law").alias("ever_potential_hys_law"), ever("cholestatic_pattern").alias("ever_cholestatic_pattern"),
        ever("alt_bili_no_alp").alias("ever_alt_bili_no_alp"),
        F.min(F.when(F.col("pattern") == "potential_hys_law", F.col("first_alt_elevation_datetime"))).alias(
            "first_potential_hys_law_datetime"))


@dp.materialized_view(name="pms_vital", cluster_by=["person_id"],
    comment="Body weight and height of cohort persons in the study window (for dose-per-kg and BMI).")
@dp.expect_all_or_drop(PERSON_RULE)
def pms_vital():
    names = F.create_map(*[x for k, v in VITAL_CODES.items() for x in (F.lit(k), F.lit(v))])
    v = (spark.read.table(f"{G}.clinical_vital_sign").where(F.col("vital_snomed_code").isin(list(VITAL_CODES)))
         .select("person_id", "patient_event_key", "encounter_id", "event_datetime", names[F.col("vital_snomed_code")].alias("vital_name"),
                 "value_number", "unit_source_value"))
    return _window(v, "event_datetime")


@dp.materialized_view(name="pms_score", cluster_by=["person_id"],
    comment="NEWS2 total scores of cohort persons in the study window (deterioration signal).")
@dp.expect_all_or_drop(PERSON_RULE)
def pms_score():
    s = (spark.read.table(f"{G}.clinical_clinical_score").where(F.col("score_name").isin(NEWS2_TOTAL))
         .select("person_id", "patient_event_key", "encounter_id", "event_datetime", F.lit("news2_total").alias("score"),
                 "value_number", "score_name"))
    return _window(s, "event_datetime")

