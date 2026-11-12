# Databricks notebook source
# MAGIC %md
# MAGIC # PMS pharma governance: column comments and ig tags
# MAGIC Runs after every `pms_pharma` pipeline update (job PMS_Datasets). Materialized-view column comments only land
# MAGIC on a first build, so every column is (re)commented and (re)tagged here; the run fails if any column has no text.
# MAGIC Comments come from this notebook's own text for derived columns, else from the gold column the value passes
# MAGIC through. Tags follow the device dataset's scheme: keys 4/2, dates and quasi-identifiers 3/2, the rest 1/1.

# COMMAND ----------

# INSR_W6_PMS_RBI_V1 (INSR_20261029 W6: PMS Pharma, PMS Device, RBI)
# Schema the pms_pharma pipeline publishes to (4_prod.pms_pharma in prod, 8_dev.pms in dev).
dbutils.widgets.text("schema", "4_prod.pms_pharma")
S = dbutils.widgets.get("schema")

# Every table the pipeline publishes, with the gold tables whose comments pass through (first listed wins).
SOURCES = {
    "pms_exposure": [],
    "pms_medication_detail": ["clinical_medication_order_attribute", "clinical_medication_admin", "clinical_medication_order"],
    "pms_exposure_summary": [],
    "pms_death": ["reference_person_death_evidence"],
    "pms_patient": ["reference_person_address", "spine_person"],
    "pms_encounter": ["spine_encounter"],
    "pms_concomitant_medication": ["clinical_medication_order"],
    "pms_comorbidity": ["clinical_condition"],
    "pms_outcome_condition": ["clinical_condition"],
    "pms_adr_signal": [],
    "_pms_lab_stage": ["clinical_pathology_result"],
    "pms_lab_reference": [],
    "pms_lab": ["clinical_pathology_result"],
    "pms_hy_law_screen": [],
    "pms_hy_law_summary": [],
    "pms_vital": ["clinical_vital_sign"],
    "pms_score": ["clinical_clinical_score"],
}

# COMMAND ----------

# Column text for values the pipeline derives or renames. Same name = same meaning in every table, except the
# few per-table overrides in TABLE_DERIVED below.
DERIVED = {
    "person_id": "Journey person identifier (gold spine_person.person_id); the only person key in the dataset.",
    "drug_set_id": "Configured drug set (drug_of_interest.drug_set_id) the row belongs to; one cohort per drug set.",
    "drug_set_ids": "Sorted drug sets the person is in the cohort for.",
    "drug_set_name": "Display name of the configured drug set.",
    "days_from_index": "Days from the person's drug-set index (first eligible exposure) to the row's date; negative = before index.",
    "period": "baseline (before index) | on_treatment (index to last eligible exposure) | post_treatment (after the last exposure, within follow-up).",
    # pms_exposure: one contract across orders, administrations, dispenses, homecare supply and SACT
    "exposure_source": "Feed the exposure record came from: order | administration | dispense | homecare_supply | sact.",
    "exposure_record_key": "Gold patient_event_key of the source record (unique per exposure_source).",
    "order_id": "Millennium ORDER_ID; on an administration it links to the order it fulfilled.",
    "encounter_id": "Millennium encounter the record belongs to.",
    "drug_code": "Drug code: OMOP-target, else SNOMED, else source code of the matched record.",
    "drug_display": "Drug name: OMOP-target, else source display of the matched record.",
    "exposure_start": "Start of the exposure: effective start (orders), performed time (administrations), event time (dispenses), supply start (homecare), start date (SACT).",
    "exposure_end": "End of the exposure where recorded: discontinued/projected stop (orders), event end, or SACT end/final treatment date.",
    "dose_value": "Per-dose amount: strength dose else volume dose (orders), administered dose, or SACT prescribed dose; see dose_basis.",
    "dose_unit": "Unit of dose_value.",
    "dose_basis": "What dose_value means: prescribed | administered | sact_prescribed.",
    "dose_text": "Free-text dose detail: order free-text dose, dispensed strength, homecare label directions or SACT regimen name.",
    "quantity_value": "Dispensed or delivered quantity (dispenses, homecare).",
    "quantity_unit": "Unit of quantity_value.",
    "route": "Route of administration as recorded.",
    "indication": "Indication recorded on the order, homecare supply or SACT record.",
    "stop_reason": "Discontinuation reason (orders) or no-further-delivery reason (homecare).",
    "status": "Source record status as recorded (dispenses: issue_type|issue_category|status_code).",
    "exposure_ineligible_reason": "Why the record is not an exposure (status:<value>, not_delivered, future_start); NULL when eligible.",
    "exposure_eligible_ind": "True when the record reached the patient; only eligible records set the index and cohort.",
    "source_feed": "Source feed of the gold record (cancer_treatment for SACT).",
    # pms_medication_detail
    "frequency_id": "Millennium frequency id of the order.",
    # pms_exposure_summary
    "index_datetime": "First eligible exposure to the drug set in any feed (the cohort index).",
    "last_exposure_datetime": "Last eligible exposure end (else start), capped at the build time so open-ended orders stop now.",
    "exposure_record_count": "Number of eligible exposure records.",
    "ineligible_record_count": "Number of the person's records for the drug set that never reached the patient.",
    "exposure_day_count": "Distinct days with an eligible exposure start.",
    "exposure_sources": "Sorted feeds contributing eligible exposures.",
    "first_indication": "Indication on the earliest eligible exposure that records one.",
    "first_route": "Route on the earliest eligible exposure that records one.",
    "order_record_count": "Eligible order records.",
    "administration_record_count": "Eligible administration records.",
    "dispense_record_count": "Eligible pharmacy dispense records.",
    "homecare_supply_record_count": "Eligible homecare supply records.",
    "sact_record_count": "Eligible SACT records.",
    # pms_death
    "death_date": "Date of death (London date): death evidence datetime, else its calculated death date, else spine deceased_datetime.",
    "death_date_basis": "Which source set death_date: death_evidence_datetime | death_evidence_calculated | spine_deceased_datetime.",
    "death_source": "Source of the death evidence record (gold reference_person_death_evidence.source_desc).",
    "death_date_precision": "Precision of the death evidence date (gold precision_desc).",
    "within_followup_ind": "True when death_date is on or before last exposure + follow-up days.",
    "death_precedes_index_ind": "True when death is dated more than a day before the first exposure (contradictory evidence, kept and flagged).",
    # pms_patient
    "sex": "gold spine_person.gender_display.",
    "year_of_birth": "Year of birth (from spine_person.birth_date).",
    "age_at_first_index": "Whole years from birth date to the person's first index in any drug set.",
    "ethnicity_code": "Ethnicity: OMOP-target code, else source code.",
    "ethnicity_display": "Ethnicity: OMOP-target display, else source display.",
    "gp_practice_ods_code": "ODS code of the current registered GP practice (gold reference_person_gp_registration).",
    "gp_registration_start": "Start of the current GP registration.",
    "first_index_datetime": "Earliest index_datetime across the person's drug sets.",
    # pms_encounter
    "encounter_class": "Encounter class (gold spine_encounter.type_class_display): Inpatient | Outpatient | Emergency | Recurring.",
    # pms_concomitant_medication
    "start_datetime": "Order effective start, else event time.",
    "end_datetime": "Order discontinued, else projected stop, else event end.",
    "is_drug_of_interest": "True when the order is itself an exposure to this drug set.",
    # conditions and outcomes
    "condition_datetime": "Condition onset, else recorded time.",
    "outcome_id": "Configured outcome (outcome_code.outcome_id).",
    "outcome_name": "Display name of the configured outcome.",
    "outcome_match_method": "How the condition matched the outcome: icd10_regex | snomed_code | snomed_display_regex.",
    "prior_history_ind": "True when the same outcome was also recorded before index (not incident).",
    # pms_adr_signal
    "signal_source": "order_discontinuation | allergy_record | datix_medication_factor | lfpse_drug_reaction.",
    "signal_record_key": "Key of the source record (order patient_event_key, allergy patient_event_key, Datix factor or LFPSE key).",
    "signal_datetime": "When the signal was recorded (order stop, allergy record, incident time).",
    "reaction_text": "Reaction as recorded: stop reason, allergy type/reaction class, Datix incident class or LFPSE reaction text.",
    "severity_text": "Severity as recorded (allergy severity, Datix severity, LFPSE physical harm).",
    "status_text": "Status as recorded (order status or allergy reaction status).",
    # labs
    "test_display": "Lower-cased test name: OMOP-target, else source display.",
    "analyte_id": "Configured analyte (analyte.analyte_id) the result matched.",
    "hy_law_role": "Role of the analyte in the Hy's law screen: alt | bilirubin | alp; NULL otherwise.",
    "analyte_match_method": "How the result matched the analyte: test_code | display_regex.",
    "uln": "Upper limit of normal used: the result's own reference_range_high, else the modal one for analyte x unit.",
    "uln_source": "result | modal | none.",
    "x_uln": "value_number / uln; NULL when no positive ULN.",
    "ucum_code": "UCUM unit of the results (gold clinical_pathology_result.ucum_code).",
    "modal_uln": "Most common reported reference_range_high for the analyte x unit across the cohort's results.",
    "result_count": "Number of cohort results for the analyte x unit.",
    "own_range_share": "Share of those results that carry their own reference range.",
    # Hy's law
    "episode_no": "Nth run of ALT >= 3x ULN results (gap <= 30 days) for the person x drug set after index.",
    "episode_key": "person_id:drug_set_id:episode_no.",
    "first_alt_elevation_datetime": "First ALT >= 3x ULN result of the episode.",
    "last_alt_elevation_datetime": "Last ALT >= 3x ULN result of the episode.",
    "peak_alt_x_uln": "Highest ALT x ULN in the episode.",
    "peak_alt_result_key": "patient_event_key of the peak ALT result.",
    "alt_elevation_count": "Number of ALT >= 3x ULN results in the episode.",
    "peak_bilirubin_x_uln": "Highest total bilirubin x ULN (>= 2) within +/-30 days of the episode's ALT results.",
    "peak_bilirubin_result_key": "patient_event_key of the peak bilirubin result.",
    "first_bilirubin_elevation_datetime": "First bilirubin >= 2x ULN within +/-30 days of the episode's ALT results.",
    "max_alp_x_uln": "Highest ALP x ULN within +/-30 days of the episode's ALT results; NULL when unmeasured.",
    "max_alp_result_key": "patient_event_key of that ALP result.",
    "pattern": "potential_hys_law (ALP < 2x ULN) | cholestatic_pattern (ALP >= 2x) | alt_bili_no_alp (ALP unmeasured).",
    "episode_count": "Qualifying Hy's law screen episodes for the person x drug set.",
    "ever_potential_hys_law": "Any episode with pattern potential_hys_law.",
    "ever_cholestatic_pattern": "Any episode with pattern cholestatic_pattern.",
    "ever_alt_bili_no_alp": "Any episode with pattern alt_bili_no_alp.",
    "first_potential_hys_law_datetime": "First ALT elevation of the first potential Hy's law episode.",
    # vitals and scores
    "vital_name": "body_weight (SNOMED 27113001) | body_height (50373000).",
    "score": "news2_total.",
    # INSR_W6 W6.03: new columns (L986, L1012, L1079, L906)
    "drug_code_system": "Code system of drug_code (the OMOP target system, http://snomed.info/sct, https://dmd.nhs.uk "
                        "or the source system), so codes from different vocabularies are never compared as one.",
    "ethnicity_code_system": "Code system of ethnicity_code: the OMOP target system, else the source system.",
    "test_display_source": "Which display test_display came from: target (OMOP target display) or source.",
    "future_start_ind": "True when the order starts after this build ran (a planned order, not yet an exposure).",
    "end_datetime_basis": "Which field gave end_datetime: discontinued | projected_stop | event_end; NULL when none.",
    "end_datetime_projected_ind": "True when end_datetime is a projected stop rather than a recorded discontinuation.",
    "measurement_basis": "measured (SNOMED 27113001 / 50373000) | estimated (OMOP 3026600 Weight Estimated / 3035463 "
                         "Height/Length Estimated).",
}
# INSR_W6 W6.03: coverage notes measured in INSR_20261029 W0 (values are 2026-10-29 estate figures).
DERIVED["exposure_source"] += (" Coverage differs by feed: administrations exist from 2021-08 (eMAR go-live, raw and "
                               "gold agree), and order-detail eras step in 2017-06, 2019-05 and 2021-09, so per-feed "
                               "counts are not comparable across those dates (L808).")
DERIVED["dose_value"] += (" NULL share among eligible records: administrations 0%; dispenses 100% (dispenses carry "
                          "quantity, not dose); orders 66% from 2022 and 90-98% before (free-text dose orders with no "
                          "structured dose, nothing recoverable from order attributes) (L835).")
DERIVED["vital_name"] = ("body_weight | body_height. Measured rows use SNOMED 27113001 / 50373000, estimated rows OMOP "
                         "3026600 / 3035463 (see measurement_basis); a charting change from 2024-04 roughly doubled "
                         "measured weight rows while estimated ones fell, so use both bases for trends (L906).")
DERIVED["gp_registration_start"] = ("Start of the current GP registration. 1948-07-04 23:00 UTC (the NHS founding day) "
                                    "is a placeholder that gold sets to NULL; 2014-05-08 and 2017-09-01 heaps are system "
                                    "cut-over effective dates, not registration starts (L1060).")
DERIVED["encounter_class"] += (" Admission source is filled on about 9.6% of encounters in raw and gold alike (source "
                               "capture); period_end and length of stay are NULL while the encounter is open (L1079).")
TABLE_DERIVED = {
    ("pms_concomitant_medication", "status"): "Order status as recorded.",
    ("pms_adr_signal", "drug_display"): "Drug as recorded on the signal (LFPSE: free-text drugs-involved list).",
    ("pms_adr_signal", "drug_code"): "Drug code of the signal where coded (SNOMED/dm+d); NULL for LFPSE.",
    ("pms_death", "days_from_index"): "Days from the drug-set index date to death_date.",
    # INSR_W8_COMMENTS_V1 W8.09: documented limitations (INSR_20261029 coverage register; W0 ledger figures).
    ("pms_lab", "uln_source"): "result | modal | none. Own ranges exist only on Cerner-linked results; the legacy LIMS feed carries no reference range, so most ULNs are the modal value (Hy's law screen relies on it).",
    ("pms_lab", "uln"): "Upper limit of normal used: the result's own reference_range_high (Cerner-linked results only), else the modal one for analyte x unit; NULL when neither exists.",
    ("pms_comorbidity", "condition_datetime"): "Condition onset, else recorded time. Condition history starts in about 2005; earlier years are sparse at source.",
    ("pms_comorbidity", "clinical_status_display"): "Clinical status as recorded; ICD-10 coded diagnoses carry no status at source, so it is set only on problem-list records (SNOMED and Cerner nomenclatures).",
    ("pms_comorbidity", "verification_status_display"): "Verification status as recorded; ICD-10 coded diagnoses carry none at source, so it is set only on problem-list records (SNOMED and Cerner nomenclatures).",
    ("pms_adr_signal", "signal_source"): "order_discontinuation (from 2021-11) | allergy_record (from 2014-11) | datix_medication_factor (from 2014-03) | lfpse_drug_reaction (from 2023-10). Adverse reactions are under-reported in every source.",
    ("pms_exposure", "dose_value"): "Per-dose amount (see dose_basis). Complete on administrations; on orders and dispenses it depends on the source record and may be NULL.",
    ("pms_exposure", "route"): "Route of administration as recorded; complete on administrations, sparser on orders and dispenses.",
}

# COMMAND ----------

# INSR_W6 W6.02 (J1, 2026-10-29): estate tags from the one shared helper (W7.02). shared_path points at the
# folder holding estate_tags.py (prod: Prod/Pipelines/_shared; dev: the CC staging copy).
import sys
dbutils.widgets.text("shared_path", "/Workspace/Shared/ADC-DB/Prod/Pipelines/_shared")
sys.path.insert(0, dbutils.widgets.get("shared_path"))
from estate_tags import estate_tags  # noqa: E402

ESTATE_TAGS = estate_tags(spark)  # column_name -> (ig_risk, ig_severity) as integers (W7.02 contract); see tag()

# ig tags for columns gold does not have (the pre-J1 classes): keys and record identifiers 4/2; every date/timestamp
# column, the area and practice codes, year of birth and free text (which can embed identifiers) 3/2; the rest 1/1.
TAG_4_2 = {"person_id", "encounter_id", "order_id", "patient_event_key", "exposure_record_key", "signal_record_key",
           "peak_alt_result_key", "peak_bilirubin_result_key", "max_alp_result_key", "episode_key"}
TAG_3_2 = {"lsoa", "gp_practice_ods_code", "year_of_birth", "dose_text", "reaction_text", "indication",
           "first_indication", "stop_reason", "prninstructions_display", "cause_of_death", "drug_display",
           "condition_source_display"}


def own_class(column, data_type):
    """The pre-J1 ig class of a column with no gold counterpart."""
    if column in TAG_4_2:
        return "4", "2"
    if column in TAG_3_2 or data_type in ("DATE", "TIMESTAMP", "TIMESTAMP_NTZ"):
        return "3", "2"
    return "1", "1"


def tag(column, data_type):
    """ig_risk / ig_severity for one column: the estate's gold tag when gold has the column name, else own_class.
    Always strings: estate_tags returns integers, UC tag values (and the before -> after diff) are strings."""
    if column in ESTATE_TAGS:
        return tuple(str(v) for v in ESTATE_TAGS[column])
    return own_class(column, data_type)


def gold_comments(tables):
    """Column -> comment from the listed gold tables; the first listed table wins."""
    if not tables:
        return {}
    rows = spark.sql("SELECT table_name, column_name, comment FROM 4_prod.information_schema.columns "
                     "WHERE table_schema = 'gold' AND comment IS NOT NULL AND comment <> '' AND table_name IN (%s)"
                     % ", ".join(f"'{t}'" for t in tables)).collect()
    out = {}
    for t in tables:
        for r in rows:
            if r.table_name == t:
                out.setdefault(r.column_name, r.comment)
    return out


def sql_text(s):
    """Escape a comment for a single-quoted SQL literal."""
    return s.replace("\\", "\\\\").replace("'", "\\'")

# COMMAND ----------

cat, sch = S.split(".")  # INCR_FIX_20261018: no published-table gate here (removed at Ben's direction)


def current_tags():
    """(table, column) -> (ig_risk, ig_severity) as tagged now, for the before -> after diff (INSR_W6 W6.02)."""
    out = {}
    for r in spark.sql(f"SELECT table_name, column_name, tag_name, tag_value FROM `{cat}`.information_schema.column_tags "
                       f"WHERE schema_name = '{sch}' AND tag_name IN ('ig_risk', 'ig_severity')").collect():
        out.setdefault((r.table_name, r.column_name), {})[r.tag_name] = r.tag_value
    return {k: (v.get("ig_risk"), v.get("ig_severity")) for k, v in out.items()}


before_tags = current_tags()
missing, changed = [], []
for t, sources in SOURCES.items():
    src = gold_comments(sources)
    cols = spark.sql(f"SELECT column_name, upper(data_type) data_type FROM `{cat}`.information_schema.columns "
                     f"WHERE table_schema = '{sch}' AND table_name = '{t}'").collect()
    for c in cols:
        text = TABLE_DERIVED.get((t, c.column_name)) or DERIVED.get(c.column_name) or src.get(c.column_name)
        if not text:
            missing.append(f"{t}.{c.column_name}")
            continue
        spark.sql(f"COMMENT ON COLUMN {S}.`{t}`.`{c.column_name}` IS '{sql_text(text)}'")
        r, s = tag(c.column_name, c.data_type)
        if before_tags.get((t, c.column_name)) != (r, s):
            changed.append((t, c.column_name, before_tags.get((t, c.column_name)), (r, s)))
        spark.sql(f"ALTER MATERIALIZED VIEW {S}.`{t}` ALTER COLUMN `{c.column_name}` "
                  f"SET TAGS ('ig_risk' = '{r}', 'ig_severity' = '{s}')")
print(f"tag changes ({len(changed)}):")
for row in changed:
    print("  ", row)
assert not missing, f"columns without a comment: {missing}"
dbutils.notebook.exit(f"governed {len(SOURCES)} tables in {S}; {len(changed)} tag changes")

