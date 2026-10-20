# Databricks notebook source
# MAGIC %md
# MAGIC # PMS device governance: column comments and ig tags
# MAGIC Runs after every `pms_device` pipeline update (job PMS_Datasets). Materialized-view column comments only land
# MAGIC on a first build, so every column is (re)commented and (re)tagged here; the run fails if any column has no text.

# COMMAND ----------

import re
from pyspark.sql import functions as F

# Schema the pms_device pipeline publishes to (4_prod.pms_device in prod, 8_dev.device_pms in dev).
dbutils.widgets.text("schema", "4_prod.pms_device")
S = dbutils.widgets.get("schema")
TABLES = ["pms_device_exposure", "pms_procedure_context", "pms_patient", "pms_baseline", "pms_outcome_event",
          "pms_patient_evidence", "pms_followup", "pms_registry_device_procedure", "pms_dataset"]
# Gold tables whose column comments pass through (first match wins).
SOURCES = {"pms_device_exposure": ["clinical_procedure", "reference_device_mapping"],
           "pms_procedure_context": ["reference_theatre_case"], "pms_patient": ["spine_person", "reference_person_address"],
           "pms_baseline": ["clinical_form_smoking_status"], "pms_outcome_event": [], "pms_patient_evidence": [],
           "pms_followup": [], "pms_registry_device_procedure": [], "pms_dataset": []}
DERIVED = {
    "exposure_id": "One implanted device: gold clinical_procedure.patient_event_key of the implant row.",
    "implant_event_id": "Millennium implant EVENT_ID (map_implant_details) for the device row.",
    "index_datetime": "Implant date-time as recorded (gold performed_start); may be epoch-truncated, see index_date_status.",
    "index_date_status": "recorded | epoch_truncated (1970 raw-load truncation) | missing.",
    "index_date": "Implant date; NULL when index_date_status is not recorded, so no window starts in 1970.",
    "udi_capture_evidence": "gs1_scan | hibcc_scan | udi_other | manual_or_none, from the implant row's UDI standard and DI.",
    "implant_procedure_code": "Primary procedure recorded on the implant row.",
    "implant_procedure_display": "Primary procedure description recorded on the implant row.",
    "implanter_practitioner_id": "Native Millennium personnel id recorded as performer on the implant row.",
    "laterality_snomed_code": "SNOMED side from OPCS Z94.1/.2/.3 in the implant encounter within a day; NULL when none or conflicting.",
    "laterality_display": "Left | Right | Bilateral, from laterality_snomed_code.",
    "laterality_method": "opcs_z94_encounter | opcs_z94_conflict | opcs_z94_unilateral_unspecified | opcs_z94_other | none.",
    "rules_version": "Version of the PMS derivation rules that produced the row.",
    "case_link_method": "How the theatre case was linked: implant_log_crosswalk:<method> | encounter_time_window | person_same_day | none.",
    "case_link_rank": "1 crosswalk, 2 encounter+time window, 3 person+same day; the best available rank was used.",
    "case_link_ambiguous_ind": "True when more than one case matched at the chosen rank; nearest start was kept.",
    "operator_practitioner_id": "Case surgeon, else the implant performer.",
    "operator_device_type_case_ordinal": "Operator's Nth case with this GMDN device type in the Barts record (left-censored learning-curve covariate).",
    "exposure_count": "Number of device exposures for the person.",
    "first_index_date": "Earliest recorded implant date for the person.",
    "sex": "gold spine_person.gender_display.",
    "age_at_first_index": "Whole years from birth_date to first_index_date; NULL outside 0-110 (rule age_plausible_0_110, e.g. placeholder birth date 1910-01-01).",
    "imd_method": "in_force_at_index | latest_on_record | none: which address supplied LSOA/IMD.",
    "index_encounter_icd10_codes": "Sorted distinct ICD-10 codes recorded in the implant encounter (indication context).",
    "bmi": "Latest BMI (SNOMED 60621009) within 365 days before index, 10-80.",
    "bmi_date": "Date of bmi.", "weight_kg": "Latest body weight (27113001) within 365 days before index, 20-350 kg.",
    "weight_kg_date": "Date of weight_kg.", "height_cm": "Latest body height (50373000) within 365 days before index, in cm (m values x100), 90-250.",
    "height_date": "Date of height_cm.",
    "outcome_type": "Outcome category; see rule_id for the exact surveillance rule.",
    "rule_id": "Stable id of the surveillance rule that produced the event (documented in the PMS execution plan).",
    "event_datetime": "When the outcome event was recorded.", "source_table": "Gold table the event came from.",
    "source_key": "Code or identifier of the source row (ICD-10 code, incident id, encounter id, ...).",
    "detail": "Short source detail (code, organism | specimen, severity, ...).",
    "in_index_encounter_ind": "True when the event was recorded in the implant encounter.",
    "days_from_index": "event date minus index_date; negative only for device-serial Datix matches (timing = before).",
    "last_contact_date": "Latest completed Barts contact (not preadmit/cancelled/pending) on or before data_as_of.",
    "death_date": "Date of death from the spine, on or before data_as_of.",
    "followup_end_date": "death_date, else the later of last_contact_date and index_date, capped at data_as_of.",
    "censor_reason": "death | death_before_index (recorded death precedes the implant: contradictory, no follow-up) | last_contact | data_as_of (implant after the cutoff) | index_only.",
    "followup_days": "followup_end_date minus index_date; NULL for implants after data_as_of or when death precedes the implant.",
    "data_as_of": "Last complete UTC day before the pipeline update ran; nothing later is counted.",
    "site_families": "OPCS site families (hip, knee, shoulder, cardiac_rhythm, heart_valve, coronary_stent, hernia_mesh, breast, lens) coded in the implant encounter within a day.",
    "implant_size": "Implant SIZE attribute; latest by valid_from when several values were recorded.",
    "implant_expiry_raw": "Raw EXPIRY_DATE_RAW attribute value.", "implant_expiry_date": "Parsed device expiry date.",
    "implant_sterilisation_raw": "Raw STERILISATION_DATE_RAW attribute value.",
    "implant_sterilisation_date": "Parsed device sterilisation date.",
    "implant_expired_at_index_ind": "True when the implant date is after the recorded expiry date; NULL if either is unknown.",
    "attribute_conflicts": "Other recorded values (ATTRIBUTE=value) that lost to the latest for size/expiry/sterilisation.",
    "same_day_date_only_icd10_codes": "ICD-10 codes recorded date-only on the implant day: cannot be ordered against the implant, so not used as baseline.",
    "smoking_status_date": "Date of the latest structured smoking form in the 5 years before the implant time.",
    "timing": "after | before | same_day_date_only, comparing the event timestamp with the implant timestamp.",
    "attribution": "How the event was tied to this exposure: person | site_side | site_side_unknown | same_gmdn_side | serial_person | serial_manufacturer | serial_only | rejected_person_conflict | rejected_manufacturer_conflict.",
    "candidate_count": "Distinct prior operations (revision/repeat) or persons (serial) the evidence could belong to; >1 = ambiguous.",
    "counted_ind": "True when the row feeds pms_dataset: timing after, accepted attribution, candidate_count <= 1.",
    "evidence_type": "Kind of patient-level evidence (revision_procedure).",
    "procedure_code": "OPCS code of the revision procedure.", "procedure_display": "OPCS description of the revision procedure.",
    "site_family": "OPCS or registry site family of the procedure; NULL when none of the families apply.",
    "unattributed_reason": "no_site_family | no_matching_prior_exposure.",
    "registry_key": "iWeb registry and entry id (registry:entry_id).", "registry": "iweb_coronary | iweb_noncoronary | iweb_surgery_valve.",
    "procedure_datetime": "Registry procedure date-time.", "device_text": "Registry free-text list of implanted devices.",
    "stents_used": "PCI: number of stents used.", "drug_eluting_stents_used": "PCI: number of drug-eluting stents.",
    "drugs_eluted": "PCI: drugs eluted by the stents.", "valve_implant_name": "Surgical valve/ring implant name.",
    "valve_model_number": "Surgical valve/ring model number.", "valve_serial_number": "Surgical valve/ring serial number.",
    "valve_size": "Surgical valve/ring size.", "valve_implant_type": "Surgical valve implant type.",
    "valve_explant_type": "Type of valve explanted (Native, Biological, Mechanical, ...).",
    "registry_device_failure_raw": "Registry device-failure answer as recorded.",
    "registry_device_failure": "Registry device failure (rule iweb_device_failure): Probably intrinsic | Probably iatrogenic; in pms_dataset the sorted set over linked registry procedures.",
    "linked_exposure_ids": "Exposures of the same person on the same day with a compatible site family.",
    "link_method": "person_same_day_site | none.", "registry_keys": "Registry procedures linked to this exposure.",
    "n_pre_index_device_serial_incidents": "Datix incidents naming this device serial before implant (accepted attribution); never counted as outcomes.",
    "first_pre_index_device_serial_days": "Earliest pre-implant serial incident, days from index (negative).",
    "n_same_day_date_only_events": "Outcome-type events recorded date-only on the implant day (not counted).",
    "n_patient_revisions_unattributed_after_index": "Unattributed revisions (pms_patient_evidence) for the person after this implant; context only.",
    "datix_person_link_rate": "Share of Datix patient incidents (PATIE1/PATIE2) linked to a person; safety_incident_person columns are NULL below 0.8.",
}
for n in ("haemoglobin", "albumin", "creatinine", "egfr", "hba1c", "crp", "wbc"):
    DERIVED[f"preop_{n}"] = f"Latest preferred {n} result (unit-filtered) in the 90 days before the implant time."
    DERIVED[f"preop_{n}_date"] = f"Date of preop_{n}."
for k in ("diabetes", "obesity", "chronic_kidney_disease", "heart_failure", "ischaemic_heart_disease",
          "cerebrovascular_disease", "peripheral_vascular_disease", "copd", "rheumatoid_arthritis", "osteoporosis",
          "malignancy", "liver_disease", "dementia", "immunodeficiency", "smoking", "anticoagulation_long_term"):
    DERIVED[f"cm_{k}"] = f"ICD-10 rule cm_{k} coded in the 5 years before index (false = no coded record, not absence)."
OUT_TYPES = ["infection", "mechanical_complication", "other_device_complication", "periprosthetic_fracture",
             "microbiology_isolate", "revision_procedure", "repeat_same_gmdn_implant", "safety_incident_person",
             "safety_incident_device_serial", "readmission", "ed_attendance", "icu_admission", "death"]
for t in OUT_TYPES:
    DERIVED[f"first_{t}_days"] = f"Days from index to the first {t} event (pms_outcome_event)."
    DERIVED[f"n_{t}"] = f"Number of {t} events for the exposure."
    for h in (30, 90, 365):
        DERIVED[f"{t}_{h}d"] = f"{t} within {h} days of index; false if none and follow-up >= {h} days; NULL if follow-up is shorter."

# Phase 1 columns: gold carries these comments only after the Task 7 apply (texts copied from the PMS-*-gold
# components); until then the gold source has no comment, so they are supplied here. Gold comments win once live.
PHASE1_COMMENTS = {
 "add_on_ind": "SurgiNet add-on flag: case added to a list after scheduling (emergency/urgent proxy).",
 "anaesthesia_type_display": "Anaesthesia type description.",
 "anaesthetist_practitioner_id": "Native Millennium personnel PERSON_ID of the anaesthetist.",
 "asa_class_display": "ASA physical status class description.",
 "case_level_display": "Case level description.",
 "case_status": "Case status derived in bronze: PERFORMED, CANCELLED or SCHEDULED_ONLY.",
 "device_snomed_code": "SNOMED CT device concept mapped for the implant (gold clinical_procedure device coding).",
 "device_snomed_display": "Preferred term of device_snomed_code.",
 "ethnicity_snomed_code": "SNOMED CT ethnicity concept from the spine person record.",
 "gmdn_code_status": "GMDN reference status (active/obsolete) of the mapped term.",
 "gmdn_implantable": "GMDN reference implantable flag for the mapped GMDN term, carried from bronze; null when the term has no reference row.",
 "gudid_brand_name": "GUDID brand name for the matched device identifier.",
 "gudid_catalogue_number": "GUDID catalogue number for the matched device identifier.",
 "gudid_company_name": "GUDID labeler (company) name for the matched device identifier.",
 "gudid_device_status": "GUDID device record status for the matched identifier.",
 "gudid_model_number": "GUDID version or model number for the matched device identifier.",
 "gudid_primary_di": "FDA GUDID primary device identifier matched for this implant; null when no GUDID match.",
 "implant_expected_ind": "True when any active procedure on the case is implant-flagged at scheduling or in the procedure catalogue; null when the case has no procedure rows.",
 "implant_expected_source": "scheduled_procedure when any procedure was implant-flagged at scheduling, else procedure_catalogue; null when not expected.",
 "institution_display": "Institution (hospital site) description.",
 "mapped_device_name": "Device name chosen by the mapping (GUDID or GMDN side) for display.",
 "operating_location_display": "Operating room description.",
 "patient_type_display": "Patient type description (inpatient, day case, ...).",
 "scheduled_duration_minutes": "Scheduled case duration in minutes.",
 "smoking_status_snomed_code": "SNOMED CT smoking status from the latest structured smoking form in the 5 years before the implant time.",
 "smoking_status_snomed_display": "Preferred term of smoking_status_snomed_code.",
 "source_catalogue_number": "Catalogue number recorded in SurgiNet for this implant, as entered.",
 "source_manufacturer": "Manufacturer recorded in SurgiNet for this implant, as entered.",
 "surgeon_practitioner_id": "Native Millennium personnel PERSON_ID of the primary surgeon.",
 "surgery_duration_minutes": "Surgery stop minus start in minutes, as derived in bronze.",
 "surgery_start": "Surgery start as a UTC instant (Millennium); use surgery_start_local for Europe/London wall-clock time.",
 "surgery_stop": "Surgery stop as a UTC instant (Millennium).",
 "surgical_case_number": "Formatted SurgiNet case number as displayed to staff.",
 "surgical_specialty_id": "SurgiNet surgical specialty (prsnl_group) id.",
 "theatre_case_key": "SurgiNet SURG_CASE_ID as string; the same value children carry in theatre_case_key.",
 "wound_class_display": "Wound class description (clean, clean-contaminated, ...)."
}

TAG_4_2 = {"nhs_number", "person_id", "encounter_id", "exposure_id", "implant_event_id", "serial_number",
           "batch_number", "theatre_case_key", "surgical_case_number", "source_key", "valve_serial_number",
           "registry_key", "registry_keys", "linked_exposure_ids"}
TAG_3_2 = {"birth_date", "lsoa", "death_date", "deceased_datetime", "index_datetime", "index_date", "first_index_date",
           "event_datetime", "last_contact_date", "followup_end_date", "surgery_start", "surgery_stop",
           "bmi_date", "weight_kg_date", "height_date", "detail", "procedure_datetime", "device_text",
           "smoking_status_date"} | {f"preop_{n}_date" for n in ("haemoglobin", "albumin", "creatinine", "egfr",
                                                                    "hba1c", "crp", "wbc")}
TAG_3_1 = {"implanter_practitioner_id", "operator_practitioner_id", "surgeon_practitioner_id",
           "anaesthetist_practitioner_id"}


def tag(c):
    if c in TAG_4_2: return 4, 2
    if c in TAG_3_2: return 3, 2
    if c in TAG_3_1: return 3, 1
    return 1, 1


def gold_comments(tables):
    if not tables:
        return {}
    rows = spark.sql("SELECT table_name, column_name, comment FROM 4_prod.information_schema.columns "
                     "WHERE table_schema = 'gold' AND table_name IN (%s) AND comment IS NOT NULL"
                     % ", ".join(f"'{t}'" for t in tables)).collect()
    out = {}
    for t in tables:  # first listed table wins
        for r in rows:
            if r.table_name == t:
                out.setdefault(r.column_name, r.comment)
    return out


def lit(s):
    return s.replace("\\", "\\\\").replace("'", "\\'")


known = {}
missing = []
for t in TABLES:
    T = f"{S}.{t}"
    src = gold_comments(SOURCES[t])
    for c in spark.table(T).columns:
        text = DERIVED.get(c) or src.get(c) or known.get(c) or PHASE1_COMMENTS.get(c)
        if not text:
            missing.append(f"{t}.{c}")
            continue
        known.setdefault(c, text)
        spark.sql(f"COMMENT ON COLUMN {T}.`{c}` IS '{lit(text)}'")
        r, s = tag(c)
        spark.sql(f"ALTER MATERIALIZED VIEW {T} ALTER COLUMN `{c}` SET TAGS ('ig_risk' = '{r}', 'ig_severity' = '{s}')")
assert not missing, f"columns without a comment: {missing}"
print("governed", len(TABLES), "tables")

