# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Care Pathways
# MAGIC Appointments, referrals, waiting lists, RTT, community care and costing.
# MAGIC
# MAGIC Numbers indicate reading order; Lakeflow uses dataset dependencies to schedule work.
# MAGIC Mandatory rules use `dp.expect_all_or_drop`; advisory rules use `dp.expect_all`.
# MAGIC Repairs and nulling stay in the projection. Public names and identifiers are preserved.
# MAGIC Helpers are in `gold_journey_shared.py`, an ordinary Python file.

# COMMAND ----------

from gold_journey_shared import (
    _n,
    _qc,
    _src,
    _with_comments,
    _with_parent_status,
    dp,
)

# COMMAND ----------

# ==== journey_clinical.appointment ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_APPOINTMENT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`sch_event_id` AS `sch_event_id`',
    '`recurrence_parent_sch_event_id` AS `recurrence_parent_sch_event_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = '0' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    "CASE WHEN UPPER(TRIM(CAST(`appointment_type_code` AS STRING))) = '0' THEN NULL ELSE `appointment_type_code` END AS `appointment_type_code`",
    '`appointment_type_display` AS `appointment_type_display`',
    "CASE WHEN UPPER(TRIM(CAST(`status_code` AS STRING))) = '0' THEN NULL ELSE `status_code` END AS `status_code`",
    '`status_display` AS `status_display`',
    '`status_meaning` AS `status_meaning`',
    '`referral_identifier` AS `referral_identifier`',
    '`requested_datetime` AS `requested_datetime`',
    '`original_requested_start` AS `original_requested_start`',
    "CASE WHEN `original_requested_start` IS NOT NULL AND `original_requested_end` IS NOT NULL AND `original_requested_start` > `original_requested_end` THEN NULL ELSE CASE WHEN CAST(`original_requested_end` AS DATE) = DATE'2100-12-31' THEN NULL ELSE `original_requested_end` END END AS `original_requested_end`",
    '`first_booked_datetime` AS `first_booked_datetime`',
    '`requested_practitioner_id` AS `requested_practitioner_id`',
    '`allocated_practitioner_id` AS `allocated_practitioner_id`',
    '`location_code` AS `location_code`',
    '`organization_id` AS `organization_id`',
    '`recurrence_parent_key` AS `recurrence_parent_key`',
    '`recurrence_type_flag` AS `recurrence_type_flag`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_appointment_metadata; source history stays on the main research table.
CLINICAL_APPOINTMENT_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 31,446,236 rows of
    # 32,293,081 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_APPOINTMENT_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 846,845 of 32,293,081 at the profile.
    "gold.clinical.appointment.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 64,860 of 32,293,081 rows (0.201%) when profiled on 2026-08-24.
    "gold.clinical.appointment.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 451 of 32,293,081 rows (0.0014%) when profiled on 2026-08-24.
    "gold.clinical.appointment.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_APPOINTMENT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "sch_event_id": "Millennium SCH_EVENT_ID; primary key of this table.",
    "recurrence_parent_sch_event_id": "Millennium parent SCH_EVENT_ID for recurring appointments when supplied.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID when supplied; native encounter foreign key.",
    "event_datetime": "Booked slot start where available",
    "event_end_datetime": "Booked slot end where available.",
    "source_coding_system": "Verbatim appointment-type coding system.",
    "source_code": "Verbatim appointment-type code.",
    "source_display": "Verbatim appointment-type display.",
    "appointment_type_code": "Source appointment-type code.",
    "appointment_type_display": "Source appointment-type display.",
    "status_code": "Source scheduling-state code.",
    "status_display": "Source scheduling-state display.",
    "status_meaning": "Source scheduling-state meaning.",
    "referral_identifier": "UBRN or other referral alias retained as linkage evidence only.",
    "requested_datetime": "Source referral/request timestamp.",
    "original_requested_start": "Original requested slot start.",
    "original_requested_end": "Original requested slot end.",
    "first_booked_datetime": "First booking timestamp supplied by scheduling.",
    "requested_practitioner_id": "Millennium personnel PERSON_ID for the requested practitioner.",
    "allocated_practitioner_id": "Millennium personnel PERSON_ID for the representative allocated practitioner.",
    "location_code": "Millennium scheduling location code.",
    "organization_id": "Millennium ORGANIZATION_ID for the scheduling organization.",
    "recurrence_parent_key": "Deterministic SHA-256 key of the parent appointment.",
    "recurrence_type_flag": "Raw source recurrence flag.",
    "confidentiality_code": "Source confidentiality code when supplied.",
    "vip_ind": "Source VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld-identity indicator when supplied.",
    "fact_category": "Whether this fact is clinical or administrative in the v2 plane merge.",
    "source_feed": "Registered feed owning the fact.",
    "record_status": "Superseded when map_appointment.SOURCE_PRESENT_IND cast to Boolean is false, ACTIVE_IND cast to LONG is zero, or a non-null END_EFFECTIVE_DT_TM is before 2100-01-01; otherwise active. Missing presence defaults true and missing activity defaults 1. Source absence does not emit retracted; scheduling status and booking/resource flags are separate.",
    "record_status_effective_from": "map_appointment.BEG_EFFECTIVE_DT_TM carried unchanged. No scheduled-slot, requested-start, first-booked or ingestion-time fallback is applied; a missing value remains null.",
    "record_status_effective_to": "For superseded appointment rows, first non-null SOURCE_ABSENT_DETECTED_TS, END_EFFECTIVE_DT_TM, then ADC_UPDT; otherwise null. The selected end is not range-clamped, so an inactive/absent row can retain a far-future end. This is row-status history, not the scheduled slot end.",
    "source_update_timestamp": "Greatest of map_appointment.SOURCE_ADC_UPDT and the per-SCH_EVENT_ID maxima of SOURCE_ADC_UPDT from map_appointment_schedule and map_appointment_resource. All contributing booking/resource rows participate, not only current rows or those supplying a chosen slot/location. No ADC_UPDT ingestion fallback or Silver refresh timestamp is added.",
    "loaded_at": "Greatest of map_appointment.ADC_UPDT and the per-SCH_EVENT_ID maxima of ADC_UPDT from map_appointment_schedule and map_appointment_resource. This combines all contributing appointment/booking/resource load clocks, independently of chosen slot times, location or personnel; it is bronze ingestion provenance, not Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_appointment"),
    comment="Internal quality-controlled twin of clinical_appointment: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_APPOINTMENT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_APPOINTMENT_ADVISORY_RULES)
def _gold_qc_clinical_appointment():
    """Quality-controlled twin of journey_clinical.appointment."""
    df = _qc("clinical_appointment", CLINICAL_APPOINTMENT_SELECT)
    return _with_comments(df, CLINICAL_APPOINTMENT_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.appointment"),
    comment=(
        "One scheduling appointment with current state and ordered booking/resource history. "
        "Gold QC twin of the silver product: 7 columns are repaired or nulled, 1 rule(s) drop "
        "rows, 3 check(s) are advisory. Each rule states its reason in the pipeline notebook, "
        "and Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_appointment():
    """Contract-v2 public twin of clinical_appointment; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_appointment")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`sch_event_id` AS `sch_event_id`',
        '`recurrence_parent_sch_event_id` AS `recurrence_parent_sch_event_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`appointment_type_code` AS `appointment_type_code`',
        '`appointment_type_display` AS `appointment_type_display`',
        '`status_code` AS `status_code`',
        '`status_display` AS `status_display`',
        '`status_meaning` AS `status_meaning`',
        '`referral_identifier` AS `referral_identifier`',
        '`requested_datetime` AS `requested_datetime`',
        '`original_requested_start` AS `original_requested_start`',
        '`original_requested_end` AS `original_requested_end`',
        '`first_booked_datetime` AS `first_booked_datetime`',
        '`requested_practitioner_id` AS `requested_practitioner_id`',
        '`allocated_practitioner_id` AS `allocated_practitioner_id`',
        '`location_code` AS `location_code`',
        '`organization_id` AS `organization_id`',
        '`recurrence_parent_key` AS `recurrence_parent_key`',
        '`recurrence_type_flag` AS `recurrence_type_flag`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_APPOINTMENT_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.community_care_activity ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_COMMUNITY_CARE_ACTIVITY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`community_care_activity_key` AS `community_care_activity_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    "CASE WHEN CAST(`care_activity_date` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`care_activity_date` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`care_activity_date` AS DATE)) < 9999 THEN NULL ELSE `care_activity_date` END END AS `care_activity_date`",
    '`care_activity_date_quality_status` AS `care_activity_date_quality_status`',
    '`community_contact_key` AS `community_contact_key`',
    '`contact_match_status` AS `contact_match_status`',
    '`contact_candidate_count` AS `contact_candidate_count`',
    '`same_date_contact_candidate_count` AS `same_date_contact_candidate_count`',
    '`community_database_id` AS `community_database_id`',
    '`care_activity_id` AS `care_activity_id`',
    '`community_patient_key` AS `community_patient_key`',
    '`service_id` AS `service_id`',
    '`service_name` AS `service_name`',
    '`care_professional_local_id` AS `care_professional_local_id`',
    '`duration_minutes` AS `duration_minutes`',
    '`duration_quality_status` AS `duration_quality_status`',
    '`source_clinical_term` AS `source_clinical_term`',
    '`source_clinical_term_key` AS `source_clinical_term_key`',
    '`observation_type_id` AS `observation_type_id`',
    '`code_category_id` AS `code_category_id`',
    '`observation_value_raw` AS `observation_value_raw`',
    '`observation_value_numeric` AS `observation_value_numeric`',
    '`unit_source_value` AS `unit_source_value`',
    '`normalized_ucum_code` AS `normalized_ucum_code`',
    '`unit_concept_id` AS `unit_concept_id`',
    '`unit_concept_name` AS `unit_concept_name`',
    '`unit_mapping_status` AS `unit_mapping_status`',
    '`unit_mapping_method` AS `unit_mapping_method`',
    '`snomed_candidate_count` AS `snomed_candidate_count`',
    '`snomed_candidate_concept_id` AS `snomed_candidate_concept_id`',
    '`snomed_candidate_code` AS `snomed_candidate_code`',
    '`snomed_candidate_name` AS `snomed_candidate_name`',
    '`snomed_candidate_domain` AS `snomed_candidate_domain`',
    '`snomed_candidate_class` AS `snomed_candidate_class`',
    '`snomed_candidate_method` AS `snomed_candidate_method`',
    '`snomed_candidate_status` AS `snomed_candidate_status`',
    '`person_match_status` AS `person_match_status`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_community_care_activity_metadata; source history stays on the main research table.
CLINICAL_COMMUNITY_CARE_ACTIVITY_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 15,436,295 rows of 15,483,317
    # that are current and attributable; identity_status = 'resolved' keeps the 15,293,141
    # rows of 15,483,317 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_COMMUNITY_CARE_ACTIVITY_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 190,176 of 15,483,317 at the profile.
    "gold.clinical.community_care_activity.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 47,022 of 15,483,317 at the profile.
    "gold.clinical.community_care_activity.record_status.default_view_active":
        "record_status = 'active'",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 2 of 15,483,317
    # rows (1.29e-05%) when profiled on 2026-08-24.
    "gold.clinical.community_care_activity.record_status_effective_from.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 7,362 of 15,483,317 rows (0.0475%) when profiled on 2026-08-24.
    "gold.clinical.community_care_activity.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 99 of 15,483,317 rows (0.000639%) when profiled on 2026-08-24.
    "gold.clinical.community_care_activity.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_COMMUNITY_CARE_ACTIVITY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "community_care_activity_key": "Source-preserved community care activity key; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Content-addressed key identifying the community patient, derived from the source patient number and registering source database identifier in the CSDS patient identifier lookup.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date on which the care activity was recorded as taking place.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each community care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "CSDS care activity type code recorded for the activity in the source extract.",
    "source_code": "CSDS care activity type code recorded for the activity in the source extract.",
    "source_display": "Description corresponding to the CSDS care activity type code.",
    "care_activity_date": "Date on which the care activity was recorded as taking place.",
    "care_activity_date_quality_status": "Derived data-quality flag describing the state of the source care activity date, such as whether it is absent or falls within an acceptable range.",
    "community_contact_key": "Deterministic SHA-256 key for the parent community care contact.",
    "contact_match_status": "UNIQUE_ID_DATE_MATCH, UNIQUE_ID_DATE_MISMATCH, DUPLICATE_ID_RESOLVED_BY_DATE, AMBIGUOUS_SAME_DATE, DUPLICATE_ID_NO_DATE_MATCH or NO_CONTACT.",
    "contact_candidate_count": "Number of candidate care contact rows matched to this activity's care contact identifier during contact linkage.",
    "same_date_contact_candidate_count": "Number of candidate care contact rows whose contact date equals the care activity date, used to resolve duplicate contact identifiers.",
    "community_database_id": "Source community system database identifier (CDB) from the CSDS CYP202 care activity extract, used with care_activity_id to scope source records.",
    "care_activity_id": "Source CSDS care activity identifier (CAI) for the activity row, unique only within its source database.",
    "community_patient_key": "Content-addressed key identifying the community patient, derived from the source patient number and registering source database identifier in the CSDS patient identifier lookup.",
    "service_id": "Identifier of the registering community service or organisation from the CSDS patient identifier lookup.",
    "service_name": "Name of the registering community service or organisation associated with the activity.",
    "care_professional_local_id": "CSDS Care Professional Local Identifier; not a Millennium personnel ID.",
    "duration_minutes": "Recorded duration of the clinical contact for this activity, in minutes.",
    "duration_quality_status": "Derived data-quality flag describing the state of the recorded clinical contact duration, such as whether it is absent or within an acceptable range.",
    "source_clinical_term": "Original clinical term text recorded against the activity in the source legacy code field, used as input to candidate SNOMED enrichment.",
    "source_clinical_term_key": "Normalised form of the source clinical term used as the deterministic join key for SNOMED candidate matching.",
    "observation_type_id": "Numeric observation type identifier carried unchanged from the source CSDS CYP202 care activity ObservationType field.",
    "code_category_id": "Numeric code category identifier carried unchanged from the source CSDS CYP202 care activity CodeCategoryId field.",
    "observation_value_raw": "Observation value as recorded in the source CSDS CYP202 care activity ObsValue field, retained unaltered before numeric parsing.",
    "observation_value_numeric": "Numeric interpretation of the source observation value where the recorded ObsValue could be parsed as a number.",
    "unit_source_value": "Unit of measure text as recorded in the source CSDS CYP202 care activity unit field, before normalization.",
    "normalized_ucum_code": "Exact or approved-alias normalized UCUM code.",
    "unit_concept_id": "OMOP standard concept identifier for the mapped unit of measure.",
    "unit_concept_name": "OMOP standard concept name for the mapped unit of measure.",
    "unit_mapping_status": "Status flag describing the outcome of mapping the source unit text to an OMOP unit concept.",
    "unit_mapping_method": "Indicator of the technique used to derive the unit mapping, such as direct code match or alias dictionary lookup.",
    "snomed_candidate_count": "Number of distinct SNOMED candidate concepts matched for the source clinical term on this activity row.",
    "snomed_candidate_concept_id": "Unreviewed deterministic exact-name/synonym candidate; not an approved resolved mapping.",
    "snomed_candidate_code": "SNOMED CT code of the candidate concept matched to the source clinical term.",
    "snomed_candidate_name": "Concept name of the SNOMED CT candidate matched to the source clinical term.",
    "snomed_candidate_domain": "OMOP domain of the SNOMED CT candidate concept.",
    "snomed_candidate_class": "OMOP concept class of the SNOMED CT candidate concept.",
    "snomed_candidate_method": "Indicator of the deterministic matching technique used to select the SNOMED candidate, such as exact concept name or exact synonym match.",
    "snomed_candidate_status": "CANDIDATE_UNREVIEWED, AMBIGUOUS, UNMAPPED, NO_SOURCE_TERM or DISABLED.",
    "person_match_status": "Outcome status of the linkage from the community patient identifier to a Millennium person record.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each community care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each community care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each community care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each community care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each community care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_community_care_activity.SOURCE_PRESENT_IND is false, otherwise active; missing presence defaults true. No superseded status is emitted. This is source-row presence, not the clinical finding, contact-match status or code-eligibility decision.",
    "record_status_effective_from": "Date on which the care activity was recorded as taking place.",
    "record_status_effective_to": "map_community_care_activity.SOURCE_ABSENT_DETECTED_TS only when SOURCE_PRESENT_IND is false; otherwise null. A missing absence timestamp stays null, with no ingestion-time fallback. This is the row-absence end proxy, not a clinical activity or contact end.",
    "source_update_timestamp": "Always null as a TIMESTAMP in the current community-care activity projection. No native application-update or ingestion clock is supplied for this field; neither the activity date nor Silver refresh time is substituted.",
    "loaded_at": "Always null as a TIMESTAMP in the current community-care activity projection. No contributing load clock is supplied or taken from the parent contact; neither care_activity_date nor Silver refresh time is substituted.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_community_care_activity"),
    comment="Internal quality-controlled twin of clinical_community_care_activity: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_COMMUNITY_CARE_ACTIVITY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_COMMUNITY_CARE_ACTIVITY_ADVISORY_RULES)
def _gold_qc_clinical_community_care_activity():
    """Quality-controlled twin of journey_clinical.community_care_activity."""
    df = _qc(
        "clinical_community_care_activity",
        CLINICAL_COMMUNITY_CARE_ACTIVITY_SELECT,
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, CLINICAL_COMMUNITY_CARE_ACTIVITY_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.community_care_activity"),
    comment=(
        "One admitted CSDS community-care activity. Gold QC twin of the silver product: 3 "
        "columns are repaired or nulled, 1 rule(s) drop rows, 5 check(s) are advisory. Each "
        "rule states its reason in the pipeline notebook, and Lakeflow expectation metrics "
        "report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_community_care_activity():
    """Contract-v2 public twin of clinical_community_care_activity; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_community_care_activity")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`community_care_activity_key` AS `community_care_activity_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`care_activity_date` AS `care_activity_date`',
        '`care_activity_date_quality_status` AS `care_activity_date_quality_status`',
        '`community_contact_key` AS `community_contact_key`',
        '`contact_match_status` AS `contact_match_status`',
        '`contact_candidate_count` AS `contact_candidate_count`',
        '`same_date_contact_candidate_count` AS `same_date_contact_candidate_count`',
        '`community_database_id` AS `community_database_id`',
        '`care_activity_id` AS `care_activity_id`',
        '`community_patient_key` AS `community_patient_key`',
        '`service_id` AS `service_id`',
        '`service_name` AS `service_name`',
        '`care_professional_local_id` AS `care_professional_local_id`',
        '`duration_minutes` AS `duration_minutes`',
        '`duration_quality_status` AS `duration_quality_status`',
        '`source_clinical_term` AS `source_clinical_term`',
        '`source_clinical_term_key` AS `source_clinical_term_key`',
        '`observation_type_id` AS `observation_type_id`',
        '`code_category_id` AS `code_category_id`',
        '`observation_value_raw` AS `observation_value_raw`',
        '`observation_value_numeric` AS `observation_value_numeric`',
        '`unit_source_value` AS `unit_source_value`',
        '`normalized_ucum_code` AS `normalized_ucum_code`',
        '`unit_concept_id` AS `unit_concept_id`',
        '`unit_concept_name` AS `unit_concept_name`',
        '`unit_mapping_status` AS `unit_mapping_status`',
        '`unit_mapping_method` AS `unit_mapping_method`',
        '`snomed_candidate_count` AS `snomed_candidate_count`',
        '`snomed_candidate_concept_id` AS `snomed_candidate_concept_id`',
        '`snomed_candidate_code` AS `snomed_candidate_code`',
        '`snomed_candidate_name` AS `snomed_candidate_name`',
        '`snomed_candidate_domain` AS `snomed_candidate_domain`',
        '`snomed_candidate_class` AS `snomed_candidate_class`',
        '`snomed_candidate_method` AS `snomed_candidate_method`',
        '`snomed_candidate_status` AS `snomed_candidate_status`',
        '`person_match_status` AS `person_match_status`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_COMMUNITY_CARE_ACTIVITY_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.community_care_contact ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_COMMUNITY_CARE_CONTACT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`community_care_contact_key` AS `community_care_contact_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`care_contact_date` AS `care_contact_date`',
    '`community_database_id` AS `community_database_id`',
    '`care_contact_id` AS `care_contact_id`',
    '`community_patient_key` AS `community_patient_key`',
    '`service_request_id` AS `service_request_id`',
    '`service_id` AS `service_id`',
    '`service_name` AS `service_name`',
    '`team_id` AS `team_id`',
    '`care_contact_id_variant_count` AS `care_contact_id_variant_count`',
    '`person_match_status` AS `person_match_status`',
    '`duration_minutes` AS `duration_minutes`',
    '`duration_quality_status` AS `duration_quality_status`',
    '`earliest_reasonable_offer_date` AS `earliest_reasonable_offer_date`',
    "CASE WHEN CAST(`earliest_clinically_appropriate_date` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`earliest_clinically_appropriate_date` AS DATE)) < 9999 THEN NULL ELSE `earliest_clinically_appropriate_date` END AS `earliest_clinically_appropriate_date`",
    '`commissioner_ods_code` AS `commissioner_ods_code`',
    '`commissioner_organization_id` AS `commissioner_organization_id`',
    '`commissioner_organization_name` AS `commissioner_organization_name`',
    '`consultation_mechanism_code` AS `consultation_mechanism_code`',
    '`consultation_mechanism_display` AS `consultation_mechanism_display`',
    '`location_type_code` AS `location_type_code`',
    '`location_type_display` AS `location_type_display`',
    '`service_team_type_code` AS `service_team_type_code`',
    '`service_team_type_display` AS `service_team_type_display`',
    '`service_team_type_mapping_status` AS `service_team_type_mapping_status`',
    '`source_consultation_term` AS `source_consultation_term`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_community_care_contact_metadata; source history stays on the main research table.
CLINICAL_COMMUNITY_CARE_CONTACT_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 2,978,082 rows of 2,978,381
    # that are current and attributable; identity_status = 'resolved' keeps the 2,942,152
    # rows of 2,978,381 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_COMMUNITY_CARE_CONTACT_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 36,229 of 2,978,381 at the profile.
    "gold.clinical.community_care_contact.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 299 of 2,978,381 at the profile.
    "gold.clinical.community_care_contact.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1,700 of 2,978,381 rows (0.0571%) when profiled on 2026-08-24.
    "gold.clinical.community_care_contact.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 13 of 2,978,381 rows (0.000436%) when profiled on 2026-08-24.
    "gold.clinical.community_care_contact.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_COMMUNITY_CARE_CONTACT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "community_care_contact_key": "Source-preserved community care contact key; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Pseudonymised patient key derived from the source community system patient number and its registering database identifier.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date of the care contact as submitted in the CSDS CYP201 record.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each community care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Coded consultation type submitted with the CSDS CYP201 care contact record.",
    "source_code": "Coded consultation type submitted with the CSDS CYP201 care contact record.",
    "source_display": "Description corresponding to the coded consultation type submitted with the care contact record.",
    "care_contact_date": "Date of the care contact as submitted in the CSDS CYP201 record.",
    "community_database_id": "Source community system database identifier (CDB) from the CSDS CYP201 care contact extract, forming part of the record grain.",
    "care_contact_id": "Source CYP201 Care Contact Identifier; not unique by itself.",
    "community_patient_key": "Pseudonymised patient key derived from the source community system patient number and its registering database identifier.",
    "service_request_id": "Source CYP201 Service Request Identifier.",
    "service_id": "Identifier of the registering service or organisation recorded in the source community system patient lookup.",
    "service_name": "Name of the registering service or organisation recorded in the source community system patient lookup.",
    "team_id": "Local team identifier submitted with the CSDS CYP201 care contact record.",
    "care_contact_id_variant_count": "Rows sharing source_database_id and care_contact_id.",
    "person_match_status": "Status flag indicating the outcome of matching the community patient record to a curated person record.",
    "duration_minutes": "Clinical contact duration in minutes as submitted in the CSDS CYP201 record.",
    "duration_quality_status": "Derived data-quality status describing the completeness or plausibility of the submitted clinical contact duration.",
    "earliest_reasonable_offer_date": "Earliest reasonable offer date submitted with the CSDS CYP201 care contact record.",
    "earliest_clinically_appropriate_date": "Earliest clinically appropriate date submitted with the CSDS CYP201 care contact record.",
    "commissioner_ods_code": "Submitted commissioner ODS code.",
    "commissioner_organization_id": "Internal organisation identifier for the commissioner resolved by matching the submitted ODS code to organisation alias records.",
    "commissioner_organization_name": "Name of the commissioner organisation resolved from the organisation reference data for the submitted commissioner ODS code.",
    "consultation_mechanism_code": "Consultation mechanism (medium of contact) code standardised by the pipeline from the submitted source value.",
    "consultation_mechanism_display": "Description corresponding to the standardised consultation mechanism code.",
    "location_type_code": "CSDS activity location type (site type) code submitted on the CYP201 care contact, derived from the source location code field.",
    "location_type_display": "Descriptive label for the CSDS activity location type code recorded on the care contact.",
    "service_team_type_code": "CSDS team type code identifying the type of service team delivering the care contact.",
    "service_team_type_display": "Descriptive label for the CSDS service team type code recorded on the care contact.",
    "service_team_type_mapping_status": "Derived status flag indicating whether the submitted CSDS team type value was successfully resolved to a recognised service team type reference code.",
    "source_consultation_term": "Original consultation mechanism term as recorded in the source system before normalisation.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each community care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each community care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each community care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each community care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each community care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_community_care_contact.SOURCE_PRESENT_IND is false, otherwise active; missing presence defaults true. No superseded status is emitted. This describes source-row presence, not attendance, consultation outcome or identity-match status.",
    "record_status_effective_from": "First non-null care_contact_datetime_local cast to TIMESTAMP then care_contact_date cast to TIMESTAMP from map_community_care_contact. This contact-time proxy is not an independent status transition. Unlike event_datetime, this history start is not clamped to the [1950,2100) range, and has no ingestion-time fallback.",
    "record_status_effective_to": "map_community_care_contact.SOURCE_ABSENT_DETECTED_TS only when SOURCE_PRESENT_IND is false; otherwise null. A missing absence timestamp stays null, with no ingestion-time fallback. This is the row-absence end proxy, not the clinical contact end.",
    "source_update_timestamp": "Always null as a TIMESTAMP in the current community-care contact projection. No native application-update or ingestion clock is supplied for this field; neither the contact date nor Silver refresh time is substituted.",
    "loaded_at": "Always null as a TIMESTAMP in the current community-care contact projection. No contributing load timestamp is supplied; neither the contact date nor Silver refresh time is substituted.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_community_care_contact"),
    comment="Internal quality-controlled twin of clinical_community_care_contact: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_COMMUNITY_CARE_CONTACT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_COMMUNITY_CARE_CONTACT_ADVISORY_RULES)
def _gold_qc_clinical_community_care_contact():
    """Quality-controlled twin of journey_clinical.community_care_contact."""
    df = _qc(
        "clinical_community_care_contact",
        CLINICAL_COMMUNITY_CARE_CONTACT_SELECT,
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, CLINICAL_COMMUNITY_CARE_CONTACT_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.community_care_contact"),
    comment=(
        "One CSDS community-care contact. Gold QC twin of the silver product: 1 columns are "
        "repaired or nulled, 1 rule(s) drop rows, 4 check(s) are advisory. Each rule states "
        "its reason in the pipeline notebook, and Lakeflow expectation metrics report what "
        "every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_community_care_contact():
    """Contract-v2 public twin of clinical_community_care_contact; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_community_care_contact")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`community_care_contact_key` AS `community_care_contact_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`care_contact_date` AS `care_contact_date`',
        '`community_database_id` AS `community_database_id`',
        '`care_contact_id` AS `care_contact_id`',
        '`community_patient_key` AS `community_patient_key`',
        '`service_request_id` AS `service_request_id`',
        '`service_id` AS `service_id`',
        '`service_name` AS `service_name`',
        '`team_id` AS `team_id`',
        '`care_contact_id_variant_count` AS `care_contact_id_variant_count`',
        '`person_match_status` AS `person_match_status`',
        '`duration_minutes` AS `duration_minutes`',
        '`duration_quality_status` AS `duration_quality_status`',
        '`earliest_reasonable_offer_date` AS `earliest_reasonable_offer_date`',
        '`earliest_clinically_appropriate_date` AS `earliest_clinically_appropriate_date`',
        '`commissioner_ods_code` AS `commissioner_ods_code`',
        '`commissioner_organization_id` AS `commissioner_organization_id`',
        '`commissioner_organization_name` AS `commissioner_organization_name`',
        '`consultation_mechanism_code` AS `consultation_mechanism_code`',
        '`consultation_mechanism_display` AS `consultation_mechanism_display`',
        '`location_type_code` AS `location_type_code`',
        '`location_type_display` AS `location_type_display`',
        '`service_team_type_code` AS `service_team_type_code`',
        '`service_team_type_display` AS `service_team_type_display`',
        '`service_team_type_mapping_status` AS `service_team_type_mapping_status`',
        '`source_consultation_term` AS `source_consultation_term`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_COMMUNITY_CARE_CONTACT_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.costed_activity ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_COSTED_ACTIVITY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`extract_cd` AS `extract_cd`',
    '`activity_record_id` AS `activity_record_id`',
    '`feed_type` AS `feed_type`',
    '`plemi` AS `plemi`',
    '`nhs_number_status_cd` AS `nhs_number_status_cd`',
    '`cds_id` AS `cds_id`',
    '`attendance_id` AS `attendance_id`',
    '`arrival_date` AS `arrival_date`',
    '`arrival_time` AS `arrival_time`',
    '`departure_date` AS `departure_date`',
    '`departure_time` AS `departure_time`',
    '`departure_type_cd` AS `departure_type_cd`',
    '`provider_org_cd` AS `provider_org_cd`',
    '`patient_org_cd` AS `patient_org_cd`',
    '`pathway_id` AS `pathway_id`',
    '`pod_cd` AS `pod_cd`',
    '`treatment_function_cd` AS `treatment_function_cd`',
    '`source_los` AS `source_los`',
    '`cf_band_cd` AS `cf_band_cd`',
    '`episode_number` AS `episode_number`',
    '`episode_start_datetime` AS `episode_start_datetime`',
    'CASE WHEN `episode_start_datetime` IS NOT NULL AND `episode_end_datetime` IS NOT NULL AND `episode_start_datetime` > `episode_end_datetime` THEN NULL ELSE `episode_end_datetime` END AS `episode_end_datetime`',
    '`episode_type_cd` AS `episode_type_cd`',
    '`hosp_spell_id` AS `hosp_spell_id`',
    '`hrg_cd` AS `hrg_cd`',
    '`hrg_desc` AS `hrg_desc`',
    '`fce_hrg_cd` AS `fce_hrg_cd`',
    '`fce_hrg_desc` AS `fce_hrg_desc`',
    '`spell_hrg_cd` AS `spell_hrg_cd`',
    '`spell_hrg_desc` AS `spell_hrg_desc`',
    '`appointment_date` AS `appointment_date`',
    '`appointment_time` AS `appointment_time`',
    '`critical_care_unit_function_cd` AS `critical_care_unit_function_cd`',
    '`organs_supported` AS `organs_supported`',
    '`critical_care_period_type_cd` AS `critical_care_period_type_cd`',
    '`critical_care_level_ind` AS `critical_care_level_ind`',
    '`unbundled_activity_datetime` AS `unbundled_activity_datetime`',
    '`unbundled_activity_cd` AS `unbundled_activity_cd`',
    '`unbundled_hrg_cd` AS `unbundled_hrg_cd`',
    '`unbundled_hrg_desc` AS `unbundled_hrg_desc`',
    '`partial_costing_ind` AS `partial_costing_ind`',
    '`care_datetime` AS `care_datetime`',
    '`care_id` AS `care_id`',
    '`clinical_contact_duration` AS `clinical_contact_duration`',
    '`chs_currency_cd` AS `chs_currency_cd`',
    '`team_type_cd` AS `team_type_cd`',
    '`contact_subject_cd` AS `contact_subject_cd`',
    '`consult_type_cd` AS `consult_type_cd`',
    '`consult_medium_cd` AS `consult_medium_cd`',
    '`location_cd` AS `location_cd`',
    '`gp_therapy_ind` AS `gp_therapy_ind`',
    '`service_request_id` AS `service_request_id`',
    '`cost_line_count` AS `cost_line_count`',
    '`total_cost_sum` AS `total_cost_sum`',
    '`total_o_cost_sum` AS `total_o_cost_sum`',
    '`person_link_method` AS `person_link_method`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_costed_activity_metadata; source history stays on the main research table.
CLINICAL_COSTED_ACTIVITY_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 5,562,087 rows of 5,562,087
    # that are current and attributable; identity_status = 'resolved' keeps the 5,380,935
    # rows of 5,562,087 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_COSTED_ACTIVITY_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 181,152 of 5,562,087 at the profile.
    "gold.clinical.costed_activity.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 5,562,087 at the profile.
    "gold.clinical.costed_activity.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 630 of 5,562,087 rows (0.0113%) when profiled on 2026-08-24.
    "gold.clinical.costed_activity.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 5,201 of 5,562,087 rows (0.0935%) when profiled on 2026-08-24.
    "gold.clinical.costed_activity.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_COSTED_ACTIVITY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Internal Millennium person identifier assigned by the person-alias linkage, used to group activity for the same patient without publishing direct identifiers.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Start date and time of the costed activity as supplied by the source PLICS extract (ActivityStartDate).",
    "event_end_datetime": "End date and time of the costed activity as supplied by the source PLICS extract (ActivityEndDate).",
    "source_coding_system": "Source point-of-delivery classification; preserved without an invented mapping.",
    "source_code": "Source point-of-delivery classification; preserved without an invented mapping.",
    "source_display": "Human-readable label supplied by the source system for the source code for each costed activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "extract_cd": "SLAM EXTRACT_CD; first component of the primary key.",
    "activity_record_id": "SLAM ACTIVITY_RECORD_ID as BIGINT; second component of the primary key.",
    "feed_type": "Source-supplied code identifying the activity feed the costed record came from, preserved without an invented mapping.",
    "plemi": "PLICS matching identifier reused across extracts; never join without EXTRACT_CD.",
    "nhs_number_status_cd": "Non-identifying source quality/status code used during linkage.",
    "cds_id": "Commissioning Data Set record identifier carried from the source costing extract and used in the CDS-to-MRN person linkage fallback.",
    "attendance_id": "Source attendance identifier for the activity record as supplied by the costing extract.",
    "arrival_date": "Arrival date for the attendance, carried from the source costing extract.",
    "arrival_time": "Arrival time for the attendance, carried from the source costing extract and held as a timestamp.",
    "departure_date": "Departure date for the attendance, carried from the source costing extract (DepDate).",
    "departure_time": "Departure time for the attendance, carried from the source costing extract and held as a timestamp.",
    "departure_type_cd": "Source code describing the type of departure recorded for the attendance; preserved without an invented mapping.",
    "provider_org_cd": "Organisation code of the provider submitting the costed activity, as supplied by the source extract (OrgId).",
    "patient_org_cd": "Organisation code associated with the patient's commissioner or responsible organisation, as supplied by the source extract.",
    "pathway_id": "Patient pathway identifier for the activity record as supplied by the source costing extract (PathID); use with EXTRACT_CD as extract-scoped identifiers are not unique across extracts.",
    "pod_cd": "Source point-of-delivery classification; preserved without an invented mapping.",
    "treatment_function_cd": "Treatment function code for the costed activity as supplied by the source costing extract (Tfc).",
    "source_los": "Length-of-stay value carried from the source costing extract without recalculation.",
    "cf_band_cd": "Cystic Fibrosis year-of-care currency band supplied by the source.",
    "episode_number": "Sequence number of the episode within the hospital spell, as supplied by the source costing extract (EpiNo).",
    "episode_start_datetime": "Episode start date and time as supplied by the source PLICS costing extract (EpStDte).",
    "episode_end_datetime": "Episode end date and time as supplied by the source PLICS costing extract (EpEnDte).",
    "episode_type_cd": "Source-supplied episode type classification code from the PLICS costing extract, preserved without an invented mapping.",
    "hosp_spell_id": "Source hospital spell identifier for the activity record, as supplied by the PLICS costing extract.",
    "hrg_cd": "Healthcare Resource Group (HRG4+) code assigned to the costed activity record in the source extract.",
    "hrg_desc": "Description of HRG_CD sourced from the HRG v4 reference lookup.",
    "fce_hrg_cd": "Healthcare Resource Group code derived at finished consultant episode level in the source costing extract.",
    "fce_hrg_desc": "Description of FCE_HRG_CD sourced from the HRG v4 reference lookup.",
    "spell_hrg_cd": "Healthcare Resource Group code derived at hospital spell level in the source costing extract.",
    "spell_hrg_desc": "Description of SPELL_HRG_CD sourced from the HRG v4 reference lookup.",
    "appointment_date": "Appointment date for the costed outpatient or non-admitted activity, as supplied by the source extract.",
    "appointment_time": "Appointment time for the costed activity as supplied by the source extract, stored in a timestamp column.",
    "critical_care_unit_function_cd": "Critical care unit function code for the activity record, supplied by the source costing extract without an invented mapping.",
    "organs_supported": "Source-supplied critical care organs supported value for the activity record.",
    "critical_care_period_type_cd": "Critical care period type code supplied by the source costing extract, preserved without an invented mapping.",
    "critical_care_level_ind": "Critical care level indicator supplied by the source costing extract.",
    "unbundled_activity_datetime": "Date and time of the unbundled activity component associated with the costed record.",
    "unbundled_activity_cd": "Code identifying the unbundled activity component of the costed record, as supplied by the source extract.",
    "unbundled_hrg_cd": "Healthcare Resource Group code for the unbundled activity component of the costed record.",
    "unbundled_hrg_desc": "Description of UNBUNDLED_HRG_CD sourced from the HRG v4 reference lookup.",
    "partial_costing_ind": "Source indicator flagging partially costed activity, available only from the PE2122 NCC extract.",
    "care_datetime": "Community care contact date and time supplied by the PE2122 NCC extract.",
    "care_id": "Source identifier for the community care contact record, supplied by the PE2122 NCC extract.",
    "clinical_contact_duration": "Duration of the clinical contact recorded in the PE2122 NCC extract, in the units supplied by the source.",
    "chs_currency_cd": "Community health services currency code supplied by the PE2122 NCC costing extract.",
    "team_type_cd": "Coded team type for the community contact, supplied as an integer by the PE2122 NCC extract.",
    "contact_subject_cd": "Community contact subject code supplied by the PE2122 NCC costing extract (source CCSubject), preserved without an invented mapping.",
    "consult_type_cd": "Consultation type code for the community contact as supplied by the PE2122 NCC costing extract (source ConsultType).",
    "consult_medium_cd": "Consultation medium code recording how the community contact was conducted, as supplied by the PE2122 NCC costing extract (source CMedium).",
    "location_cd": "Source location code for the site or place where the costed activity or contact took place, as supplied by the PE2122 NCC extract (source LocCode).",
    "gp_therapy_ind": "Source-supplied therapy indicator flag for the community contact taken from the PE2122 NCC extract (source GPTherapyInd), preserved without an invented mapping.",
    "service_request_id": "Source service request identifier for the community contact carried from the PE2122 NCC costing extract (source SerReqID); use with EXTRACT_CD as extract-scoped identifiers are not unique across extracts.",
    "cost_line_count": "Duplicate-weighted count of map_slam_cost_line_item rows aggregated for this activity record, using the collapsed source duplicate counts.",
    "total_cost_sum": "Duplicate-weighted sum of TOTAL_COST across the cost line items for this activity record, retained at exact decimal precision.",
    "total_o_cost_sum": "Duplicate-weighted sum of TOTAL_O_COST, a component of total cost, across the cost line items for this activity record, retained at exact decimal precision.",
    "person_link_method": "Code recording the linkage path used to derive PERSON_ID, distinguishing the NHS-number alias match from the CDS-to-MRN fallback.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each costed activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each costed activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each costed activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each costed activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each costed activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_slam_costed_activity.SOURCE_PRESENT_IND is false, otherwise active; missing presence defaults true. No superseded status is emitted. This is source-row presence, not the clinical activity's completion state or the partial-costing indicator.",
    "record_status_effective_from": "Start date and time of the costed activity as supplied by the source PLICS extract (ActivityStartDate).",
    "record_status_effective_to": "map_slam_costed_activity.SOURCE_ABSENT_DETECTED_TS only when SOURCE_PRESENT_IND is false; otherwise null. Missing absence time remains null; ACTIVITY_END_DT_TM and ADC_UPDT are not fallbacks. This is the row-absence end proxy, not the clinical activity end.",
    "source_update_timestamp": "map_slam_costed_activity.ADC_UPDT carried unchanged, identical to loaded_at. The Silver projection supplies no independent native application-update clock and does not substitute Silver refresh time.",
    "loaded_at": "map_slam_costed_activity.ADC_UPDT carried unchanged for the contributing activity row, identical to source_update_timestamp. No cost-line child clock is joined or aggregated here. This is the input row's load provenance, not the clinical activity date or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_costed_activity"),
    comment="Internal quality-controlled twin of clinical_costed_activity: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_COSTED_ACTIVITY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_COSTED_ACTIVITY_ADVISORY_RULES)
def _gold_qc_clinical_costed_activity():
    """Quality-controlled twin of journey_clinical.costed_activity."""
    # 20 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "clinical_costed_activity",
        CLINICAL_COSTED_ACTIVITY_SELECT,
        fk_columns=["person_id"],
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, CLINICAL_COSTED_ACTIVITY_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.costed_activity"),
    comment=(
        "One frozen PLICS costed activity. Gold QC twin of the silver product: 3 columns are "
        "repaired or nulled, 1 rule(s) drop rows, 4 check(s) are advisory. Each rule states "
        "its reason in the pipeline notebook, and Lakeflow expectation metrics report what "
        "every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_costed_activity():
    """Contract-v2 public twin of clinical_costed_activity; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_costed_activity")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`extract_cd` AS `extract_cd`',
        '`activity_record_id` AS `activity_record_id`',
        '`feed_type` AS `feed_type`',
        '`plemi` AS `plemi`',
        '`nhs_number_status_cd` AS `nhs_number_status_cd`',
        '`cds_id` AS `cds_id`',
        '`attendance_id` AS `attendance_id`',
        '`arrival_date` AS `arrival_date`',
        '`arrival_time` AS `arrival_time`',
        '`departure_date` AS `departure_date`',
        '`departure_time` AS `departure_time`',
        '`departure_type_cd` AS `departure_type_cd`',
        '`provider_org_cd` AS `provider_org_cd`',
        '`patient_org_cd` AS `patient_org_cd`',
        '`pathway_id` AS `pathway_id`',
        '`pod_cd` AS `pod_cd`',
        '`treatment_function_cd` AS `treatment_function_cd`',
        '`source_los` AS `source_los`',
        '`cf_band_cd` AS `cf_band_cd`',
        '`episode_number` AS `episode_number`',
        '`episode_start_datetime` AS `episode_start_datetime`',
        '`episode_end_datetime` AS `episode_end_datetime`',
        '`episode_type_cd` AS `episode_type_cd`',
        '`hosp_spell_id` AS `hosp_spell_id`',
        '`hrg_cd` AS `hrg_cd`',
        '`hrg_desc` AS `hrg_desc`',
        '`fce_hrg_cd` AS `fce_hrg_cd`',
        '`fce_hrg_desc` AS `fce_hrg_desc`',
        '`spell_hrg_cd` AS `spell_hrg_cd`',
        '`spell_hrg_desc` AS `spell_hrg_desc`',
        '`appointment_date` AS `appointment_date`',
        '`appointment_time` AS `appointment_time`',
        '`critical_care_unit_function_cd` AS `critical_care_unit_function_cd`',
        '`organs_supported` AS `organs_supported`',
        '`critical_care_period_type_cd` AS `critical_care_period_type_cd`',
        '`critical_care_level_ind` AS `critical_care_level_ind`',
        '`unbundled_activity_datetime` AS `unbundled_activity_datetime`',
        '`unbundled_activity_cd` AS `unbundled_activity_cd`',
        '`unbundled_hrg_cd` AS `unbundled_hrg_cd`',
        '`unbundled_hrg_desc` AS `unbundled_hrg_desc`',
        '`partial_costing_ind` AS `partial_costing_ind`',
        '`care_datetime` AS `care_datetime`',
        '`care_id` AS `care_id`',
        '`clinical_contact_duration` AS `clinical_contact_duration`',
        '`chs_currency_cd` AS `chs_currency_cd`',
        '`team_type_cd` AS `team_type_cd`',
        '`contact_subject_cd` AS `contact_subject_cd`',
        '`consult_type_cd` AS `consult_type_cd`',
        '`consult_medium_cd` AS `consult_medium_cd`',
        '`location_cd` AS `location_cd`',
        '`gp_therapy_ind` AS `gp_therapy_ind`',
        '`service_request_id` AS `service_request_id`',
        '`cost_line_count` AS `cost_line_count`',
        '`total_cost_sum` AS `total_cost_sum`',
        '`total_o_cost_sum` AS `total_o_cost_sum`',
        '`person_link_method` AS `person_link_method`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_COSTED_ACTIVITY_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.drug_expenditure ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_DRUG_EXPENDITURE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_row_hash` AS `source_row_hash`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`transaction_id` AS `transaction_id`',
    '`financial_year` AS `financial_year`',
    '`financial_month` AS `financial_month`',
    '`reporting_year` AS `reporting_year`',
    '`reporting_month` AS `reporting_month`',
    '`provider_org_cd` AS `provider_org_cd`',
    '`site_cd` AS `site_cd`',
    '`site_name` AS `site_name`',
    '`specialty_cd` AS `specialty_cd`',
    '`consultant_cd` AS `consultant_cd`',
    '`patient_type` AS `patient_type`',
    '`pod_cd` AS `pod_cd`',
    '`chargeable_item` AS `chargeable_item`',
    '`additional_info` AS `additional_info`',
    '`dmd_raw` AS `dmd_raw`',
    '`dmd_code` AS `dmd_code`',
    '`dmd_concept_id` AS `dmd_concept_id`',
    '`dmd_concept_name` AS `dmd_concept_name`',
    '`drug_standard_concept_id` AS `drug_standard_concept_id`',
    '`drug_standard_concept_name` AS `drug_standard_concept_name`',
    '`dmd_mapping_status` AS `dmd_mapping_status`',
    '`dmd_taxonomy_cd` AS `dmd_taxonomy_cd`',
    '`route_of_administration` AS `route_of_administration`',
    '`strength` AS `strength`',
    '`volume` AS `volume`',
    '`pack_size` AS `pack_size`',
    'CASE WHEN `quantity` < 0 THEN NULL ELSE `quantity` END AS `quantity`',
    '`unit_of_measure` AS `unit_of_measure`',
    '`dispensing_route` AS `dispensing_route`',
    '`dispensing_location` AS `dispensing_location`',
    '`indication` AS `indication`',
    '`funding_reference` AS `funding_reference`',
    '`hcdr_category_cd` AS `hcdr_category_cd`',
    '`hcdr_category_desc` AS `hcdr_category_desc`',
    '`ccg_residence_cd` AS `ccg_residence_cd`',
    '`ccg_gp_cd` AS `ccg_gp_cd`',
    '`commissioner_cd` AS `commissioner_cd`',
    '`commissioner_type` AS `commissioner_type`',
    '`service_line` AS `service_line`',
    '`service_category_cd` AS `service_category_cd`',
    '`unit_price_supplier` AS `unit_price_supplier`',
    '`unit_price_commissioner` AS `unit_price_commissioner`',
    '`vat` AS `vat`',
    '`vat_cd` AS `vat_cd`',
    '`income` AS `income`',
    '`cost` AS `cost`',
    '`margin` AS `margin`',
    '`lloyds_dispensing_fee` AS `lloyds_dispensing_fee`',
    '`production_fee` AS `production_fee`',
    '`fixed_patient_income` AS `fixed_patient_income`',
    '`cost_centre_desc` AS `cost_centre_desc`',
    '`drug_feed` AS `drug_feed`',
    '`data_set` AS `data_set`',
    '`drug_category` AS `drug_category`',
    '`ledger_cd` AS `ledger_cd`',
    '`exclusion_flag` AS `exclusion_flag`',
    '`exclusion_reason` AS `exclusion_reason`',
    '`ledger_lv3_cd` AS `ledger_lv3_cd`',
    '`ledger_lv3_desc` AS `ledger_lv3_desc`',
    '`ledger_lv6_cd` AS `ledger_lv6_cd`',
    '`ledger_lv6_desc` AS `ledger_lv6_desc`',
    '`ledger_lv7_cd` AS `ledger_lv7_cd`',
    '`ledger_lv7_desc` AS `ledger_lv7_desc`',
    '`ledger_lv9_cd` AS `ledger_lv9_cd`',
    '`ledger_lv9_desc` AS `ledger_lv9_desc`',
    '`slr_cd` AS `slr_cd`',
    '`diabetic_flag` AS `diabetic_flag`',
    '`imcoe_flag` AS `imcoe_flag`',
    '`source_duplicate_count` AS `source_duplicate_count`',
    '`person_link_method` AS `person_link_method`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_drug_expenditure_metadata; source history stays on the main research table.
CLINICAL_DRUG_EXPENDITURE_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 1,167,749 rows of 1,167,749
    # that are current and attributable; identity_status = 'resolved' keeps the 1,167,731
    # rows of 1,167,749 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_DRUG_EXPENDITURE_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 18 of 1,167,749 at the profile.
    "gold.clinical.drug_expenditure.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 1,167,749 at the profile.
    "gold.clinical.drug_expenditure.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 3,486 of 1,167,749 rows (0.299%) when profiled on 2026-08-24.
    "gold.clinical.drug_expenditure.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 12 of 1,167,749 rows (0.00103%) when profiled on 2026-08-24.
    "gold.clinical.drug_expenditure.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_DRUG_EXPENDITURE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "source_row_hash": "HCD ROW_HASH supplied by the source; evidence for the exception where no durable native row id exists.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Internal Millennium person identifier resolved from source patient identifiers via person alias (MRN first, then NHS number); null when unresolved.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Effective date/time as supplied; two known rows dated 2122 are retained losslessly.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each drug expenditure record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Strict trimmed all-numeric dm+d code; null for blank or non-numeric source values.",
    "source_code": "Name of the chargeable high-cost drug or device as recorded in the source.",
    "source_display": "Name of the chargeable high-cost drug or device as recorded in the source.",
    "transaction_id": "Source SLR transaction reference as supplied, blank or null on a material share of rows and therefore not usable as a key.",
    "financial_year": "Financial year to which the high-cost drug or device expenditure is attributed, as supplied by the source.",
    "financial_month": "Financial period (month) within the financial year to which the expenditure is attributed, as supplied by the source.",
    "reporting_year": "Year in which the expenditure was reported, as supplied by the source.",
    "reporting_month": "Month number within the reporting year in which the expenditure was reported, as supplied by the source.",
    "provider_org_cd": "Organisation code of the provider recording the expenditure, as supplied by the source.",
    "site_cd": "Source site code identifying the hospital site associated with the charge.",
    "site_name": "Curated mapping for RLH, SBH, WXH, NUH and NGH only; other values are unresolved by design.",
    "specialty_cd": "Treatment specialty code associated with the charge, as supplied by the source.",
    "consultant_cd": "Local code identifying the responsible consultant for the charge; may be blank or null.",
    "patient_type": "Source-supplied categorisation of the care setting under which the chargeable item was supplied.",
    "pod_cd": "Point of delivery code classifying the activity under which the item is reimbursed, as supplied by the source.",
    "chargeable_item": "Name of the chargeable high-cost drug or device as recorded in the source.",
    "additional_info": "Free-text supplementary description of the supplied item, including presentation and administration details, as recorded in the source.",
    "dmd_raw": "Verbatim source dm+d value; includes junk values such as No SNOMED.",
    "dmd_code": "Strict trimmed all-numeric dm+d code; null for blank or non-numeric source values.",
    "dmd_concept_id": "OMOP concept identifier for the dm+d source concept matched from DMD_CODE; null when no match is made.",
    "dmd_concept_name": "OMOP concept name of the matched dm+d source concept; null when no match is made.",
    "drug_standard_concept_id": "OMOP standard concept reached only when exactly one valid Maps-to target exists.",
    "drug_standard_concept_name": "OMOP standard concept name for the drug, populated from the OMOP concept table only where the dm+d code resolved to exactly one Maps-to standard concept.",
    "dmd_mapping_status": "dm+d mapping state: NO_CODE, CODE_UNMAPPED, MAPPED_STANDARD, or MAPPED_DMD_ONLY.",
    "dmd_taxonomy_cd": "dm+d taxonomy level code as supplied in the source DMDTaxonomyCode field, indicating the dm+d concept type of the coded item.",
    "route_of_administration": "Route of administration for the dispensed item as supplied by the source, typically a SNOMED CT route code but occasionally free-text route wording.",
    "strength": "Strength of the supplied drug or device as recorded in the source, held as free text and sometimes blank or non-numeric placeholder wording.",
    "volume": "Product volume as supplied by the source, held as free text and frequently blank.",
    "pack_size": "Pack or dispensed unit description for the item as supplied by the source, held as free text.",
    "quantity": "Quantity of the item attributed to this expenditure row, which may be fractional; multiply by SOURCE_DUPLICATE_COUNT to reconstruct totals.",
    "unit_of_measure": "Unit of measure for the recorded quantity as supplied by the source, generally a SNOMED CT unit code and sometimes blank.",
    "dispensing_route": "Source-supplied code indicating the dispensing route or supply channel used for the item.",
    "dispensing_location": "Description of the dispensing location or supply point recorded in the source, such as a hospital pharmacy, clinic or homecare service.",
    "indication": "Clinical indication for the high-cost drug or device as recorded in the source, held as free text.",
    "funding_reference": "Source-supplied funding or approval reference for the item, frequently null.",
    "hcdr_category_cd": "Local high-cost-drug reimbursement category from source HRGCode; not an HRG4+ code.",
    "hcdr_category_desc": "Description of the local high-cost-drug reimbursement category, taken from the source HRGDesc field and paired with HCDR_CATEGORY_CD.",
    "ccg_residence_cd": "Code of the CCG or successor commissioning organisation associated with the patient's area of residence, as supplied by the source.",
    "ccg_gp_cd": "Code of the CCG or successor commissioning organisation associated with the patient's registered GP practice, as supplied by the source.",
    "commissioner_cd": "Code of the commissioning organisation responsible for funding the item, as supplied by the source.",
    "commissioner_type": "Type or category of the responsible commissioner as supplied by the source, and blank where not recorded.",
    "service_line": "Service line identifier assigned to the expenditure row in Service Line Reporting, often null.",
    "service_category_cd": "Source-supplied service category code classifying the expenditure row within the finance hierarchy.",
    "unit_price_supplier": "Supplier unit price for the item as supplied by the source finance report.",
    "unit_price_commissioner": "Commissioner unit price for the item as supplied by the source finance report.",
    "vat": "Source-supplied indicator of whether VAT applies to the item.",
    "vat_cd": "Source-supplied VAT code applied to the item for finance reporting.",
    "income": "Reimbursement income value for the drug or device line as supplied by the SLR source; multiply by SOURCE_DUPLICATE_COUNT to reconstruct totals.",
    "cost": "Expenditure cost value for the drug or device line as supplied by the SLR source; multiply by SOURCE_DUPLICATE_COUNT to reconstruct totals.",
    "margin": "Margin value for the line, the difference between income and cost as supplied by the SLR source; multiply by SOURCE_DUPLICATE_COUNT to reconstruct totals.",
    "lloyds_dispensing_fee": "Dispensing fee element charged by the homecare/dispensing provider for the line as supplied by the source finance report, null where no such fee applies.",
    "production_fee": "Production (compounding/preparation) fee element for the line as supplied by the source, null where no such fee applies.",
    "fixed_patient_income": "Fixed per-patient income element for the line as supplied by the source, null where income is not charged on a fixed-patient basis.",
    "cost_centre_desc": "Finance cost centre description for the line, which commonly embeds the site abbreviation, the responsible consultant's name and the specialty.",
    "drug_feed": "Source-supplied identifier of the data feed or supply system from which the drug expenditure line was obtained.",
    "data_set": "Submission dataset grouping identifying the site or sites and feed combination the line was reported under.",
    "drug_category": "Source-supplied categorisation of the drug expenditure line by supply or treatment category for finance reporting.",
    "ledger_cd": "General ledger account code to which the expenditure or income line is posted.",
    "exclusion_flag": "Source indicator showing whether the line is excluded from the reported SLR expenditure position.",
    "exclusion_reason": "Source-supplied reason accompanying EXCLUSION_FLAG explaining why the line was excluded, null when not excluded.",
    "ledger_lv3_cd": "Level 3 code of the finance ledger hierarchy, representing the hospital site or division to which the line is attributed.",
    "ledger_lv3_desc": "Description for LEDGER_LV3_CD, naming the hospital site or division level of the finance ledger hierarchy.",
    "ledger_lv6_cd": "Level 6 code of the finance ledger hierarchy, representing the directorate or service grouping for the line.",
    "ledger_lv6_desc": "Description for LEDGER_LV6_CD, naming the directorate or service grouping in the finance ledger hierarchy.",
    "ledger_lv7_cd": "Level 7 code of the finance ledger hierarchy, representing the site-level specialty or department for the line.",
    "ledger_lv7_desc": "Description for LEDGER_LV7_CD, naming the site-level specialty or department in the finance ledger hierarchy.",
    "ledger_lv9_cd": "Level 9 code of the finance ledger hierarchy, the lowest-level cost centre or account for the line.",
    "ledger_lv9_desc": "Description for LEDGER_LV9_CD, naming the lowest-level cost centre or account in the finance ledger hierarchy.",
    "slr_cd": "Service Line Reporting service line code assigned to the expenditure line.",
    "diabetic_flag": "Source-supplied indicator marking lines classified as diabetes-related expenditure; null where not flagged.",
    "imcoe_flag": "Source-supplied indicator marking lines attributed to the IMCoE service classification; null where not flagged.",
    "source_duplicate_count": "Number of exact-identical source rows represented by this published row; reconstruct additive measures as measure × count.",
    "person_link_method": "How PERSON_ID was resolved: MRN_ALIAS first, NHS_ALIAS fallback, or null when unresolved.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each drug expenditure record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each drug expenditure record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each drug expenditure record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each drug expenditure record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each drug expenditure record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when SOURCE_PRESENT_IND is false on 4_prod.tmp.journey_finance_hcd_expenditure_s42, otherwise active; missing presence defaults true. No superseded status is emitted. This is staged source-row presence, not the financial EXCLUSION_FLAG or drug administration status.",
    "record_status_effective_from": "EFFECTIVE_DT_TM carried unchanged from 4_prod.tmp.journey_finance_hcd_expenditure_s42. This expenditure-effective-time proxy is not an independently observed status transition; the Silver projection applies no [1950,2100) clamp or ingestion-time fallback.",
    "record_status_effective_to": "ADC_UPDT from 4_prod.tmp.journey_finance_hcd_expenditure_s42 only when its SOURCE_PRESENT_IND is false; otherwise null. This is the staged row's load-time end proxy, not the expenditure date or an independently observed source-status transition.",
    "source_update_timestamp": "ADC_UPDT carried unchanged from 4_prod.tmp.journey_finance_hcd_expenditure_s42, identical to loaded_at. The Silver projection reads this staging table directly, not map_finance_hcd_expenditure, and supplies no independent native application-update clock.",
    "loaded_at": "ADC_UPDT carried unchanged from the contributing row of 4_prod.tmp.journey_finance_hcd_expenditure_s42, identical to source_update_timestamp. No extra source clock is joined or aggregated here; the value is not the current Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_drug_expenditure"),
    comment="Internal quality-controlled twin of clinical_drug_expenditure: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_DRUG_EXPENDITURE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_DRUG_EXPENDITURE_ADVISORY_RULES)
def _gold_qc_clinical_drug_expenditure():
    """Quality-controlled twin of journey_clinical.drug_expenditure."""
    df = _qc(
        "clinical_drug_expenditure",
        CLINICAL_DRUG_EXPENDITURE_SELECT,
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, CLINICAL_DRUG_EXPENDITURE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.drug_expenditure"),
    comment=(
        "One HCD drug-expenditure row keyed by ROW_HASH. Gold QC twin of the silver product: "
        "1 columns are repaired or nulled, 1 rule(s) drop rows, 4 check(s) are advisory. Each "
        "rule states its reason in the pipeline notebook, and Lakeflow expectation metrics "
        "report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_drug_expenditure():
    """Contract-v2 public twin of clinical_drug_expenditure; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_drug_expenditure")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_row_hash` AS `source_row_hash`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`transaction_id` AS `transaction_id`',
        '`financial_year` AS `financial_year`',
        '`financial_month` AS `financial_month`',
        '`reporting_year` AS `reporting_year`',
        '`reporting_month` AS `reporting_month`',
        '`provider_org_cd` AS `provider_org_cd`',
        '`site_cd` AS `site_cd`',
        '`site_name` AS `site_name`',
        '`specialty_cd` AS `specialty_cd`',
        '`consultant_cd` AS `consultant_cd`',
        '`patient_type` AS `patient_type`',
        '`pod_cd` AS `pod_cd`',
        '`chargeable_item` AS `chargeable_item`',
        '`additional_info` AS `additional_info`',
        '`dmd_raw` AS `dmd_raw`',
        '`dmd_code` AS `dmd_code`',
        '`dmd_concept_id` AS `dmd_concept_id`',
        '`dmd_concept_name` AS `dmd_concept_name`',
        '`drug_standard_concept_id` AS `drug_standard_concept_id`',
        '`drug_standard_concept_name` AS `drug_standard_concept_name`',
        '`dmd_mapping_status` AS `dmd_mapping_status`',
        '`dmd_taxonomy_cd` AS `dmd_taxonomy_cd`',
        '`route_of_administration` AS `route_of_administration`',
        '`strength` AS `strength`',
        '`volume` AS `volume`',
        '`pack_size` AS `pack_size`',
        '`quantity` AS `quantity`',
        '`unit_of_measure` AS `unit_of_measure`',
        '`dispensing_route` AS `dispensing_route`',
        '`dispensing_location` AS `dispensing_location`',
        '`indication` AS `indication`',
        '`funding_reference` AS `funding_reference`',
        '`hcdr_category_cd` AS `hcdr_category_cd`',
        '`hcdr_category_desc` AS `hcdr_category_desc`',
        '`ccg_residence_cd` AS `ccg_residence_cd`',
        '`ccg_gp_cd` AS `ccg_gp_cd`',
        '`commissioner_cd` AS `commissioner_cd`',
        '`commissioner_type` AS `commissioner_type`',
        '`service_line` AS `service_line`',
        '`service_category_cd` AS `service_category_cd`',
        '`unit_price_supplier` AS `unit_price_supplier`',
        '`unit_price_commissioner` AS `unit_price_commissioner`',
        '`vat` AS `vat`',
        '`vat_cd` AS `vat_cd`',
        '`income` AS `income`',
        '`cost` AS `cost`',
        '`margin` AS `margin`',
        '`lloyds_dispensing_fee` AS `lloyds_dispensing_fee`',
        '`production_fee` AS `production_fee`',
        '`fixed_patient_income` AS `fixed_patient_income`',
        '`cost_centre_desc` AS `cost_centre_desc`',
        '`drug_feed` AS `drug_feed`',
        '`data_set` AS `data_set`',
        '`drug_category` AS `drug_category`',
        '`ledger_cd` AS `ledger_cd`',
        '`exclusion_flag` AS `exclusion_flag`',
        '`exclusion_reason` AS `exclusion_reason`',
        '`ledger_lv3_cd` AS `ledger_lv3_cd`',
        '`ledger_lv3_desc` AS `ledger_lv3_desc`',
        '`ledger_lv6_cd` AS `ledger_lv6_cd`',
        '`ledger_lv6_desc` AS `ledger_lv6_desc`',
        '`ledger_lv7_cd` AS `ledger_lv7_cd`',
        '`ledger_lv7_desc` AS `ledger_lv7_desc`',
        '`ledger_lv9_cd` AS `ledger_lv9_cd`',
        '`ledger_lv9_desc` AS `ledger_lv9_desc`',
        '`slr_cd` AS `slr_cd`',
        '`diabetic_flag` AS `diabetic_flag`',
        '`imcoe_flag` AS `imcoe_flag`',
        '`source_duplicate_count` AS `source_duplicate_count`',
        '`person_link_method` AS `person_link_method`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_DRUG_EXPENDITURE_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.elective_access_entry ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_ELECTIVE_ACCESS_ENTRY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`waiting_list_oid` AS `waiting_list_oid`',
    '`source_system_oid` AS `source_system_oid`',
    '`pathway_oid` AS `pathway_oid`',
    '`referral_oid` AS `referral_oid`',
    '`patient_oid` AS `patient_oid`',
    '`waiting_list_id` AS `waiting_list_id`',
    '`legacy_waiting_list_id` AS `legacy_waiting_list_id`',
    '`waiting_list_name` AS `waiting_list_name`',
    '`waiting_list_code` AS `waiting_list_code`',
    '`department` AS `department`',
    '`division` AS `division`',
    '`business_unit` AS `business_unit`',
    '`clinical_priority_code` AS `clinical_priority_code`',
    '`clinical_priority_recorded_datetime` AS `clinical_priority_recorded_datetime`',
    '`site_rvid` AS `site_rvid`',
    '`treatment_function_rvid` AS `treatment_function_rvid`',
    '`admin_category_rvid` AS `admin_category_rvid`',
    '`intended_management_rvid` AS `intended_management_rvid`',
    '`admit_method_rvid` AS `admit_method_rvid`',
    '`priority_rvid` AS `priority_rvid`',
    '`status_rvid` AS `status_rvid`',
    '`elective_admission_type_rvid` AS `elective_admission_type_rvid`',
    '`encounter_type_rvid` AS `encounter_type_rvid`',
    '`removal_reason_rvid` AS `removal_reason_rvid`',
    '`division_rvid` AS `division_rvid`',
    '`tci_location_rvid` AS `tci_location_rvid`',
    '`admit_offer_outcome_rvid` AS `admit_offer_outcome_rvid`',
    '`lead_clinician_prid` AS `lead_clinician_prid`',
    '`status_reason` AS `status_reason`',
    '`status_change_datetime` AS `status_change_datetime`',
    '`decided_to_admit_datetime` AS `decided_to_admit_datetime`',
    '`tci_datetime` AS `tci_datetime`',
    '`tci_future_ind` AS `tci_future_ind`',
    '`tci_created_datetime` AS `tci_created_datetime`',
    '`guaranteed_activity_datetime` AS `guaranteed_activity_datetime`',
    '`actual_guaranteed_activity_datetime` AS `actual_guaranteed_activity_datetime`',
    '`planned_datetime` AS `planned_datetime`',
    '`earliest_reasonable_offer_datetime` AS `earliest_reasonable_offer_datetime`',
    '`admit_datetime` AS `admit_datetime`',
    '`comments` AS `comments`',
    '`active_ind` AS `active_ind`',
    '`created_datetime` AS `created_datetime`',
    '`created_by_prid` AS `created_by_prid`',
    '`modified_datetime` AS `modified_datetime`',
    '`modified_by_prid` AS `modified_by_prid`',
    '`person_link_status` AS `person_link_status`',
    '`person_link_method` AS `person_link_method`',
    '`identifier_link_status` AS `identifier_link_status`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_elective_access_entry_metadata; source history stays on the main research table.
CLINICAL_ELECTIVE_ACCESS_ENTRY_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 3,149,964 rows of
    # 3,150,053 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_ELECTIVE_ACCESS_ENTRY_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 89 of 3,150,053 at the profile.
    "gold.clinical.elective_access_entry.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 20,927 of 3,150,053 rows (0.664%) when profiled on 2026-08-24.
    "gold.clinical.elective_access_entry.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 4 of 3,150,053 rows (0.000127%) when profiled on 2026-08-24.
    "gold.clinical.elective_access_entry.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",

    # Left as a warning because it fires on 488,498 of 3,150,053 rows (15.5%) when profiled
    # on 2026-08-24 -- at that rate the rule's assumption about what event_datetime and
    # event_end_datetime mean is the thing in doubt, not the data. The inverted gaps are
    # mostly minutes, which reads as two clocks rather than two events in the wrong order.
    "gold.clinical.elective_access_entry.table.ordering_violation_event_datetime_event_end_datetime":
        "NOT COALESCE((`event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime`), FALSE)",
}

CLINICAL_ELECTIVE_ACCESS_ENTRY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "B3 bronze field PATIENT_OID; source value retained unless documented as derived.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "B3 bronze field CREATED_DT_TM; source value retained unless documented as derived.",
    "event_end_datetime": "B3 bronze field ADMIT_DT_TM_CLEAN; source value retained unless documented as derived.",
    "source_coding_system": "B3 bronze field PROCEDURE_CODE; source value retained unless documented as derived.",
    "source_code": "B3 bronze field PROCEDURE_CODE; source value retained unless documented as derived.",
    "source_display": "B3 bronze field PROCEDURE_DESC; source value retained unless documented as derived.",
    "waiting_list_oid": "LUNA WAITING_LIST_OID as BIGINT; second component of the primary key.",
    "source_system_oid": "LUNA SOURCE_SYSTEM_OID as BIGINT; first component of the primary key.",
    "pathway_oid": "B3 bronze field PATHWAY_OID; source value retained unless documented as derived.",
    "referral_oid": "B3 bronze field REFERRAL_OID; source value retained unless documented as derived.",
    "patient_oid": "B3 bronze field PATIENT_OID; source value retained unless documented as derived.",
    "waiting_list_id": "B3 bronze field WAITING_LIST_ID; source value retained unless documented as derived.",
    "legacy_waiting_list_id": "B3 bronze field LEGACY_WAITING_LIST_ID; source value retained unless documented as derived.",
    "waiting_list_name": "B3 bronze field WAITING_LIST_NAME; source value retained unless documented as derived.",
    "waiting_list_code": "B3 bronze field WAITING_LIST_CODE; source value retained unless documented as derived.",
    "department": "B3 bronze field DEPARTMENT; source value retained unless documented as derived.",
    "division": "B3 bronze field DIVISION; source value retained unless documented as derived.",
    "business_unit": "B3 bronze field BUSINESS_UNIT; source value retained unless documented as derived.",
    "clinical_priority_code": "B3 bronze field FIELD_VALUE_VAR; source value retained unless documented as derived.",
    "clinical_priority_recorded_datetime": "B3 bronze field FIELD_VALUE_DATE_CLEAN; source value retained unless documented as derived.",
    "site_rvid": "B3 bronze field SITE_RVID; source value retained unless documented as derived.",
    "treatment_function_rvid": "B3 bronze field TREATMENT_FUNCTION_RVID; source value retained unless documented as derived.",
    "admin_category_rvid": "B3 bronze field ADMIN_CATEGORY_RVID; source value retained unless documented as derived.",
    "intended_management_rvid": "B3 bronze field INTENDED_MANAGEMENT_RVID; source value retained unless documented as derived.",
    "admit_method_rvid": "B3 bronze field ADMIT_METHOD_RVID; source value retained unless documented as derived.",
    "priority_rvid": "B3 bronze field WAITING_LIST_PRIORITY_RVID; source value retained unless documented as derived.",
    "status_rvid": "B3 bronze field WAITING_LIST_STATUS_RVID; source value retained unless documented as derived.",
    "elective_admission_type_rvid": "B3 bronze field ELECTIVE_ADMISSION_TYPE_RVID; source value retained unless documented as derived.",
    "encounter_type_rvid": "B3 bronze field ENCOUNTER_TYPE_RVID; source value retained unless documented as derived.",
    "removal_reason_rvid": "B3 bronze field REMOVAL_REASON_RVID; source value retained unless documented as derived.",
    "division_rvid": "B3 bronze field DIVISION_RVID; source value retained unless documented as derived.",
    "tci_location_rvid": "B3 bronze field TCI_LOCATION_RVID; source value retained unless documented as derived.",
    "admit_offer_outcome_rvid": "B3 bronze field ADMIT_OFFER_OUTCOME_RVID; source value retained unless documented as derived.",
    "lead_clinician_prid": "B3 bronze field LEAD_CLINICIAN_PRID; source value retained unless documented as derived.",
    "status_reason": "B3 bronze field WAITING_LIST_STATUS_REASON; source value retained unless documented as derived.",
    "status_change_datetime": "B3 bronze field WAITING_LIST_STATUS_CHANGE_DT_TM_CLEAN; source value retained unless documented as derived.",
    "decided_to_admit_datetime": "B3 bronze field DECIDED_TO_ADMIT_DT_TM_CLEAN; source value retained unless documented as derived.",
    "tci_datetime": "B3 bronze field TCI_DT_TM_CLEAN; source value retained unless documented as derived.",
    "tci_future_ind": "B3 bronze field TCI_DT_TM_FUTURE_IND; source value retained unless documented as derived.",
    "tci_created_datetime": "B3 bronze field TCI_CREATED_DT_TM; source value retained unless documented as derived.",
    "guaranteed_activity_datetime": "B3 bronze field GUARANTEED_ACTIVITY_DT_TM_CLEAN; source value retained unless documented as derived.",
    "actual_guaranteed_activity_datetime": "B3 bronze field ACTUAL_GUARANTEED_ACTIVITY_DT_TM_CLEAN; source value retained unless documented as derived.",
    "planned_datetime": "B3 bronze field PLANNED_DT_TM_CLEAN; source value retained unless documented as derived.",
    "earliest_reasonable_offer_datetime": "B3 bronze field EARLIEST_REASONABLE_OFFER_DT_TM_CLEAN; source value retained unless documented as derived.",
    "admit_datetime": "B3 bronze field ADMIT_DT_TM_CLEAN; source value retained unless documented as derived.",
    "comments": "Direct identifier published and IG-governed at serve time",
    "active_ind": "Source closure state retained verbatim; false does not mean source absence.",
    "created_datetime": "B3 bronze field CREATED_DT_TM; source value retained unless documented as derived.",
    "created_by_prid": "B3 bronze field CREATED_BY_PRID; source value retained unless documented as derived.",
    "modified_datetime": "B3 bronze field MODIFIED_DT_TM; source value retained unless documented as derived.",
    "modified_by_prid": "B3 bronze field MODIFIED_BY_PRID; source value retained unless documented as derived.",
    "person_link_status": "Cross-arm consensus status; conflicts never publish PERSON_ID.",
    "person_link_method": "B3 bronze field PERSON_LINK_METHOD; source value retained unless documented as derived.",
    "identifier_link_status": "B3 bronze field IDENTIFIER_LINK_STATUS; source value retained unless documented as derived.",
    "linkage_historical_fallback_ind": "B3 bronze field LINKAGE_HISTORICAL_FALLBACK_IND; source value retained unless documented as derived.",
    "linkage_fallback_conflict_ind": "B3 bronze field LINKAGE_FALLBACK_CONFLICT_IND; source value retained unless documented as derived.",
    "nhs_number_valid_ind": "B3 bronze field NHS_NUMBER_VALID_IND; source value retained unless documented as derived.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each elective access entry record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each elective access entry record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each elective access entry record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each elective access entry record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each elective access entry record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_elective_access_list.SOURCE_PRESENT_IND is false; otherwise superseded when ACTIVE_IND is false; otherwise active. Each missing flag defaults true, and retraction takes priority over supersession. This is a derived label, not the source closure flag.",
    "record_status_effective_from": "map_elective_access_list.CREATED_DT_TM carried directly as the record-status start, without event-time clamping. No admission, procedure or child-attribute date replaces it.",
    "record_status_effective_to": "map_elective_access_list.SOURCE_ABSENT_DETECTED_TS for a retracted row; otherwise MODIFIED_DT_TM for a superseded row; otherwise null. Retraction takes priority. A missing timestamp in the selected branch remains null: there is no fallback to the other branch or ADC_UPDT.",
    "source_update_timestamp": "map_elective_access_list.MODIFIED_DT_TM carried unchanged, not a maximum over the joined procedure or attribute children. No ingestion or Silver refresh timestamp is substituted.",
    "loaded_at": "map_elective_access_list.ADC_UPDT carried unchanged. Procedure and attribute children contribute clinical fields but their clocks are not selected or aggregated here; this is not the latest child load time or the current Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_elective_access_entry"),
    comment="Internal quality-controlled twin of clinical_elective_access_entry: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_ELECTIVE_ACCESS_ENTRY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_ELECTIVE_ACCESS_ENTRY_ADVISORY_RULES)
def _gold_qc_clinical_elective_access_entry():
    """Quality-controlled twin of journey_clinical.elective_access_entry."""
    # 1 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "clinical_elective_access_entry",
        CLINICAL_ELECTIVE_ACCESS_ENTRY_SELECT,
        fk_columns=["person_id"],
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, CLINICAL_ELECTIVE_ACCESS_ENTRY_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.elective_access_entry"),
    comment=(
        "One LUNA elective-access entry. Gold QC twin of the silver product: 1 columns are "
        "repaired or nulled, 1 rule(s) drop rows, 4 check(s) are advisory. Each rule states "
        "its reason in the pipeline notebook, and Lakeflow expectation metrics report what "
        "every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_elective_access_entry():
    """Contract-v2 public twin of clinical_elective_access_entry; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_elective_access_entry")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`waiting_list_oid` AS `waiting_list_oid`',
        '`source_system_oid` AS `source_system_oid`',
        '`pathway_oid` AS `pathway_oid`',
        '`referral_oid` AS `referral_oid`',
        '`patient_oid` AS `patient_oid`',
        '`waiting_list_id` AS `waiting_list_id`',
        '`legacy_waiting_list_id` AS `legacy_waiting_list_id`',
        '`waiting_list_name` AS `waiting_list_name`',
        '`waiting_list_code` AS `waiting_list_code`',
        '`department` AS `department`',
        '`division` AS `division`',
        '`business_unit` AS `business_unit`',
        '`clinical_priority_code` AS `clinical_priority_code`',
        '`clinical_priority_recorded_datetime` AS `clinical_priority_recorded_datetime`',
        '`site_rvid` AS `site_rvid`',
        '`treatment_function_rvid` AS `treatment_function_rvid`',
        '`admin_category_rvid` AS `admin_category_rvid`',
        '`intended_management_rvid` AS `intended_management_rvid`',
        '`admit_method_rvid` AS `admit_method_rvid`',
        '`priority_rvid` AS `priority_rvid`',
        '`status_rvid` AS `status_rvid`',
        '`elective_admission_type_rvid` AS `elective_admission_type_rvid`',
        '`encounter_type_rvid` AS `encounter_type_rvid`',
        '`removal_reason_rvid` AS `removal_reason_rvid`',
        '`division_rvid` AS `division_rvid`',
        '`tci_location_rvid` AS `tci_location_rvid`',
        '`admit_offer_outcome_rvid` AS `admit_offer_outcome_rvid`',
        '`lead_clinician_prid` AS `lead_clinician_prid`',
        '`status_reason` AS `status_reason`',
        '`status_change_datetime` AS `status_change_datetime`',
        '`decided_to_admit_datetime` AS `decided_to_admit_datetime`',
        '`tci_datetime` AS `tci_datetime`',
        '`tci_future_ind` AS `tci_future_ind`',
        '`tci_created_datetime` AS `tci_created_datetime`',
        '`guaranteed_activity_datetime` AS `guaranteed_activity_datetime`',
        '`actual_guaranteed_activity_datetime` AS `actual_guaranteed_activity_datetime`',
        '`planned_datetime` AS `planned_datetime`',
        '`earliest_reasonable_offer_datetime` AS `earliest_reasonable_offer_datetime`',
        '`admit_datetime` AS `admit_datetime`',
        '`comments` AS `comments`',
        '`active_ind` AS `active_ind`',
        '`created_datetime` AS `created_datetime`',
        '`created_by_prid` AS `created_by_prid`',
        '`modified_datetime` AS `modified_datetime`',
        '`modified_by_prid` AS `modified_by_prid`',
        '`person_link_status` AS `person_link_status`',
        '`person_link_method` AS `person_link_method`',
        '`identifier_link_status` AS `identifier_link_status`',
        '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
        '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
        '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_ELECTIVE_ACCESS_ENTRY_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.hrg_grouping ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_HRG_GROUPING_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`cds_id` AS `cds_id`',
    '`source_object` AS `source_object`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`cds_record_id` AS `cds_record_id`',
    '`person_link_method` AS `person_link_method`',
    "CASE WHEN CAST(`admission_datetime` AS DATE) = DATE'1800-01-01' OR CAST(`admission_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`admission_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `admission_datetime` END AS `admission_datetime`",
    '`discharge_datetime` AS `discharge_datetime`',
    '`episode_start_datetime` AS `episode_start_datetime`',
    'CASE WHEN `episode_start_datetime` IS NOT NULL AND `episode_end_datetime` IS NOT NULL AND `episode_start_datetime` > `episode_end_datetime` THEN NULL ELSE `episode_end_datetime` END AS `episode_end_datetime`',
    '`hosp_prov_spell_num` AS `hosp_prov_spell_num`',
    '`provider_org_cd` AS `provider_org_cd`',
    '`episode_order` AS `episode_order`',
    '`episode_duration_days` AS `episode_duration_days`',
    '`main_specialty_cd` AS `main_specialty_cd`',
    '`treatment_function_cd` AS `treatment_function_cd`',
    '`admission_method_cd` AS `admission_method_cd`',
    '`admission_source_cd` AS `admission_source_cd`',
    '`admission_source_desc` AS `admission_source_desc`',
    '`discharge_method_cd` AS `discharge_method_cd`',
    '`discharge_dest_cd` AS `discharge_dest_cd`',
    '`discharge_dest_desc` AS `discharge_dest_desc`',
    '`patient_class_cd` AS `patient_class_cd`',
    '`patient_class_desc` AS `patient_class_desc`',
    '`source_age` AS `source_age`',
    '`source_sex_cd` AS `source_sex_cd`',
    '`neonatal_care_level_cd` AS `neonatal_care_level_cd`',
    '`critical_care_days` AS `critical_care_days`',
    '`rehab_days` AS `rehab_days`',
    '`fce_hrg_cd` AS `fce_hrg_cd`',
    '`fce_hrg_desc` AS `fce_hrg_desc`',
    '`fce_grouping_method_flag` AS `fce_grouping_method_flag`',
    '`fce_dominant_proc_cd` AS `fce_dominant_proc_cd`',
    '`fce_dominant_proc_desc` AS `fce_dominant_proc_desc`',
    '`fce_pbc_cd` AS `fce_pbc_cd`',
    '`fce_calc_episode_duration` AS `fce_calc_episode_duration`',
    '`fce_reporting_episode_duration` AS `fce_reporting_episode_duration`',
    '`dominant_episode_flag` AS `dominant_episode_flag`',
    '`spell_hrg_cd` AS `spell_hrg_cd`',
    '`spell_hrg_desc` AS `spell_hrg_desc`',
    '`spell_grouping_method_flag` AS `spell_grouping_method_flag`',
    '`spell_dominant_proc_cd` AS `spell_dominant_proc_cd`',
    '`spell_dominant_proc_desc` AS `spell_dominant_proc_desc`',
    '`spell_primary_diag_cd` AS `spell_primary_diag_cd`',
    '`spell_primary_diag_desc` AS `spell_primary_diag_desc`',
    '`spell_secondary_diag_cd` AS `spell_secondary_diag_cd`',
    '`spell_secondary_diag_desc` AS `spell_secondary_diag_desc`',
    '`spell_episode_count` AS `spell_episode_count`',
    '`spell_los` AS `spell_los`',
    '`spell_reporting_los` AS `spell_reporting_los`',
    '`spell_critical_care_days` AS `spell_critical_care_days`',
    '`spell_ssc_cd` AS `spell_ssc_cd`',
    '`spell_best_practice_cd` AS `spell_best_practice_cd`',
    '`first_attend_cd` AS `first_attend_cd`',
    '`first_attend_desc` AS `first_attend_desc`',
    '`grouping_method_flag` AS `grouping_method_flag`',
    '`dominant_proc_cd` AS `dominant_proc_cd`',
    '`dominant_proc_desc` AS `dominant_proc_desc`',
    '`grouper_errors` AS `grouper_errors`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_hrg_grouping_metadata; source history stays on the main research table.
CLINICAL_HRG_GROUPING_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 47,137,282 rows of 47,137,282
    # that are current and attributable; identity_status = 'resolved' keeps the 43,616,250
    # rows of 47,137,282 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_HRG_GROUPING_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 3,521,032 of 47,137,282 at the profile.
    "gold.clinical.hrg_grouping.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 47,137,282 at the profile.
    "gold.clinical.hrg_grouping.record_status.default_view_active":
        "record_status = 'active'",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 13 of 47,137,282
    # rows (2.76e-05%) when profiled on 2026-08-24.
    "gold.clinical.hrg_grouping.record_status_effective_from.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 147,553 of 47,137,282 rows (0.313%) when profiled on 2026-08-24.
    "gold.clinical.hrg_grouping.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 43,005 of 47,137,282 rows (0.0912%) when profiled on 2026-08-24.
    "gold.clinical.hrg_grouping.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_HRG_GROUPING_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "cds_id": "Native CDS_APC_ID or CDS_OPA_ID; primary key within source_object.",
    "source_object": "SLAM source arm (slam_apc_hrg or slam_op_hrg); part of the primary key with cds_id.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Millennium PERSON_ID resolved from source MRN via mill_person_alias type 10 (deterministic tiebreak); null when unresolved — source MRN is retained only in raw.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date and time the finished consultant episode started.",
    "event_end_datetime": "Date and time the finished consultant episode ended.",
    "source_coding_system": "HRG4+ code assigned to this finished consultant episode.",
    "source_code": "HRG4+ code assigned to this finished consultant episode.",
    "source_display": "Description of the episode-level HRG4+ code, mapped from the HRG v4 reference lookup.",
    "cds_record_id": "Trimmed CDS admitted-patient-care episode identifier; unique key; joins back to raw.",
    "person_link_method": "How PERSON_ID was resolved: MRN_ALIAS, or null when unlinked.",
    "admission_datetime": "Date and time the patient was admitted for the hospital provider spell.",
    "discharge_datetime": "Date and time the patient was discharged from the hospital provider spell.",
    "episode_start_datetime": "Date and time the finished consultant episode started.",
    "episode_end_datetime": "Date and time the finished consultant episode ended.",
    "hosp_prov_spell_num": "Hospital provider spell number grouping episodes into spells.",
    "provider_org_cd": "NHS organisation (provider) code identifying the submitting healthcare provider for the episode.",
    "episode_order": "Sequence number of this episode within the hospital provider spell.",
    "episode_duration_days": "Length of the episode in days as submitted to the grouper.",
    "main_specialty_cd": "NHS main specialty code recorded for the episode.",
    "treatment_function_cd": "NHS Treatment Function Code (national standard).",
    "admission_method_cd": "NHS admission method code for the hospital provider spell.",
    "admission_source_cd": "NHS source of admission code for the hospital provider spell.",
    "admission_source_desc": "Description of the source of admission code, mapped from the CDS admission source lookup.",
    "discharge_method_cd": "NHS discharge method code for the hospital provider spell.",
    "discharge_dest_cd": "NHS discharge destination code for the hospital provider spell.",
    "discharge_dest_desc": "Description of the discharge destination code, mapped from the CDS discharge destination lookup.",
    "patient_class_cd": "CDS patient classification code for the episode, such as ordinary or day case admission.",
    "patient_class_desc": "Description of the patient classification code, mapped from the CDS patient class lookup.",
    "source_age": "NON-CANONICAL grouper input age as submitted to SLAM; demographics of record are in map_person.",
    "source_sex_cd": "NON-CANONICAL grouper input sex code as submitted to SLAM; demographics of record are in map_person.",
    "neonatal_care_level_cd": "Coded level of neonatal care recorded for the episode as submitted to the grouper.",
    "critical_care_days": "Number of critical care days for the episode as submitted to the grouper.",
    "rehab_days": "Number of rehabilitation days for the episode as submitted to the grouper.",
    "fce_hrg_cd": "HRG4+ code assigned to this finished consultant episode.",
    "fce_hrg_desc": "Description of the episode-level HRG4+ code, mapped from the HRG v4 reference lookup.",
    "fce_grouping_method_flag": "Grouper-returned flag indicating the method by which the HRG4+ code was derived for this finished consultant episode.",
    "fce_dominant_proc_cd": "OPCS-4 procedure code identified by the HRG4+ grouper as the dominant procedure for this episode; null where no procedure drove the grouping.",
    "fce_dominant_proc_desc": "Description of the episode dominant procedure code, mapped from the OPCS-4.10 reference lookup.",
    "fce_pbc_cd": "Programme Budgeting Category code assigned by the HRG4+ grouper to this episode.",
    "fce_calc_episode_duration": "Grouper-calculated episode duration in days for this finished consultant episode.",
    "fce_reporting_episode_duration": "Episode duration in days as used for HRG reporting purposes by the grouper.",
    "dominant_episode_flag": "Grouper flag indicating whether this episode is the dominant episode used to derive the spell HRG.",
    "spell_hrg_cd": "HRG4+ code assigned at hospital-spell level (repeated on every episode of the spell).",
    "spell_hrg_desc": "Description of the spell-level HRG4+ code, mapped from the HRG v4 reference lookup.",
    "spell_grouping_method_flag": "Grouper-returned flag indicating the method by which the spell-level HRG4+ code was derived.",
    "spell_dominant_proc_cd": "OPCS-4 procedure code identified by the HRG4+ grouper as the dominant procedure for the hospital provider spell.",
    "spell_dominant_proc_desc": "Description of the spell dominant procedure code, mapped from the OPCS-4.10 reference lookup.",
    "spell_primary_diag_cd": "ICD-10 primary diagnosis code used by the grouper at hospital provider spell level.",
    "spell_primary_diag_desc": "Description of the spell primary diagnosis code, mapped from the ICD diagnosis reference lookup.",
    "spell_secondary_diag_cd": "ICD-10 secondary diagnosis code used by the grouper at hospital provider spell level.",
    "spell_secondary_diag_desc": "Description of the spell secondary diagnosis code, mapped from the ICD diagnosis reference lookup.",
    "spell_episode_count": "Number of finished consultant episodes counted by the grouper within the hospital provider spell.",
    "spell_los": "Grouper-calculated length of stay in days for the hospital provider spell.",
    "spell_reporting_los": "Spell length of stay in days as used for HRG reporting purposes by the grouper.",
    "spell_critical_care_days": "Number of critical care days recorded for the hospital provider spell as submitted to the grouper.",
    "spell_ssc_cd": "Specialised service code assigned by the HRG4+ grouper at hospital provider spell level.",
    "spell_best_practice_cd": "Best practice tariff code assigned by the HRG4+ grouper at hospital provider spell level, null where no best practice classification applies.",
    "first_attend_cd": "CDS first attendance code indicating whether the attendance was a first or follow-up contact and its modality.",
    "first_attend_desc": "Description of FIRST_ATTEND_CD obtained from the lookup 3_lookup.dwh.cds_first_attend.",
    "grouping_method_flag": "HRG4+ grouper flag indicating the method by which the HRG was derived for the attendance.",
    "dominant_proc_cd": "OPCS-4 code of the dominant procedure determined by the grouper for the attendance.",
    "dominant_proc_desc": "Description of DOMINANT_PROC_CD obtained from the lookup 3_lookup.dwh.opcs_410.",
    "grouper_errors": "Grouper error/quality messages.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each HRG grouping record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each HRG grouping record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each HRG grouping record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each HRG grouping record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each HRG grouping record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "For both SLAM APC and outpatient HRG arms, retracted when the arm's SOURCE_PRESENT_IND is false, otherwise active; a missing presence flag defaults true. Neither arm emits superseded. This is a source-row presence status, not an HRG grouper result or clinical episode state.",
    "record_status_effective_from": "map_slam_apc_hrg.EPISODE_START_DT_TM for the APC arm, or map_slam_op_hrg.ATTENDANCE_DT_TM for the outpatient arm, cast to TIMESTAMP. Unlike event_datetime, this history field is not range-clamped to [1950,2100). It is an event-time proxy rather than an independently observed row-status transition; missing values remain null.",
    "record_status_effective_to": "The relevant SLAM arm's SOURCE_ABSENT_DETECTED_TS only when that row is retracted; otherwise null. A missing absence-detection time remains null: episode end, discharge and ADC_UPDT are not substituted.",
    "source_update_timestamp": "SOURCE_RECORD_UPDATED_DT carried unchanged from the contributing map_slam_apc_hrg or map_slam_op_hrg row. It is separate from ADC_UPDT ingestion provenance; the union does not aggregate clocks across arms and Silver refresh time is not substituted.",
    "loaded_at": "ADC_UPDT carried unchanged from the contributing map_slam_apc_hrg or map_slam_op_hrg row through the common HRG stage. This is per-row bronze ingestion provenance, not episode/attendance time or Silver refresh time; missing values remain null.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_hrg_grouping"),
    comment="Internal quality-controlled twin of clinical_hrg_grouping: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_HRG_GROUPING_MANDATORY_RULES)
@dp.expect_all(CLINICAL_HRG_GROUPING_ADVISORY_RULES)
def _gold_qc_clinical_hrg_grouping():
    """Quality-controlled twin of journey_clinical.hrg_grouping."""
    # 5 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "clinical_hrg_grouping",
        CLINICAL_HRG_GROUPING_SELECT,
        fk_columns=["person_id"],
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, CLINICAL_HRG_GROUPING_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.hrg_grouping"),
    comment=(
        "One SLAM APC or outpatient HRG grouping row. Gold QC twin of the silver product: 6 "
        "columns are repaired or nulled, 1 rule(s) drop rows, 5 check(s) are advisory. Each "
        "rule states its reason in the pipeline notebook, and Lakeflow expectation metrics "
        "report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_hrg_grouping():
    """Contract-v2 public twin of clinical_hrg_grouping; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_hrg_grouping")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`cds_id` AS `cds_id`',
        '`source_object` AS `source_object`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`cds_record_id` AS `cds_record_id`',
        '`person_link_method` AS `person_link_method`',
        '`admission_datetime` AS `admission_datetime`',
        '`discharge_datetime` AS `discharge_datetime`',
        '`episode_start_datetime` AS `episode_start_datetime`',
        '`episode_end_datetime` AS `episode_end_datetime`',
        '`hosp_prov_spell_num` AS `hosp_prov_spell_num`',
        '`provider_org_cd` AS `provider_org_cd`',
        '`episode_order` AS `episode_order`',
        '`episode_duration_days` AS `episode_duration_days`',
        '`main_specialty_cd` AS `main_specialty_cd`',
        '`treatment_function_cd` AS `treatment_function_cd`',
        '`admission_method_cd` AS `admission_method_cd`',
        '`admission_source_cd` AS `admission_source_cd`',
        '`admission_source_desc` AS `admission_source_desc`',
        '`discharge_method_cd` AS `discharge_method_cd`',
        '`discharge_dest_cd` AS `discharge_dest_cd`',
        '`discharge_dest_desc` AS `discharge_dest_desc`',
        '`patient_class_cd` AS `patient_class_cd`',
        '`patient_class_desc` AS `patient_class_desc`',
        '`source_age` AS `source_age`',
        '`source_sex_cd` AS `source_sex_cd`',
        '`neonatal_care_level_cd` AS `neonatal_care_level_cd`',
        '`critical_care_days` AS `critical_care_days`',
        '`rehab_days` AS `rehab_days`',
        '`fce_hrg_cd` AS `fce_hrg_cd`',
        '`fce_hrg_desc` AS `fce_hrg_desc`',
        '`fce_grouping_method_flag` AS `fce_grouping_method_flag`',
        '`fce_dominant_proc_cd` AS `fce_dominant_proc_cd`',
        '`fce_dominant_proc_desc` AS `fce_dominant_proc_desc`',
        '`fce_pbc_cd` AS `fce_pbc_cd`',
        '`fce_calc_episode_duration` AS `fce_calc_episode_duration`',
        '`fce_reporting_episode_duration` AS `fce_reporting_episode_duration`',
        '`dominant_episode_flag` AS `dominant_episode_flag`',
        '`spell_hrg_cd` AS `spell_hrg_cd`',
        '`spell_hrg_desc` AS `spell_hrg_desc`',
        '`spell_grouping_method_flag` AS `spell_grouping_method_flag`',
        '`spell_dominant_proc_cd` AS `spell_dominant_proc_cd`',
        '`spell_dominant_proc_desc` AS `spell_dominant_proc_desc`',
        '`spell_primary_diag_cd` AS `spell_primary_diag_cd`',
        '`spell_primary_diag_desc` AS `spell_primary_diag_desc`',
        '`spell_secondary_diag_cd` AS `spell_secondary_diag_cd`',
        '`spell_secondary_diag_desc` AS `spell_secondary_diag_desc`',
        '`spell_episode_count` AS `spell_episode_count`',
        '`spell_los` AS `spell_los`',
        '`spell_reporting_los` AS `spell_reporting_los`',
        '`spell_critical_care_days` AS `spell_critical_care_days`',
        '`spell_ssc_cd` AS `spell_ssc_cd`',
        '`spell_best_practice_cd` AS `spell_best_practice_cd`',
        '`first_attend_cd` AS `first_attend_cd`',
        '`first_attend_desc` AS `first_attend_desc`',
        '`grouping_method_flag` AS `grouping_method_flag`',
        '`dominant_proc_cd` AS `dominant_proc_cd`',
        '`dominant_proc_desc` AS `dominant_proc_desc`',
        '`grouper_errors` AS `grouper_errors`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_HRG_GROUPING_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.pathway_tracking ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_PATHWAY_TRACKING_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`ptl_unique_id` AS `ptl_unique_id`',
    '`ptl_group_id` AS `ptl_group_id`',
    '`archived_pathway` AS `archived_pathway`',
    '`pathway_oid` AS `pathway_oid`',
    '`referral_oid` AS `referral_oid`',
    '`patient_oid` AS `patient_oid`',
    '`ptl_activity_oid` AS `ptl_activity_oid`',
    '`parent_ptl_unique_id` AS `parent_ptl_unique_id`',
    '`parent_ptl_activity_oid` AS `parent_ptl_activity_oid`',
    '`parent_present_ind` AS `parent_present_ind`',
    '`latest_activity_type` AS `latest_activity_type`',
    '`latest_activity_oid` AS `latest_activity_oid`',
    '`latest_activity_date_future_ind` AS `latest_activity_date_future_ind`',
    '`days_waited` AS `days_waited`',
    '`specialty` AS `specialty`',
    '`treatment_function` AS `treatment_function`',
    "CASE WHEN UPPER(TRIM(CAST(`treatment_function_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `treatment_function_code` END AS `treatment_function_code`",
    '`site` AS `site`',
    '`site_group` AS `site_group`',
    '`division` AS `division`',
    '`lead_clinician` AS `lead_clinician`',
    '`lead_clinician_prid` AS `lead_clinician_prid`',
    '`site_rvid` AS `site_rvid`',
    '`source_key_status` AS `source_key_status`',
    '`patient_spine_ind` AS `patient_spine_ind`',
    '`pathway_spine_ind` AS `pathway_spine_ind`',
    '`referral_spine_ind` AS `referral_spine_ind`',
    '`patient_spine_link_status` AS `patient_spine_link_status`',
    '`pathway_spine_link_status` AS `pathway_spine_link_status`',
    '`referral_spine_link_status` AS `referral_spine_link_status`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
    '`person_link_status` AS `person_link_status`',
    '`person_link_method` AS `person_link_method`',
    '`identifier_link_status` AS `identifier_link_status`',
    '`source_system_oid` AS `source_system_oid`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_pathway_tracking_metadata; source history stays on the main research table.
CLINICAL_PATHWAY_TRACKING_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 16,277,672 rows of 16,305,277
    # that are current and attributable; identity_status = 'resolved' keeps the 16,253,290
    # rows of 16,305,277 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_PATHWAY_TRACKING_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 51,987 of 16,305,277 at the profile.
    "gold.clinical.pathway_tracking.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 27,605 of 16,305,277 at the profile.
    "gold.clinical.pathway_tracking.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 85,762 of 16,305,277 rows (0.526%) when profiled on 2026-08-24.
    "gold.clinical.pathway_tracking.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 917 of 16,305,277 rows (0.00562%) when profiled on 2026-08-24.
    "gold.clinical.pathway_tracking.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_PATHWAY_TRACKING_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "B3 bronze field PATIENT_OID; source value retained unless documented as derived.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "B3 bronze field LATEST_ACTIVITY_DATE_CLEAN; source value retained unless documented as derived.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each pathway tracking record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each pathway tracking record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "Code supplied by the originating source system for the represented concept for each pathway tracking record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_display": "Human-readable label supplied by the source system for the source code for each pathway tracking record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "ptl_unique_id": "Pathfinder PTL_UNIQUE_ID as BIGINT; primary key of this table.",
    "ptl_group_id": "B3 bronze field PTL_GROUP_ID; source value retained unless documented as derived.",
    "archived_pathway": "Source archived-pathway state retained verbatim; never used as a publication gate.",
    "pathway_oid": "B3 bronze field PATHWAY_OID; source value retained unless documented as derived.",
    "referral_oid": "B3 bronze field REFERRAL_OID; source value retained unless documented as derived.",
    "patient_oid": "B3 bronze field PATIENT_OID; source value retained unless documented as derived.",
    "ptl_activity_oid": "B3 bronze field PTL_ACTIVITY_OID; source value retained unless documented as derived.",
    "parent_ptl_unique_id": "B3 bronze field PARENT_PTL_UNIQUE_ID; source value retained unless documented as derived.",
    "parent_ptl_activity_oid": "B3 bronze field PARENT_PTL_ACTIVITY_OID; source value retained unless documented as derived.",
    "parent_present_ind": "Whether a non-null parent PTL_UNIQUE_ID exists in the pinned source snapshot.",
    "latest_activity_type": "B3 bronze field LATEST_ACTIVITY_TYPE; source value retained unless documented as derived.",
    "latest_activity_oid": "B3 bronze field LATEST_ACTIVITY_OID; source value retained unless documented as derived.",
    "latest_activity_date_future_ind": "B3 bronze field LATEST_ACTIVITY_DATE_FUTURE_IND; source value retained unless documented as derived.",
    "days_waited": "B3 bronze field DAYS_WAITED; source value retained unless documented as derived.",
    "specialty": "B3 bronze field SPECIALTY; source value retained unless documented as derived.",
    "treatment_function": "B3 bronze field TREATMENT_FUNCTION; source value retained unless documented as derived.",
    "treatment_function_code": "B3 bronze field TREATMENT_FUNCTION_CODE; source value retained unless documented as derived.",
    "site": "B3 bronze field SITE; source value retained unless documented as derived.",
    "site_group": "B3 bronze field SITE_GROUP; source value retained unless documented as derived.",
    "division": "B3 bronze field DIVISION; source value retained unless documented as derived.",
    "lead_clinician": "B3 bronze field LEAD_CLINICIAN; source value retained unless documented as derived.",
    "lead_clinician_prid": "B3 bronze field LEAD_CLINICIAN_PRID; source value retained unless documented as derived.",
    "site_rvid": "B3 bronze field SITE_RVID; source value retained unless documented as derived.",
    "source_key_status": "COMPLETE or NAMESPACE_MISSING; missing namespaces publish and are never dropped.",
    "patient_spine_ind": "B3 bronze field PATIENT_SPINE_IND; source value retained unless documented as derived.",
    "pathway_spine_ind": "B3 bronze field PATHWAY_SPINE_IND; source value retained unless documented as derived.",
    "referral_spine_ind": "B3 bronze field REFERRAL_SPINE_IND; source value retained unless documented as derived.",
    "patient_spine_link_status": "B3 bronze field PATIENT_SPINE_LINK_STATUS; source value retained unless documented as derived.",
    "pathway_spine_link_status": "B3 bronze field PATHWAY_SPINE_LINK_STATUS; source value retained unless documented as derived.",
    "referral_spine_link_status": "B3 bronze field REFERRAL_SPINE_LINK_STATUS; source value retained unless documented as derived.",
    "nhs_number_valid_ind": "B3 bronze field NHS_NUMBER_VALID_IND; source value retained unless documented as derived.",
    "linkage_historical_fallback_ind": "B3 bronze field LINKAGE_HISTORICAL_FALLBACK_IND; source value retained unless documented as derived.",
    "linkage_fallback_conflict_ind": "B3 bronze field LINKAGE_FALLBACK_CONFLICT_IND; source value retained unless documented as derived.",
    "person_link_status": "Consensus-safe published linkage status; conflicting arms never publish PERSON_ID.",
    "person_link_method": "B3 bronze field PERSON_LINK_METHOD; source value retained unless documented as derived.",
    "identifier_link_status": "B3 bronze field IDENTIFIER_LINK_STATUS; source value retained unless documented as derived.",
    "source_system_oid": "B3 bronze field SOURCE_SYSTEM_OID; source value retained unless documented as derived.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each pathway tracking record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each pathway tracking record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each pathway tracking record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each pathway tracking record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each pathway tracking record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_cancer_ptl.SOURCE_PRESENT_IND is false, otherwise active; missing presence defaults true. No superseded status is emitted. ARCHIVED_PATHWAY, parent-presence and linkage flags do not set this label.",
    "record_status_effective_from": "map_cancer_ptl.LATEST_ACTIVITY_DATE_CLEAN carried directly as the record-status start. The projection does not replace it with the pipeline clock or a joined parent timestamp.",
    "record_status_effective_to": "map_cancer_ptl.SOURCE_ABSENT_DETECTED_TS when the source-presence flag is false; otherwise null. A missing absence-detection timestamp stays null, without an ADC_UPDT fallback.",
    "source_update_timestamp": "Bronze run-as-of timestamp carried from map_cancer_ptl.PIPELINE_UPDT_DT_TM for the whole-trust Pathfinder tracker. The bronze writer stamps inserted, changed, revived or newly absent rows; unchanged present rows retain their prior stamp. Run-as-of defaults to processing time but can be supplied explicitly. This is not a clinical event time or native-system update timestamp.",
    "loaded_at": "map_cancer_ptl.ADC_UPDT carried unchanged, distinct from the PIPELINE_UPDT_DT_TM used for source_update_timestamp. Parent and pathway keys are derived without adding parent load clocks; this is not the current Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_pathway_tracking"),
    comment="Internal quality-controlled twin of clinical_pathway_tracking: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_PATHWAY_TRACKING_MANDATORY_RULES)
@dp.expect_all(CLINICAL_PATHWAY_TRACKING_ADVISORY_RULES)
def _gold_qc_clinical_pathway_tracking():
    """Quality-controlled twin of journey_clinical.pathway_tracking."""
    # 7 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "clinical_pathway_tracking",
        CLINICAL_PATHWAY_TRACKING_SELECT,
        fk_columns=["person_id"],
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, CLINICAL_PATHWAY_TRACKING_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.pathway_tracking"),
    comment=(
        "One whole-trust Pathfinder PTL activity row. Gold QC twin of the silver product: 2 "
        "columns are repaired or nulled, 1 rule(s) drop rows, 4 check(s) are advisory. Each "
        "rule states its reason in the pipeline notebook, and Lakeflow expectation metrics "
        "report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_pathway_tracking():
    """Contract-v2 public twin of clinical_pathway_tracking; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_pathway_tracking")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`ptl_unique_id` AS `ptl_unique_id`',
        '`ptl_group_id` AS `ptl_group_id`',
        '`archived_pathway` AS `archived_pathway`',
        '`pathway_oid` AS `pathway_oid`',
        '`referral_oid` AS `referral_oid`',
        '`patient_oid` AS `patient_oid`',
        '`ptl_activity_oid` AS `ptl_activity_oid`',
        '`parent_ptl_unique_id` AS `parent_ptl_unique_id`',
        '`parent_ptl_activity_oid` AS `parent_ptl_activity_oid`',
        '`parent_present_ind` AS `parent_present_ind`',
        '`latest_activity_type` AS `latest_activity_type`',
        '`latest_activity_oid` AS `latest_activity_oid`',
        '`latest_activity_date_future_ind` AS `latest_activity_date_future_ind`',
        '`days_waited` AS `days_waited`',
        '`specialty` AS `specialty`',
        '`treatment_function` AS `treatment_function`',
        '`treatment_function_code` AS `treatment_function_code`',
        '`site` AS `site`',
        '`site_group` AS `site_group`',
        '`division` AS `division`',
        '`lead_clinician` AS `lead_clinician`',
        '`lead_clinician_prid` AS `lead_clinician_prid`',
        '`site_rvid` AS `site_rvid`',
        '`source_key_status` AS `source_key_status`',
        '`patient_spine_ind` AS `patient_spine_ind`',
        '`pathway_spine_ind` AS `pathway_spine_ind`',
        '`referral_spine_ind` AS `referral_spine_ind`',
        '`patient_spine_link_status` AS `patient_spine_link_status`',
        '`pathway_spine_link_status` AS `pathway_spine_link_status`',
        '`referral_spine_link_status` AS `referral_spine_link_status`',
        '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
        '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
        '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
        '`person_link_status` AS `person_link_status`',
        '`person_link_method` AS `person_link_method`',
        '`identifier_link_status` AS `identifier_link_status`',
        '`source_system_oid` AS `source_system_oid`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_PATHWAY_TRACKING_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.referral ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_REFERRAL_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`ubrn` AS `ubrn`',
    '`waiting_list_oid` AS `waiting_list_oid`',
    '`pathway_oid` AS `pathway_oid`',
    '`referral_priority_code` AS `referral_priority_code`',
    '`referral_priority_display` AS `referral_priority_display`',
    '`referral_source_code` AS `referral_source_code`',
    '`referral_source_display` AS `referral_source_display`',
    '`status_code` AS `status_code`',
    '`status_display` AS `status_display`',
    '`status_change_reason_code` AS `status_change_reason_code`',
    '`status_change_reason_display` AS `status_change_reason_display`',
    '`status_change_datetime` AS `status_change_datetime`',
    '`encounter_type_code` AS `encounter_type_code`',
    '`encounter_type_display` AS `encounter_type_display`',
    '`suspected_cancer_site_code` AS `suspected_cancer_site_code`',
    '`suspected_cancer_site_display` AS `suspected_cancer_site_display`',
    '`treatment_function_code` AS `treatment_function_code`',
    '`treatment_function_display` AS `treatment_function_display`',
    '`service_type_requested_code` AS `service_type_requested_code`',
    '`service_type_requested_display` AS `service_type_requested_display`',
    '`site_code` AS `site_code`',
    '`site_display` AS `site_display`',
    '`referring_facility_code` AS `referring_facility_code`',
    '`referring_facility_display` AS `referring_facility_display`',
    '`referred_by_org_id` AS `referred_by_org_id`',
    '`booking_type_code` AS `booking_type_code`',
    '`booking_type_display` AS `booking_type_display`',
    '`admin_category_code` AS `admin_category_code`',
    '`admin_category_display` AS `admin_category_display`',
    '`business_unit` AS `business_unit`',
    '`division` AS `division`',
    'CASE WHEN CAST(`original_received_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `original_received_datetime` END AS `original_received_datetime`',
    '`ers_ubrn_received` AS `ers_ubrn_received`',
    '`ers_pathway_start` AS `ers_pathway_start`',
    '`ers_service_name` AS `ers_service_name`',
    '`ers_specialty` AS `ers_specialty`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`source_system_oid` AS `source_system_oid`',
    '`referral_oid` AS `referral_oid`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_referral_metadata; source history stays on the main research table.
CLINICAL_REFERRAL_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 10,572,634 rows of
    # 10,573,076 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_REFERRAL_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 442 of 10,573,076 at the profile.
    "gold.clinical.referral.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 544 of 10,573,076 rows (0.00515%) when profiled on 2026-08-24.
    "gold.clinical.referral.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 4,649 of 10,573,076 rows (0.044%) when profiled on 2026-08-24.
    "gold.clinical.referral.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_REFERRAL_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 referral identity.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID when resolved; native person foreign key.",
    "encounter_id": "Nullable Millennium ENCNTR_ID; the LUNA referral feed supplies no encounter.",
    "event_datetime": "Referral received timestamp.",
    "event_end_datetime": "Event end timestamp; not supplied by LUNA referrals.",
    "source_coding_system": "Verbatim LUNA treatment-function coding system.",
    "source_code": "Verbatim treatment-function code.",
    "source_display": "Verbatim treatment-function display.",
    "ubrn": "Referral linkage evidence only; never a join key.",
    "waiting_list_oid": "Raw LUNA waiting-list object identifier.",
    "pathway_oid": "Raw LUNA pathway object identifier.",
    "referral_priority_code": "Source referral-priority code.",
    "referral_priority_display": "Source referral-priority display.",
    "referral_source_code": "Source referral-source code.",
    "referral_source_display": "Source referral-source display.",
    "status_code": "Source referral-status code.",
    "status_display": "Source referral-status display.",
    "status_change_reason_code": "Source referral status-change reason code.",
    "status_change_reason_display": "Source referral status-change reason display.",
    "status_change_datetime": "Source referral status-change timestamp.",
    "encounter_type_code": "Source encounter-type code.",
    "encounter_type_display": "Source encounter-type display.",
    "suspected_cancer_site_code": "Source suspected-cancer-site code.",
    "suspected_cancer_site_display": "Source suspected-cancer-site display.",
    "treatment_function_code": "Source treatment-function code.",
    "treatment_function_display": "Source treatment-function display.",
    "service_type_requested_code": "Source requested-service-type code.",
    "service_type_requested_display": "Source requested-service-type display.",
    "site_code": "Source site code.",
    "site_display": "Source site display.",
    "referring_facility_code": "Source referring-facility code.",
    "referring_facility_display": "Source referring-facility display.",
    "referred_by_org_id": "Raw referring organization identifier.",
    "booking_type_code": "Source booking-type code.",
    "booking_type_display": "Source booking-type display.",
    "admin_category_code": "Source administrative-category code.",
    "admin_category_display": "Source administrative-category display.",
    "business_unit": "Source business unit.",
    "division": "Name of the clinical division responsible for the referral within the organisational hierarchy.",
    "original_received_datetime": "Original referral received timestamp.",
    "ers_ubrn_received": "e-Referral UBRN received date.",
    "ers_pathway_start": "e-Referral pathway start date.",
    "ers_service_name": "e-Referral service name.",
    "ers_specialty": "e-Referral specialty recorded in the source referral field `ERS_SPECIALTY`.",
    "confidentiality_code": "Source confidentiality code when supplied.",
    "vip_ind": "Source VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld-identity indicator when supplied.",
    "fact_category": "Whether this fact is clinical or administrative in the v2 plane merge.",
    "source_feed": "Registered feed owning the fact.",
    "source_system_oid": "LUNA SOURCE_SYSTEM_OID; component of the native primary key.",
    "referral_oid": "LUNA REFERRAL_OID; component of the native primary key.",
    "record_status": "Superseded when map_referral.SOURCE_PRESENT_IND or ACTIVE_IND is false; each missing flag defaults true. Otherwise active. Source absence and inactivity both map to superseded, not retracted; the clinical status code does not set this row-history label.",
    "record_status_effective_from": "Date and time the referral record was created in the source system.",
    "record_status_effective_to": "For a superseded referral row, the first non-null map_referral.SOURCE_ABSENT_DETECTED_TS, MODIFIED_DATETIME or ADC_UPDT, in that order; otherwise null. The same source-presence/inactivity predicate sets record_status. No event-time clamp or clinical-event timestamp is substituted.",
    "source_update_timestamp": "map_referral.SOURCE_ADC_UPDT carried unchanged to the research row, distinct from ADC_UPDT used for loaded_at. It is not the clinical event timestamp or the current Silver refresh time.",
    "loaded_at": "map_referral.ADC_UPDT carried unchanged through the source and public projections. No parent or other source clock is joined into this value; it differs from SOURCE_ADC_UPDT used for source_update_timestamp and from the current Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_referral"),
    comment="Internal quality-controlled twin of clinical_referral: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_REFERRAL_MANDATORY_RULES)
@dp.expect_all(CLINICAL_REFERRAL_ADVISORY_RULES)
def _gold_qc_clinical_referral():
    """Quality-controlled twin of journey_clinical.referral."""
    # 4 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "clinical_referral",
        CLINICAL_REFERRAL_SELECT,
        fk_columns=["person_id"],
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, CLINICAL_REFERRAL_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.referral"),
    comment=(
        "LUNA referral fact activated as a recorded contract change from a zero-row stub with "
        "no consumer break. Gold QC twin of the silver product: 2 columns are repaired or "
        "nulled, 1 rule(s) drop rows, 3 check(s) are advisory. Each rule states its reason in "
        "the pipeline notebook, and Lakeflow expectation metrics report what every rule "
        "matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_referral():
    """Contract-v2 public twin of clinical_referral; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_referral")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`ubrn` AS `ubrn`',
        '`waiting_list_oid` AS `waiting_list_oid`',
        '`pathway_oid` AS `pathway_oid`',
        '`referral_priority_code` AS `referral_priority_code`',
        '`referral_priority_display` AS `referral_priority_display`',
        '`referral_source_code` AS `referral_source_code`',
        '`referral_source_display` AS `referral_source_display`',
        '`status_code` AS `status_code`',
        '`status_display` AS `status_display`',
        '`status_change_reason_code` AS `status_change_reason_code`',
        '`status_change_reason_display` AS `status_change_reason_display`',
        '`status_change_datetime` AS `status_change_datetime`',
        '`encounter_type_code` AS `encounter_type_code`',
        '`encounter_type_display` AS `encounter_type_display`',
        '`suspected_cancer_site_code` AS `suspected_cancer_site_code`',
        '`suspected_cancer_site_display` AS `suspected_cancer_site_display`',
        '`treatment_function_code` AS `treatment_function_code`',
        '`treatment_function_display` AS `treatment_function_display`',
        '`service_type_requested_code` AS `service_type_requested_code`',
        '`service_type_requested_display` AS `service_type_requested_display`',
        '`site_code` AS `site_code`',
        '`site_display` AS `site_display`',
        '`referring_facility_code` AS `referring_facility_code`',
        '`referring_facility_display` AS `referring_facility_display`',
        '`referred_by_org_id` AS `referred_by_org_id`',
        '`booking_type_code` AS `booking_type_code`',
        '`booking_type_display` AS `booking_type_display`',
        '`admin_category_code` AS `admin_category_code`',
        '`admin_category_display` AS `admin_category_display`',
        '`business_unit` AS `business_unit`',
        '`division` AS `division`',
        '`original_received_datetime` AS `original_received_datetime`',
        '`ers_ubrn_received` AS `ers_ubrn_received`',
        '`ers_pathway_start` AS `ers_pathway_start`',
        '`ers_service_name` AS `ers_service_name`',
        '`ers_specialty` AS `ers_specialty`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`source_system_oid` AS `source_system_oid`',
        '`referral_oid` AS `referral_oid`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_REFERRAL_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.rtt_activity ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_RTT_ACTIVITY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`rtt_activity_oid` AS `rtt_activity_oid`',
    '`pathway_oid` AS `pathway_oid`',
    '`referral_key` AS `referral_key`',
    '`appointment_oid` AS `appointment_oid`',
    '`activity_code` AS `activity_code`',
    '`activity_display` AS `activity_display`',
    '`activity_type_code` AS `activity_type_code`',
    '`activity_type_display` AS `activity_type_display`',
    '`status_code` AS `status_code`',
    '`status_display` AS `status_display`',
    '`status_sequence_asc` AS `status_sequence_asc`',
    '`status_sequence_desc` AS `status_sequence_desc`',
    '`activity_sequence_asc` AS `activity_sequence_asc`',
    '`activity_sequence_desc` AS `activity_sequence_desc`',
    '`activity_datetime_quality` AS `activity_datetime_quality`',
    '`treatment_function_code` AS `treatment_function_code`',
    '`treatment_function_display` AS `treatment_function_display`',
    '`site_code` AS `site_code`',
    '`site_display` AS `site_display`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`source_system_oid` AS `source_system_oid`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_rtt_activity_metadata; source history stays on the main research table.
CLINICAL_RTT_ACTIVITY_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 68,818,631 rows of
    # 68,820,429 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_RTT_ACTIVITY_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 1,798 of 68,820,429 at the profile.
    "gold.clinical.rtt_activity.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 30,737 of 68,820,429 rows (0.0447%) when profiled on 2026-08-24.
    "gold.clinical.rtt_activity.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 2,170 of 68,820,429 rows (0.00315%) when profiled on 2026-08-24.
    "gold.clinical.rtt_activity.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_RTT_ACTIVITY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 RTT activity identity.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID when resolved; native person foreign key.",
    "encounter_id": "Nullable Millennium ENCNTR_ID; the LUNA activity feed supplies no encounter.",
    "event_datetime": "RTT activity timestamp.",
    "event_end_datetime": "Event end timestamp; not supplied by LUNA RTT activities.",
    "source_coding_system": "Verbatim LUNA RTT-status coding system.",
    "source_code": "Verbatim RTT-status code.",
    "source_display": "Verbatim RTT-status display.",
    "rtt_activity_oid": "Raw LUNA RTT activity object identifier.",
    "pathway_oid": "Raw LUNA pathway object identifier.",
    "referral_key": "Deterministic SHA-256 key of the linked referral.",
    "appointment_oid": "Raw appointment linkage evidence; resolution is gated and no edge is emitted.",
    "activity_code": "Source RTT activity code.",
    "activity_display": "Source RTT activity display.",
    "activity_type_code": "Source RTT activity-type code.",
    "activity_type_display": "Source RTT activity-type display.",
    "status_code": "Source RTT-status code.",
    "status_display": "Source RTT-status display.",
    "status_sequence_asc": "Ascending RTT-status sequence.",
    "status_sequence_desc": "Descending RTT-status sequence.",
    "activity_sequence_asc": "Ascending RTT-activity sequence.",
    "activity_sequence_desc": "Descending RTT-activity sequence.",
    "activity_datetime_quality": "Bronze quality classification for the activity timestamp.",
    "treatment_function_code": "Source treatment-function code.",
    "treatment_function_display": "Source treatment-function display.",
    "site_code": "Source site code.",
    "site_display": "Source site display.",
    "confidentiality_code": "Source confidentiality code when supplied.",
    "vip_ind": "Source VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld-identity indicator when supplied.",
    "fact_category": "Whether this fact is clinical or administrative in the v2 plane merge.",
    "source_feed": "Registered feed owning the fact.",
    "source_system_oid": "LUNA SOURCE_SYSTEM_OID; component of the native primary key.",
    "record_status": "Superseded when map_rtt_activity.SOURCE_PRESENT_IND or ACTIVE_IND is false; each missing flag defaults true. Otherwise active. Source absence and inactivity both map to superseded, not retracted; the clinical status code does not set this row-history label.",
    "record_status_effective_from": "Source system timestamp recording when the RTT activity record was created.",
    "record_status_effective_to": "For a superseded rtt activity row, the first non-null map_rtt_activity.SOURCE_ABSENT_DETECTED_TS, MODIFIED_DATETIME or ADC_UPDT, in that order; otherwise null. The same source-presence/inactivity predicate sets record_status. No event-time clamp or clinical-event timestamp is substituted.",
    "source_update_timestamp": "map_rtt_activity.SOURCE_ADC_UPDT carried unchanged to the research row, distinct from ADC_UPDT used for loaded_at. It is not the clinical event timestamp or the current Silver refresh time.",
    "loaded_at": "map_rtt_activity.ADC_UPDT carried unchanged through the source and public projections. No parent or other source clock is joined into this value; it differs from SOURCE_ADC_UPDT used for source_update_timestamp and from the current Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_rtt_activity"),
    comment="Internal quality-controlled twin of clinical_rtt_activity: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_RTT_ACTIVITY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_RTT_ACTIVITY_ADVISORY_RULES)
def _gold_qc_clinical_rtt_activity():
    """Quality-controlled twin of journey_clinical.rtt_activity."""
    # 6 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "clinical_rtt_activity",
        CLINICAL_RTT_ACTIVITY_SELECT,
        fk_columns=["person_id"],
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, CLINICAL_RTT_ACTIVITY_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.rtt_activity"),
    comment=(
        "LUNA clock-affecting RTT activity and status events with quality flags retained. "
        "Gold QC twin of the silver product: 1 columns are repaired or nulled, 1 rule(s) drop "
        "rows, 3 check(s) are advisory. Each rule states its reason in the pipeline notebook, "
        "and Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_rtt_activity():
    """Contract-v2 public twin of clinical_rtt_activity; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_rtt_activity")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`rtt_activity_oid` AS `rtt_activity_oid`',
        '`pathway_oid` AS `pathway_oid`',
        '`referral_key` AS `referral_key`',
        '`appointment_oid` AS `appointment_oid`',
        '`activity_code` AS `activity_code`',
        '`activity_display` AS `activity_display`',
        '`activity_type_code` AS `activity_type_code`',
        '`activity_type_display` AS `activity_type_display`',
        '`status_code` AS `status_code`',
        '`status_display` AS `status_display`',
        '`status_sequence_asc` AS `status_sequence_asc`',
        '`status_sequence_desc` AS `status_sequence_desc`',
        '`activity_sequence_asc` AS `activity_sequence_asc`',
        '`activity_sequence_desc` AS `activity_sequence_desc`',
        '`activity_datetime_quality` AS `activity_datetime_quality`',
        '`treatment_function_code` AS `treatment_function_code`',
        '`treatment_function_display` AS `treatment_function_display`',
        '`site_code` AS `site_code`',
        '`site_display` AS `site_display`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`source_system_oid` AS `source_system_oid`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_RTT_ACTIVITY_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.rtt_pathway ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_RTT_PATHWAY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_system_oid` AS `source_system_oid`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    'CASE WHEN CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`pathway_oid` AS `pathway_oid`',
    '`period_oid` AS `period_oid`',
    '`is_latest_period` AS `is_latest_period`',
    '`start_status_code` AS `start_status_code`',
    '`start_status_display` AS `start_status_display`',
    '`stop_status_code` AS `stop_status_code`',
    '`stop_status_display` AS `stop_status_display`',
    '`current_status_code` AS `current_status_code`',
    '`current_status_display` AS `current_status_display`',
    '`sequence_asc` AS `sequence_asc`',
    '`sequence_desc` AS `sequence_desc`',
    '`clock_discrepant` AS `clock_discrepant`',
    '`core_clock_start` AS `core_clock_start`',
    '`core_clock_stop` AS `core_clock_stop`',
    '`pathway_start_date` AS `pathway_start_date`',
    '`pathway_type_code` AS `pathway_type_code`',
    '`pathway_type_display` AS `pathway_type_display`',
    '`breach_date` AS `breach_date`',
    '`days_waited` AS `days_waited`',
    '`days_waited_active` AS `days_waited_active`',
    '`op_appt_dna_count` AS `op_appt_dna_count`',
    '`treatment_function_code` AS `treatment_function_code`',
    '`treatment_function_display` AS `treatment_function_display`',
    '`site_code` AS `site_code`',
    '`site_display` AS `site_display`',
    '`referring_facility_code` AS `referring_facility_code`',
    '`referring_facility_display` AS `referring_facility_display`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_rtt_pathway_metadata; source history stays on the main research table.
CLINICAL_RTT_PATHWAY_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 7,862,177 rows of
    # 7,864,172 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_RTT_PATHWAY_ADVISORY_RULES = {
    # Counted rather than nulled because a breach date is a deadline counted forward from
    # the clock start, so it is in the future for every pathway still inside its target.
    # Seen on 70,601 of 7,864,172 rows (0.898%) when profiled on 2026-08-24.
    "gold.clinical.rtt_pathway.breach_date.future_owner":
        "NOT COALESCE((CAST(`breach_date` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 1,995 of 7,864,172 at the profile.
    "gold.clinical.rtt_pathway.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 277 of 7,864,172 rows (0.00352%) when profiled on 2026-08-24.
    "gold.clinical.rtt_pathway.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 832 of 7,864,172 rows (0.0106%) when profiled on 2026-08-24.
    "gold.clinical.rtt_pathway.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_RTT_PATHWAY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "source_system_oid": "LUNA SOURCE_SYSTEM_OID; component of the native pathway-period primary key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID when resolved; native person foreign key.",
    "encounter_id": "Nullable Millennium ENCNTR_ID; the LUNA pathway feed supplies no encounter.",
    "event_datetime": "RTT clock-period start timestamp.",
    "event_end_datetime": "RTT clock-period stop timestamp.",
    "source_coding_system": "Verbatim LUNA RTT-status coding system.",
    "source_code": "Verbatim current RTT-status code.",
    "source_display": "Verbatim current RTT-status display.",
    "pathway_oid": "Raw LUNA pathway object identifier.",
    "period_oid": "Raw LUNA period object identifier; 0 marks the clockless-pathway sentinel and the column is never null.",
    "is_latest_period": "Whether this is the latest period for the pathway.",
    "start_status_code": "Source clock-start RTT-status code.",
    "start_status_display": "Source clock-start RTT-status display.",
    "stop_status_code": "Source clock-stop RTT-status code.",
    "stop_status_display": "Source clock-stop RTT-status display.",
    "current_status_code": "Source current RTT-status code.",
    "current_status_display": "Source current RTT-status display.",
    "sequence_asc": "Ascending pathway-period sequence.",
    "sequence_desc": "Descending pathway-period sequence.",
    "clock_discrepant": "Bronze flag indicating disagreement with the core clock timestamps.",
    "core_clock_start": "Core-system clock start retained as discrepant sidecar data.",
    "core_clock_stop": "Core-system clock stop retained as discrepant sidecar data.",
    "pathway_start_date": "Source pathway start date.",
    "pathway_type_code": "Source pathway-type code.",
    "pathway_type_display": "Source pathway-type display.",
    "breach_date": "Source breach timestamp.",
    "days_waited": "Source total days waited.",
    "days_waited_active": "Source active days waited.",
    "op_appt_dna_count": "Source outpatient did-not-attend count.",
    "treatment_function_code": "Source treatment-function code.",
    "treatment_function_display": "Source treatment-function display.",
    "site_code": "Source site code.",
    "site_display": "Source site display.",
    "referring_facility_code": "Source referring-facility code.",
    "referring_facility_display": "Source referring-facility display.",
    "confidentiality_code": "Source confidentiality code when supplied.",
    "vip_ind": "Source VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld-identity indicator when supplied.",
    "fact_category": "Whether this fact is clinical or administrative in the v2 plane merge.",
    "source_feed": "Registered feed owning the fact.",
    "record_status": "Derived LUNA RTT row status: superseded when SOURCE_PRESENT_IND cast to Boolean, PERIOD_ACTIVE_IND or CORE_ACTIVE_IND is false; otherwise active. Each missing flag defaults true. Source absence maps to superseded, not retracted, and this label is separate from the clinical RTT status or clock-stop state.",
    "record_status_effective_from": "map_rtt_pathway.PERIOD_CREATED_DATETIME carried unchanged as the history start. No RTT clock-start, pathway-start or ingestion-time fallback is applied; a missing source value remains null.",
    "record_status_effective_to": "For a superseded RTT row, first non-null map_rtt_pathway.SOURCE_ABSENT_DETECTED_TS, PERIOD_MODIFIED_DATETIME, then ADC_UPDT. Null for active rows or when every fallback is missing. This is a derived row-status end that may use ingestion time; clinical STOP_DATETIME is not used as the fallback.",
    "source_update_timestamp": "map_rtt_pathway.SOURCE_ADC_UPDT carried unchanged for the contributing pathway/period row. It is distinct from ADC_UPDT ingestion provenance and is not PERIOD_MODIFIED_DATETIME, an RTT clock start/stop or Silver refresh time; a missing source timestamp remains null.",
    "loaded_at": "map_rtt_pathway.ADC_UPDT carried unchanged through the RTT primitive and quality wrappers. This is bronze ingestion provenance, not a clinical RTT clock, period creation/modification time or Silver refresh time; a missing source timestamp remains null.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_rtt_pathway"),
    comment="Internal quality-controlled twin of clinical_rtt_pathway: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_RTT_PATHWAY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_RTT_PATHWAY_ADVISORY_RULES)
def _gold_qc_clinical_rtt_pathway():
    """Quality-controlled twin of journey_clinical.rtt_pathway."""
    df = _qc("clinical_rtt_pathway", CLINICAL_RTT_PATHWAY_SELECT)
    return _with_comments(df, CLINICAL_RTT_PATHWAY_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.rtt_pathway"),
    comment=(
        "LUNA RTT pathway and clock-period facts including clockless sentinel rows. Gold QC "
        "twin of the silver product: 6 columns are repaired or nulled, 1 rule(s) drop rows, 4 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_rtt_pathway():
    """Contract-v2 public twin of clinical_rtt_pathway; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_rtt_pathway")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_system_oid` AS `source_system_oid`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`pathway_oid` AS `pathway_oid`',
        '`period_oid` AS `period_oid`',
        '`is_latest_period` AS `is_latest_period`',
        '`start_status_code` AS `start_status_code`',
        '`start_status_display` AS `start_status_display`',
        '`stop_status_code` AS `stop_status_code`',
        '`stop_status_display` AS `stop_status_display`',
        '`current_status_code` AS `current_status_code`',
        '`current_status_display` AS `current_status_display`',
        '`sequence_asc` AS `sequence_asc`',
        '`sequence_desc` AS `sequence_desc`',
        '`clock_discrepant` AS `clock_discrepant`',
        '`core_clock_start` AS `core_clock_start`',
        '`core_clock_stop` AS `core_clock_stop`',
        '`pathway_start_date` AS `pathway_start_date`',
        '`pathway_type_code` AS `pathway_type_code`',
        '`pathway_type_display` AS `pathway_type_display`',
        '`breach_date` AS `breach_date`',
        '`days_waited` AS `days_waited`',
        '`days_waited_active` AS `days_waited_active`',
        '`op_appt_dna_count` AS `op_appt_dna_count`',
        '`treatment_function_code` AS `treatment_function_code`',
        '`treatment_function_display` AS `treatment_function_display`',
        '`site_code` AS `site_code`',
        '`site_display` AS `site_display`',
        '`referring_facility_code` AS `referring_facility_code`',
        '`referring_facility_display` AS `referring_facility_display`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_RTT_PATHWAY_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.waiting_list_entry ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_WAITING_LIST_ENTRY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    "CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_datetime` AS DATE)) < 9999 THEN NULL ELSE `event_datetime` END END AS `event_datetime`",
    "CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` OR CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_end_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_end_datetime` AS DATE)) < 9999 OR CAST(`event_end_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`event_end_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `event_end_datetime` END END AS `event_end_datetime`",
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = '0' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    '`pm_wait_list_id` AS `pm_wait_list_id`',
    '`row_source` AS `row_source`',
    '`source_version_id` AS `source_version_id`',
    '`hist_action` AS `hist_action`',
    '`version_datetime` AS `version_datetime`',
    '`is_current` AS `is_current`',
    '`updt_cnt` AS `updt_cnt`',
    '`sch_event_id` AS `sch_event_id`',
    "CASE WHEN UPPER(TRIM(CAST(`status_code` AS STRING))) = '0' THEN NULL ELSE `status_code` END AS `status_code`",
    '`status_display` AS `status_display`',
    '`sub_status_display` AS `sub_status_display`',
    '`active_status_display` AS `active_status_display`',
    '`urgency_display` AS `urgency_display`',
    '`stand_by_display` AS `stand_by_display`',
    '`admit_category_display` AS `admit_category_display`',
    '`admit_booking_display` AS `admit_booking_display`',
    '`admit_type_display` AS `admit_type_display`',
    '`admit_offer_outcome_display` AS `admit_offer_outcome_display`',
    '`management_display` AS `management_display`',
    '`attendance_display` AS `attendance_display`',
    '`reason_for_change_display` AS `reason_for_change_display`',
    '`reason_for_removal_display` AS `reason_for_removal_display`',
    '`anesthetic_display` AS `anesthetic_display`',
    "CASE WHEN UPPER(TRIM(CAST(`planned_procedure_code` AS STRING))) = '0' THEN NULL ELSE `planned_procedure_code` END AS `planned_procedure_code`",
    '`planned_procedure_display` AS `planned_procedure_display`',
    '`referral_source_display` AS `referral_source_display`',
    '`referral_type_display` AS `referral_type_display`',
    '`service_type_requested_display` AS `service_type_requested_display`',
    '`from_ed_ind` AS `from_ed_ind`',
    '`suspended_days` AS `suspended_days`',
    'CASE WHEN CAST(`recommend_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `recommend_datetime` END AS `recommend_datetime`',
    "CASE WHEN CAST(`referral_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`referral_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`referral_datetime` AS DATE)) < 9999 THEN NULL ELSE `referral_datetime` END END AS `referral_datetime`",
    "CASE WHEN CAST(`original_request_received_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`original_request_received_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`original_request_received_datetime` AS DATE)) < 9999 THEN NULL ELSE `original_request_received_datetime` END END AS `original_request_received_datetime`",
    'CASE WHEN CAST(`admit_decision_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `admit_decision_datetime` END AS `admit_decision_datetime`',
    "CASE WHEN CAST(`admit_guaranteed_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`admit_guaranteed_datetime` AS DATE)) < 9999 OR CAST(`admit_guaranteed_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`admit_guaranteed_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `admit_guaranteed_datetime` END AS `admit_guaranteed_datetime`",
    "CASE WHEN CAST(`provisional_admit_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`provisional_admit_datetime` AS DATE)) < 9999 THEN NULL ELSE `provisional_admit_datetime` END AS `provisional_admit_datetime`",
    "CASE WHEN CAST(`previous_provisional_admit_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`previous_provisional_admit_datetime` AS DATE)) < 9999 THEN NULL ELSE `previous_provisional_admit_datetime` END AS `previous_provisional_admit_datetime`",
    "CASE WHEN CAST(`adjusted_waiting_start_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`adjusted_waiting_start_datetime` AS DATE)) < 9999 THEN NULL ELSE `adjusted_waiting_start_datetime` END AS `adjusted_waiting_start_datetime`",
    '`scheduled_datetime` AS `scheduled_datetime`',
    "CASE WHEN CAST(`requested_datetime` AS DATE) = DATE'2100-12-31' THEN NULL ELSE `requested_datetime` END AS `requested_datetime`",
    "CASE WHEN CAST(`removal_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`removal_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`removal_datetime` AS DATE)) < 9999 OR CAST(`removal_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`removal_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `removal_datetime` END END AS `removal_datetime`",
    "CASE WHEN CAST(`last_dna_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`last_dna_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`last_dna_datetime` AS DATE)) < 9999 THEN NULL ELSE `last_dna_datetime` END END AS `last_dna_datetime`",
    '`status_datetime` AS `status_datetime`',
    "CASE WHEN CAST(`status_end_datetime` AS DATE) = DATE'2100-12-31' THEN NULL ELSE `status_end_datetime` END AS `status_end_datetime`",
    '`location_code` AS `location_code`',
    '`location_display` AS `location_display`',
    '`facility_display` AS `facility_display`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_waiting_list_entry_metadata; source history stays on the main research table.
CLINICAL_WAITING_LIST_ENTRY_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 233,491,987 rows of
    # 233,491,987 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_WAITING_LIST_ENTRY_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 233,491,987 at the profile.
    "gold.clinical.waiting_list_entry.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 952 of 233,491,987 rows (0.000408%) when profiled on 2026-08-24.
    "gold.clinical.waiting_list_entry.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 6,193 of 233,491,987 rows (0.00265%) when profiled on
    # 2026-08-24.
    "gold.clinical.waiting_list_entry.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_WAITING_LIST_ENTRY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 waiting-list entry identity shared by physical versions.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID when supplied; native encounter foreign key.",
    "event_datetime": "Recorded waiting-period start timestamp carried from `WAITING_START_DT_TM`.",
    "event_end_datetime": "Recorded waiting-period end timestamp carried from `WAITING_END_DT_TM`.",
    "source_coding_system": "Verbatim Millennium planned-procedure coding system.",
    "source_code": "Verbatim planned-procedure code.",
    "source_display": "Verbatim planned-procedure display.",
    "pm_wait_list_id": "Native waiting-list entry identifier.",
    "row_source": "Physical bronze row source.",
    "source_version_id": "Native source version identifier; CURRENT rows carry -1.",
    "hist_action": "Source history action when the row is historical.",
    "version_datetime": "Descriptive fallback version timestamp only; it defines no effective interval.",
    "is_current": "Whether this is the current physical version.",
    "updt_cnt": "Native source update counter.",
    "sch_event_id": "Raw scheduling event linkage evidence.",
    "status_code": "Source waiting-list status code.",
    "status_display": "Source waiting-list status display.",
    "sub_status_display": "Source waiting-list sub-status display.",
    "active_status_display": "Source active-status display.",
    "urgency_display": "Source urgency display.",
    "stand_by_display": "Source stand-by display.",
    "admit_category_display": "Source admission-category display.",
    "admit_booking_display": "Source admission-booking display.",
    "admit_type_display": "Source admission-type display.",
    "admit_offer_outcome_display": "Source admission-offer outcome display.",
    "management_display": "Source management display.",
    "attendance_display": "Source attendance display.",
    "reason_for_change_display": "Source reason-for-change display.",
    "reason_for_removal_display": "Source reason-for-removal display.",
    "anesthetic_display": "Source anesthetic display.",
    "planned_procedure_code": "Source planned-procedure code.",
    "planned_procedure_display": "Source planned-procedure display.",
    "referral_source_display": "Source referral-source display.",
    "referral_type_display": "Source referral-type display.",
    "service_type_requested_display": "Source requested-service-type display.",
    "from_ed_ind": "Raw source indicator for origin in the emergency department.",
    "suspended_days": "Source count of suspended days.",
    "recommend_datetime": "Source recommendation timestamp.",
    "referral_datetime": "Source referral timestamp.",
    "original_request_received_datetime": "Original request-received timestamp.",
    "admit_decision_datetime": "Source decision-to-admit timestamp.",
    "admit_guaranteed_datetime": "Source guaranteed-admission timestamp.",
    "provisional_admit_datetime": "Source provisional-admission timestamp.",
    "previous_provisional_admit_datetime": "Previous provisional-admission timestamp.",
    "adjusted_waiting_start_datetime": "Adjusted waiting-start timestamp.",
    "scheduled_datetime": "Source scheduled timestamp.",
    "requested_datetime": "Source requested timestamp.",
    "removal_datetime": "Source removal timestamp.",
    "last_dna_datetime": "Source last did-not-attend timestamp.",
    "status_datetime": "Source status timestamp.",
    "status_end_datetime": "Source status-end timestamp.",
    "location_code": "Millennium nurse-unit code.",
    "location_display": "Source nurse-unit display.",
    "facility_display": "Source facility display.",
    "confidentiality_code": "Source confidentiality code when supplied.",
    "vip_ind": "Source VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld-identity indicator when supplied.",
    "fact_category": "Whether this fact is clinical or administrative in the v2 plane merge.",
    "source_feed": "Registered feed owning the fact.",
    "record_status": "Derived map_waiting_list version status: superseded when SOURCE_PRESENT_IND cast to Boolean is false or IS_CURRENT is false; otherwise active. Null presence defaults to true and null IS_CURRENT defaults to false. Source absence therefore maps to superseded, not retracted; this is distinct from the clinical waiting-list status code.",
    "record_status_effective_from": "map_waiting_list.BEG_EFFECTIVE_DT_TM carried unchanged for the physical CURRENT or HIST version. VERSION_DT_TM and waiting/request dates are not used as fallbacks; a missing value remains null.",
    "record_status_effective_to": "For a superseded waiting-list version, first non-null map_waiting_list.SOURCE_ABSENT_DETECTED_TS, END_EFFECTIVE_DT_TM, then ADC_UPDT. Null for active versions or when all three are missing. This expression does not filter far-future END_EFFECTIVE_DT_TM sentinels and can use ingestion time as its last fallback.",
    "source_update_timestamp": "map_waiting_list.SOURCE_ADC_UPDT carried unchanged for the physical CURRENT or HIST version. It is separate from ADC_UPDT ingestion provenance and is not VERSION_DT_TM, the clinical status date or Silver refresh time; a missing timestamp remains null.",
    "loaded_at": "map_waiting_list.ADC_UPDT carried unchanged for the physical CURRENT or HIST version. This is ingestion provenance, not waiting-start/end, descriptive VERSION_DT_TM or Silver refresh time; a missing source timestamp remains null.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_waiting_list_entry"),
    comment="Internal quality-controlled twin of clinical_waiting_list_entry: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_WAITING_LIST_ENTRY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_WAITING_LIST_ENTRY_ADVISORY_RULES)
def _gold_qc_clinical_waiting_list_entry():
    """Quality-controlled twin of journey_clinical.waiting_list_entry."""
    # 63 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "clinical_waiting_list_entry",
        CLINICAL_WAITING_LIST_ENTRY_SELECT,
        fk_columns=["person_id"],
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, CLINICAL_WAITING_LIST_ENTRY_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.waiting_list_entry"),
    comment=(
        "Versioned Millennium waiting-list entry fact activated as a recorded contract change "
        "from a zero-row stub with no consumer break. Gold QC twin of the silver product: 19 "
        "columns are repaired or nulled, 1 rule(s) drop rows, 3 check(s) are advisory. Each "
        "rule states its reason in the pipeline notebook, and Lakeflow expectation metrics "
        "report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_waiting_list_entry():
    """Contract-v2 public twin of clinical_waiting_list_entry; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_waiting_list_entry")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`pm_wait_list_id` AS `pm_wait_list_id`',
        '`row_source` AS `row_source`',
        '`source_version_id` AS `source_version_id`',
        '`hist_action` AS `hist_action`',
        '`version_datetime` AS `version_datetime`',
        '`is_current` AS `is_current`',
        '`updt_cnt` AS `updt_cnt`',
        '`sch_event_id` AS `sch_event_id`',
        '`status_code` AS `status_code`',
        '`status_display` AS `status_display`',
        '`sub_status_display` AS `sub_status_display`',
        '`active_status_display` AS `active_status_display`',
        '`urgency_display` AS `urgency_display`',
        '`stand_by_display` AS `stand_by_display`',
        '`admit_category_display` AS `admit_category_display`',
        '`admit_booking_display` AS `admit_booking_display`',
        '`admit_type_display` AS `admit_type_display`',
        '`admit_offer_outcome_display` AS `admit_offer_outcome_display`',
        '`management_display` AS `management_display`',
        '`attendance_display` AS `attendance_display`',
        '`reason_for_change_display` AS `reason_for_change_display`',
        '`reason_for_removal_display` AS `reason_for_removal_display`',
        '`anesthetic_display` AS `anesthetic_display`',
        '`planned_procedure_code` AS `planned_procedure_code`',
        '`planned_procedure_display` AS `planned_procedure_display`',
        '`referral_source_display` AS `referral_source_display`',
        '`referral_type_display` AS `referral_type_display`',
        '`service_type_requested_display` AS `service_type_requested_display`',
        '`from_ed_ind` AS `from_ed_ind`',
        '`suspended_days` AS `suspended_days`',
        '`recommend_datetime` AS `recommend_datetime`',
        '`referral_datetime` AS `referral_datetime`',
        '`original_request_received_datetime` AS `original_request_received_datetime`',
        '`admit_decision_datetime` AS `admit_decision_datetime`',
        '`admit_guaranteed_datetime` AS `admit_guaranteed_datetime`',
        '`provisional_admit_datetime` AS `provisional_admit_datetime`',
        '`previous_provisional_admit_datetime` AS `previous_provisional_admit_datetime`',
        '`adjusted_waiting_start_datetime` AS `adjusted_waiting_start_datetime`',
        '`scheduled_datetime` AS `scheduled_datetime`',
        '`requested_datetime` AS `requested_datetime`',
        '`removal_datetime` AS `removal_datetime`',
        '`last_dna_datetime` AS `last_dna_datetime`',
        '`status_datetime` AS `status_datetime`',
        '`status_end_datetime` AS `status_end_datetime`',
        '`location_code` AS `location_code`',
        '`location_display` AS `location_display`',
        '`facility_display` AS `facility_display`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_WAITING_LIST_ENTRY_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.waiting_list_snapshot ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_WAITING_LIST_SNAPSHOT_SELECT = [
    '`waiting_list_snapshot_key` AS `waiting_list_snapshot_key`',
    '`snapshot_date` AS `snapshot_date`',
    '`snapshot_cutoff_ts` AS `snapshot_cutoff_ts`',
    '`pm_wait_list_id` AS `pm_wait_list_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`sch_event_id` AS `sch_event_id`',
    "CASE WHEN UPPER(TRIM(CAST(`status_code` AS STRING))) = '0' THEN NULL ELSE `status_code` END AS `status_code`",
    '`status_display` AS `status_display`',
    '`sub_status_display` AS `sub_status_display`',
    '`active_status_display` AS `active_status_display`',
    '`urgency_display` AS `urgency_display`',
    '`stand_by_display` AS `stand_by_display`',
    '`admit_category_display` AS `admit_category_display`',
    '`admit_booking_display` AS `admit_booking_display`',
    '`admit_type_display` AS `admit_type_display`',
    '`admit_offer_outcome_display` AS `admit_offer_outcome_display`',
    '`management_display` AS `management_display`',
    '`attendance_display` AS `attendance_display`',
    '`reason_for_change_display` AS `reason_for_change_display`',
    '`reason_for_removal_display` AS `reason_for_removal_display`',
    '`anesthetic_display` AS `anesthetic_display`',
    "CASE WHEN UPPER(TRIM(CAST(`planned_procedure_code` AS STRING))) = '0' THEN NULL ELSE `planned_procedure_code` END AS `planned_procedure_code`",
    '`planned_procedure_display` AS `planned_procedure_display`',
    '`referral_source_display` AS `referral_source_display`',
    '`referral_type_display` AS `referral_type_display`',
    '`service_type_requested_display` AS `service_type_requested_display`',
    '`from_ed_ind` AS `from_ed_ind`',
    '`suspended_days` AS `suspended_days`',
    '`recommend_datetime` AS `recommend_datetime`',
    'CASE WHEN CAST(`referral_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `referral_datetime` END AS `referral_datetime`',
    '`original_request_received_datetime` AS `original_request_received_datetime`',
    '`admit_decision_datetime` AS `admit_decision_datetime`',
    "CASE WHEN CAST(`admit_guaranteed_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`admit_guaranteed_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `admit_guaranteed_datetime` END AS `admit_guaranteed_datetime`",
    '`provisional_admit_datetime` AS `provisional_admit_datetime`',
    '`previous_provisional_admit_datetime` AS `previous_provisional_admit_datetime`',
    '`waiting_start_datetime` AS `waiting_start_datetime`',
    'CASE WHEN `waiting_start_datetime` IS NOT NULL AND `waiting_end_datetime` IS NOT NULL AND `waiting_start_datetime` > `waiting_end_datetime` THEN NULL ELSE `waiting_end_datetime` END AS `waiting_end_datetime`',
    '`adjusted_waiting_start_datetime` AS `adjusted_waiting_start_datetime`',
    '`scheduled_datetime` AS `scheduled_datetime`',
    '`requested_datetime` AS `requested_datetime`',
    '`removal_datetime` AS `removal_datetime`',
    'CASE WHEN CAST(`last_dna_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `last_dna_datetime` END AS `last_dna_datetime`',
    '`status_datetime` AS `status_datetime`',
    "CASE WHEN CAST(`status_end_datetime` AS DATE) = DATE'2100-12-31' THEN NULL ELSE `status_end_datetime` END AS `status_end_datetime`",
    '`location_code` AS `location_code`',
    '`location_display` AS `location_display`',
    '`facility_display` AS `facility_display`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_waiting_list_snapshot_metadata; source history stays on the main research table.
CLINICAL_WAITING_LIST_SNAPSHOT_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 4,771,446 rows of
    # 4,771,446 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_WAITING_LIST_SNAPSHOT_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 4,771,446 at the profile.
    "gold.clinical.waiting_list_snapshot.identity_status.default_view_resolved":
        "identity_status = 'resolved'",
}

CLINICAL_WAITING_LIST_SNAPSHOT_COLUMN_COMMENTS = {
    "waiting_list_snapshot_key": "Deterministic SHA-256 census-row key.",
    "snapshot_date": "Census snapshot date.",
    "snapshot_cutoff_ts": "Immutable census cutoff timestamp.",
    "pm_wait_list_id": "Native waiting-list entry identifier.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID when supplied; native encounter foreign key.",
    "sch_event_id": "Raw scheduling event linkage evidence.",
    "status_code": "Source waiting-list status code.",
    "status_display": "Source waiting-list status display.",
    "sub_status_display": "Source waiting-list sub-status display.",
    "active_status_display": "Source active-status display.",
    "urgency_display": "Source urgency display.",
    "stand_by_display": "Source stand-by display.",
    "admit_category_display": "Source admission-category display.",
    "admit_booking_display": "Source admission-booking display.",
    "admit_type_display": "Source admission-type display.",
    "admit_offer_outcome_display": "Source admission-offer outcome display.",
    "management_display": "Source management display.",
    "attendance_display": "Source attendance display.",
    "reason_for_change_display": "Source reason-for-change display.",
    "reason_for_removal_display": "Source reason-for-removal display.",
    "anesthetic_display": "Source anesthetic display.",
    "planned_procedure_code": "Source planned-procedure code.",
    "planned_procedure_display": "Source planned-procedure display.",
    "referral_source_display": "Source referral-source display.",
    "referral_type_display": "Source referral-type display.",
    "service_type_requested_display": "Source requested-service-type display.",
    "from_ed_ind": "Raw source indicator for origin in the emergency department.",
    "suspended_days": "Source count of suspended days.",
    "recommend_datetime": "Source recommendation timestamp.",
    "referral_datetime": "Source referral timestamp.",
    "original_request_received_datetime": "Original request-received timestamp.",
    "admit_decision_datetime": "Source decision-to-admit timestamp.",
    "admit_guaranteed_datetime": "Source guaranteed-admission timestamp.",
    "provisional_admit_datetime": "Source provisional-admission timestamp.",
    "previous_provisional_admit_datetime": "Previous provisional-admission timestamp.",
    "waiting_start_datetime": "Source waiting-start timestamp.",
    "waiting_end_datetime": "Source waiting-end timestamp.",
    "adjusted_waiting_start_datetime": "Adjusted waiting-start timestamp.",
    "scheduled_datetime": "Source scheduled timestamp.",
    "requested_datetime": "Source requested timestamp.",
    "removal_datetime": "Source removal timestamp.",
    "last_dna_datetime": "Source last did-not-attend timestamp.",
    "status_datetime": "Source status timestamp.",
    "status_end_datetime": "Source status-end timestamp.",
    "location_code": "Millennium nurse-unit code.",
    "location_display": "Source nurse-unit display.",
    "facility_display": "Source facility display.",
    "fact_category": "Whether this fact is clinical or administrative in the v2 plane merge.",
    "source_feed": "Registered feed owning the census row.",
    "loaded_at": "Single immutable UTC cutoff timestamp shared by every row for SNAPSHOT_DATE.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_waiting_list_snapshot"),
    comment="Internal quality-controlled twin of clinical_waiting_list_snapshot: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_WAITING_LIST_SNAPSHOT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_WAITING_LIST_SNAPSHOT_ADVISORY_RULES)
def _gold_qc_clinical_waiting_list_snapshot():
    """Quality-controlled twin of journey_clinical.waiting_list_snapshot."""
    # 12 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "clinical_waiting_list_snapshot",
        CLINICAL_WAITING_LIST_SNAPSHOT_SELECT,
        fk_columns=["person_id"],
    )
    return _with_comments(df, CLINICAL_WAITING_LIST_SNAPSHOT_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.waiting_list_snapshot"),
    comment=(
        "Immutable Millennium waiting-list census activated as a recorded contract change "
        "from a zero-row stub with no consumer break. Gold QC twin of the silver product: 8 "
        "columns are repaired or nulled, 1 rule(s) drop rows, 1 check(s) are advisory. Each "
        "rule states its reason in the pipeline notebook, and Lakeflow expectation metrics "
        "report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_waiting_list_snapshot():
    """Contract-v2 public twin of clinical_waiting_list_snapshot; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_waiting_list_snapshot")).selectExpr(
        '`waiting_list_snapshot_key` AS `waiting_list_snapshot_key`',
        '`snapshot_date` AS `snapshot_date`',
        '`snapshot_cutoff_ts` AS `snapshot_cutoff_ts`',
        '`pm_wait_list_id` AS `pm_wait_list_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`sch_event_id` AS `sch_event_id`',
        '`status_code` AS `status_code`',
        '`status_display` AS `status_display`',
        '`sub_status_display` AS `sub_status_display`',
        '`active_status_display` AS `active_status_display`',
        '`urgency_display` AS `urgency_display`',
        '`stand_by_display` AS `stand_by_display`',
        '`admit_category_display` AS `admit_category_display`',
        '`admit_booking_display` AS `admit_booking_display`',
        '`admit_type_display` AS `admit_type_display`',
        '`admit_offer_outcome_display` AS `admit_offer_outcome_display`',
        '`management_display` AS `management_display`',
        '`attendance_display` AS `attendance_display`',
        '`reason_for_change_display` AS `reason_for_change_display`',
        '`reason_for_removal_display` AS `reason_for_removal_display`',
        '`anesthetic_display` AS `anesthetic_display`',
        '`planned_procedure_code` AS `planned_procedure_code`',
        '`planned_procedure_display` AS `planned_procedure_display`',
        '`referral_source_display` AS `referral_source_display`',
        '`referral_type_display` AS `referral_type_display`',
        '`service_type_requested_display` AS `service_type_requested_display`',
        '`from_ed_ind` AS `from_ed_ind`',
        '`suspended_days` AS `suspended_days`',
        '`recommend_datetime` AS `recommend_datetime`',
        '`referral_datetime` AS `referral_datetime`',
        '`original_request_received_datetime` AS `original_request_received_datetime`',
        '`admit_decision_datetime` AS `admit_decision_datetime`',
        '`admit_guaranteed_datetime` AS `admit_guaranteed_datetime`',
        '`provisional_admit_datetime` AS `provisional_admit_datetime`',
        '`previous_provisional_admit_datetime` AS `previous_provisional_admit_datetime`',
        '`waiting_start_datetime` AS `waiting_start_datetime`',
        '`waiting_end_datetime` AS `waiting_end_datetime`',
        '`adjusted_waiting_start_datetime` AS `adjusted_waiting_start_datetime`',
        '`scheduled_datetime` AS `scheduled_datetime`',
        '`requested_datetime` AS `requested_datetime`',
        '`removal_datetime` AS `removal_datetime`',
        '`last_dna_datetime` AS `last_dna_datetime`',
        '`status_datetime` AS `status_datetime`',
        '`status_end_datetime` AS `status_end_datetime`',
        '`location_code` AS `location_code`',
        '`location_display` AS `location_display`',
        '`facility_display` AS `facility_display`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_WAITING_LIST_SNAPSHOT_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.admission_metrics ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_ADMISSION_METRICS_SELECT = [
    '`feed_id` AS `feed_id`',
    '`route_id` AS `route_id`',
    '`admitted` AS `admitted`',
    '`load_date` AS `load_date`',
    '`row_count` AS `row_count`',
    '`latest_loaded_at` AS `latest_loaded_at`',
]

REFERENCE_ADMISSION_METRICS_COLUMN_COMMENTS = {
    "feed_id": "Source-system identifier for the feed associated with each admission metrics record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "route_id": "Route or exclusion reason.",
    "admitted": "Whether these rows entered silver.",
    "load_date": "Bronze load date bucket for per-load accounting.",
    "row_count": "Rows in this bucket.",
    "latest_loaded_at": "Latest bronze load time observed in the bucket.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_admission_metrics"),
    comment="Internal quality-controlled twin of reference_admission_metrics: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
def _gold_qc_reference_admission_metrics():
    """Quality-controlled twin of journey_reference.admission_metrics."""
    df = _qc("reference_admission_metrics", REFERENCE_ADMISSION_METRICS_SELECT)
    return _with_comments(df, REFERENCE_ADMISSION_METRICS_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.admission_metrics"),
    comment=(
        "Admission-gate accounting per registered route; route_id doubles as the exclusion "
        "reason for admitted=false rows. The product-wide usable-code gate covers every "
        "source_code-bearing fact lane; document is exempt as a narrative route, and "
        "structured admin facts without source_code are exempt; registry_entry rows carry no "
        "source codes by construction and are exempt as a class. Gold QC twin of the silver "
        "product: 0 columns are repaired or nulled, 0 check(s) are advisory. Each rule states "
        "its reason in the pipeline notebook, and Lakeflow expectation metrics report what "
        "every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_admission_metrics():
    """Contract-v2 public twin of reference_admission_metrics; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_admission_metrics")).selectExpr(
        '`feed_id` AS `feed_id`',
        '`route_id` AS `route_id`',
        '`admitted` AS `admitted`',
        '`load_date` AS `load_date`',
        '`row_count` AS `row_count`',
        '`latest_loaded_at` AS `latest_loaded_at`',
    )
    return _with_comments(df, REFERENCE_ADMISSION_METRICS_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.cost_line_item ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_COST_LINE_ITEM_SELECT = [
    '`cost_line_item_key` AS `cost_line_item_key`',
    '`costed_activity_key` AS `costed_activity_key`',
    '`extract_cd` AS `extract_cd`',
    '`activity_record_id` AS `activity_record_id`',
    '`line_hash` AS `line_hash`',
    '`activity_cost_item_cd` AS `activity_cost_item_cd`',
    '`resource_cost_item_cd` AS `resource_cost_item_cd`',
    '`activity_count` AS `activity_count`',
    '`unbundled_subtype_cd` AS `unbundled_subtype_cd`',
    '`unbundled_currency_cd` AS `unbundled_currency_cd`',
    '`unbundled_currency_datetime` AS `unbundled_currency_datetime`',
    '`total_cost` AS `total_cost`',
    '`total_o_cost` AS `total_o_cost`',
    '`source_duplicate_count` AS `source_duplicate_count`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_COST_LINE_ITEM_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 181,295,498 rows of
    # 181,295,498 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(record_status = 'active')",
}

REFERENCE_COST_LINE_ITEM_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 181,295,498 at the profile.
    "gold.reference.cost_line_item.record_status.default_view_active":
        "record_status = 'active'",

    # Negative values here are signed by design -- credit lines in the costing feed and
    # below-zero readings on a calibrated scale -- so they are counted, not removed. Seen on
    # 11,112,901 of 181,295,498 rows (6.13%) when profiled on 2026-08-24.
    "gold.reference.cost_line_item.total_cost.negative_mass_scale_owner":
        "NOT COALESCE((`total_cost` < 0), FALSE)",

    # Negative values here are signed by design -- credit lines in the costing feed and
    # below-zero readings on a calibrated scale -- so they are counted, not removed. Seen on
    # 16,457,918 of 181,295,498 rows (9.08%) when profiled on 2026-08-24.
    "gold.reference.cost_line_item.total_o_cost.negative_mass_scale_owner":
        "NOT COALESCE((`total_o_cost` < 0), FALSE)",
}

REFERENCE_COST_LINE_ITEM_COLUMN_COMMENTS = {
    "cost_line_item_key": "Deterministic SHA-256 key for this SLAM cost line.",
    "costed_activity_key": "Deterministic SHA-256 key for the parent costed activity.",
    "extract_cd": "Source costing extract discriminator; required in every cost-line join.",
    "activity_record_id": "SLAM ACTIVITY_RECORD_ID as BIGINT; joins with extract_cd to the parent native key.",
    "line_hash": "Content-hash key derived from the source line attributes, used to identify a distinct cost line after collapsing exact duplicates.",
    "activity_cost_item_cd": "National Cost Collection activity cost item code for the cost line, preserved as supplied by the source extract without mapping.",
    "resource_cost_item_cd": "National Cost Collection resource cost item code for the cost line, preserved as supplied by the source extract without mapping.",
    "activity_count": "Source-reported activity count for the cost line, retained as text as supplied by the extract.",
    "unbundled_subtype_cd": "Observed values: 1 high-cost drug, 2 device, 3 diagnostic/other, 0 default; null on ordinary lines.",
    "unbundled_currency_cd": "Unbundled currency code recorded against the cost line by the source extract, null on ordinary bundled lines.",
    "unbundled_currency_datetime": "Date and time recorded against the unbundled currency for the cost line, as reported by the source extract.",
    "total_cost": "Total cost reported by the source extract for the cost line, retained at exact decimal(36,18) precision.",
    "total_o_cost": "Component of the line total cost reported by the source extract, retained at exact decimal(36,18) precision.",
    "source_duplicate_count": "Exact source rows represented by this collapsed line.",
    "record_status": "Retracted when map_slam_cost_line_item.SOURCE_PRESENT_IND is false, otherwise active; a missing presence flag defaults true. No superseded status is emitted, and the value is a derived STRING label rather than the original Boolean presence flag.",
    "loaded_at": "map_slam_cost_line_item.ADC_UPDT carried unchanged for the cost-line row. The parent activity key is derived without a parent join or added parent clock; this is bronze load provenance, not the currency date/time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_cost_line_item"),
    comment="Internal quality-controlled twin of reference_cost_line_item: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(REFERENCE_COST_LINE_ITEM_MANDATORY_RULES)
@dp.expect_all(REFERENCE_COST_LINE_ITEM_ADVISORY_RULES)
def _gold_qc_reference_cost_line_item():
    """Quality-controlled twin of journey_reference.cost_line_item."""
    df = _qc("reference_cost_line_item", REFERENCE_COST_LINE_ITEM_SELECT)
    return _with_comments(df, REFERENCE_COST_LINE_ITEM_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.cost_line_item"),
    comment=(
        "One frozen PLICS cost line; excluded from the event plane. Gold QC twin of the "
        "silver product: 0 columns are repaired or nulled, 1 rule(s) drop rows, 3 check(s) "
        "are advisory. Each rule states its reason in the pipeline notebook, and Lakeflow "
        "expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_cost_line_item():
    """Contract-v2 public twin of reference_cost_line_item; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_cost_line_item")).selectExpr(
        '`cost_line_item_key` AS `cost_line_item_key`',
        '`costed_activity_key` AS `costed_activity_key`',
        '`extract_cd` AS `extract_cd`',
        '`activity_record_id` AS `activity_record_id`',
        '`line_hash` AS `line_hash`',
        '`activity_cost_item_cd` AS `activity_cost_item_cd`',
        '`resource_cost_item_cd` AS `resource_cost_item_cd`',
        '`activity_count` AS `activity_count`',
        '`unbundled_subtype_cd` AS `unbundled_subtype_cd`',
        '`unbundled_currency_cd` AS `unbundled_currency_cd`',
        '`unbundled_currency_datetime` AS `unbundled_currency_datetime`',
        '`total_cost` AS `total_cost`',
        '`total_o_cost` AS `total_o_cost`',
        '`source_duplicate_count` AS `source_duplicate_count`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_COST_LINE_ITEM_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.elective_access_procedure ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_ELECTIVE_ACCESS_PROCEDURE_SELECT = [
    '`elective_access_procedure_key` AS `elective_access_procedure_key`',
    '`elective_access_entry_key` AS `elective_access_entry_key`',
    '`waiting_list_oid` AS `waiting_list_oid`',
    '`procedure_code` AS `procedure_code`',
    '`procedure_desc` AS `procedure_desc`',
    '`procedure_catalog` AS `procedure_catalog`',
    '`procedure_rvid` AS `procedure_rvid`',
    '`procedure_type_seq` AS `procedure_type_seq`',
    '`procedure_seq` AS `procedure_seq`',
    '`active_ind` AS `active_ind`',
    '`parent_present_ind` AS `parent_present_ind`',
    '`source_system_oid` AS `source_system_oid`',
    '`source_system_oid_inherited_ind` AS `source_system_oid_inherited_ind`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_ELECTIVE_ACCESS_PROCEDURE_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 2,922,493 rows of 2,922,503
    # that are current and attributable. Superseded versions and rows whose identity was
    # never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(record_status = 'active')",
}

REFERENCE_ELECTIVE_ACCESS_PROCEDURE_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 10 of 2,922,503 at the profile.
    "gold.reference.elective_access_procedure.record_status.default_view_active":
        "record_status = 'active'",
}

REFERENCE_ELECTIVE_ACCESS_PROCEDURE_COLUMN_COMMENTS = {
    "elective_access_procedure_key": "Deterministic SHA-256 key for this LUNA procedure row.",
    "elective_access_entry_key": "Deterministic SHA-256 key for the parent elective access entry.",
    "waiting_list_oid": "LUNA WAITING_LIST_OID as BIGINT.",
    "procedure_code": "B3 bronze field PROCEDURE_CODE; source value retained unless documented as derived.",
    "procedure_desc": "B3 bronze field PROCEDURE_DESC; source value retained unless documented as derived.",
    "procedure_catalog": "B3 bronze field PROCEDURE_CATALOG; source value retained unless documented as derived.",
    "procedure_rvid": "B3 bronze field PROCEDURE_RVID; source value retained unless documented as derived.",
    "procedure_type_seq": "B3 bronze field PROCEDURE_TYPE_SEQ; source value retained unless documented as derived.",
    "procedure_seq": "B3 bronze field PROCEDURE_SEQ; source value retained unless documented as derived.",
    "active_ind": "B3 bronze field ACTIVE_IND; source value retained unless documented as derived.",
    "parent_present_ind": "B3 bronze field PARENT_PRESENT_IND; source value retained unless documented as derived.",
    "source_system_oid": "LUNA SOURCE_SYSTEM_OID as BIGINT.",
    "source_system_oid_inherited_ind": "Always true on present rows; proves namespace provenance is the parent.",
    "record_status": "Retracted when SOURCE_PRESENT_IND is false; otherwise superseded when ACTIVE_IND is false; otherwise active. Both missing flags default true, and retraction has priority. PARENT_PRESENT_IND is published separately and does not determine this row status.",
    "loaded_at": "map_elective_access_list_procedure.ADC_UPDT carried unchanged for the planned-procedure slot. No parent waiting-list load clock is joined or added; this is bronze load provenance, not procedure time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_elective_access_procedure"),
    comment="Internal quality-controlled twin of reference_elective_access_procedure: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(REFERENCE_ELECTIVE_ACCESS_PROCEDURE_MANDATORY_RULES)
@dp.expect_all(REFERENCE_ELECTIVE_ACCESS_PROCEDURE_ADVISORY_RULES)
def _gold_qc_reference_elective_access_procedure():
    """Quality-controlled twin of journey_reference.elective_access_procedure."""
    df = _qc("reference_elective_access_procedure", REFERENCE_ELECTIVE_ACCESS_PROCEDURE_SELECT)
    return _with_comments(df, REFERENCE_ELECTIVE_ACCESS_PROCEDURE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.elective_access_procedure"),
    comment=(
        "One LUNA elective-access planned-procedure slot. Gold QC twin of the silver product: "
        "0 columns are repaired or nulled, 1 rule(s) drop rows, 1 check(s) are advisory. Each "
        "rule states its reason in the pipeline notebook, and Lakeflow expectation metrics "
        "report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_elective_access_procedure():
    """Contract-v2 public twin of reference_elective_access_procedure; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_elective_access_procedure")).selectExpr(
        '`elective_access_procedure_key` AS `elective_access_procedure_key`',
        '`elective_access_entry_key` AS `elective_access_entry_key`',
        '`waiting_list_oid` AS `waiting_list_oid`',
        '`procedure_code` AS `procedure_code`',
        '`procedure_desc` AS `procedure_desc`',
        '`procedure_catalog` AS `procedure_catalog`',
        '`procedure_rvid` AS `procedure_rvid`',
        '`procedure_type_seq` AS `procedure_type_seq`',
        '`procedure_seq` AS `procedure_seq`',
        '`active_ind` AS `active_ind`',
        '`parent_present_ind` AS `parent_present_ind`',
        '`source_system_oid` AS `source_system_oid`',
        '`source_system_oid_inherited_ind` AS `source_system_oid_inherited_ind`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_ELECTIVE_ACCESS_PROCEDURE_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.appointment_booking ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_appointment applies the parent Gold drop rule.
CLINICAL_APPOINTMENT_BOOKING_SELECT = [
    '`sch_event_id` AS `sch_event_id`',
    '`sequence` AS `sequence`',
    '`schedule_id` AS `schedule_id`',
    '`schedule_sequence` AS `schedule_sequence`',
    '`status_code` AS `status_code`',
    '`status_display` AS `status_display`',
    '`status_meaning` AS `status_meaning`',
    '`effective_start` AS `effective_start`',
    '`effective_end` AS `effective_end`',
    '`location_code` AS `location_code`',
    '`location_display` AS `location_display`',
    '`source_present_ind` AS `source_present_ind`',
    '`loaded_at` AS `loaded_at`',
]

CLINICAL_APPOINTMENT_BOOKING_COLUMN_COMMENTS = {
    "sch_event_id": "Millennium SCH_EVENT_ID of the parent clinical_appointment row.",
    "sequence": "Booking sequence, falling back to SCHEDULE_ID.",
    "schedule_id": "Source scheduling iteration identifier.",
    "schedule_sequence": "Source scheduling sequence.",
    "status_code": "Source scheduling-state code for this iteration.",
    "status_display": "Source scheduling-state display for this iteration.",
    "status_meaning": "Source scheduling-state meaning for this iteration.",
    "effective_start": "Source effective-start timestamp.",
    "effective_end": "Source effective-end timestamp.",
    "location_code": "Source scheduling location code.",
    "location_display": "Source location description, falling back to free text.",
    "source_present_ind": "Whether the source row remains present.",
    "loaded_at": "Bronze load timestamp of the scheduling row.",
}

CLINICAL_APPOINTMENT_BOOKING_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_appointment_booking.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_appointment_booking"),
    comment="Internal QC of clinical_appointment_booking; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_APPOINTMENT_BOOKING_MANDATORY_RULES)
def _gold_qc_clinical_appointment_booking():
    """Task 3 Gold child of clinical_appointment; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.appointment")).select(
        'sch_event_id',
    ).dropDuplicates(['sch_event_id'])
    child = spark.read.table(_src("clinical_appointment_booking"))
    df = _with_parent_status(child, parent, ['sch_event_id']).selectExpr(*CLINICAL_APPOINTMENT_BOOKING_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_APPOINTMENT_BOOKING_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.appointment_booking"),
    comment='One booking or location iteration per scheduling appointment.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_appointment_booking():
    """Publish clinical_appointment_booking without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_appointment_booking")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.appointment_resource ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_appointment applies the parent Gold drop rule.
CLINICAL_APPOINTMENT_RESOURCE_SELECT = [
    '`sch_event_id` AS `sch_event_id`',
    '`sequence` AS `sequence`',
    '`appointment_role_id` AS `appointment_role_id`',
    '`schedule_id` AS `schedule_id`',
    '`role_code` AS `role_code`',
    '`role_display` AS `role_display`',
    '`practitioner_key` AS `practitioner_key`',
    '`location_code` AS `location_code`',
    '`slot_start` AS `slot_start`',
    '`slot_end` AS `slot_end`',
    '`source_present_ind` AS `source_present_ind`',
    '`loaded_at` AS `loaded_at`',
]

CLINICAL_APPOINTMENT_RESOURCE_COLUMN_COMMENTS = {
    "sch_event_id": "Millennium SCH_EVENT_ID of the parent clinical_appointment row.",
    "sequence": "Resource sequence, falling back to SCH_APPT_ID.",
    "appointment_role_id": "Source scheduling appointment-role identifier.",
    "schedule_id": "Source schedule identifier.",
    "role_code": "Source scheduling role code.",
    "role_display": "Source scheduling role display, falling back to role meaning.",
    "practitioner_key": "Deterministic practitioner key derived from ALLOCATED_PERSONNEL_ID.",
    "location_code": "Source appointment location code.",
    "slot_start": "Source allocated slot start.",
    "slot_end": "Source allocated slot end.",
    "source_present_ind": "Whether the source row remains present.",
    "loaded_at": "Bronze load timestamp of the resource row.",
}

CLINICAL_APPOINTMENT_RESOURCE_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_appointment_resource.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_appointment_resource"),
    comment="Internal QC of clinical_appointment_resource; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_APPOINTMENT_RESOURCE_MANDATORY_RULES)
def _gold_qc_clinical_appointment_resource():
    """Task 3 Gold child of clinical_appointment; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.appointment")).select(
        'sch_event_id',
    ).dropDuplicates(['sch_event_id'])
    child = spark.read.table(_src("clinical_appointment_resource"))
    df = _with_parent_status(child, parent, ['sch_event_id']).selectExpr(*CLINICAL_APPOINTMENT_RESOURCE_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_APPOINTMENT_RESOURCE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.appointment_resource"),
    comment='One scheduling resource or slot row per appointment.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_appointment_resource():
    """Publish clinical_appointment_resource without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_appointment_resource")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.hrg_grouping_diagnosis ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_hrg_grouping applies the parent Gold drop rule.
CLINICAL_HRG_GROUPING_DIAGNOSIS_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_object` AS `source_object`',
    '`cds_id` AS `cds_id`',
    '`sequence` AS `sequence`',
    '`icd10_code` AS `icd10_code`',
    '`display` AS `display`',
    '`loaded_at` AS `loaded_at`',
]

CLINICAL_HRG_GROUPING_DIAGNOSIS_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic key of the parent clinical_hrg_grouping row.",
    "source_object": "SLAM source arm; part of the native parent key.",
    "cds_id": "Native CDS_APC_ID or CDS_OPA_ID within source_object.",
    "sequence": "One-based source order of the ICD-10 diagnosis code.",
    "icd10_code": "Verbatim ICD-10 diagnosis code from the HRG source array.",
    "display": "Reserved display text; populated in Session 3.",
    "loaded_at": "Bronze load timestamp inherited from the HRG grouping row.",
}

CLINICAL_HRG_GROUPING_DIAGNOSIS_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_hrg_grouping_diagnosis.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_hrg_grouping_diagnosis"),
    comment="Internal QC of clinical_hrg_grouping_diagnosis; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_HRG_GROUPING_DIAGNOSIS_MANDATORY_RULES)
def _gold_qc_clinical_hrg_grouping_diagnosis():
    """Task 3 Gold child of clinical_hrg_grouping; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.hrg_grouping")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_hrg_grouping_diagnosis"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_HRG_GROUPING_DIAGNOSIS_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_HRG_GROUPING_DIAGNOSIS_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.hrg_grouping_diagnosis"),
    comment='One ICD-10 diagnosis code per HRG grouping row in source order.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_hrg_grouping_diagnosis():
    """Publish clinical_hrg_grouping_diagnosis without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_hrg_grouping_diagnosis")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.hrg_grouping_procedure ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_hrg_grouping applies the parent Gold drop rule.
CLINICAL_HRG_GROUPING_PROCEDURE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_object` AS `source_object`',
    '`cds_id` AS `cds_id`',
    '`sequence` AS `sequence`',
    '`opcs4_code` AS `opcs4_code`',
    '`display` AS `display`',
    '`loaded_at` AS `loaded_at`',
]

CLINICAL_HRG_GROUPING_PROCEDURE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic key of the parent clinical_hrg_grouping row.",
    "source_object": "SLAM source arm; part of the native parent key.",
    "cds_id": "Native CDS_APC_ID or CDS_OPA_ID within source_object.",
    "sequence": "One-based source order of the OPCS-4 procedure code.",
    "opcs4_code": "Verbatim OPCS-4 procedure code from the HRG source array.",
    "display": "Reserved display text; populated in Session 3.",
    "loaded_at": "Bronze load timestamp inherited from the HRG grouping row.",
}

CLINICAL_HRG_GROUPING_PROCEDURE_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_hrg_grouping_procedure.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_hrg_grouping_procedure"),
    comment="Internal QC of clinical_hrg_grouping_procedure; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_HRG_GROUPING_PROCEDURE_MANDATORY_RULES)
def _gold_qc_clinical_hrg_grouping_procedure():
    """Task 3 Gold child of clinical_hrg_grouping; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.hrg_grouping")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_hrg_grouping_procedure"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_HRG_GROUPING_PROCEDURE_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_HRG_GROUPING_PROCEDURE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.hrg_grouping_procedure"),
    comment='One OPCS-4 procedure code per HRG grouping row in source order.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_hrg_grouping_procedure():
    """Publish clinical_hrg_grouping_procedure without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_hrg_grouping_procedure")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.rtt_pathway_encounter_type ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_rtt_pathway applies the parent Gold drop rule.
CLINICAL_RTT_PATHWAY_ENCOUNTER_TYPE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_system_oid` AS `source_system_oid`',
    '`pathway_oid` AS `pathway_oid`',
    '`period_oid` AS `period_oid`',
    '`sequence` AS `sequence`',
    '`encounter_type` AS `encounter_type`',
    '`loaded_at` AS `loaded_at`',
]

CLINICAL_RTT_PATHWAY_ENCOUNTER_TYPE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic key of the parent clinical_rtt_pathway row.",
    "source_system_oid": "LUNA SOURCE_SYSTEM_OID; first native pathway-period key component.",
    "pathway_oid": "LUNA PATHWAY_OID; second native pathway-period key component.",
    "period_oid": "LUNA PERIOD_OID; third native pathway-period key component.",
    "sequence": "One-based order of the encounter-type tag in the source array.",
    "encounter_type": "Verbatim LUNA pathway encounter-type tag.",
    "loaded_at": "Bronze load timestamp inherited from the parent pathway row.",
}

CLINICAL_RTT_PATHWAY_ENCOUNTER_TYPE_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_rtt_pathway_encounter_type.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_rtt_pathway_encounter_type"),
    comment="Internal QC of clinical_rtt_pathway_encounter_type; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_RTT_PATHWAY_ENCOUNTER_TYPE_MANDATORY_RULES)
def _gold_qc_clinical_rtt_pathway_encounter_type():
    """Task 3 Gold child of clinical_rtt_pathway; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.rtt_pathway")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_rtt_pathway_encounter_type"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_RTT_PATHWAY_ENCOUNTER_TYPE_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_RTT_PATHWAY_ENCOUNTER_TYPE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.rtt_pathway_encounter_type"),
    comment='One ordered encounter-type tag per LUNA RTT pathway period.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_rtt_pathway_encounter_type():
    """Publish clinical_rtt_pathway_encounter_type without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_rtt_pathway_encounter_type")).drop("__gold_parent_present")

# COMMAND ----------

