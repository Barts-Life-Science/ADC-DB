# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Forms
# MAGIC Forms, response-grain observations and smoking-status instruments.
# MAGIC
# MAGIC Numbers indicate reading order; Lakeflow uses dataset dependencies to schedule work.
# MAGIC Mandatory rules use `dp.expect_all_or_drop`; advisory rules use `dp.expect_all`.
# MAGIC Repairs and nulling stay in the projection. Public names and identifiers are preserved.
# MAGIC Helpers are in `gold_journey_shared.py`, an ordinary Python file.

# COMMAND ----------

from gold_journey_shared import (
    F,
    _n,
    _qc,
    _s3_axis_columns,
    _s3_axis_gate_and_policy,
    _s3_gate_direct_public,
    _src,
    _with_comments,
    _with_parent_status,
    dp,
)

# COMMAND ----------

# ==== journey_clinical.form ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_FORM_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`dcp_forms_activity_id` AS `dcp_forms_activity_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`form_type_code` AS `form_type_code`',
    '`form_type_display` AS `form_type_display`',
    '`form_status_code` AS `form_status_code`',
    '`form_status_display` AS `form_status_display`',
    'CASE WHEN CAST(`authored_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `authored_datetime` END AS `authored_datetime`',
    '`completed_datetime` AS `completed_datetime`',
    '`performed_practitioner_id` AS `performed_practitioner_id`',
    '`organization_id` AS `organization_id`',
    '`response_row_count` AS `response_row_count`',
    '`active_response_row_count` AS `active_response_row_count`',
    '`empty_response_row_count` AS `empty_response_row_count`',
    '`invalid_response_row_count` AS `invalid_response_row_count`',
    '`matched_response_row_count` AS `matched_response_row_count`',
    '`unmatched_response_row_count` AS `unmatched_response_row_count`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_form_metadata; source history stays on the main research table.
CLINICAL_FORM_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 55,387,704 rows of
    # 55,393,916 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_FORM_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 6,212 of 55,393,916 at the profile.
    "gold.clinical.form.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 45 of 55,393,916
    # rows (8.12e-05%) when profiled on 2026-08-24.
    "gold.clinical.form.record_status_effective_from.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 8,310 of 55,393,916 rows (0.015%) when profiled on 2026-08-24.
    "gold.clinical.form.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 12,285 of 55,393,916 rows (0.0222%) when profiled on 2026-08-24.
    "gold.clinical.form.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_FORM_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "dcp_forms_activity_id": "Millennium DCP_FORMS_ACTIVITY_ID; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "event_datetime": "First documented timestamp.",
    "event_end_datetime": "Last documented or performed timestamp.",
    "source_coding_system": "Source form coding system.",
    "source_code": "Source form reference identifier.",
    "source_display": "Source form description.",
    "form_type_code": "Source form type code.",
    "form_type_display": "Source form type display.",
    "form_status_code": "Source form status code.",
    "form_status_display": "Source form status display.",
    "authored_datetime": "Earliest form documentation timestamp.",
    "completed_datetime": "Latest documentation or performed timestamp.",
    "performed_practitioner_id": "Millennium personnel PERSON_ID for the performer.",
    "organization_id": "Millennium ORGANIZATION_ID for the source organization.",
    "response_row_count": "Number of response rows grouped into the form.",
    "active_response_row_count": "Number of active response rows.",
    "empty_response_row_count": "Number of retained empty response rows.",
    "invalid_response_row_count": "Number of source-classified invalid response rows.",
    "matched_response_row_count": "Number of canonical matched response rows.",
    "unmatched_response_row_count": "Number of canonical unmatched response rows.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Registered owning feed.",
    "record_status": "Derived PowerForm row status: retracted when the grouped maximum of map_powerform_assessment.SOURCE_PRESENT_IND is 0; otherwise superseded when the grouped maximum of ASSESSMENT_ACTIVE_IND is 0 or no mill_form_activity response has ACTIVE_IND = 1; otherwise active. Missing assessment flags alone do not trigger retraction or supersession. This is a status label, not the raw Boolean assessment flag.",
    "record_status_effective_from": "Earliest mill_form_activity.DOCUMENTATION_DT_TM for the form instance, carried from authored_datetime. This is the form documentation start used by the research status projection, not an independently recorded status-change timestamp; null when all contributing documentation timestamps are null.",
    "record_status_effective_to": "For a retracted or superseded form, completed_datetime: the maximum of coalesce(mill_form_activity.LAST_DOCUMENTED_DT_TM, mill_form_activity.PERFORMED_DT_TM) across its responses. Null for active forms or when no contributing completion timestamp is available. This is a documentation/completion boundary, not a Boolean assessment flag or an independently recorded status-change time.",
    "source_update_timestamp": "Maximum, across the form's mill_form_activity rows, of the greatest ADC_UPDT, SOURCE_ACTIVITY_ADC_UPDT, SOURCE_EVENT_ADC_UPDT and SOURCE_LABEL_ADC_UPDT. This combines the contributing activity, event, label and bronze-ingestion clocks; it is not uniformly a native clinical-system update time. Null only when every contributing clock is null.",
    "loaded_at": "Greatest of the form-level maximum ADC_UPDT values from mill_form_activity, map_powerform_assessment_item and map_powerform_assessment, and maximum Source_ADC_UPDT from map_mat_vte_assessment. This is the contributing-source ingestion/provenance clock, not the Silver refresh time or form event time; null only when every contributing clock is null.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_form"),
    comment="Internal quality-controlled twin of clinical_form: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_FORM_MANDATORY_RULES)
@dp.expect_all(CLINICAL_FORM_ADVISORY_RULES)
def _gold_qc_clinical_form():
    """Quality-controlled twin of journey_clinical.form."""
    df = _qc("clinical_form", CLINICAL_FORM_SELECT)
    return _with_comments(df, CLINICAL_FORM_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.form"),
    comment=(
        "One completed or retained PowerForm instance with ordered response content. Gold QC "
        "twin of the silver product: 8 columns are repaired or nulled, 1 rule(s) drop rows, 4 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_form():
    """Contract-v2 public twin of clinical_form; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_form")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`dcp_forms_activity_id` AS `dcp_forms_activity_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`form_type_code` AS `form_type_code`',
        '`form_type_display` AS `form_type_display`',
        '`form_status_code` AS `form_status_code`',
        '`form_status_display` AS `form_status_display`',
        '`authored_datetime` AS `authored_datetime`',
        '`completed_datetime` AS `completed_datetime`',
        '`performed_practitioner_id` AS `performed_practitioner_id`',
        '`organization_id` AS `organization_id`',
        '`response_row_count` AS `response_row_count`',
        '`active_response_row_count` AS `active_response_row_count`',
        '`empty_response_row_count` AS `empty_response_row_count`',
        '`invalid_response_row_count` AS `invalid_response_row_count`',
        '`matched_response_row_count` AS `matched_response_row_count`',
        '`unmatched_response_row_count` AS `unmatched_response_row_count`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_FORM_COLUMN_COMMENTS)

# COMMAND ----------

CLINICAL_FORM_SMOKING_STATUS_MANDATORY_RULES = {
    # Preserve the existing parent requirement, with native rejection counts.
    "gold.clinical_form_smoking_status.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

CLINICAL_FORM_RESPONSE_MANDATORY_RULES = {
    # Preserve the existing parent requirement, with native rejection counts.
    "gold.clinical_form_response.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

# ==== S3b form response and smoking-status products ====

def _s3b_form_qc(table_name, axes):
    """Retain parent-admission evidence and prepare each form axis's mapping decision."""
    source = spark.read.table(_src(table_name))
    parent = spark.read.table(_n("gold_clinical.form")).select("patient_event_key").distinct()
    df = _with_parent_status(source, parent, ["patient_event_key"])
    for axis in axes:
        required = _s3_axis_columns(axis)
        if any(name not in df.columns for name in required):
            continue
        item = {name[len(axis)+1:]: F.col(name) for name in required}
        gate, policy_id = _s3_axis_gate_and_policy(item)
        has_target = item["target_code"].isNotNull() | item["snomed_code"].isNotNull() | item["omop_concept_id"].isNotNull()
        df = (df.withColumn(f"_{axis}_gate_pass", F.when(has_target, gate))
              .withColumn(f"_{axis}_policy_id", F.when(has_target, policy_id)))
    return df

@dp.materialized_view(
    name=_n("gold_qc._clinical_form_response"),
    comment="Internal S3b Gold twin of response-grain PowerForm observations.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_FORM_RESPONSE_MANDATORY_RULES)
def _gold_qc_clinical_form_response():
    """Prepare internal QC data for clinical_form_response; native rules run on the returned rows."""
    return _s3b_form_qc("clinical_form_response", ("question", "answer", "unit"))

@dp.materialized_view(
    name=_n("gold_clinical.form_response"), table_properties={"quality": "gold"},
    comment="Response-grain PowerForm observations with governed question, answer and unit targets.",
    refresh_policy="incremental",
)
def gold_clinical_form_response():
    """Publish clinical_form_response with its existing research columns and quality policy."""
    df = spark.read.table(_n("gold_qc._clinical_form_response")).drop("__gold_parent_present")
    return (_s3_gate_direct_public(df, "clinical_form_response")
            .drop(*[f"_{axis}_{suffix}" for axis in ("question", "answer", "unit")
                    for suffix in ("gate_pass", "policy_id")]))

@dp.materialized_view(
    name=_n("gold_qc._clinical_form_smoking_status"),
    comment="Internal S3b Gold twin of the smoking-status instrument.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_FORM_SMOKING_STATUS_MANDATORY_RULES)
def _gold_qc_clinical_form_smoking_status():
    """Prepare internal QC data for clinical_form_smoking_status; native rules run on the returned rows."""
    return _s3b_form_qc(
        "clinical_form_smoking_status",
        ("smoking_status", "cessation_advice", "cessation_referral", "nicotine_treatment", "dependence_level"),
    )

@dp.materialized_view(
    name=_n("gold_clinical.form_smoking_status"), table_properties={"quality": "gold"},
    comment="Smoking status and cessation support with separate governed semantic axes.",
    refresh_policy="incremental",
)
def gold_clinical_form_smoking_status():
    """Publish clinical_form_smoking_status with its existing research columns and quality policy."""
    axes = ("smoking_status", "cessation_advice", "cessation_referral", "nicotine_treatment", "dependence_level")
    df = spark.read.table(_n("gold_qc._clinical_form_smoking_status")).drop("__gold_parent_present")
    return (_s3_gate_direct_public(df, "clinical_form_smoking_status")
            .drop(*[f"_{axis}_{suffix}" for axis in axes for suffix in ("gate_pass", "policy_id")]))

