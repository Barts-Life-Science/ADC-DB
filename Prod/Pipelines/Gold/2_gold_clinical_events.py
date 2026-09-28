# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Clinical Events
# MAGIC Conditions, procedures, medicines, pathology, imaging and other clinical observations.
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
    _s3_flatten_gold_public,
    _s3_gate_direct_public,
    _src,
    _with_comments,
    _with_parent_status,
    dp,
)

# COMMAND ----------

# ==== journey_clinical.allergy_intolerance ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_ALLERGY_INTOLERANCE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`allergy_instance_id` AS `allergy_instance_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    '`substance_code` AS `substance_code`',
    '`substance_type_code` AS `substance_type_code`',
    '`substance_type_display` AS `substance_type_display`',
    '`reaction_class_code` AS `reaction_class_code`',
    '`reaction_class_display` AS `reaction_class_display`',
    '`reaction_status_code` AS `reaction_status_code`',
    '`reaction_status_display` AS `reaction_status_display`',
    '`severity_code` AS `severity_code`',
    '`severity_display` AS `severity_display`',
    '`absence_assertion_ind` AS `absence_assertion_ind`',
    '`onset_precision_display` AS `onset_precision_display`',
    '`source_of_info_display` AS `source_of_info_display`',
    '`verified_status_flag` AS `verified_status_flag`',
    '`reviewed_datetime` AS `reviewed_datetime`',
    '`cancel_reason_display` AS `cancel_reason_display`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_allergy_intolerance_metadata; source history stays on the main research table.
CLINICAL_ALLERGY_INTOLERANCE_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 518,748 rows of 518,748
    # that are current and attributable. Superseded versions and rows whose identity was
    # never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_ALLERGY_INTOLERANCE_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 518,748 at the profile.
    "gold.clinical.allergy_intolerance.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 762 of 518,748 rows (0.147%) when profiled on 2026-08-24.
    "gold.clinical.allergy_intolerance.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 109 of 518,748 rows (0.021%) when profiled on 2026-08-24.
    "gold.clinical.allergy_intolerance.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_ALLERGY_INTOLERANCE_COLUMN_COMMENTS = {
    "patient_event_key": "Stable silver-v2 key identifying the patient event associated with each allergy intolerance record. It is derived from bronze field `ALLERGY_INSTANCE_ID` in `4_prod.bronze.map_allergy`. This is a contract identifier rather than a display value; null means the source identifiers were insufficient to derive the key.",
    "allergy_instance_id": "Source-system identifier for the allergy instance associated with each allergy intolerance record. It is carried from bronze field `ALLERGY_INSTANCE_ID` in `4_prod.bronze.map_allergy`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "subject_key": "Always-populated deterministic subject key.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium person identifier when available.",
    "encounter_id": "Recording encounter reference when supplied.",
    "event_datetime": "Allergy onset timestamp from `ONSET_DT_TM_CLEAN`, falling back to `CREATED_DT_TM_CLEAN` when onset is absent; null when both cleaned timestamps are absent.",
    "event_end_datetime": "Reaction-status end when resolved or cancelled.",
    "source_coding_system": "Verbatim substance code system.",
    "source_code": "Substance code (SNOMED then source identifier then nomenclature id)",
    "source_display": "Verbatim substance display text.",
    "substance_code": "Source and mapped substance codings as a one-level CodeableConcept VARIANT.",
    "substance_type_code": "Source substance type code.",
    "substance_type_display": "Substance type (Drug/Food/Environment/...).",
    "reaction_class_code": "Source reaction class code.",
    "reaction_class_display": "Reaction class (Allergy/Intolerance/Side Effect/...).",
    "reaction_status_code": "Source reaction status code.",
    "reaction_status_display": "Reaction status (Active/Cancelled/Resolved/Proposed).",
    "severity_code": "Source severity code.",
    "severity_display": "Source allergy severity description carried from `SEVERITY_DESC`, for example mild, moderate or severe.",
    "absence_assertion_ind": "True when the row asserts ABSENCE of allergy (e.g. no known allergies) rather than a positive assertion.",
    "onset_precision_display": "Source onset precision.",
    "source_of_info_display": "Allergy information-source description from `SOURCE_OF_INFO_DESC`, falling back to the recorded free text in `SOURCE_OF_INFO_FT` when the description is null.",
    "verified_status_flag": "Pharmacy-verified flag verbatim.",
    "reviewed_datetime": "Sentinel-cleaned last review timestamp.",
    "cancel_reason_display": "Cancel reason when cancelled.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld-identity indicator when supplied.",
    "record_status": "Retracted when upper(coalesce(REACTION_STATUS_DESC,'')) equals CANCELLED, without trimming; otherwise superseded when END_EFFECTIVE_DT_TM is before 2100-01-01 or ACTIVE_IND cast to LONG equals zero; otherwise active. A null end defaults to not-ended and a null activity flag defaults to 1. Cancellation takes priority, and other nonzero activity values do not trigger supersession.",
    "record_status_effective_from": "First non-null map_allergy.REACTION_STATUS_DT_TM_CLEAN, ACTIVE_STATUS_DT_TM, then BEG_EFFECTIVE_DT_TM. This source-status/validity timestamp is distinct from the onset/created fallbacks used for event_datetime; null when all three are missing.",
    "record_status_effective_to": "CANCEL_DT_TM_CLEAN for cancelled allergy rows, even if null; otherwise END_EFFECTIVE_DT_TM for ended/inactive rows; otherwise null. There is no fallback between those branches. Supersession caused by ACTIVE_IND=0 can retain a far-future end value because the selected end is not range-clamped.",
    "source_update_timestamp": "Bronze pipeline processing timestamp carried from `PIPELINE_UPDT_DT_TM` in `4_prod.bronze.map_allergy`. It records when bronze processed the source row, not a native clinical-system update timestamp, clinical event time, or the current Silver refresh time.",
    "loaded_at": "map_allergy.ADC_UPDT carried unchanged through usable-code filtering and quality/public projections. This is the contributing bronze load clock, distinct from PIPELINE_UPDT_DT_TM published as source_update_timestamp; it is not allergy onset, status-change time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_allergy_intolerance"),
    comment="Internal quality-controlled twin of clinical_allergy_intolerance: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_ALLERGY_INTOLERANCE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_ALLERGY_INTOLERANCE_ADVISORY_RULES)
def _gold_qc_clinical_allergy_intolerance():
    """Quality-controlled twin of journey_clinical.allergy_intolerance."""
    df = _qc("clinical_allergy_intolerance", CLINICAL_ALLERGY_INTOLERANCE_SELECT)
    return _with_comments(df, CLINICAL_ALLERGY_INTOLERANCE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.allergy_intolerance"),
    comment=(
        "One Millennium allergy/intolerance assertion (map_allergy) with reaction class, "
        "severity, absence assertions, and cancel/review lifecycle. Gold QC twin of the "
        "silver product: 5 columns are repaired or nulled, 1 rule(s) drop rows, 3 check(s) "
        "are advisory. Each rule states its reason in the pipeline notebook, and Lakeflow "
        "expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_allergy_intolerance():
    """Contract-v2 public twin of clinical_allergy_intolerance; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_allergy_intolerance")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`allergy_instance_id` AS `allergy_instance_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`__gold_json_substance_code` AS `substance_code`',
        '`substance_type_code` AS `substance_type_code`',
        '`substance_type_display` AS `substance_type_display`',
        '`reaction_class_code` AS `reaction_class_code`',
        '`reaction_class_display` AS `reaction_class_display`',
        '`reaction_status_code` AS `reaction_status_code`',
        '`reaction_status_display` AS `reaction_status_display`',
        '`severity_code` AS `severity_code`',
        '`severity_display` AS `severity_display`',
        '`absence_assertion_ind` AS `absence_assertion_ind`',
        '`onset_precision_display` AS `onset_precision_display`',
        '`source_of_info_display` AS `source_of_info_display`',
        '`verified_status_flag` AS `verified_status_flag`',
        '`reviewed_datetime` AS `reviewed_datetime`',
        '`cancel_reason_display` AS `cancel_reason_display`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _s3_flatten_gold_public(_with_comments(df, CLINICAL_ALLERGY_INTOLERANCE_COLUMN_COMMENTS), "clinical_allergy_intolerance")

# COMMAND ----------

# ==== journey_clinical.clinical_finding ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_CLINICAL_FINDING_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`event_id` AS `event_id`',
    '`sequence_nbr` AS `sequence_nbr`',
    '`source_object` AS `source_object`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    "CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS DATE) = DATE'1899-12-30' OR CAST(`event_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_datetime` AS DATE)) < 9999 OR CAST(`event_datetime` AS DATE) = DATE'1900-01-01' OR CAST(`event_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`event_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `event_datetime` END END AS `event_datetime`",
    "CASE WHEN CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_end_datetime` AS DATE) = DATE'1899-12-30' OR CAST(`event_end_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_end_datetime` AS DATE)) < 9999 THEN NULL ELSE `event_end_datetime` END END AS `event_end_datetime`",
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`finding_code` AS `finding_code`',
    '`finding_kind` AS `finding_kind`',
    "CASE WHEN TRIM(CAST(`value_text` AS STRING)) = '' THEN NULL ELSE `value_text` END AS `value_text`",
    "CASE WHEN CAST(`value_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`value_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`value_datetime` AS DATE)) < 9999 OR CAST(`value_datetime` AS DATE) = DATE'1900-01-01' OR CAST(`value_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`value_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `value_datetime` END END AS `value_datetime`",
    '`value_code` AS `value_code`',
    '`value_display` AS `value_display`',
    "CASE WHEN UPPER(TRIM(CAST(`normalcy_code` AS STRING))) = '0' THEN NULL ELSE `normalcy_code` END AS `normalcy_code`",
    '`normalcy_display` AS `normalcy_display`',
    '`result_status_code` AS `result_status_code`',
    '`result_status_display` AS `result_status_display`',
    '`order_id` AS `order_id`',
    '`parent_event_id` AS `parent_event_id`',
    '`performer_practitioner_id` AS `performer_practitioner_id`',
    '`verifier_practitioner_id` AS `verifier_practitioner_id`',
    '`organization_id` AS `organization_id`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
    '`registry_field_id` AS `registry_field_id`',
    '`event_datetime_status` AS `event_datetime_status`',
]

# contract v2: QC/batch inputs come from internal _clinical_clinical_finding_metadata; source history stays on the main research table.
CLINICAL_CLINICAL_FINDING_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 1,591,253,116 rows of
    # 1,591,253,116 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_CLINICAL_FINDING_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 1,591,253,116 at the profile.
    "gold.clinical.clinical_finding.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 250,156 of 1,591,253,116 rows (0.0157%) when profiled on
    # 2026-08-24.
    "gold.clinical.clinical_finding.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 29,903,501 of 1,591,253,116 rows (1.88%) when profiled on
    # 2026-08-24.
    "gold.clinical.clinical_finding.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",

    # Left as a warning because it fires on 42,016,359 of 1,591,253,116 rows (2.64%) when
    # profiled on 2026-08-24 -- at that rate the rule's assumption about what event_datetime
    # and event_end_datetime mean is the thing in doubt, not the data. The inverted gaps are
    # mostly minutes, which reads as two clocks rather than two events in the wrong order.
    "gold.clinical.clinical_finding.table.ordering_violation_event_datetime_event_end_datetime":
        "NOT COALESCE((`event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime`), FALSE)",
}

CLINICAL_CLINICAL_FINDING_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "event_id": "Millennium EVENT_ID; native key for coded, nomenclature, date, and text event arms.",
    "sequence_nbr": "Sequence number that makes the coded result primary key unique when a single result spans more than one row.",
    "source_object": "Native source arm: coded_event, nomen_event, date_event, or text_event.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "event_datetime": "Clinically relevant finding timestamp.",
    "event_end_datetime": "Source effective end timestamp.",
    "source_coding_system": "Verbatim source event coding system.",
    "source_code": "Verbatim source event code.",
    "source_display": "Verbatim source event display.",
    "finding_code": "Source finding CodeableConcept.",
    "finding_kind": "Generic result representation carried by the source feed.",
    "value_text": "Verbatim text result or descriptor.",
    "value_datetime": "Date or date-time result value.",
    "value_code": "Source coded result value.",
    "value_display": "Source coded result display.",
    "normalcy_code": "Source normalcy or interpretation code.",
    "normalcy_display": "Source normalcy or interpretation display.",
    "result_status_code": "Source result-status code.",
    "result_status_display": "Source result-status display.",
    "order_id": "Source order identifier when supplied.",
    "parent_event_id": "Source parent-event identifier.",
    "performer_practitioner_id": "Millennium personnel PERSON_ID for the performer.",
    "verifier_practitioner_id": "Millennium personnel PERSON_ID for the verifier.",
    "organization_id": "Millennium ORGANIZATION_ID for the source organization.",
    "confidentiality_code": "Source confidentiality code.",
    "vip_ind": "Source VIP indicator.",
    "withheld_identity_ind": "Identity-withholding status is not populated by the current source projections; null does not mean identity was not withheld.",
    "source_feed": "Registered generic-event feed.",
    "record_status": "Coded-event rows are superseded only when the least CR_VALID_UNTIL_DT_TM and CE_VALID_UNTIL_DT_TM is non-null and before 2100-01-01; otherwise active, never retracted. Nomenclature, date and text rows are retracted for SOURCE_DELETED_IND cast to BOOLEAN true (null defaults false); otherwise superseded for the least result/event validity end being non-null and before 2100-01-01 or AUTHENTIC_FLAG cast to LONG zero (null defaults one); otherwise active. Retraction takes priority, and clinical result-status codes are separate.",
    "record_status_effective_from": "First non-null result-valid-from, event-valid-from, then clinical event time. Coded/nomenclature use CR_VALID_FROM_DT_TM and CE_VALID_FROM_DT_TM; date uses DATE_RESULT_VALID_FROM_DT_TM and CLINICAL_EVENT_VALID_FROM_DT_TM; text substitutes STRING_RESULT_VALID_FROM_DT_TM. Coded event time falls back through PERFORMED_DT_TM, EVENT_END_DT_TM, EVENT_START_DT_TM. Nomenclature uses CLINICAL_EVENT_DT_TM, EVENT_END_DT_TM, PERFORMED_DT_TM, EVENT_START_DT_TM. Date/text use RESULT_DT_TM, PERFORMED_DT_TM, EVENT_END_DT_TM, EVENT_START_DT_TM. No status-bound clamp or ingestion-time fallback is added.",
    "record_status_effective_to": "Coded rows use the least CR_VALID_UNTIL_DT_TM and CE_VALID_UNTIL_DT_TM only when superseded, otherwise null. Nomenclature, date and text rows use their computed source_update_timestamp when retracted; otherwise their least result/event validity end when superseded, otherwise null. Nomenclature uses CR/CE ends, date DATE_RESULT/CLINICAL_EVENT ends and text STRING_RESULT/CLINICAL_EVENT ends. An AUTHENTIC_FLAG-only supersession can therefore retain a null or future/sentinel end; no extra end clamp or fallback is applied.",
    "source_update_timestamp": "Per-arm greatest contributing clock, including own ADC_UPDT: coded adds SOURCE_ADC_UPDT, CR_UPDT_DT_TM and CE_UPDT_DT_TM; nomenclature adds SOURCE_CHANGE_TS and NOMENCLATURE_ADC_UPDT; date adds DATE_RESULT_EFFECTIVE_UPDT_DT_TM, CLINICAL_EVENT_ADC_UPDT and LOOKUP_ADC_UPDT; text adds STRING_RESULT_EFFECTIVE_UPDT_DT_TM, CLINICAL_EVENT_ADC_UPDT, LONG_TEXT_ADC_UPDT and LOOKUP_ADC_UPDT. Null inputs are ignored and all null gives null. These are not uniformly native application-update clocks, and the union takes no cross-arm maximum.",
    "loaded_at": "Own ADC_UPDT from map_coded_events, map_nomen_events, map_date_events or map_text_events, carried per arm through routing/usable-code filters, the union and QC/public projections. The additional result, clinical-event, lookup and long-text clocks used by source_update_timestamp are not added here; no current Silver refresh timestamp is substituted.",
    "registry_field_id": "Governed SHA-256 registry product and field identifier for registry observations; null on native rows.",
    "event_datetime_status": "registry_dated or registry_undated for registry observations; null on native rows.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_clinical_finding"),
    comment="Internal quality-controlled twin of clinical_clinical_finding: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_CLINICAL_FINDING_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CLINICAL_FINDING_ADVISORY_RULES)
def _gold_qc_clinical_clinical_finding():
    """Quality-controlled twin of journey_clinical.clinical_finding."""
    df = _qc("clinical_clinical_finding", CLINICAL_CLINICAL_FINDING_SELECT)
    return _with_comments(df, CLINICAL_CLINICAL_FINDING_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.clinical_finding"),
    comment=(
        "One governed residual clinical observation from a generic Millennium event feed, "
        "without diagnosis inference. Gold QC twin of the silver product: 10 columns are "
        "repaired or nulled, 1 rule(s) drop rows, 4 check(s) are advisory. Each rule states "
        "its reason in the pipeline notebook, and Lakeflow expectation metrics report what "
        "every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_clinical_finding():
    """Contract-v2 public twin of clinical_clinical_finding; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_clinical_finding")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`event_id` AS `event_id`',
        '`sequence_nbr` AS `sequence_nbr`',
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
        '`__gold_json_finding_code` AS `finding_code`',
        '`finding_kind` AS `finding_kind`',
        '`value_text` AS `value_text`',
        '`value_datetime` AS `value_datetime`',
        '`value_code` AS `value_code`',
        '`value_display` AS `value_display`',
        '`normalcy_code` AS `normalcy_code`',
        '`normalcy_display` AS `normalcy_display`',
        '`result_status_code` AS `result_status_code`',
        '`result_status_display` AS `result_status_display`',
        '`order_id` AS `order_id`',
        '`parent_event_id` AS `parent_event_id`',
        '`performer_practitioner_id` AS `performer_practitioner_id`',
        '`verifier_practitioner_id` AS `verifier_practitioner_id`',
        '`organization_id` AS `organization_id`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
        '`registry_field_id` AS `registry_field_id`',
        '`event_datetime_status` AS `event_datetime_status`',
    )
    return _s3_flatten_gold_public(_with_comments(df, CLINICAL_CLINICAL_FINDING_COLUMN_COMMENTS), "clinical_clinical_finding")

# COMMAND ----------

# ==== journey_clinical.clinical_score ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_CLINICAL_SCORE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`event_id` AS `event_id`',
    '`doc_response_key` AS `doc_response_key`',
    '`source_dcp_forms_activity_id` AS `source_dcp_forms_activity_id`',
    '`source_object` AS `source_object`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    "CASE WHEN YEAR(CAST(`event_datetime` AS TIMESTAMP)) = 1970 AND timestamp_seconds(CAST(unix_timestamp(`event_datetime`) AS BIGINT) * 1000) BETWEEN TIMESTAMP'1990-01-01 00:00:00' AND `loaded_at` THEN timestamp_seconds(CAST(unix_timestamp(`event_datetime`) AS BIGINT) * 1000) WHEN YEAR(CAST(`event_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `event_datetime` END AS `event_datetime`",
    'CASE WHEN CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`score_code` AS `score_code`',
    '`score_name` AS `score_name`',
    'CASE WHEN ABS(CAST(`value_number` AS DOUBLE)) > 1e12 OR `value_number` < 0 THEN NULL ELSE `value_number` END AS `value_number`',
    '`value_text` AS `value_text`',
    '`unit_source_value` AS `unit_source_value`',
    '`unit_concept_id` AS `unit_concept_id`',
    '`component_count` AS `component_count`',
    "CASE WHEN UPPER(TRIM(CAST(`interpretation_code` AS STRING))) = '0' THEN NULL ELSE `interpretation_code` END AS `interpretation_code`",
    '`interpretation_display` AS `interpretation_display`',
    '`result_status_code` AS `result_status_code`',
    '`result_status_display` AS `result_status_display`',
    '`performer_practitioner_id` AS `performer_practitioner_id`',
    '`source_form_key` AS `source_form_key`',
    '`promotion_rule_id` AS `promotion_rule_id`',
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

# contract v2: QC/batch inputs come from internal _clinical_clinical_score_metadata; source history stays on the main research table.
CLINICAL_CLINICAL_SCORE_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 303,073,419 rows of
    # 303,073,548 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_CLINICAL_SCORE_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 129 of 303,073,548 at the profile.
    "gold.clinical.clinical_score.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 35,015 of 303,073,548 rows (0.0116%) when profiled on
    # 2026-08-24.
    "gold.clinical.clinical_score.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 6,147,121 of 303,073,548 rows (2.03%) when profiled on
    # 2026-08-24.
    "gold.clinical.clinical_score.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",

    # Left as a warning because it fires on 247,483,238 of 303,073,548 rows (81.7%) when
    # profiled on 2026-08-24 -- at that rate the rule's assumption about what event_datetime
    # and event_end_datetime mean is the thing in doubt, not the data. The inverted gaps are
    # mostly minutes, which reads as two clocks rather than two events in the wrong order.
    "gold.clinical.clinical_score.table.ordering_violation_event_datetime_event_end_datetime":
        "NOT COALESCE((`event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime`), FALSE)",
}

CLINICAL_CLINICAL_SCORE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "event_id": "Millennium numeric EVENT_ID; native key for the numeric_event arm.",
    "doc_response_key": "PowerForm DOC_RESPONSE_KEY; native key for the form_promotion arm.",
    "source_dcp_forms_activity_id": "Millennium DCP_FORMS_ACTIVITY_ID of the source form for promoted rows.",
    "source_object": "Native source arm: numeric_event or form_promotion.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "event_datetime": "Clinical start date and time of the event as recorded on the Millennium CLINICAL_EVENT row.",
    "event_end_datetime": "Score end timestamp.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each clinical score record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "Source score code.",
    "source_display": "Source score display.",
    "score_code": "Source and mapped score CodeableConcept.",
    "score_name": "Human-readable score or scale name.",
    "value_number": "Numeric score value.",
    "value_text": "Verbatim score text.",
    "unit_source_value": "Source unit when supplied.",
    "unit_concept_id": "Mapped unit concept identifier.",
    "component_count": "Number of ordered component objects.",
    "interpretation_code": "Source interpretation code.",
    "interpretation_display": "Source interpretation display.",
    "result_status_code": "Source result status code.",
    "result_status_display": "Source result status display.",
    "performer_practitioner_id": "Millennium personnel PERSON_ID for the performer.",
    "source_form_key": "Deterministic SHA-256 key of the source PowerForm instance for promoted rows.",
    "promotion_rule_id": "Governed promotion-rule identifier.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Registered source route.",
    "record_status": "Arm-specific row status. Numeric events are retracted when map_numeric_events.SOURCE_DELETED_IND is true (null defaults false), otherwise superseded when CLINICAL_EVENT_VALID_UNTIL_DT_TM is before 2100-01-01, otherwise active. Form-promoted rows are retracted when CANONICAL_SOURCE_DELETED_IND is true or SOURCE_PRESENT_IND is false or missing; otherwise superseded when ACTIVE_IND is zero or missing, otherwise active. Retraction has priority over supersession; this is not the clinical result-status code.",
    "record_status_effective_from": "map_numeric_events.CLINICAL_EVENT_VALID_FROM_DT_TM on the numeric-event arm; map_powerform_assessment_item.FIRST_DOCUMENTED_DT_TM on the form-promotion arm. Each is carried unchanged, with no performed/response/ingestion-time fallback; a missing value remains null.",
    "record_status_effective_to": "Numeric events return CLINICAL_EVENT_VALID_UNTIL_DT_TM when deleted or ended, otherwise null; deletion can therefore retain a far-future end value. Form-promoted rows return LAST_DOCUMENTED_DT_TM when deleted, absent or inactive, otherwise null. No missing-end fallback is applied, and the form documentation end is not an independently observed status-transition time.",
    "source_update_timestamp": "For numeric events, greatest of map_numeric_events.STRING_RESULT_UPDT_DT_TM, CLINICAL_EVENT_UPDT_DT_TM and ADC_UPDT. For form-promoted rows, map_powerform_assessment_item.ADC_UPDT directly. This mixes source and ingestion provenance rather than uniformly representing a native modification clock; the promotion lookup adds no clock and Silver refresh time is not substituted.",
    "loaded_at": "map_numeric_events.ADC_UPDT for numeric-event rows, or map_powerform_assessment_item.ADC_UPDT for form-promoted rows. Each arm's bronze load clock is carried unchanged through its staging, union and quality wrappers. No form-parent or promotion-configuration clock is added; missing values remain null and this is not Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_clinical_score"),
    comment="Internal quality-controlled twin of clinical_clinical_score: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_CLINICAL_SCORE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CLINICAL_SCORE_ADVISORY_RULES)
def _gold_qc_clinical_clinical_score():
    """Quality-controlled twin of journey_clinical.clinical_score."""
    df = _qc("clinical_clinical_score", CLINICAL_CLINICAL_SCORE_SELECT)
    return _with_comments(df, CLINICAL_CLINICAL_SCORE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.clinical_score"),
    comment=(
        "One native or form-promoted clinical score with ordered component evidence. Gold QC "
        "twin of the silver product: 8 columns are repaired or nulled, 1 rule(s) drop rows, 4 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_clinical_score():
    """Contract-v2 public twin of clinical_clinical_score; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_clinical_score")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`event_id` AS `event_id`',
        '`doc_response_key` AS `doc_response_key`',
        '`source_dcp_forms_activity_id` AS `source_dcp_forms_activity_id`',
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
        '`__gold_json_score_code` AS `score_code`',
        '`score_name` AS `score_name`',
        '`value_number` AS `value_number`',
        '`value_text` AS `value_text`',
        '`unit_source_value` AS `unit_source_value`',
        '`unit_concept_id` AS `unit_concept_id`',
        '`component_count` AS `component_count`',
        '`interpretation_code` AS `interpretation_code`',
        '`interpretation_display` AS `interpretation_display`',
        '`result_status_code` AS `result_status_code`',
        '`result_status_display` AS `result_status_display`',
        '`performer_practitioner_id` AS `performer_practitioner_id`',
        '`source_form_key` AS `source_form_key`',
        '`promotion_rule_id` AS `promotion_rule_id`',
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
    return _s3_flatten_gold_public(_with_comments(df, CLINICAL_CLINICAL_SCORE_COLUMN_COMMENTS), "clinical_clinical_score")

# COMMAND ----------

# ==== journey_clinical.condition ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_CONDITION_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_patient_event_key` AS `source_patient_event_key`',
    '`source_object` AS `source_object`',
    '`diagnosis_id` AS `diagnosis_id`',
    '`problem_id` AS `problem_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    "CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS DATE) = DATE'1900-01-01' OR CAST(`event_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`event_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `event_datetime` END END AS `event_datetime`",
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    '`condition_code` AS `condition_code`',
    "CASE WHEN UPPER(TRIM(CAST(`category_code` AS STRING))) = '0' THEN NULL ELSE `category_code` END AS `category_code`",
    '`category_display` AS `category_display`',
    '`clinical_status_code` AS `clinical_status_code`',
    '`clinical_status_display` AS `clinical_status_display`',
    "CASE WHEN UPPER(TRIM(CAST(`verification_status_code` AS STRING))) = '0' THEN NULL ELSE `verification_status_code` END AS `verification_status_code`",
    '`verification_status_display` AS `verification_status_display`',
    "CASE WHEN CAST(`onset_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`onset_datetime` AS DATE) = DATE'1900-01-01' OR CAST(`onset_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`onset_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `onset_datetime` END END AS `onset_datetime`",
    '`abatement_datetime` AS `abatement_datetime`',
    '`body_site_code` AS `body_site_code`',
    '`body_site_display` AS `body_site_display`',
    "CASE WHEN UPPER(TRIM(CAST(`severity_code` AS STRING))) = '0' THEN NULL ELSE `severity_code` END AS `severity_code`",
    '`severity_display` AS `severity_display`',
    "CASE WHEN UPPER(TRIM(CAST(`laterality_code` AS STRING))) = '0' THEN NULL ELSE `laterality_code` END AS `laterality_code`",
    '`laterality_display` AS `laterality_display`',
    '`asserted_datetime` AS `asserted_datetime`',
    '`asserter_practitioner_id` AS `asserter_practitioner_id`',
    '`recorder_practitioner_id` AS `recorder_practitioner_id`',
    '`revision_history_count` AS `revision_history_count`',
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

# contract v2: QC/batch inputs come from internal _clinical_condition_metadata; source history stays on the main research table.
CLINICAL_CONDITION_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 49,615,752 rows of
    # 49,615,774 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_CONDITION_ADVISORY_RULES = {
    # Kept as a regression guard on the accepted set ('resolved', 'provisional',
    # 'unresolved'): the research-surface filter already removes every row that fails it, so
    # this expectation should read zero forever and is worth watching for the day it does
    # not. Measured at 20 of 49,615,774 rows (4.03e-05%) when profiled on 2026-08-24.
    "gold.clinical.condition.identity_status.accepted_values":
        "NOT COALESCE((`identity_status` IS NOT NULL AND `identity_status` NOT IN ('resolved', 'provisional', 'unresolved')), FALSE)",

    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 22 of 49,615,774 at the profile.
    "gold.clinical.condition.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 56,771 of 49,615,774 rows (0.114%) when profiled on 2026-08-24.
    "gold.clinical.condition.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 358,178 of 49,615,774 rows (0.722%) when profiled on 2026-08-24.
    "gold.clinical.condition.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_CONDITION_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "source_patient_event_key": "Stable patient-event identity retained from the originating Silver row before source splitting.",
    "source_object": "Source arm: diagnosis, problem, or maternity.",
    "diagnosis_id": "Millennium DIAGNOSIS_ID; primary key for the diagnosis arm.",
    "problem_id": "Millennium PROBLEM_ID; primary key for the problem arm.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Primary assertion timestamp.",
    "event_end_datetime": "Source effective end where supplied.",
    "source_coding_system": "Verbatim source coding system.",
    "source_code": "Source condition code",
    "source_display": "Verbatim source condition display.",
    "condition_code": "Source and mapped condition codings.",
    "category_code": "Source condition category code.",
    "category_display": "Source condition category display.",
    "clinical_status_code": "Source clinical-status code.",
    "clinical_status_display": "Source clinical-status display.",
    "verification_status_code": "Source verification-status code.",
    "verification_status_display": "Source verification-status display.",
    "onset_datetime": "Source onset timestamp.",
    "abatement_datetime": "Source abatement or effective-end timestamp.",
    "body_site_code": "Body-site code where supplied.",
    "body_site_display": "Body-site display where supplied.",
    "severity_code": "Source severity code.",
    "severity_display": "Source severity display.",
    "laterality_code": "Source laterality code.",
    "laterality_display": "Source laterality display.",
    "asserted_datetime": "Source assertion timestamp.",
    "asserter_practitioner_id": "Millennium personnel PERSON_ID as BIGINT when available.",
    "recorder_practitioner_id": "Millennium personnel PERSON_ID as BIGINT when available.",
    "revision_history_count": "Number of retained problem revision rows.",
    "confidentiality_code": "Source confidentiality code.",
    "vip_ind": "Source VIP indicator.",
    "withheld_identity_ind": "Identity-withholding status is not populated by the current source projections; null does not mean identity was not withheld.",
    "source_feed": "Registered source feed owning the assertion.",
    "record_status": "Diagnosis and problem arms are superseded when their END_EFFECTIVE_DT_TM is before 2100-01-01, otherwise active; a null end defaults to not-ended. The maternity-diagnosis arm is always active. No arm emits retracted here, and clinical status, source-presence flags and problem-history revision flags do not determine this label.",
    "record_status_effective_from": "ACTIVE_STATUS_DT_TM from map_diagnosis or map_problem on the corresponding Millennium arm; DIAGDATE_CLEAN from map_maternity_diagnosis on the maternity arm. Values are carried unchanged without a fallback. The maternity value is a diagnosis-date proxy rather than an independently observed row-status change.",
    "record_status_effective_to": "END_EFFECTIVE_DT_TM from the diagnosis or problem row only when it is before 2100-01-01; otherwise null. Always null on the maternity-diagnosis arm. No problem-history end, pregnancy end or ingestion timestamp is substituted.",
    "source_update_timestamp": "Source application update datetime.",
    "loaded_at": "Diagnosis and maternity arms carry their own ADC_UPDT. The problem arm takes greatest(map_problem.ADC_UPDT, maximum map_problem_history.ADC_UPDT for that PROBLEM_ID), across all retained revisions rather than only the latest. Problem-history UPDT_DT_TM is not added to the parent's source_update_timestamp, which remains its own UPDT_DT_TM; diagnosis uses its own UPDT_DT_TM and maternity uses RECORD_UPDATED_DT. The maternity identity join adds no pregnancy-spine clock, and the union adds no cross-arm timestamp maximum.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_condition"),
    comment="Internal quality-controlled twin of clinical_condition: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_CONDITION_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CONDITION_ADVISORY_RULES)
def _gold_qc_clinical_condition():
    """Quality-controlled twin of journey_clinical.condition."""
    df = _qc("clinical_condition", CLINICAL_CONDITION_SELECT)
    return _with_comments(df, CLINICAL_CONDITION_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.condition"),
    comment=(
        "One diagnosis or problem assertion from a registered source feed, without cross-feed "
        "deduplication. Gold QC twin of the silver product: 13 columns are repaired or "
        "nulled, 1 rule(s) drop rows, 4 check(s) are advisory. Each rule states its reason in "
        "the pipeline notebook, and Lakeflow expectation metrics report what every rule "
        "matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_condition():
    """Contract-v2 public twin of clinical_condition; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_condition")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_patient_event_key` AS `source_patient_event_key`',
        '`source_object` AS `source_object`',
        '`diagnosis_id` AS `diagnosis_id`',
        '`problem_id` AS `problem_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`__gold_json_condition_code` AS `condition_code`',
        '`category_code` AS `category_code`',
        '`category_display` AS `category_display`',
        '`clinical_status_code` AS `clinical_status_code`',
        '`clinical_status_display` AS `clinical_status_display`',
        '`verification_status_code` AS `verification_status_code`',
        '`verification_status_display` AS `verification_status_display`',
        '`onset_datetime` AS `onset_datetime`',
        '`abatement_datetime` AS `abatement_datetime`',
        '`body_site_code` AS `body_site_code`',
        '`body_site_display` AS `body_site_display`',
        '`severity_code` AS `severity_code`',
        '`severity_display` AS `severity_display`',
        '`laterality_code` AS `laterality_code`',
        '`laterality_display` AS `laterality_display`',
        '`asserted_datetime` AS `asserted_datetime`',
        '`asserter_practitioner_id` AS `asserter_practitioner_id`',
        '`recorder_practitioner_id` AS `recorder_practitioner_id`',
        '`revision_history_count` AS `revision_history_count`',
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
    return _s3_flatten_gold_public(_with_comments(df, CLINICAL_CONDITION_COLUMN_COMMENTS), "clinical_condition")

# COMMAND ----------

# ==== journey_clinical.condition_stage ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_CONDITION_STAGE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`aria_pt_id` AS `aria_pt_id`',
    '`aria_dx_id` AS `aria_dx_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`stage_code` AS `stage_code`',
    '`stage_of_disease` AS `stage_of_disease`',
    '`stage_criteria` AS `stage_criteria`',
    '`dx_type` AS `dx_type`',
    '`dx_confirmed` AS `dx_confirmed`',
    '`dx_method` AS `dx_method`',
    '`history_ind` AS `history_ind`',
    '`current_entry_ind` AS `current_entry_ind`',
    '`cause_of_death_ind` AS `cause_of_death_ind`',
    '`onset_datetime` AS `onset_datetime`',
    '`resolution_datetime` AS `resolution_datetime`',
    '`clinical_description` AS `clinical_description`',
    "CASE WHEN TRIM(CAST(`dx_comment` AS STRING)) = '' THEN NULL ELSE `dx_comment` END AS `dx_comment`",
    '`person_link_status` AS `person_link_status`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_condition_stage_metadata; source history stays on the main research table.
CLINICAL_CONDITION_STAGE_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 130,441 rows of 133,086
    # that are current and attributable. Superseded versions and rows whose identity was
    # never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_CONDITION_STAGE_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 2,645 of 133,086 at the profile.
    "gold.clinical.condition_stage.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 41 of 133,086 rows (0.0308%) when profiled on 2026-08-24.
    "gold.clinical.condition_stage.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 26 of 133,086 rows (0.0195%) when profiled on 2026-08-24.
    "gold.clinical.condition_stage.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_CONDITION_STAGE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "aria_pt_id": "ARIA patient identifier; first component of the primary key.",
    "aria_dx_id": "ARIA diagnosis identifier; second component of the primary key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Sentinel-cleaned diagnosis onset timestamp.",
    "event_end_datetime": "Sentinel-cleaned resolution timestamp.",
    "source_coding_system": "ICD-10 coding system.",
    "source_code": "ICD diagnosis code with diagnosis-name fallback.",
    "source_display": "Diagnosis name or description.",
    "stage_code": "Source ICD and mapped OMOP diagnosis codings.",
    "stage_of_disease": "Source disease stage.",
    "stage_criteria": "Staging criteria description.",
    "dx_type": "Code indicating the type or category of the diagnosis entry.",
    "dx_confirmed": "Diagnosis confirmation state.",
    "dx_method": "Coded value recording the method by which the diagnosis was made.",
    "history_ind": "Indicator showing whether the entry represents a history of the condition rather than an active diagnosis.",
    "current_entry_ind": "True when the ARIA current-entry indicator `CUR_ENTRY_IND` is Y after uppercasing; null source values and other values become false.",
    "cause_of_death_ind": "True when the ARIA cause-of-death indicator `CS_OF_DTH_IND` is Y after uppercasing; null source values and other values become false.",
    "onset_datetime": "Source onset timestamp.",
    "resolution_datetime": "Source resolution timestamp.",
    "clinical_description": "Clinical diagnosis description.",
    "dx_comment": "Free-text comment recorded against the diagnosis entry.",
    "person_link_status": "Bronze person-link status.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Identity-withholding status is not populated by the current source projections; null does not mean identity was not withheld.",
    "record_status": "Superseded when coalesce(CUR_ENTRY_IND,'Y') differs from the exact case-sensitive value Y, without trimming, or SOURCE_PRESENT_IND is false (null defaults true); otherwise active. No retracted status is emitted. This differs from current_entry_ind, which uppercases CUR_ENTRY_IND and defaults a missing value to N.",
    "record_status_effective_from": "map_aria_diagnosis_staging.EVLV_TSTAMP_CLEAN carried unchanged as the history start. It is the source-supplied cleaned timestamp, not a fallback to diagnosis onset, resolution or ingestion time; a missing value remains null.",
    "record_status_effective_to": "map_aria_diagnosis_staging.ADC_UPDT only when the row is superseded; otherwise null. This is an ingestion-time proxy, not RESOLUTION_DATE_CLEAN or an independently observed status-transition timestamp; a missing ADC_UPDT remains null.",
    "source_update_timestamp": "Bronze pipeline processing timestamp carried from `PIPELINE_UPDT_DT_TM` in `4_prod.bronze.map_aria_diagnosis_staging`. It records when bronze processed the source row, not a native clinical-system update timestamp, clinical event time, or the current Silver refresh time.",
    "loaded_at": "map_aria_diagnosis_staging.ADC_UPDT carried unchanged through code filtering and quality/public projections. This is the contributing bronze load clock, distinct from PIPELINE_UPDT_DT_TM published as source_update_timestamp; it is not disease onset/resolution or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_condition_stage"),
    comment="Internal quality-controlled twin of clinical_condition_stage: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_CONDITION_STAGE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CONDITION_STAGE_ADVISORY_RULES)
def _gold_qc_clinical_condition_stage():
    """Quality-controlled twin of journey_clinical.condition_stage."""
    df = _qc("clinical_condition_stage", CLINICAL_CONDITION_STAGE_SELECT)
    return _with_comments(df, CLINICAL_CONDITION_STAGE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.condition_stage"),
    comment=(
        "One frozen ARIA diagnosis-and-staging assertion with ICD source coding and mapped "
        "OMOP diagnosis evidence. Gold QC twin of the silver product: 2 columns are repaired "
        "or nulled, 1 rule(s) drop rows, 3 check(s) are advisory. Each rule states its reason "
        "in the pipeline notebook, and Lakeflow expectation metrics report what every rule "
        "matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_condition_stage():
    """Contract-v2 public twin of clinical_condition_stage; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_condition_stage")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`aria_pt_id` AS `aria_pt_id`',
        '`aria_dx_id` AS `aria_dx_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`__gold_json_stage_code` AS `stage_code`',
        '`stage_of_disease` AS `stage_of_disease`',
        '`stage_criteria` AS `stage_criteria`',
        '`dx_type` AS `dx_type`',
        '`dx_confirmed` AS `dx_confirmed`',
        '`dx_method` AS `dx_method`',
        '`history_ind` AS `history_ind`',
        '`current_entry_ind` AS `current_entry_ind`',
        '`cause_of_death_ind` AS `cause_of_death_ind`',
        '`onset_datetime` AS `onset_datetime`',
        '`resolution_datetime` AS `resolution_datetime`',
        '`clinical_description` AS `clinical_description`',
        '`dx_comment` AS `dx_comment`',
        '`person_link_status` AS `person_link_status`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _s3_flatten_gold_public(_with_comments(df, CLINICAL_CONDITION_STAGE_COLUMN_COMMENTS), "clinical_condition_stage")

# COMMAND ----------

# ==== journey_clinical.device ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_DEVICE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`mc_device_id` AS `mc_device_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`device_code` AS `device_code`',
    '`device_role` AS `device_role`',
    '`model_name` AS `model_name`',
    "CASE WHEN UPPER(TRIM(CAST(`model_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `model_code` END AS `model_code`",
    '`manufacturer` AS `manufacturer`',
    '`manufacturer_parent` AS `manufacturer_parent`',
    '`serial_number` AS `serial_number`',
    '`implanted_datetime` AS `implanted_datetime`',
    '`implant_date_quality` AS `implant_date_quality`',
    '`explanted_ind` AS `explanted_ind`',
    '`lead_chamber` AS `lead_chamber`',
    '`lead_location` AS `lead_location`',
    '`pocket_site` AS `pocket_site`',
    '`status_display` AS `status_display`',
    '`device_mapping_status` AS `device_mapping_status`',
    "CASE WHEN TRIM(CAST(`comment_text` AS STRING)) = '' THEN NULL ELSE `comment_text` END AS `comment_text`",
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_device_metadata; source history stays on the main research table.
CLINICAL_DEVICE_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 37,089 rows of 37,089 that
    # are current and attributable; identity_status = 'resolved' keeps the 37,063 rows of
    # 37,089 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_DEVICE_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 26 of 37,089 at the profile.
    "gold.clinical.device.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 37,089 at the profile.
    "gold.clinical.device.record_status.default_view_active": "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 364 of 37,089 rows (0.981%) when profiled on 2026-08-24.
    "gold.clinical.device.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 79 of 37,089 rows (0.213%) when profiled on 2026-08-24.
    "gold.clinical.device.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_DEVICE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "mc_device_id": "MediConnect MC_DEVICE_ID; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Sentinel-cleaned implantation timestamp.",
    "event_end_datetime": "Device lifecycle end timestamp when available.",
    "source_coding_system": "MediConnect device-type coding system.",
    "source_code": "Device type with type/model display fallback for unknown type zero.",
    "source_display": "Device type or model display.",
    "device_code": "Source and mapped device codings.",
    "device_role": "GENERATOR | LEAD | MONITOR | OTHER",
    "model_name": "Device model name.",
    "model_code": "Device model code.",
    "manufacturer": "Raw manufacturer name for the device or lead exactly as recorded in the source registry.",
    "manufacturer_parent": "Parent or successor corporate group for the recorded manufacturer, derived from the raw MANUFACTURER value and null where no parent applies.",
    "serial_number": "Device serial number.",
    "implanted_datetime": "Source implantation timestamp.",
    "implant_date_quality": "Implant-date quality class.",
    "explanted_ind": "Inferred explant indicator; not independently confirmed.",
    "lead_chamber": "Cardiac chamber for a lead, parsed from the structured source LOCATION1 text and null for non-lead records.",
    "lead_location": "Anatomical implant location of a lead, parsed from the structured source LOCATION1 text and null for non-lead records.",
    "pocket_site": "Device pocket site.",
    "status_display": "Source device status display.",
    "device_mapping_status": "Device terminology mapping status.",
    "comment_text": "Source device comment.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Identity-withholding status is not populated by the current source projections; null does not mean identity was not withheld.",
    "record_status": "Superseded when mediconnect_device.SOURCE_PRESENT_IND is false, otherwise active; missing presence defaults true. No retracted status is emitted. STATUS, EXPLANTED_IND and mapping status do not set this derived label, and registry rows in the shared internal stage are filtered out before device publication.",
    "record_status_effective_from": "Validated implant date derived from IMPLANTED_DATE, excluding sentinel, future and missing values flagged by IMPLANTED_DATE_QUALITY.",
    "record_status_effective_to": "For a source-absent mediconnect_device row, first non-null SOURCE_ABSENT_DETECTED_TS then ADC_UPDT; otherwise null. This is not an explant timestamp. Registry-family absence clocks and device-type mapping clocks are not used.",
    "source_update_timestamp": "Always null as a TIMESTAMP in the current MediConnect device projection. Neither ADC_UPDT nor registry DATE_LAST_CHANGED is substituted. Registry rows are filtered out of the shared internal stage before device publication; device-type mappings select no clock.",
    "loaded_at": "mediconnect_device.ADC_UPDT carried unchanged. The device-type lookup contributes mapping fields but no clocks, and registry rows in the shared internal stage are excluded before device QC/public projections. This is not a maximum with registry or lookup clocks, implant time or the current Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_device"),
    comment="Internal quality-controlled twin of clinical_device: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_DEVICE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_DEVICE_ADVISORY_RULES)
def _gold_qc_clinical_device():
    """Quality-controlled twin of journey_clinical.device."""
    df = _qc("clinical_device", CLINICAL_DEVICE_SELECT)
    return _with_comments(df, CLINICAL_DEVICE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.device"),
    comment=(
        "One frozen MediConnect implanted-device registry row with source-type and mapped "
        "SNOMED device coding. Gold QC twin of the silver product: 2 columns are repaired or "
        "nulled, 1 rule(s) drop rows, 4 check(s) are advisory. Each rule states its reason in "
        "the pipeline notebook, and Lakeflow expectation metrics report what every rule "
        "matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_device():
    """Contract-v2 public twin of clinical_device; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_device")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`mc_device_id` AS `mc_device_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`__gold_json_device_code` AS `device_code`',
        '`device_role` AS `device_role`',
        '`model_name` AS `model_name`',
        '`model_code` AS `model_code`',
        '`manufacturer` AS `manufacturer`',
        '`manufacturer_parent` AS `manufacturer_parent`',
        '`serial_number` AS `serial_number`',
        '`implanted_datetime` AS `implanted_datetime`',
        '`implant_date_quality` AS `implant_date_quality`',
        '`explanted_ind` AS `explanted_ind`',
        '`lead_chamber` AS `lead_chamber`',
        '`lead_location` AS `lead_location`',
        '`pocket_site` AS `pocket_site`',
        '`status_display` AS `status_display`',
        '`device_mapping_status` AS `device_mapping_status`',
        '`comment_text` AS `comment_text`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _s3_flatten_gold_public(_with_comments(df, CLINICAL_DEVICE_COLUMN_COMMENTS), "clinical_device")

# COMMAND ----------

# ==== journey_clinical.endoscopy_finding ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_ENDOSCOPY_FINDING_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`endobase_exam_term_id` AS `endobase_exam_term_id`',
    '`endobase_exam_id` AS `endobase_exam_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`finding_code` AS `finding_code`',
    '`endoscopy_exam_event_key` AS `endoscopy_exam_event_key`',
    '`section_id` AS `section_id`',
    '`subsection_id` AS `subsection_id`',
    '`parent_term_id` AS `parent_term_id`',
    '`display_order` AS `display_order`',
    '`confirmed_ind` AS `confirmed_ind`',
    '`text_changed_ind` AS `text_changed_ind`',
    '`free_text_ind` AS `free_text_ind`',
    '`term_mapping_status` AS `term_mapping_status`',
    '`person_link_status` AS `person_link_status`',
    '`authored_datetime` AS `authored_datetime`',
    '`event_time_source` AS `event_time_source`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_endoscopy_finding_metadata; source history stays on the main research table.
CLINICAL_ENDOSCOPY_FINDING_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 9,220,706 rows of 9,220,706
    # that are current and attributable. Superseded versions and rows whose identity was
    # never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(record_status = 'active')",
}

CLINICAL_ENDOSCOPY_FINDING_ADVISORY_RULES = {
    # Warned rather than filtered on identity_status = 'resolved': Endobase has no sound
    # person linkage; the mandatory predicate would empty the 9,220,706-row product.
    "gold.clinical.endoscopy_finding.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 9,220,706 at the profile.
    "gold.clinical.endoscopy_finding.record_status.default_view_active":
        "record_status = 'active'",
}

CLINICAL_ENDOSCOPY_FINDING_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "endobase_exam_term_id": "Endobase ENDOBASE_EXAM_TERM_ID; primary key of this table.",
    "endobase_exam_id": "Endobase ENDOBASE_EXAM_ID; native parent examination identifier.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Parent exam time",
    "event_end_datetime": "Finding end timestamp when supplied.",
    "source_coding_system": "DGVS term coding system.",
    "source_code": "DGVS term identifier with term-text fallback.",
    "source_display": "Endobase term text.",
    "finding_code": "Source DGVS and mapped SNOMED/OMOP codings.",
    "endoscopy_exam_event_key": "Deterministic SHA-256 key for the parent Endobase examination.",
    "section_id": "Report section identifier.",
    "subsection_id": "Report subsection identifier.",
    "parent_term_id": "Parent term identifier.",
    "display_order": "Source display order.",
    "confirmed_ind": "Finding confirmation indicator.",
    "text_changed_ind": "Source text-changed indicator.",
    "free_text_ind": "Free-text route indicator.",
    "term_mapping_status": "C4 term-mapping status.",
    "person_link_status": "Bronze person-link status.",
    "authored_datetime": "Creation timestamp of the EndoBase examination-term record, carried from `CREATED_TS`; not a report-level authoring timestamp.",
    "event_time_source": "Whether event time came from the exam or authored fallback.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Identity-withholding status is not populated by the current source projections; null does not mean identity was not withheld.",
    "record_status": "Superseded when map_endobase_exam_term.SOURCE_PRESENT_IND is false, otherwise active; missing presence defaults true. No retracted status is emitted. The non-free-text and usable-code filters control eligibility separately, while parent-exam fields do not determine this label.",
    "record_status_effective_from": "map_endobase_exam_term.CREATED_TS carried directly as the record-status start. This differs from clinical event time, which can use the joined parent exam's performed/exam/start timestamps before falling back to term creation time.",
    "record_status_effective_to": "map_endobase_exam_term.ADC_UPDT when its source-presence flag is false; otherwise null. The parent-exam load clock and clinical event time are not used as status ends, even when they contribute to other fields.",
    "source_update_timestamp": "map_endobase_exam_term.ADC_UPDT carried unchanged. The parent-exam ADC_UPDT contributes to loaded_at only, not this field; no independent native application-update clock or current Silver refresh timestamp is substituted.",
    "loaded_at": "Greatest of map_endobase_exam_term.ADC_UPDT and the left-joined map_endobase_exam.ADC_UPDT, with missing parent time first replaced by term time. Parent lookup uses ENDOBASE_EXAM_ID. A non-null parent time survives a null term time; both null gives null. This can differ from source_update_timestamp and the status end, which use only the term clock; it is not clinical event time or the current Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_endoscopy_finding"),
    comment="Internal quality-controlled twin of clinical_endoscopy_finding: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_ENDOSCOPY_FINDING_MANDATORY_RULES)
@dp.expect_all(CLINICAL_ENDOSCOPY_FINDING_ADVISORY_RULES)
def _gold_qc_clinical_endoscopy_finding():
    """Quality-controlled twin of journey_clinical.endoscopy_finding."""
    df = _qc("clinical_endoscopy_finding", CLINICAL_ENDOSCOPY_FINDING_SELECT)
    return _with_comments(df, CLINICAL_ENDOSCOPY_FINDING_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.endoscopy_finding"),
    comment=(
        "One coded Endobase exam term with parent-exam clinical time and C4 mapping columns "
        "wired for later application. Gold QC twin of the silver product: 0 columns are "
        "repaired or nulled, 1 rule(s) drop rows, 2 check(s) are advisory. Each rule states "
        "its reason in the pipeline notebook, and Lakeflow expectation metrics report what "
        "every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_endoscopy_finding():
    """Contract-v2 public twin of clinical_endoscopy_finding; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_endoscopy_finding")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`endobase_exam_term_id` AS `endobase_exam_term_id`',
        '`endobase_exam_id` AS `endobase_exam_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`__gold_json_finding_code` AS `finding_code`',
        '`endoscopy_exam_event_key` AS `endoscopy_exam_event_key`',
        '`section_id` AS `section_id`',
        '`subsection_id` AS `subsection_id`',
        '`parent_term_id` AS `parent_term_id`',
        '`display_order` AS `display_order`',
        '`confirmed_ind` AS `confirmed_ind`',
        '`text_changed_ind` AS `text_changed_ind`',
        '`free_text_ind` AS `free_text_ind`',
        '`term_mapping_status` AS `term_mapping_status`',
        '`person_link_status` AS `person_link_status`',
        '`authored_datetime` AS `authored_datetime`',
        '`event_time_source` AS `event_time_source`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _s3_flatten_gold_public(_with_comments(df, CLINICAL_ENDOSCOPY_FINDING_COLUMN_COMMENTS), "clinical_endoscopy_finding")

# COMMAND ----------

# ==== journey_clinical.family_history ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_FAMILY_HISTORY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_patient_event_key` AS `source_patient_event_key`',
    '`fhx_activity_id` AS `fhx_activity_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`condition_code` AS `condition_code`',
    '`relationship_code` AS `relationship_code`',
    '`relationship_display` AS `relationship_display`',
    '`relationship_type_code` AS `relationship_type_code`',
    '`relationship_type_display` AS `relationship_type_display`',
    '`onset_age` AS `onset_age`',
    '`onset_age_unit` AS `onset_age_unit`',
    "CASE WHEN UPPER(TRIM(CAST(`severity_code` AS STRING))) = '0' THEN NULL ELSE `severity_code` END AS `severity_code`",
    '`severity_display` AS `severity_display`',
    '`source_lifecycle_status` AS `source_lifecycle_status`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`asserter_practitioner_id` AS `asserter_practitioner_id`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_family_history_metadata; source history stays on the main research table.
CLINICAL_FAMILY_HISTORY_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 10,640 rows of 10,640
    # that are current and attributable. Superseded versions and rows whose identity was
    # never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_FAMILY_HISTORY_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 10,640 at the profile.
    "gold.clinical.family_history.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 21 of 10,640 rows (0.197%) when profiled on 2026-08-24.
    "gold.clinical.family_history.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",
}

CLINICAL_FAMILY_HISTORY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "source_patient_event_key": "Stable patient-event identity retained from the originating Silver row before source splitting.",
    "fhx_activity_id": "Millennium FHX_ACTIVITY_ID; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Millennium ORIGINATING_ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "event_datetime": "Source family-history effective start.",
    "event_end_datetime": "Source effective end when the assertion is no longer current.",
    "source_coding_system": "Verbatim source code system.",
    "source_code": "Source condition code",
    "source_display": "Verbatim source condition display.",
    "condition_code": "Source and mapped condition codings as a one-level CodeableConcept VARIANT.",
    "relationship_code": "Source family relationship code.",
    "relationship_display": "Source family relationship display.",
    "relationship_type_code": "Source relationship record type code.",
    "relationship_type_display": "Source relationship record type display.",
    "onset_age": "Source recorded age at onset.",
    "onset_age_unit": "Unit for recorded onset age.",
    "severity_code": "Source severity code.",
    "severity_display": "Source severity display.",
    "source_lifecycle_status": "Verbatim source life-cycle status.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld-identity indicator when supplied.",
    "asserter_practitioner_id": "Millennium personnel PERSON_ID when supplied.",
    "record_status": "Superseded when map_family_history.END_EFFECTIVE_DT_TM is non-null and earlier than 2100-01-01; otherwise active. No retracted status is emitted. The separately retained LIFE_CYCLE_STATUS does not determine this derived label.",
    "record_status_effective_from": "Source-supplied timestamp from which the normalized record status applies for each family history record. It is derived from bronze field `FHX_ACTIVE_STATUS_DT_TM` in `4_prod.bronze.map_family_history`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "record_status_effective_to": "map_family_history.END_EFFECTIVE_DT_TM only when non-null and earlier than 2100-01-01; otherwise null. This is the same predicate that sets superseded status. No 1950 lower clamp, event-end substitution or ingestion-time fallback is applied.",
    "source_update_timestamp": "map_family_history.ADC_UPDT carried unchanged, identical to loaded_at. No independent native application-update clock is projected for this field, and the current Silver refresh time is not substituted.",
    "loaded_at": "map_family_history.ADC_UPDT carried unchanged through usable-code filtering and QC/public projections, identical to source_update_timestamp. QC identity/encounter joins add no load clocks; neither clinical event time nor the current Silver refresh time replaces it.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_family_history"),
    comment="Internal quality-controlled twin of clinical_family_history: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_FAMILY_HISTORY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_FAMILY_HISTORY_ADVISORY_RULES)
def _gold_qc_clinical_family_history():
    """Quality-controlled twin of journey_clinical.family_history."""
    df = _qc("clinical_family_history", CLINICAL_FAMILY_HISTORY_SELECT)
    return _with_comments(df, CLINICAL_FAMILY_HISTORY_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.family_history"),
    comment=(
        "One family-history assertion with the standard event block and retained source "
        "lifecycle. Gold QC twin of the silver product: 2 columns are repaired or nulled, 1 "
        "rule(s) drop rows, 2 check(s) are advisory. Each rule states its reason in the "
        "pipeline notebook, and Lakeflow expectation metrics report what every rule matched "
        "on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_family_history():
    """Contract-v2 public twin of clinical_family_history; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_family_history")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_patient_event_key` AS `source_patient_event_key`',
        '`fhx_activity_id` AS `fhx_activity_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`__gold_json_condition_code` AS `condition_code`',
        '`relationship_code` AS `relationship_code`',
        '`relationship_display` AS `relationship_display`',
        '`relationship_type_code` AS `relationship_type_code`',
        '`relationship_type_display` AS `relationship_type_display`',
        '`onset_age` AS `onset_age`',
        '`onset_age_unit` AS `onset_age_unit`',
        '`severity_code` AS `severity_code`',
        '`severity_display` AS `severity_display`',
        '`source_lifecycle_status` AS `source_lifecycle_status`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`asserter_practitioner_id` AS `asserter_practitioner_id`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _s3_flatten_gold_public(_with_comments(df, CLINICAL_FAMILY_HISTORY_COLUMN_COMMENTS), "clinical_family_history")

# COMMAND ----------

# ==== journey_clinical.genomic_result ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_GENOMIC_RESULT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`genetic_result_id` AS `genetic_result_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`genomic_test_key` AS `genomic_test_key`',
    '`report_version_key` AS `report_version_key`',
    '`hgnc_id` AS `hgnc_id`',
    '`reported_gene_symbol` AS `reported_gene_symbol`',
    '`normalized_gene_symbol` AS `normalized_gene_symbol`',
    '`partner_hgnc_id` AS `partner_hgnc_id`',
    '`partner_gene_symbol` AS `partner_gene_symbol`',
    '`alteration_type` AS `alteration_type`',
    '`detection_status` AS `detection_status`',
    '`hgvs_c_raw` AS `hgvs_c_raw`',
    '`hgvs_c_parsed` AS `hgvs_c_parsed`',
    '`hgvs_p_raw` AS `hgvs_p_raw`',
    '`hgvs_p_parsed` AS `hgvs_p_parsed`',
    '`transcript` AS `transcript`',
    '`hgvs_validation_status` AS `hgvs_validation_status`',
    '`genome_build` AS `genome_build`',
    '`chromosome` AS `chromosome`',
    '`position_start` AS `position_start`',
    '`position_end` AS `position_end`',
    '`vaf_raw` AS `vaf_raw`',
    '`vaf` AS `vaf`',
    '`zygosity` AS `zygosity`',
    '`reported_classification` AS `reported_classification`',
    '`reported_tier` AS `reported_tier`',
    '`copy_number` AS `copy_number`',
    '`ratio_raw` AS `ratio_raw`',
    '`iscn_raw` AS `iscn_raw`',
    '`clinvar_concept_id` AS `clinvar_concept_id`',
    '`omop_genomic_concept_id` AS `omop_genomic_concept_id`',
    '`snomed_code` AS `snomed_code`',
    '`evidence_text` AS `evidence_text`',
    '`evidence_start` AS `evidence_start`',
    '`evidence_end` AS `evidence_end`',
    '`parser_profile_id` AS `parser_profile_id`',
    '`parser_version` AS `parser_version`',
    '`review_status` AS `review_status`',
    '`lifecycle_status` AS `lifecycle_status`',
    '`is_current` AS `is_current`',
    '`research_qi_only` AS `research_qi_only`',
    '`specimen_key` AS `specimen_key`',
    '`accession_identifier` AS `accession_identifier`',
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

# contract v2: QC/batch inputs come from internal _clinical_genomic_result_metadata; source history stays on the main research table.
CLINICAL_GENOMIC_RESULT_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 0 rows of 0 that are current
    # and attributable; identity_status = 'resolved' keeps the 0 rows of 0 that are current
    # and attributable. Superseded versions and rows whose identity was never resolved are
    # not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_GENOMIC_RESULT_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 0 at the profile.
    "gold.clinical.genomic_result.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 0 at the profile.
    "gold.clinical.genomic_result.record_status.default_view_active":
        "record_status = 'active'",
}

CLINICAL_GENOMIC_RESULT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "genetic_result_id": "Pathology genetic_result_id; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace selected for `subject_key`: Millennium person identifier first, then MRN, then NHS number; nosubject denotes the per-source-record fallback when none is present.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT through the shared pathology identity projection.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Report issue time with accession fallback.",
    "event_end_datetime": "Event end time.",
    "source_coding_system": "Gene-symbol or alteration coding namespace.",
    "source_code": "SNV, indel, fusion, CNV, rearrangement, repeat, karyotype, or other.",
    "source_display": "SNV, indel, fusion, CNV, rearrangement, repeat, karyotype, or other.",
    "genomic_test_key": "Deterministic SHA-256 key of the parent genomic test; equals clinical_genomic_test.patient_event_key.",
    "report_version_key": "Deterministic report-version key.",
    "hgnc_id": "Primary HGNC identifier.",
    "reported_gene_symbol": "Gene symbol as reported.",
    "normalized_gene_symbol": "Approved HGNC symbol.",
    "partner_hgnc_id": "Partner HGNC identifier.",
    "partner_gene_symbol": "Fusion/rearrangement partner symbol.",
    "alteration_type": "Reported alteration type.",
    "detection_status": "Finding detection status.",
    "hgvs_c_raw": "Raw coding HGVS.",
    "hgvs_c_parsed": "Parsed coding HGVS.",
    "hgvs_p_raw": "Raw protein HGVS.",
    "hgvs_p_parsed": "Parsed protein HGVS.",
    "transcript": "Reported transcript accession.",
    "hgvs_validation_status": "HGVS validation state.",
    "genome_build": "Explicit genome build.",
    "chromosome": "Explicitly reported chromosome.",
    "position_start": "Genomic start coordinate.",
    "position_end": "Genomic end coordinate.",
    "vaf_raw": "Raw variant allele frequency.",
    "vaf": "Parsed variant allele fraction.",
    "zygosity": "Value describing zygosity for the genomic result record. It is carried from bronze field `zygosity` in `4_prod.bronze.map_pathology_genetic_result`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "reported_classification": "Classification or tier reported by the laboratory.",
    "reported_tier": "Reported clinical/actionability tier.",
    "copy_number": "Reported copy number.",
    "ratio_raw": "Raw molecular ratio.",
    "iscn_raw": "Raw ISCN notation.",
    "clinvar_concept_id": "ClinVar concept identifier.",
    "omop_genomic_concept_id": "OMOP Genomic concept identifier.",
    "snomed_code": "SNOMED finding code.",
    "evidence_text": "Minimal narrative evidence span.",
    "evidence_start": "Evidence start offset.",
    "evidence_end": "Evidence end offset.",
    "parser_profile_id": "Deterministic parser profile identifier.",
    "parser_version": "Parser implementation version.",
    "review_status": "Source finding review status.",
    "lifecycle_status": "Inherited report lifecycle.",
    "is_current": "Whether the source report version is current.",
    "research_qi_only": "Research/QI release flag.",
    "specimen_key": "Deterministic specimen key.",
    "accession_identifier": "Alias-resolved accession identifier.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each genomic result record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each genomic result record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Originating data feed responsible for the record for each genomic result record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_pathology_genetic_result.lifecycle_status is exactly cancelled or entered_in_error; otherwise superseded when is_current is false (null defaults true), otherwise active. Comparisons are case-sensitive without trimming; a null lifecycle_status does not select retraction. Retraction takes priority over supersession; clinical findings and joined identity/link fields are separate.",
    "record_status_effective_from": "Always null as a TIMESTAMP in the current genomic-result projection. No source-status start is supplied here; the clinical event's report/sample/request date fallbacks and ADC_UPDT are not used for this field.",
    "record_status_effective_to": "Always null as a TIMESTAMP in the current genomic-result projection, including when its status or source-current flag changes. No report, clinical-event or ingestion timestamp is substituted as a status end.",
    "source_update_timestamp": "map_pathology_genetic_result.ADC_UPDT carried unchanged from the contributing row, identical to loaded_at. No independent native application-update clock is projected here, and Silver refresh time is not substituted.",
    "loaded_at": "map_pathology_genetic_result.ADC_UPDT carried unchanged through its joins, usable-code filtering and source/public projections. Alias resolution and accession identity add no load clocks; neither joined report/parent timestamps nor the separately aggregated accession evidence clock replace this value. It is identical to source_update_timestamp, not the clinical event time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_genomic_result"),
    comment="Internal quality-controlled twin of clinical_genomic_result: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_GENOMIC_RESULT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_GENOMIC_RESULT_ADVISORY_RULES)
def _gold_qc_clinical_genomic_result():
    """Quality-controlled twin of journey_clinical.genomic_result."""
    df = _qc("clinical_genomic_result", CLINICAL_GENOMIC_RESULT_SELECT)
    return _with_comments(df, CLINICAL_GENOMIC_RESULT_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.genomic_result"),
    comment=(
        "One reportable molecular or cytogenetic finding. Schema-first and empty_by_design on "
        "2026-08-18 because no detected assays exist upstream. Evidence text quotes report "
        "narrative and is ig_risk 4, ig_severity 2 with serve-time stripping only. Gold QC "
        "twin of the silver product: 0 columns are repaired or nulled, 1 rule(s) drop rows, 2 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_genomic_result():
    """Contract-v2 public twin of clinical_genomic_result; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_genomic_result")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`genetic_result_id` AS `genetic_result_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`genomic_test_key` AS `genomic_test_key`',
        '`report_version_key` AS `report_version_key`',
        '`hgnc_id` AS `hgnc_id`',
        '`reported_gene_symbol` AS `reported_gene_symbol`',
        '`normalized_gene_symbol` AS `normalized_gene_symbol`',
        '`partner_hgnc_id` AS `partner_hgnc_id`',
        '`partner_gene_symbol` AS `partner_gene_symbol`',
        '`alteration_type` AS `alteration_type`',
        '`detection_status` AS `detection_status`',
        '`hgvs_c_raw` AS `hgvs_c_raw`',
        '`hgvs_c_parsed` AS `hgvs_c_parsed`',
        '`hgvs_p_raw` AS `hgvs_p_raw`',
        '`hgvs_p_parsed` AS `hgvs_p_parsed`',
        '`transcript` AS `transcript`',
        '`hgvs_validation_status` AS `hgvs_validation_status`',
        '`genome_build` AS `genome_build`',
        '`chromosome` AS `chromosome`',
        '`position_start` AS `position_start`',
        '`position_end` AS `position_end`',
        '`vaf_raw` AS `vaf_raw`',
        '`vaf` AS `vaf`',
        '`zygosity` AS `zygosity`',
        '`reported_classification` AS `reported_classification`',
        '`reported_tier` AS `reported_tier`',
        '`copy_number` AS `copy_number`',
        '`ratio_raw` AS `ratio_raw`',
        '`iscn_raw` AS `iscn_raw`',
        '`clinvar_concept_id` AS `clinvar_concept_id`',
        '`omop_genomic_concept_id` AS `omop_genomic_concept_id`',
        '`snomed_code` AS `snomed_code`',
        '`evidence_text` AS `evidence_text`',
        '`evidence_start` AS `evidence_start`',
        '`evidence_end` AS `evidence_end`',
        '`parser_profile_id` AS `parser_profile_id`',
        '`parser_version` AS `parser_version`',
        '`review_status` AS `review_status`',
        '`lifecycle_status` AS `lifecycle_status`',
        '`is_current` AS `is_current`',
        '`research_qi_only` AS `research_qi_only`',
        '`specimen_key` AS `specimen_key`',
        '`accession_identifier` AS `accession_identifier`',
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
    return _with_comments(df, CLINICAL_GENOMIC_RESULT_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.genomic_test ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_GENOMIC_TEST_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`genetic_test_id` AS `genetic_test_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`assay_code` AS `assay_code`',
    '`assay_name` AS `assay_name`',
    '`method` AS `method`',
    '`analysis_context` AS `analysis_context`',
    '`overall_result_status` AS `overall_result_status`',
    '`panel_code` AS `panel_code`',
    '`panel_version` AS `panel_version`',
    '`panel_version_inferred` AS `panel_version_inferred`',
    '`parser_profile_id` AS `parser_profile_id`',
    '`report_version_key` AS `report_version_key`',
    '`pathology_report_key` AS `pathology_report_key`',
    '`specimen_key` AS `specimen_key`',
    '`accession_identifier` AS `accession_identifier`',
    '`test_snomed_code` AS `test_snomed_code`',
    '`test_loinc_code` AS `test_loinc_code`',
    '`test_omop_concept_id` AS `test_omop_concept_id`',
    '`is_current` AS `is_current`',
    '`research_qi_only` AS `research_qi_only`',
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

# contract v2: QC/batch inputs come from internal _clinical_genomic_test_metadata; source history stays on the main research table.
CLINICAL_GENOMIC_TEST_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 40,540 rows of 51,845
    # that are current and attributable. Superseded versions and rows whose identity was
    # never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_GENOMIC_TEST_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 11,305 of 51,845 at the profile.
    "gold.clinical.genomic_test.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 282 of 51,845 rows (0.544%) when profiled on 2026-08-24.
    "gold.clinical.genomic_test.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",
}

CLINICAL_GENOMIC_TEST_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "genetic_test_id": "Pathology genetic_test_id; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace selected for `subject_key`: Millennium person identifier first, then MRN, then NHS number; nosubject denotes the per-source-record fallback when none is present.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Report issue time with accession fallback.",
    "event_end_datetime": "Event end time.",
    "source_coding_system": "Assay coding namespace.",
    "source_code": "Source assay code.",
    "source_display": "Source assay display.",
    "assay_code": "Source assay or report code.",
    "assay_name": "Resolved assay name.",
    "method": "Reported assay method.",
    "analysis_context": "Somatic or germline analysis context.",
    "overall_result_status": "Source assay-level result status.",
    "panel_code": "Governed panel code.",
    "panel_version": "Explicit or inferred panel version.",
    "panel_version_inferred": "Whether panel version was inferred.",
    "parser_profile_id": "Deterministic parser profile.",
    "report_version_key": "Deterministic report-version key.",
    "pathology_report_key": "Deterministic pathology-report series key.",
    "specimen_key": "Deterministic specimen key.",
    "accession_identifier": "Alias-resolved accession identifier.",
    "test_snomed_code": "Approved SNOMED assay code.",
    "test_loinc_code": "Approved LOINC assay code.",
    "test_omop_concept_id": "Standard OMOP assay concept.",
    "is_current": "Whether the source report version is current.",
    "research_qi_only": "Research/QI release flag.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each genomic test record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each genomic test record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Originating data feed responsible for the record for each genomic test record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Superseded when map_pathology_genetic_test.is_current is false, otherwise active; missing is_current defaults true. No retracted status is emitted. The assay's overall_result_status is separate, and joined report/identity fields do not set this label.",
    "record_status_effective_from": "Always null as a TIMESTAMP in the current genomic-test projection. No source-status start is supplied here; the clinical event's report/sample/request date fallbacks and ADC_UPDT are not used for this field.",
    "record_status_effective_to": "Always null as a TIMESTAMP in the current genomic-test projection, including when its status or source-current flag changes. No report, clinical-event or ingestion timestamp is substituted as a status end.",
    "source_update_timestamp": "map_pathology_genetic_test.ADC_UPDT carried unchanged from the contributing row, identical to loaded_at. No independent native application-update clock is projected here, and Silver refresh time is not substituted.",
    "loaded_at": "map_pathology_genetic_test.ADC_UPDT carried unchanged through its joins, usable-code filtering and source/public projections. Alias resolution and accession identity add no load clocks; neither joined report/parent timestamps nor the separately aggregated accession evidence clock replace this value. It is identical to source_update_timestamp, not the clinical event time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_genomic_test"),
    comment="Internal quality-controlled twin of clinical_genomic_test: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_GENOMIC_TEST_MANDATORY_RULES)
@dp.expect_all(CLINICAL_GENOMIC_TEST_ADVISORY_RULES)
def _gold_qc_clinical_genomic_test():
    """Quality-controlled twin of journey_clinical.genomic_test."""
    df = _qc(
        "clinical_genomic_test",
        CLINICAL_GENOMIC_TEST_SELECT,
        date_flags=["event_after_death_30d"],
    )
    return _with_comments(df, CLINICAL_GENOMIC_TEST_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.genomic_test"),
    comment=(
        "One molecular or cytogenetic assay per report version; all versions publish and "
        "is_current determines active versus superseded. Panel and vocabulary columns were "
        "all NULL on 2026-08-18, and ADC_UPDT was a single static-build instant. Gold QC twin "
        "of the silver product: 0 columns are repaired or nulled, 1 rule(s) drop rows, 2 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_genomic_test():
    """Contract-v2 public twin of clinical_genomic_test; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_genomic_test")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`genetic_test_id` AS `genetic_test_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`assay_code` AS `assay_code`',
        '`assay_name` AS `assay_name`',
        '`method` AS `method`',
        '`analysis_context` AS `analysis_context`',
        '`overall_result_status` AS `overall_result_status`',
        '`panel_code` AS `panel_code`',
        '`panel_version` AS `panel_version`',
        '`panel_version_inferred` AS `panel_version_inferred`',
        '`parser_profile_id` AS `parser_profile_id`',
        '`report_version_key` AS `report_version_key`',
        '`pathology_report_key` AS `pathology_report_key`',
        '`specimen_key` AS `specimen_key`',
        '`accession_identifier` AS `accession_identifier`',
        '`test_snomed_code` AS `test_snomed_code`',
        '`test_loinc_code` AS `test_loinc_code`',
        '`test_omop_concept_id` AS `test_omop_concept_id`',
        '`is_current` AS `is_current`',
        '`research_qi_only` AS `research_qi_only`',
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
    return _with_comments(df, CLINICAL_GENOMIC_TEST_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.imaging_exam ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_IMAGING_EXAM_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`event_id` AS `event_id`',
    '`pacs_examination_id` AS `pacs_examination_id`',
    '`organization_key` AS `organization_key`',
    '`source_object` AS `source_object`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    "CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS DATE) = DATE'1899-12-30' OR CAST(`event_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`event_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `event_datetime` END END AS `event_datetime`",
    'CASE WHEN YEAR(CAST(`event_end_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    '`exam_code` AS `exam_code`',
    '`status_code` AS `status_code`',
    '`accession_identifier` AS `accession_identifier`',
    '`study_instance_uid` AS `study_instance_uid`',
    '`modality_code` AS `modality_code`',
    "CASE WHEN UPPER(TRIM(CAST(`body_site_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `body_site_code` END AS `body_site_code`",
    '`report_patient_event_key` AS `report_patient_event_key`',
    '`requester_practitioner_id` AS `requester_practitioner_id`',
    '`performer_practitioner_id` AS `performer_practitioner_id`',
    '`sectra_accession_number` AS `sectra_accession_number`',
    '`accession_format` AS `accession_format`',
    '`accession_parse_method` AS `accession_parse_method`',
    '`accession_rule_version` AS `accession_rule_version`',
    '`accession_evidence_level` AS `accession_evidence_level`',
    '`accession_identity_status` AS `accession_identity_status`',
    '`accession_candidate_count` AS `accession_candidate_count`',
    '`linked_pacs_examination_id` AS `linked_pacs_examination_id`',
    '`pacs_link_method` AS `pacs_link_method`',
    '`source_event_class_code` AS `source_event_class_code`',
    '`source_status_code` AS `source_status_code`',
    '`performed_datetime` AS `performed_datetime`',
    '`performed_datetime_source` AS `performed_datetime_source`',
    '`performed_evidence` AS `performed_evidence`',
    '`performed_evidence_verified_ind` AS `performed_evidence_verified_ind`',
    '`report_available_ind` AS `report_available_ind`',
    '`report_link_status` AS `report_link_status`',
    '`report_count` AS `report_count`',
    '`report_reference_count` AS `report_reference_count`',
    '`linked_report_event_id` AS `linked_report_event_id`',
    '`native_image_count` AS `native_image_count`',
    '`native_series_count` AS `native_series_count`',
    '`measured_series_count` AS `measured_series_count`',
    '`request_clinical_question` AS `request_clinical_question`',
    '`request_clinical_history` AS `request_clinical_history`',
    '`request_clinical_question_exam_segment` AS `request_clinical_question_exam_segment`',
    '`request_clinical_question_segment_status` AS `request_clinical_question_segment_status`',
    '`request_clinical_history_exam_segment` AS `request_clinical_history_exam_segment`',
    '`request_clinical_history_segment_status` AS `request_clinical_history_segment_status`',
    '`request_clinical_question_anonymised` AS `request_clinical_question_anonymised`',
    '`request_clinical_history_anonymised` AS `request_clinical_history_anonymised`',
    '`request_clinical_question_exam_segment_anonymised` AS `request_clinical_question_exam_segment_anonymised`',
    '`request_clinical_history_exam_segment_anonymised` AS `request_clinical_history_exam_segment_anonymised`',
    '`request_text_anonymisation_status` AS `request_text_anonymisation_status`',
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

# contract v2: QC/batch inputs come from internal _clinical_imaging_exam_metadata; source history stays on the main research table.
CLINICAL_IMAGING_EXAM_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 48,943,391 rows of
    # 50,008,147 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_IMAGING_EXAM_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 1,064,756 of 50,008,147 at the profile.
    "gold.clinical.imaging_exam.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 36,156 of 50,008,147 rows (0.0723%) when profiled on 2026-08-24.
    "gold.clinical.imaging_exam.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1,247,681 of 50,008,147 rows (2.49%) when profiled on
    # 2026-08-24.
    "gold.clinical.imaging_exam.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",

    # Left as a warning because it fires on 2,567,329 of 50,008,147 rows (5.13%) when
    # profiled on 2026-08-24 -- at that rate the rule's assumption about what event_datetime
    # and event_end_datetime mean is the thing in doubt, not the data. The inverted gaps are
    # mostly minutes, which reads as two clocks rather than two events in the wrong order.
    "gold.clinical.imaging_exam.table.ordering_violation_event_datetime_event_end_datetime":
        "NOT COALESCE((`event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime`), FALSE)",
}

CLINICAL_IMAGING_EXAM_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "event_id": "Millennium radiology EVENT_ID; native key for the millennium arm.",
    "pacs_examination_id": "PACS_EXAMINATION_ID; native key for the pacs arm.",
    "organization_key": "Nullable deterministic organization key; no organization expression is present in the imaging source cells.",
    "source_object": "Native source arm: millennium or pacs.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID when resolved; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID when supplied; native encounter foreign key.",
    "event_datetime": "Imaging study start timestamp.",
    "event_end_datetime": "Imaging study end timestamp.",
    "source_coding_system": "Source exam coding system.",
    "source_code": "Source exam code",
    "source_display": "Source exam display.",
    "exam_code": "Source and mapped imaging-exam CodeableConcept.",
    "status_code": "Imaging study status.",
    "accession_identifier": "Imaging accession identifier, unchanged: PACS MILL_LINK_REF, Millennium REFERENCE_NBR. For the Barts/Sectra extraction accession use sectra_accession_number.",
    "study_instance_uid": "DICOM study instance UID.",
    "modality_code": "Imaging modality code.",
    "body_site_code": "Coded body part or anatomical region examined.",
    "report_patient_event_key": "Deterministic SHA-256 key of the linked report event.",
    "requester_practitioner_id": "Nullable Millennium personnel PERSON_ID for the requester.",
    "performer_practitioner_id": "Nullable Millennium personnel PERSON_ID for the performer.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each imaging exam record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each imaging exam record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Identity-withholding status is not populated by the current source projections; null does not mean identity was not withheld.",
    "source_feed": "Registered PACS or Millennium radiology source feed.",
    "record_status": "PACS rows are retracted when map_pacs_examination.SOURCE_PRESENT_IND is false, otherwise active; missing presence does not retract. Millennium rows are superseded when map_radiology_event.IN_ERROR_IND is true, otherwise active; missing error defaults false. The arms are unioned without reconciling their labels; a Millennium validity end does not itself supersede a row.",
    "record_status_effective_from": "PACS: map_pacs_examination.SRC_ADC_UPDT. Millennium: map_radiology_event.VALID_FROM_DT_TM_CLEAN. Each arm carries its value through the union and QC/public projections; no cross-arm maximum or clinical-event timestamp is substituted.",
    "record_status_effective_to": "PACS: map_pacs_examination.ADC_UPDT only when source presence is false, otherwise null. Millennium: UPDT_DT_TM when IN_ERROR_IND is true, otherwise VALID_UNTIL_DT_TM_CLEAN directly, including a finite or sentinel end on an active row. No sentinel filtering or fallback between Millennium branches is applied.",
    "source_update_timestamp": "PACS: map_pacs_examination.SRC_ADC_UPDT. Millennium: map_radiology_event.UPDT_DT_TM. These distinct clocks are carried per arm through the union, not combined as a maximum. Neither joined report clocks nor the current Silver refresh time replaces them.",
    "loaded_at": "PACS: map_pacs_examination.ADC_UPDT. Millennium: map_radiology_event.PIPELINE_UPDT_DT_TM, not its ADC_UPDT. These are arm-specific load/processing clocks, carried unchanged through the union and QC/public projections; no report or cross-arm clock maximum is taken and the current Silver refresh time is not substituted.",
}

# PACS_INTEGRATION_GOLD_CLINICAL_V1
IMAGING_INTEGRATION_COLUMN_COMMENTS = {'sectra_accession_number': 'Barts/Sectra extraction accession. PACS: trimmed REQUEST_ID_STRING '
                            '(map_pacs_examination.SECTRA_ACCESSION_NBR). Millennium: '
                            'REFERENCE_NBR parsed by the shared bronze accession rules. Scopes an '
                            'extraction request: one accession can hold several examinations and '
                            'study UIDs. Leading zeros preserved. Distinct from '
                            'accession_identifier, which keeps its existing meaning. Identifier.',
 'accession_format': 'Bronze accession-rule format class (SECTRA_16, SITE_16, RNH_14, '
                     'OTHER_SITE_14, NUMERIC_16, LEGACY_7_8, LEGACY_6, NUMERIC_OTHER, OTHER or '
                     'UNRECOGNISED).',
 'accession_parse_method': 'PACS: ACCESSION_PARSE_METHOD. Millennium: REF_PARSE_METHOD. Provenance '
                           'of sectra_accession_number.',
 'accession_rule_version': 'Version of the shared bronze accession rules that produced '
                           'sectra_accession_number.',
 'accession_evidence_level': 'pacs_request (PACS request accession), cerner_reference_pacs_linked '
                             '(Cerner reference resolved to one PACS examination), '
                             'cerner_reference_parsed (Cerner-only parsed reference), or NULL when '
                             'no accession.',
 'accession_identity_status': 'Row-level accession identity evidence. PACS: '
                              "ACCESSION_IDENTITY_STATUS over the accession's PACS examinations. "
                              'Millennium: CONFLICTING_PERSONS when the PACS link was refused for '
                              'identity conflict, otherwise NULL. The cross-arm accession status '
                              'is on clinical_imaging_accession_member.',
 'accession_candidate_count': 'PACS: published examinations sharing the accession. Millennium: '
                              'present PACS examinations sharing the parsed accession before code '
                              'and identity filtering.',
 'linked_pacs_examination_id': 'PACS examination this row resolves to. PACS rows: their own '
                               'PACS_EXAMINATION_ID. Millennium rows: '
                               'map_radiology_event.PACS_EXAMINATION_ID, set only for a unique '
                               'accession+code or unique accession-only match.',
 'pacs_link_method': 'NATIVE_PACS_EXAMINATION for PACS rows; the bronze PACS_LINK_METHOD for '
                     'Millennium rows. Ambiguous and conflicting candidates are never resolved by '
                     'choice.',
 'source_event_class_code': 'Millennium EVENT_CLASS_CD (234 examination, 224 report document, 231 '
                            'section); NULL for PACS rows.',
 'source_status_code': 'Native status. PACS: EXAMINATION_STATUS_CD. Millennium: RESULT_STATUS_CD.',
 'performed_datetime': 'Valid performed time. PACS: PERFORMED_DT_TM_CLEAN (examination time, else '
                       'the documented arrival fallback; future and 2099 sentinel values are '
                       'NULL). Millennium: PERFORMED_DT_TM_CLEAN. Never a scheduled, request, '
                       'report or load time. event_datetime keeps its existing meaning.',
 'performed_datetime_source': 'Which clean clock performed_datetime came from (EXAMINATION_DT_TM, '
                              'ARRIVAL_DT_TM or PERFORMED_DT_TM).',
 'performed_evidence': 'PACS: bronze PERFORMED_EVIDENCE set (STATUS, REPORT, IMAGES, SERIES). '
                       'Millennium: RESULT_STATUS for an examination event whose status is not '
                       'cancelled, in error or not done.',
 'performed_evidence_verified_ind': 'True only for verified performed evidence. PACS status 40 '
                                    '(booking) and dangling-only report links are not evidence. '
                                    'Millennium: examination event with a performed result status.',
 'report_available_ind': 'PACS: REPORT_AVAILABLE_IND (at least one resolved report; a dangling '
                         'report id never sets it). Millennium: a report document event is linked '
                         'through REF_EXAM_KEY.',
 'report_link_status': 'PACS: RESOLVED, DANGLING_ONLY or NONE. Millennium: REPORT_LINK_STATUS '
                       '(SELF, SIBLING, SIBLING_LATEST_OF_MULTIPLE, AMBIGUOUS or NONE). All links '
                       'are in clinical_imaging_report_link.',
 'report_count': 'PACS: REPORT_COUNT_RESOLVED. Millennium: identity-compatible report document '
                 'events sharing REF_EXAM_KEY.',
 'report_reference_count': 'PACS: REPORT_REF_COUNT, including dangling report ids. NULL for '
                           'Millennium rows.',
 'linked_report_event_id': 'Millennium: RADIOLOGY_REPORT_EVENT_ID, the latest identity-compatible '
                           'report document event. A convenience pointer; other reports stay '
                           'relational. NULL for PACS rows.',
 'native_image_count': 'Native Sectra IMAGE_COUNT (sparsely populated; PACS feed clock). Not '
                       'evidence of present retrievability.',
 'native_series_count': 'Native Sectra SERIES_COUNT (sparsely populated).',
 'measured_series_count': 'SERIES_COUNT_MEASURED from the raw series table. The series feed has a '
                          'separate clock and ends 2024-11-02; NULL after that.',
 'request_clinical_question': 'PACS request clinical question, verbatim and request-wide. '
                              'Identifiable free text (ig_risk 4); never copied into accession '
                              'summaries or anonymised serving projections.',
 'request_clinical_history': 'PACS request clinical information (CLINICAL_ANAMNESIS), verbatim and '
                             'request-wide. Identifiable free text (ig_risk 4).',
 'request_clinical_question_exam_segment': 'The clinical-question section headed by this '
                                           "examination's code; set only when its segment status "
                                           'is EXAM_SEGMENT. Identifiable free text (ig_risk 4).',
 'request_clinical_question_segment_status': 'EXAM_SEGMENT, NOT_SEGMENTED (text is request-wide), '
                                             'CODE_NOT_IN_SEGMENTS, DUPLICATE_SEGMENT_CODE, '
                                             'DUPLICATE_EXAM_CODE_ON_REQUEST, NO_EXAM_CODE or '
                                             'NO_TEXT.',
 'request_clinical_history_exam_segment': 'The clinical-history section headed by this '
                                          "examination's code; set only when its segment status is "
                                          'EXAM_SEGMENT. Identifiable free text (ig_risk 4).',
 'request_clinical_history_segment_status': 'Same vocabulary as '
                                            'request_clinical_question_segment_status.',
 'request_clinical_question_anonymised': 'Approved-lane anonymised clinical question. NULL unless '
                                         'the registered request-text feed reports anonymized for '
                                         'the current source text hash. Never falls back to raw '
                                         "text or a report's anonymised text.",
 'request_clinical_history_anonymised': 'Approved-lane anonymised clinical history; same rule as '
                                        'request_clinical_question_anonymised.',
 'request_clinical_question_exam_segment_anonymised': 'Approved-lane anonymised '
                                                      'examination-specific question segment; same '
                                                      'rule.',
 'request_clinical_history_exam_segment_anonymised': 'Approved-lane anonymised '
                                                     'examination-specific history segment; same '
                                                     'rule.',
 'request_text_anonymisation_status': 'not_registered (feed not yet deployed), current (anonymized '
                                      'for the current text), stale (text changed since '
                                      'redaction), pending, failed, unresolved_person, or no_text. '
                                      'Millennium rows are NULL.'}
CLINICAL_IMAGING_EXAM_COLUMN_COMMENTS.update(IMAGING_INTEGRATION_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_qc._clinical_imaging_exam"),
    comment="Internal quality-controlled twin of clinical_imaging_exam: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_IMAGING_EXAM_MANDATORY_RULES)
@dp.expect_all(CLINICAL_IMAGING_EXAM_ADVISORY_RULES)
def _gold_qc_clinical_imaging_exam():
    """Quality-controlled twin of journey_clinical.imaging_exam."""
    df = _qc("clinical_imaging_exam", CLINICAL_IMAGING_EXAM_SELECT)
    return _with_comments(df, CLINICAL_IMAGING_EXAM_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.imaging_exam"),
    comment=(
        "One PACS or Millennium radiology examination/study; the Millennium report-link field "
        "is schema-only until it first populates. Gold QC twin of the silver product: 8 "
        "columns are repaired or nulled, 1 rule(s) drop rows, 4 check(s) are advisory. Each "
        "rule states its reason in the pipeline notebook, and Lakeflow expectation metrics "
        "report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_imaging_exam():
    """Contract-v2 public twin of clinical_imaging_exam; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_imaging_exam")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`event_id` AS `event_id`',
        '`pacs_examination_id` AS `pacs_examination_id`',
        '`organization_key` AS `organization_key`',
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
        '`__gold_json_exam_code` AS `exam_code`',
        '`status_code` AS `status_code`',
        '`accession_identifier` AS `accession_identifier`',
        '`study_instance_uid` AS `study_instance_uid`',
        '`modality_code` AS `modality_code`',
        '`body_site_code` AS `body_site_code`',
        '`report_patient_event_key` AS `report_patient_event_key`',
        '`requester_practitioner_id` AS `requester_practitioner_id`',
        '`performer_practitioner_id` AS `performer_practitioner_id`',
        '`sectra_accession_number` AS `sectra_accession_number`',
        '`accession_format` AS `accession_format`',
        '`accession_parse_method` AS `accession_parse_method`',
        '`accession_rule_version` AS `accession_rule_version`',
        '`accession_evidence_level` AS `accession_evidence_level`',
        '`accession_identity_status` AS `accession_identity_status`',
        '`accession_candidate_count` AS `accession_candidate_count`',
        '`linked_pacs_examination_id` AS `linked_pacs_examination_id`',
        '`pacs_link_method` AS `pacs_link_method`',
        '`source_event_class_code` AS `source_event_class_code`',
        '`source_status_code` AS `source_status_code`',
        '`performed_datetime` AS `performed_datetime`',
        '`performed_datetime_source` AS `performed_datetime_source`',
        '`performed_evidence` AS `performed_evidence`',
        '`performed_evidence_verified_ind` AS `performed_evidence_verified_ind`',
        '`report_available_ind` AS `report_available_ind`',
        '`report_link_status` AS `report_link_status`',
        '`report_count` AS `report_count`',
        '`report_reference_count` AS `report_reference_count`',
        '`linked_report_event_id` AS `linked_report_event_id`',
        '`native_image_count` AS `native_image_count`',
        '`native_series_count` AS `native_series_count`',
        '`measured_series_count` AS `measured_series_count`',
        '`request_clinical_question` AS `request_clinical_question`',
        '`request_clinical_history` AS `request_clinical_history`',
        '`request_clinical_question_exam_segment` AS `request_clinical_question_exam_segment`',
        '`request_clinical_question_segment_status` AS `request_clinical_question_segment_status`',
        '`request_clinical_history_exam_segment` AS `request_clinical_history_exam_segment`',
        '`request_clinical_history_segment_status` AS `request_clinical_history_segment_status`',
        '`request_clinical_question_anonymised` AS `request_clinical_question_anonymised`',
        '`request_clinical_history_anonymised` AS `request_clinical_history_anonymised`',
        '`request_clinical_question_exam_segment_anonymised` AS `request_clinical_question_exam_segment_anonymised`',
        '`request_clinical_history_exam_segment_anonymised` AS `request_clinical_history_exam_segment_anonymised`',
        '`request_text_anonymisation_status` AS `request_text_anonymisation_status`',
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
    return _s3_flatten_gold_public(_with_comments(df, CLINICAL_IMAGING_EXAM_COLUMN_COMMENTS), "clinical_imaging_exam")

# COMMAND ----------

# ==== journey_clinical.imaging_accession_member / imaging_report_link (PACS integration v1) ====

# Accession-level selection support. Members keep every accession-bearing examination, so the
# accession's identity status and counts stay honest; default selection is the member's
# default_selection_eligible_ind, decided once in silver. Exam concepts come from Gold
# clinical_imaging_exam, already mapped once in silver and gated by the Gold mapping policy
# here; they are NULL for examinations that table does not publish.
CLINICAL_IMAGING_ACCESSION_MEMBER_COLUMNS = ['member_key',
 'accession_key',
 'sectra_accession_number',
 'patient_event_key',
 'source_object',
 'event_id',
 'pacs_examination_id',
 'linked_pacs_examination_id',
 'examination_key',
 'study_instance_uid',
 'subject_key',
 'subject_id_system',
 'person_id',
 'identity_status',
 'encounter_id',
 'accession_format',
 'accession_parse_method',
 'accession_rule_version',
 'accession_evidence_level',
 'pacs_link_method',
 'accession_person_count',
 'accession_person_status',
 'performed_datetime',
 'performed_datetime_source',
 'performed_evidence',
 'performed_evidence_verified_ind',
 'eligibility_as_of',
 'default_selection_eligible_ind',
 'eligibility_blocking_reasons',
 'source_coding_system',
 'source_code',
 'source_display',
 'modality_code',
 'body_site_code',
 'status_code',
 'source_status_code',
 'record_status',
 'report_available_ind',
 'report_link_status',
 'report_patient_event_key',
 'linked_report_event_id',
 'native_image_count',
 'native_series_count',
 'measured_series_count',
 'source_feed',
 'source_update_timestamp',
 'loaded_at']

CLINICAL_IMAGING_ACCESSION_MEMBER_COLUMN_COMMENTS = {'member_key': 'Deterministic SHA-256 of the member (imaging_accession_member namespace + '
               'patient_event_key). One row per PACS examination or Millennium examination event '
               'that carries an accession.',
 'accession_key': 'upper(trim(sectra_accession_number)): the grouping key shared by PACS and '
                  'Millennium members of one accession. Identifier.',
 'sectra_accession_number': "Barts/Sectra extraction accession as published by the member's "
                            'source. Identifier.',
 'patient_event_key': 'patient_event_key of this member, minted exactly as clinical_imaging_exam '
                      'mints it. Joins clinical_imaging_exam when the examination is admitted '
                      'there; examinations without a usable code are retained here but not '
                      'admitted there, so their exam_* concept columns are NULL.',
 'source_object': 'pacs (Sectra examination) or millennium (Cerner examination event, '
                  'EVENT_CLASS_CD 234).',
 'event_id': 'Millennium EVENT_ID for millennium members.',
 'pacs_examination_id': 'Native PACS_EXAMINATION_ID for pacs members.',
 'linked_pacs_examination_id': 'PACS examination the member resolves to (own id for pacs; unique '
                               'bronze link for millennium).',
 'examination_key': 'Physical-examination identity used for distinct counts: pacs:<PACS '
                    'examination id> when resolved, otherwise mill:<EVENT_ID>. A Millennium event '
                    'linked to a PACS examination counts once with it.',
 'study_instance_uid': 'DICOM StudyInstanceUID (image retrieval key; pacs members only). One '
                       'accession can hold several.',
 'subject_key': 'Deterministic subject key inherited from clinical_imaging_exam.',
 'subject_id_system': 'Identifier system used for subject_key.',
 'person_id': 'Millennium PERSON_ID when resolved.',
 'identity_status': 'Member identity resolution: resolved, provisional or unresolved.',
 'encounter_id': 'Millennium ENCNTR_ID when supplied (millennium members).',
 'accession_format': 'Bronze accession-rule format class.',
 'accession_parse_method': 'Provenance of sectra_accession_number.',
 'accession_rule_version': 'Bronze accession-rule version.',
 'accession_evidence_level': 'pacs_request, cerner_reference_pacs_linked or '
                             'cerner_reference_parsed.',
 'pacs_link_method': 'NATIVE_PACS_EXAMINATION or the bronze Millennium PACS link method.',
 'accession_person_count': 'Distinct non-null person_id across all members of the accession (both '
                           'arms).',
 'accession_person_status': 'single_person, single_person_partial (some members unresolved), '
                            'conflicting_persons (more than one person_id, or a refused '
                            'identity-conflict link) or no_person. Conflicts are never resolved by '
                            'choice.',
 'performed_datetime': 'Valid performed time (future and sentinel values already NULL in bronze).',
 'performed_datetime_source': 'Clean clock that supplied performed_datetime.',
 'performed_evidence': 'Performed evidence set (see clinical_imaging_exam).',
 'performed_evidence_verified_ind': 'Verified performed evidence; PACS status 40 and dangling '
                                    'report ids are not evidence.',
 'eligibility_as_of': "Selection as-of time: the member's own bronze load clock. A performed time "
                      'after it is ineligible.',
 'default_selection_eligible_ind': 'True when eligibility_blocking_reasons is NULL. Ineligible '
                                   'members stay queryable here.',
 'eligibility_blocking_reasons': 'Comma list, NULL when eligible: source_not_current, '
                                 'no_valid_performed_time, performed_after_as_of, '
                                 'performed_evidence_unverified, accession_identity_conflict, '
                                 'unvalidated_cerner_accession. Missing codes never block.',
 'source_coding_system': 'Source examination coding system.',
 'source_code': 'Source examination code or display fallback; may be NULL (uncoded examinations '
                'are retained).',
 'source_display': 'Source examination description.',
 'modality_code': 'Observed modality: PACS MODALITY or Millennium NHSI_MODALITY_CATEGORY. '
                  "Normalised concepts come from clinical_imaging_exam's mapped exam axis.",
 'body_site_code': 'Observed body part (PACS BODY_PART).',
 'status_code': 'Silver imaging status inherited from clinical_imaging_exam.',
 'source_status_code': 'Native status code (PACS EXAMINATION_STATUS_CD or Millennium '
                       'RESULT_STATUS_CD).',
 'record_status': 'Silver record status inherited from clinical_imaging_exam (active, retracted or '
                  'superseded).',
 'report_available_ind': 'At least one resolved report linked to this member (see '
                         'clinical_imaging_report_link for every link).',
 'report_link_status': 'Source report-link status.',
 'report_patient_event_key': 'Latest resolved PACS report document key (convenience pointer only).',
 'linked_report_event_id': 'Latest Millennium report document EVENT_ID (convenience pointer only).',
 'native_image_count': 'Native Sectra IMAGE_COUNT; PACS feed clock.',
 'native_series_count': 'Native Sectra SERIES_COUNT.',
 'measured_series_count': 'Measured series count; raw series feed clock ends 2024-11-02.',
 'source_feed': 'pacs_examination or radiology_event.',
 'source_update_timestamp': 'Source update clock inherited from clinical_imaging_exam.',
 'loaded_at': 'Bronze load clock inherited from clinical_imaging_exam.'}

CLINICAL_IMAGING_ACCESSION_MEMBER_ADVISORY_RULES = {
    # Counts members of accessions whose persons conflict. They are kept, never resolved by
    # choice, and blocked from default selection by eligibility_blocking_reasons.
    "gold.clinical.imaging_accession_member.accession_person_status.not_conflicting":
        "accession_person_status <> 'conflicting_persons'",
    # Counts members excluded from default selection for any reason.
    "gold.clinical.imaging_accession_member.default_selection_eligible":
        "default_selection_eligible_ind",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_imaging_accession_member"),
    comment="Internal quality-controlled twin of clinical_imaging_accession_member; the published Gold MV reads this object.",
    refresh_policy="incremental",
)
@dp.expect_all(CLINICAL_IMAGING_ACCESSION_MEMBER_ADVISORY_RULES)
def _gold_qc_clinical_imaging_accession_member():
    """Quality-controlled twin of journey_clinical.imaging_accession_member."""
    df = spark.read.table(_src("clinical_imaging_accession_member")).select(
        *CLINICAL_IMAGING_ACCESSION_MEMBER_COLUMNS
    )
    return _with_comments(df, CLINICAL_IMAGING_ACCESSION_MEMBER_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.imaging_accession_member"),
    comment=(
        "One row per accession-bearing PACS examination or Millennium examination event, with "
        "default-selection eligibility and blocking reasons. Select accessions by filtering "
        "members, then take distinct accession_key; clinical_v_imaging_accession summarises "
        "eligible members per accession."
    ),
    table_properties={"quality": "gold"},
    cluster_by=["accession_key"],
    refresh_policy="incremental",
)
def gold_clinical_imaging_accession_member():
    """Public accession members with Gold-gated exam concepts from clinical_imaging_exam."""
    axis = _s3_axis_columns("exam", False)
    concepts = spark.read.table(_n("gold_clinical.imaging_exam")).select(
        F.col("patient_event_key").alias("_concept_key"), *axis
    )
    members = spark.read.table(_n("gold_qc._clinical_imaging_accession_member"))
    at = CLINICAL_IMAGING_ACCESSION_MEMBER_COLUMNS.index("source_display") + 1
    order = (CLINICAL_IMAGING_ACCESSION_MEMBER_COLUMNS[:at] + axis
             + CLINICAL_IMAGING_ACCESSION_MEMBER_COLUMNS[at:])
    df = (
        members.join(concepts, members.patient_event_key == concepts._concept_key, "left")
        .select(*order)
    )
    return _with_comments(df, CLINICAL_IMAGING_ACCESSION_MEMBER_COLUMN_COMMENTS)

CLINICAL_IMAGING_REPORT_LINK_COLUMNS = ['link_key',
 'link_scope',
 'link_method',
 'accession_key',
 'sectra_accession_number',
 'member_patient_event_key',
 'document_patient_event_key',
 'document_source_feed',
 'pacs_report_id',
 'cerner_document_event_id',
 'document_version_id',
 'document_version_count',
 'document_version_decision',
 'report_resolved_ind',
 'report_datetime',
 'report_status_code',
 'text_source',
 'text_sha256',
 'text_integrity_status',
 'report_text_available_ind',
 'approved_anonymised_text_available_ind',
 'person_id',
 'record_status',
 'loaded_at']

CLINICAL_IMAGING_REPORT_LINK_COLUMN_COMMENTS = {'link_key': 'Deterministic SHA-256 over scope, member/accession and document identity.',
 'link_scope': 'examination (Sectra report attached to one examination), direct_cerner_event '
               "(Cerner report document sharing the examination's REF_EXAM_KEY), or accession "
               '(Sectra report attached to the request only; member_patient_event_key is NULL).',
 'link_method': 'PACS_EXAMINATION_REPORT, PACS_REQUEST_REPORT or CERNER_REF_EXAM_KEY.',
 'accession_key': 'upper(trim(sectra_accession_number)); joins clinical_imaging_accession_member. '
                  'Identifier.',
 'sectra_accession_number': 'Accession of the linked examination or request. Identifier.',
 'member_patient_event_key': 'clinical_imaging_exam / accession member patient_event_key; NULL for '
                             'accession-scope links.',
 'document_patient_event_key': 'text_document patient_event_key of the linked document version; '
                               'NULL for a dangling PACS report id.',
 'document_source_feed': 'pacs_report or mill_blob_text.',
 'pacs_report_id': 'Sectra ReportId (PACS links). May be dangling (report_resolved_ind false).',
 'cerner_document_event_id': 'Cerner report document EVENT_ID: the REF_EXAM_KEY sibling for Cerner '
                             "links, or the bridge EVENT_ID supplying a PACS report's text.",
 'document_version_id': 'Silver document version_id of the linked version.',
 'document_version_count': 'Cerner links: current (active) silver document versions for the event. '
                           '1 for PACS links.',
 'document_version_decision': 'single_current, identical_text_collapsed (several current versions, '
                              'one text), text_version_ambiguous (conflicting current texts: no '
                              'text is certified) or no_current_version. PACS native reports: '
                              'native_report.',
 'report_resolved_ind': 'True when the report resolves to a real report/document row. A dangling '
                        'id never makes a report available.',
 'report_datetime': 'Report time (Sectra REPORT_DT_TM; Cerner document event_datetime).',
 'report_status_code': 'Source report status code.',
 'text_source': 'pacs_native, mill_blob_text_bridge (bridge v4, version-safe), mill_blob_text, or '
                'NULL when no text.',
 'text_sha256': 'SHA-256 of the linked document text (silver text_document.text_sha256).',
 'text_integrity_status': 'Bridge v4 TEXT_INTEGRITY_STATUS for bridged PACS text; NULL otherwise.',
 'report_text_available_ind': 'Non-empty report text is available for this link and not '
                              'version-ambiguous.',
 'approved_anonymised_text_available_ind': 'The approved anonymisation lane holds output for the '
                                           'current text (anon_status anonymized and matching '
                                           'source hash). False when missing or stale; never '
                                           'inferred from raw text.',
 'person_id': 'Report/document person when resolved.',
 'record_status': 'Link record status: active, or the linked document/report record status when '
                  'that is not active.',
 'loaded_at': 'Bronze load clock of the link source row.'}

CLINICAL_IMAGING_REPORT_LINK_ADVISORY_RULES = {
    # Counts PACS report ids with no report row. They are kept as evidence and never make a
    # report available.
    "gold.clinical.imaging_report_link.report_resolved": "report_resolved_ind",
    # Counts Cerner report events whose current blob versions disagree on text; no version
    # is certified for them.
    "gold.clinical.imaging_report_link.document_version_not_ambiguous":
        "COALESCE(document_version_decision, '') <> 'text_version_ambiguous'",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_imaging_report_link"),
    comment="Internal quality-controlled twin of clinical_imaging_report_link; the published Gold MV reads this object.",
    refresh_policy="incremental",
)
@dp.expect_all(CLINICAL_IMAGING_REPORT_LINK_ADVISORY_RULES)
def _gold_qc_clinical_imaging_report_link():
    """Quality-controlled twin of journey_clinical.imaging_report_link."""
    df = spark.read.table(_src("clinical_imaging_report_link")).select(
        *CLINICAL_IMAGING_REPORT_LINK_COLUMNS
    )
    return _with_comments(df, CLINICAL_IMAGING_REPORT_LINK_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.imaging_report_link"),
    comment=(
        "Relational imaging report links: examination-, request- and Cerner-event-scoped "
        "report documents with version decisions, text availability and approved-"
        "anonymisation availability. Multiple reports and addenda stay separate rows."
    ),
    table_properties={"quality": "gold"},
    cluster_by=["accession_key"],
    refresh_policy="incremental",
)
def gold_clinical_imaging_report_link():
    """Public imaging report links."""
    df = spark.read.table(_n("gold_qc._clinical_imaging_report_link"))
    return _with_comments(df, CLINICAL_IMAGING_REPORT_LINK_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.indication ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_INDICATION_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`indication_id` AS `indication_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS OR YEAR(CAST(`event_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    '`relation_type` AS `relation_type`',
    '`source_field` AS `source_field`',
    '`source_text` AS `source_text`',
    '`evidence_text` AS `evidence_text`',
    '`evidence_start` AS `evidence_start`',
    '`evidence_end` AS `evidence_end`',
    '`snomed_code` AS `snomed_code`',
    '`snomed_term` AS `snomed_term`',
    '`omop_concept_id` AS `omop_concept_id`',
    '`assertion` AS `assertion`',
    '`temporality` AS `temporality`',
    '`experiencer` AS `experiencer`',
    '`rule_id` AS `rule_id`',
    '`rule_version` AS `rule_version`',
    '`confidence` AS `confidence`',
    '`mapping_status` AS `mapping_status`',
    '`ig_release_status` AS `ig_release_status`',
    '`is_current` AS `is_current`',
    '`research_qi_only` AS `research_qi_only`',
    '`specimen_key` AS `specimen_key`',
    '`accession_identifier` AS `accession_identifier`',
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

# contract v2: QC/batch inputs come from internal _clinical_indication_metadata; source history stays on the main research table.
CLINICAL_INDICATION_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 3,282,008,054 rows of
    # 3,282,008,054 that are current and attributable; identity_status = 'resolved' keeps
    # the 3,282,007,974 rows of 3,282,008,054 that are current and attributable. Superseded
    # versions and rows whose identity was never resolved are not research data, and a
    # consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_INDICATION_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 80 of 3,282,008,054 at the profile.
    "gold.clinical.indication.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 3,282,008,054 at the profile.
    "gold.clinical.indication.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 4,323,749 of 3,282,008,054 rows (0.132%) when profiled on
    # 2026-08-24.
    "gold.clinical.indication.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 268,150 of 3,282,008,054 rows (0.00817%) when profiled on
    # 2026-08-24.
    "gold.clinical.indication.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_INDICATION_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "indication_id": "Pathology indication_id; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace selected for `subject_key`: Millennium person identifier first, then MRN, then NHS number; nosubject denotes the per-source-record fallback when none is present.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT through the shared pathology identity projection.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Accession report/sample/request fallback time.",
    "event_end_datetime": "Event end time.",
    "source_coding_system": "Indication text namespace.",
    "source_code": "Coded diagnosis display string.",
    "source_display": "Coded diagnosis display string.",
    "relation_type": "Evidence relation type.",
    "source_field": "Source evidence field.",
    "source_text": "Unmodified coded diagnosis display string.",
    "evidence_text": "Exact evidence span.",
    "evidence_start": "Evidence start offset.",
    "evidence_end": "Evidence end offset.",
    "snomed_code": "SNOMED condition code.",
    "snomed_term": "SNOMED display term.",
    "omop_concept_id": "Standard OMOP condition concept.",
    "assertion": "present, absent, possible, family_history, or unknown.",
    "temporality": "current, historical, future, or unknown.",
    "experiencer": "patient, family, or unknown.",
    "rule_id": "Deterministic rule identifier.",
    "rule_version": "Deterministic rule version.",
    "confidence": "Confidence value supplied by the pathology indication rule, carried unchanged from bronze `confidence`; Silver does not recalibrate this value.",
    "mapping_status": "Source mapping status.",
    "ig_release_status": "IG release status published verbatim.",
    "is_current": "Whether evidence remains current.",
    "research_qi_only": "Research/QI release flag.",
    "specimen_key": "Deterministic specimen key.",
    "accession_identifier": "Alias-resolved accession identifier.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each indication record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each indication record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Originating data feed responsible for the record for each indication record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Always active in the current map_pathology_indication projection. is_current, assertion, temporality, experiencer and mapping/release status do not change this literal label; usable-code filtering separately controls publication eligibility.",
    "record_status_effective_from": "Always null as a TIMESTAMP in the current indication projection. No source-status start is supplied here; the clinical event's report/sample/request date fallbacks and ADC_UPDT are not used for this field.",
    "record_status_effective_to": "Always null as a TIMESTAMP in the current indication projection, including when its status or source-current flag changes. No report, clinical-event or ingestion timestamp is substituted as a status end.",
    "source_update_timestamp": "map_pathology_indication.ADC_UPDT carried unchanged from the contributing row, identical to loaded_at. No independent native application-update clock is projected here, and Silver refresh time is not substituted.",
    "loaded_at": "map_pathology_indication.ADC_UPDT carried unchanged through its joins, usable-code filtering and source/public projections. Alias resolution and accession identity add no load clocks; neither joined report/parent timestamps nor the separately aggregated accession evidence clock replace this value. It is identical to source_update_timestamp, not the clinical event time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_indication"),
    comment="Internal quality-controlled twin of clinical_indication: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_INDICATION_MANDATORY_RULES)
@dp.expect_all(CLINICAL_INDICATION_ADVISORY_RULES)
def _gold_qc_clinical_indication():
    """Quality-controlled twin of journey_clinical.indication."""
    # 84 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "clinical_indication",
        CLINICAL_INDICATION_SELECT,
        fk_columns=["person_id"],
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, CLINICAL_INDICATION_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.indication"),
    comment=(
        "Accession-scoped diagnosis-context evidence from diagnosis_context_window_v1. "
        "Evidence spans are the complete coded diagnosis display string; event time uses "
        "accession report_dt, sample_dt, then request_dt. Text is ig_risk 4, ig_severity 2; "
        "ig_release_status is describe-only and prod activation is gated. Gold QC twin of the "
        "silver product: 3 columns are repaired or nulled, 1 rule(s) drop rows, 4 check(s) "
        "are advisory. Each rule states its reason in the pipeline notebook, and Lakeflow "
        "expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_indication():
    """Contract-v2 public twin of clinical_indication; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_indication")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`indication_id` AS `indication_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`relation_type` AS `relation_type`',
        '`source_field` AS `source_field`',
        '`source_text` AS `source_text`',
        '`evidence_text` AS `evidence_text`',
        '`evidence_start` AS `evidence_start`',
        '`evidence_end` AS `evidence_end`',
        '`snomed_code` AS `snomed_code`',
        '`snomed_term` AS `snomed_term`',
        '`omop_concept_id` AS `omop_concept_id`',
        '`assertion` AS `assertion`',
        '`temporality` AS `temporality`',
        '`experiencer` AS `experiencer`',
        '`rule_id` AS `rule_id`',
        '`rule_version` AS `rule_version`',
        '`confidence` AS `confidence`',
        '`mapping_status` AS `mapping_status`',
        '`ig_release_status` AS `ig_release_status`',
        '`is_current` AS `is_current`',
        '`research_qi_only` AS `research_qi_only`',
        '`specimen_key` AS `specimen_key`',
        '`accession_identifier` AS `accession_identifier`',
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
    return _with_comments(df, CLINICAL_INDICATION_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.medication_admin ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_MEDICATION_ADMIN_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_patient_event_key` AS `source_patient_event_key`',
    '`event_id` AS `event_id`',
    '`order_id` AS `order_id`',
    '`location_key` AS `location_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`medication_code` AS `medication_code`',
    '`medication_order_key` AS `medication_order_key`',
    '`administration_status_code` AS `administration_status_code`',
    '`administration_status_display` AS `administration_status_display`',
    "CASE WHEN UPPER(TRIM(CAST(`source_event_type_code` AS STRING))) = '0' THEN NULL ELSE `source_event_type_code` END AS `source_event_type_code`",
    '`source_event_type_display` AS `source_event_type_display`',
    '`status_history_count` AS `status_history_count`',
    'CASE WHEN ABS(CAST(`dose_value` AS DOUBLE)) > 1e12 OR `dose_value` < 0 THEN NULL ELSE `dose_value` END AS `dose_value`',
    '`dose_unit` AS `dose_unit`',
    '`initial_dose_value` AS `initial_dose_value`',
    '`initial_dose_unit` AS `initial_dose_unit`',
    '`dose_in_mg` AS `dose_in_mg`',
    '`dose_in_ml` AS `dose_in_ml`',
    '`dose_standardization_status` AS `dose_standardization_status`',
    "CASE WHEN UPPER(TRIM(CAST(`route_code` AS STRING))) = '0' THEN NULL ELSE `route_code` END AS `route_code`",
    '`route_display` AS `route_display`',
    "CASE WHEN UPPER(TRIM(CAST(`site_code` AS STRING))) = '0' THEN NULL ELSE `site_code` END AS `site_code`",
    '`site_display` AS `site_display`',
    '`infused_volume` AS `infused_volume`',
    '`infused_volume_unit` AS `infused_volume_unit`',
    '`infusion_rate` AS `infusion_rate`',
    '`infusion_rate_unit` AS `infusion_rate_unit`',
    '`ingredient_count` AS `ingredient_count`',
    '`performer_practitioner_id` AS `performer_practitioner_id`',
    '`verifier_practitioner_id` AS `verifier_practitioner_id`',
    '`location_code` AS `location_code`',
    '`organization_id` AS `organization_id`',
    '`scheduled_datetime` AS `scheduled_datetime`',
    'CASE WHEN CAST(`performed_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS OR YEAR(CAST(`performed_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `performed_datetime` END AS `performed_datetime`',
    '`verified_datetime` AS `verified_datetime`',
    '`order_status_code` AS `order_status_code`',
    '`order_status_display` AS `order_status_display`',
    '`prn_ind` AS `prn_ind`',
    '`iv_ind` AS `iv_ind`',
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

# contract v2: QC/batch inputs come from internal _clinical_medication_admin_metadata; source history stays on the main research table.
CLINICAL_MEDICATION_ADMIN_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 60,094,539 rows of
    # 60,094,539 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_MEDICATION_ADMIN_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 60,094,539 at the profile.
    "gold.clinical.medication_admin.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 9,527 of 60,094,539 rows (0.0159%) when profiled on 2026-08-24.
    "gold.clinical.medication_admin.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1,772 of 60,094,539 rows (0.00295%) when profiled on 2026-08-24.
    "gold.clinical.medication_admin.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_MEDICATION_ADMIN_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "source_patient_event_key": "Stable patient-event identity retained from the originating Silver row before source splitting.",
    "event_id": "Millennium medication-administration EVENT_ID; primary key of this table.",
    "order_id": "Millennium medication ORDER_ID when the linked order exists.",
    "location_key": "Deterministic SHA-256 nurse-unit key retained beside location_code.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Subject-key identifier system.",
    "person_id": "Millennium PERSON_ID; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "event_datetime": "Administration start or performed time.",
    "event_end_datetime": "Administration end time.",
    "source_coding_system": "Source medication coding system.",
    "source_code": "Source order-synonym identifier.",
    "source_display": "Source medication display.",
    "medication_code": "Source and mapped medication codings.",
    "medication_order_key": "Deterministic SHA-256 key of the linked medication order.",
    "administration_status_code": "Current source result status code.",
    "administration_status_display": "Current source result status display.",
    "source_event_type_code": "Source medication event type code.",
    "source_event_type_display": "Source medication event type display.",
    "status_history_count": "Number of retained status milestones.",
    "dose_value": "Effective administered dose.",
    "dose_unit": "Administered dose unit.",
    "initial_dose_value": "Initially documented dose.",
    "initial_dose_unit": "Initially documented dose unit.",
    "dose_in_mg": "Bronze-standardized milligram dose.",
    "dose_in_ml": "Bronze-standardized millilitre dose.",
    "dose_standardization_status": "Bronze dose-standardization status.",
    "route_code": "Administration route code.",
    "route_display": "Administration route display.",
    "site_code": "Administration site code.",
    "site_display": "Administration site display.",
    "infused_volume": "The volume at any one point in time that remains in the IV Bag.",
    "infused_volume_unit": "Text description of the unit of measure for infused volume.",
    "infusion_rate": "For continuously administered medications, IV or IVP, the infusion rate and unit is used to capture the flow rate of the medication into the patient.",
    "infusion_rate_unit": "Text description of the unit of measure for volume or quantity of the medication.",
    "ingredient_count": "Number of retained ingredient rows.",
    "performer_practitioner_id": "Millennium personnel PERSON_ID for the administration performer.",
    "verifier_practitioner_id": "Millennium personnel PERSON_ID for the administration verifier.",
    "location_code": "Millennium NURSE_UNIT_CD for the administration location.",
    "organization_id": "Millennium ORGANIZATION_ID for the source organization.",
    "scheduled_datetime": "Scheduled administration time.",
    "performed_datetime": "Performed administration time.",
    "verified_datetime": "Date and time at which the medication administration event was verified.",
    "order_status_code": "Linked source order status code.",
    "order_status_display": "Linked source order status display.",
    "prn_ind": "True when source `PRN_IND` equals 1, indicating as-needed medication; other non-null values become false and null remains null.",
    "iv_ind": "Indicator showing whether the order is an intravenous (IV) medication order.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each medication admin record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each medication admin record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Registered source feed.",
    "record_status": "Derived administration-row status: superseded when map_med_admin.CE_VALID_UNTIL_DT_TM is before 2100-01-01 or MR_IS_CURRENT_IND is false; otherwise active. Null MR_IS_CURRENT_IND defaults true and a null clinical-event end alone does not supersede the row. This projection does not emit retracted or derive status from the administration result/workflow code.",
    "record_status_effective_from": "First non-null map_med_admin.CE_VALID_FROM_DT_TM, MR_VALID_FROM_DT_TM, then ADMIN_START_DT_TM. This is a source validity start with an administration-time fallback, not a uniform independent status-change timestamp; null when all three are missing.",
    "record_status_effective_to": "For a superseded administration, first non-null map_med_admin.CE_VALID_UNTIL_DT_TM then MR_VALID_UNTIL_DT_TM; null for active rows or when both are missing. The end selection does not remove far-future sentinels, so supersession caused by MR_IS_CURRENT_IND=false can retain an open-ended clinical-event timestamp.",
    "source_update_timestamp": "Greatest of map_med_admin.MAE_ADC_UPDT, MR_ADC_UPDT, ORDERS_ADC_UPDT, OI_ADC_UPDT, SYNONYM_ADC_UPDT, LOOKUP_ADC_UPDT and ADC_UPDT, plus the per-EVENT_ID maximum of greatest(UPDT_DT_TM, ADC_UPDT) from map_med_admin_ingredient. This combines contributing source/lookup/ingestion clocks, not a single application update time or Silver refresh time; null only when all contributing clocks are null.",
    "loaded_at": "Greatest of map_med_admin.ADC_UPDT and the maximum map_med_admin_ingredient.ADC_UPDT for the same EVENT_ID. The order-ID existence join contributes no separate load timestamp. This combines administration and ingredient ingestion provenance, not administration event time or Silver refresh time; null only when all contributing load clocks are null.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_medication_admin"),
    comment="Internal quality-controlled twin of clinical_medication_admin: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_ADMIN_MANDATORY_RULES)
@dp.expect_all(CLINICAL_MEDICATION_ADMIN_ADVISORY_RULES)
def _gold_qc_clinical_medication_admin():
    """Quality-controlled twin of journey_clinical.medication_admin."""
    df = _qc("clinical_medication_admin", CLINICAL_MEDICATION_ADMIN_SELECT)
    return _with_comments(df, CLINICAL_MEDICATION_ADMIN_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.medication_admin"),
    comment=(
        "One Millennium medication-administration event with current state and ordered "
        "lifecycle evidence. Gold QC twin of the silver product: 8 columns are repaired or "
        "nulled, 1 rule(s) drop rows, 3 check(s) are advisory. Each rule states its reason in "
        "the pipeline notebook, and Lakeflow expectation metrics report what every rule "
        "matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_medication_admin():
    """Contract-v2 public twin of clinical_medication_admin; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_medication_admin")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_patient_event_key` AS `source_patient_event_key`',
        '`event_id` AS `event_id`',
        '`order_id` AS `order_id`',
        '`location_key` AS `location_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`__gold_json_medication_code` AS `medication_code`',
        '`medication_order_key` AS `medication_order_key`',
        '`administration_status_code` AS `administration_status_code`',
        '`administration_status_display` AS `administration_status_display`',
        '`source_event_type_code` AS `source_event_type_code`',
        '`source_event_type_display` AS `source_event_type_display`',
        '`status_history_count` AS `status_history_count`',
        '`dose_value` AS `dose_value`',
        '`dose_unit` AS `dose_unit`',
        '`initial_dose_value` AS `initial_dose_value`',
        '`initial_dose_unit` AS `initial_dose_unit`',
        '`dose_in_mg` AS `dose_in_mg`',
        '`dose_in_ml` AS `dose_in_ml`',
        '`dose_standardization_status` AS `dose_standardization_status`',
        '`route_code` AS `route_code`',
        '`route_display` AS `route_display`',
        '`site_code` AS `site_code`',
        '`site_display` AS `site_display`',
        '`infused_volume` AS `infused_volume`',
        '`infused_volume_unit` AS `infused_volume_unit`',
        '`infusion_rate` AS `infusion_rate`',
        '`infusion_rate_unit` AS `infusion_rate_unit`',
        '`ingredient_count` AS `ingredient_count`',
        '`performer_practitioner_id` AS `performer_practitioner_id`',
        '`verifier_practitioner_id` AS `verifier_practitioner_id`',
        '`location_code` AS `location_code`',
        '`organization_id` AS `organization_id`',
        '`scheduled_datetime` AS `scheduled_datetime`',
        '`performed_datetime` AS `performed_datetime`',
        '`verified_datetime` AS `verified_datetime`',
        '`order_status_code` AS `order_status_code`',
        '`order_status_display` AS `order_status_display`',
        '`prn_ind` AS `prn_ind`',
        '`iv_ind` AS `iv_ind`',
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
    return _s3_flatten_gold_public(_with_comments(df, CLINICAL_MEDICATION_ADMIN_COLUMN_COMMENTS), "clinical_medication_admin")

# COMMAND ----------

# ==== journey_clinical.medication_dispense ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_MEDICATION_DISPENSE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_patient_event_key` AS `source_patient_event_key`',
    '`pharmacy_issue_id` AS `pharmacy_issue_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    "CASE WHEN CAST(`event_datetime` AS DATE) = DATE'1970-01-01' THEN NULL ELSE `event_datetime` END AS `event_datetime`",
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`medication_code` AS `medication_code`',
    '`status_code` AS `status_code`',
    '`issue_type` AS `issue_type`',
    '`issue_category` AS `issue_category`',
    '`quantity` AS `quantity`',
    '`quantity_unit` AS `quantity_unit`',
    '`issued_containers` AS `issued_containers`',
    '`units_per_container` AS `units_per_container`',
    '`drug_form` AS `drug_form`',
    '`drug_strength` AS `drug_strength`',
    '`location_code` AS `location_code`',
    '`issue_value_gbp` AS `issue_value_gbp`',
    '`source_transaction_identifier` AS `source_transaction_identifier`',
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

# contract v2: QC/batch inputs come from internal _clinical_medication_dispense_metadata; source history stays on the main research table.
CLINICAL_MEDICATION_DISPENSE_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 7,616,956 rows of 7,673,150
    # that are current and attributable; identity_status = 'resolved' keeps the 7,146,842
    # rows of 7,673,150 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_MEDICATION_DISPENSE_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 526,308 of 7,673,150 at the profile.
    "gold.clinical.medication_dispense.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 56,194 of 7,673,150 at the profile.
    "gold.clinical.medication_dispense.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 3,063,192 of 7,673,150 rows (39.9%) when profiled on 2026-08-24.
    "gold.clinical.medication_dispense.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_MEDICATION_DISPENSE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "source_patient_event_key": "Stable patient-event identity retained from the originating Silver row before source splitting.",
    "pharmacy_issue_id": "JAC PHARMACY_ISSUE_ID; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID when resolved; native person foreign key.",
    "encounter_id": "Nullable Millennium ENCNTR_ID; JAC does not supply encounter context.",
    "event_datetime": "Source issue timestamp.",
    "event_end_datetime": "Dispense end timestamp when supplied.",
    "source_coding_system": "Verbatim JAC drug coding system.",
    "source_code": "Verbatim JAC drug identifier.",
    "source_display": "Verbatim JAC drug description.",
    "medication_code": "Source JAC and mapped dm+d medication codings.",
    "status_code": "Source-presence-derived dispense status.",
    "issue_type": "Raw JAC issue type.",
    "issue_category": "Governed JAC transaction category; only ISSUE is typed here.",
    "quantity": "Best-effort parsed total units.",
    "quantity_unit": "Source dose-unit description.",
    "issued_containers": "Parsed source container count.",
    "units_per_container": "Parsed source units per container.",
    "drug_form": "Source drug form.",
    "drug_strength": "Source drug strength.",
    "location_code": "Millennium care-site code supplied by the JAC crosswalk.",
    "issue_value_gbp": "Source-recorded issue value including legitimate negative values outside this route.",
    "source_transaction_identifier": "Raw JAC dailyissues traceability key.",
    "confidentiality_code": "Source confidentiality code when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld-identity indicator when supplied.",
    "source_feed": "Registered source feed owning the typed fact.",
    "record_status": "Retracted when map_pharmacy_issue.SOURCE_PRESENT_IND is false, otherwise active; a missing presence flag does not retract the row. No superseded status is emitted. This is distinct from clinical status_code stopped/completed. Only normalized ISSUE-category rows with usable medication codes are eligible.",
    "record_status_effective_from": "Upstream mirror timestamp; snapshot-level, not a reliable row watermark.",
    "record_status_effective_to": "map_pharmacy_issue.ADC_UPDT only when SOURCE_PRESENT_IND is false; otherwise null. This is the input-load-time end proxy, not ISSUE_DTTM or a clinical dispensing completion time; a missing ADC_UPDT remains null.",
    "source_update_timestamp": "Upstream mirror timestamp; snapshot-level, not a reliable row watermark.",
    "loaded_at": "map_pharmacy_issue.ADC_UPDT carried unchanged through ISSUE-category and usable-code filtering and the QC/public projection. It is distinct from SOURCE_RECORD_UPDATED_DT used for the source-update/history-start fields and from ISSUE_DTTM. QC adds no parent clocks; Silver refresh time is not substituted.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_medication_dispense"),
    comment="Internal quality-controlled twin of clinical_medication_dispense: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_DISPENSE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_MEDICATION_DISPENSE_ADVISORY_RULES)
def _gold_qc_clinical_medication_dispense():
    """Quality-controlled twin of journey_clinical.medication_dispense."""
    df = _qc("clinical_medication_dispense", CLINICAL_MEDICATION_DISPENSE_SELECT)
    return _with_comments(df, CLINICAL_MEDICATION_DISPENSE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.medication_dispense"),
    comment=(
        "One JAC issue/dispense transaction admitted directly by the pharmacy ISSUE route. "
        "Gold QC twin of the silver product: 2 columns are repaired or nulled, 1 rule(s) drop "
        "rows, 3 check(s) are advisory. Each rule states its reason in the pipeline notebook, "
        "and Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_medication_dispense():
    """Contract-v2 public twin of clinical_medication_dispense; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_medication_dispense")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_patient_event_key` AS `source_patient_event_key`',
        '`pharmacy_issue_id` AS `pharmacy_issue_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`__gold_json_medication_code` AS `medication_code`',
        '`status_code` AS `status_code`',
        '`issue_type` AS `issue_type`',
        '`issue_category` AS `issue_category`',
        '`quantity` AS `quantity`',
        '`quantity_unit` AS `quantity_unit`',
        '`issued_containers` AS `issued_containers`',
        '`units_per_container` AS `units_per_container`',
        '`drug_form` AS `drug_form`',
        '`drug_strength` AS `drug_strength`',
        '`location_code` AS `location_code`',
        '`issue_value_gbp` AS `issue_value_gbp`',
        '`source_transaction_identifier` AS `source_transaction_identifier`',
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
    return _s3_flatten_gold_public(_with_comments(df, CLINICAL_MEDICATION_DISPENSE_COLUMN_COMMENTS), "clinical_medication_dispense")

# COMMAND ----------

# ==== journey_clinical.medication_order ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_MEDICATION_ORDER_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_patient_event_key` AS `source_patient_event_key`',
    '`order_id` AS `order_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    "CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE CASE WHEN YEAR(CAST(`event_end_datetime` AS DATE)) >= 9999 OR CAST(`event_end_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_end_datetime` AS DATE)) < 9999 THEN NULL ELSE `event_end_datetime` END END AS `event_end_datetime`",
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`medication_code` AS `medication_code`',
    '`order_status_code` AS `order_status_code`',
    '`order_status_display` AS `order_status_display`',
    '`department_status_code` AS `department_status_code`',
    '`department_status_display` AS `department_status_display`',
    '`active_status_code` AS `active_status_code`',
    '`active_status_display` AS `active_status_display`',
    '`intent_code` AS `intent_code`',
    '`medication_order_type_code` AS `medication_order_type_code`',
    '`medication_order_type_display` AS `medication_order_type_display`',
    '`authored_datetime` AS `authored_datetime`',
    '`effective_start_datetime` AS `effective_start_datetime`',
    "CASE WHEN YEAR(CAST(`projected_stop_datetime` AS DATE)) >= 9999 OR CAST(`projected_stop_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`projected_stop_datetime` AS DATE)) < 9999 THEN NULL ELSE `projected_stop_datetime` END AS `projected_stop_datetime`",
    '`discontinued_datetime` AS `discontinued_datetime`',
    '`frequency_id` AS `frequency_id`',
    '`prn_ind` AS `prn_ind`',
    '`iv_ind` AS `iv_ind`',
    '`suspend_ind` AS `suspend_ind`',
    '`resume_ind` AS `resume_ind`',
    '`discontinue_ind` AS `discontinue_ind`',
    '`requester_practitioner_id` AS `requester_practitioner_id`',
    '`organization_id` AS `organization_id`',
    '`clinical_display_line` AS `clinical_display_line`',
    '`order_detail_display_line` AS `order_detail_display_line`',
    '`status_history_count` AS `status_history_count`',
    '`ingredient_count` AS `ingredient_count`',
    '`order_detail_count` AS `order_detail_count`',
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

# contract v2: QC/batch inputs come from internal _clinical_medication_order_metadata; source history stays on the main research table.
CLINICAL_MEDICATION_ORDER_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 365,848,704 rows of
    # 365,848,704 that are current and attributable; identity_status = 'resolved' keeps the
    # 365,848,704 rows of 365,848,704 that are current and attributable. Superseded versions
    # and rows whose identity was never resolved are not research data, and a consumer who
    # wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_MEDICATION_ORDER_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 464 of 365,848,704
    # rows (0.000127%) when profiled on 2026-08-24.
    "gold.clinical.medication_order.effective_start_datetime.future_owner":
        "NOT COALESCE((CAST(`effective_start_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counted rather than nulled because an order that is still active has a planned stop
    # date, which is in the future by construction. Seen on 7,281 of 365,848,704 rows
    # (0.00199%) when profiled on 2026-08-24.
    "gold.clinical.medication_order.event_end_datetime.future_owner":
        "NOT COALESCE((CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 365,848,704 at the profile.
    "gold.clinical.medication_order.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 7,281 of
    # 365,848,704 rows (0.00199%) when profiled on 2026-08-24.
    "gold.clinical.medication_order.projected_stop_datetime.future_owner":
        "NOT COALESCE((CAST(`projected_stop_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 365,848,704 at the profile.
    "gold.clinical.medication_order.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 180,555 of 365,848,704 rows (0.0494%) when profiled on
    # 2026-08-24.
    "gold.clinical.medication_order.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 27,575 of 365,848,704 rows (0.00754%) when profiled on
    # 2026-08-24.
    "gold.clinical.medication_order.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_MEDICATION_ORDER_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "source_patient_event_key": "Stable patient-event identity retained from the originating Silver row before source splitting.",
    "order_id": "Millennium ORDER_ID; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Subject-key identifier system.",
    "person_id": "Millennium PERSON_ID; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "event_datetime": "Order authored or start time.",
    "event_end_datetime": "Projected stop or discontinue time.",
    "source_coding_system": "Source medication coding system.",
    "source_code": "Source medication synonym identifier.",
    "source_display": "Source medication display.",
    "medication_code": "Source medication CodeableConcept.",
    "order_status_code": "Current source order status code.",
    "order_status_display": "Current source order status display.",
    "department_status_code": "Department workflow status code.",
    "department_status_display": "Department workflow status display.",
    "active_status_code": "Source active-status code.",
    "active_status_display": "Source active-status display.",
    "intent_code": "Medication request intent.",
    "medication_order_type_code": "Source medication-order type code.",
    "medication_order_type_display": "Source medication-order type display.",
    "authored_datetime": "Original order time.",
    "effective_start_datetime": "Current effective start.",
    "projected_stop_datetime": "Projected stop time.",
    "discontinued_datetime": "Discontinue effective time.",
    "frequency_id": "Source frequency identifier.",
    "prn_ind": "True when source `PRN_IND` equals 1, indicating as-needed medication; other non-null values become false and null remains null.",
    "iv_ind": "S10 medication_order; retained as a source-faithful bronze input or Journey standard-block field.",
    "suspend_ind": "Source suspended indicator.",
    "resume_ind": "Source resumed indicator.",
    "discontinue_ind": "Source discontinued indicator.",
    "requester_practitioner_id": "Millennium personnel PERSON_ID for the requesting practitioner.",
    "organization_id": "Millennium ORGANIZATION_ID for the source organization.",
    "clinical_display_line": "Source clinical display line.",
    "order_detail_display_line": "Source order-detail display line.",
    "status_history_count": "Number of retained action rows.",
    "ingredient_count": "Number of retained ingredient rows.",
    "order_detail_count": "Number of retained detail rows.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each medication order record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each medication order record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Registered source feed.",
    "record_status": "Retracted when SOURCE_PRESENT_IND is false (null defaults true); otherwise superseded when INACTIVE_ORDER_FLAG equals 1 (null defaults 0) or ACTIVE_IND equals 0 (null defaults 1); otherwise active. Retraction takes priority. Clinical order status, discontinue flags and child-source presence do not directly determine this label.",
    "record_status_effective_from": "map_medication_order.ORIG_ORDER_DT_TM carried unchanged as the history start. Unlike event_datetime, it has no CURRENT_START_DT_TM fallback; this authored-order time is not an independently observed status-transition timestamp.",
    "record_status_effective_to": "SOURCE_ABSENT_DETECTED_TS for retracted orders, even if null; otherwise first non-null DISCONTINUE_EFFECTIVE_DT_TM then STATUS_DT_TM for superseded orders; otherwise null. There is no fallback between branches, no projected/soft-stop or ingestion fallback, and no sentinel range clamp.",
    "source_update_timestamp": "Greatest of the order's SOURCE_ADC_UPDT and ADC_UPDT plus each action, ingredient and detail group's maximum of greatest(SOURCE_ADC_UPDT, ORDER_SOURCE_ADC_UPDT, ADC_UPDT), grouped by ORDER_ID across all contributing rows. This mixes source and ingestion clocks; it is not solely a native/raw-source update timestamp or Silver refresh time.",
    "loaded_at": "Greatest of map_medication_order.ADC_UPDT and the per-ORDER_ID maximum ADC_UPDT from map_medication_order_action, map_medication_order_ingredient and map_medication_order_detail. All contributing child rows participate, not only the latest action; JSON decoding and quality/public projections preserve this combined ingestion clock.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_medication_order"),
    comment="Internal quality-controlled twin of clinical_medication_order: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_ORDER_MANDATORY_RULES)
@dp.expect_all(CLINICAL_MEDICATION_ORDER_ADVISORY_RULES)
def _gold_qc_clinical_medication_order():
    """Quality-controlled twin of journey_clinical.medication_order."""
    df = _qc("clinical_medication_order", CLINICAL_MEDICATION_ORDER_SELECT)
    return _with_comments(df, CLINICAL_MEDICATION_ORDER_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.medication_order"),
    comment=(
        "One Millennium medication order with current state plus ordered action and detail "
        "evidence. Gold QC twin of the silver product: 4 columns are repaired or nulled, 1 "
        "rule(s) drop rows, 7 check(s) are advisory. Each rule states its reason in the "
        "pipeline notebook, and Lakeflow expectation metrics report what every rule matched "
        "on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_medication_order():
    """Contract-v2 public twin of clinical_medication_order; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_medication_order")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_patient_event_key` AS `source_patient_event_key`',
        '`order_id` AS `order_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`__gold_json_medication_code` AS `medication_code`',
        '`order_status_code` AS `order_status_code`',
        '`order_status_display` AS `order_status_display`',
        '`department_status_code` AS `department_status_code`',
        '`department_status_display` AS `department_status_display`',
        '`active_status_code` AS `active_status_code`',
        '`active_status_display` AS `active_status_display`',
        '`intent_code` AS `intent_code`',
        '`medication_order_type_code` AS `medication_order_type_code`',
        '`medication_order_type_display` AS `medication_order_type_display`',
        '`authored_datetime` AS `authored_datetime`',
        '`effective_start_datetime` AS `effective_start_datetime`',
        '`projected_stop_datetime` AS `projected_stop_datetime`',
        '`discontinued_datetime` AS `discontinued_datetime`',
        '`frequency_id` AS `frequency_id`',
        '`prn_ind` AS `prn_ind`',
        '`iv_ind` AS `iv_ind`',
        '`suspend_ind` AS `suspend_ind`',
        '`resume_ind` AS `resume_ind`',
        '`discontinue_ind` AS `discontinue_ind`',
        '`requester_practitioner_id` AS `requester_practitioner_id`',
        '`organization_id` AS `organization_id`',
        '`clinical_display_line` AS `clinical_display_line`',
        '`order_detail_display_line` AS `order_detail_display_line`',
        '`status_history_count` AS `status_history_count`',
        '`ingredient_count` AS `ingredient_count`',
        '`order_detail_count` AS `order_detail_count`',
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
    return _s3_flatten_gold_public(_with_comments(df, CLINICAL_MEDICATION_ORDER_COLUMN_COMMENTS), "clinical_medication_order")

# COMMAND ----------

# ==== journey_clinical.medication_supply ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_MEDICATION_SUPPLY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`homecare_request_item_id` AS `homecare_request_item_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`request_key` AS `request_key`',
    '`item_seq` AS `item_seq`',
    '`request_status_code` AS `request_status_code`',
    '`request_status_display` AS `request_status_display`',
    '`item_status_code` AS `item_status_code`',
    '`item_status_display` AS `item_status_display`',
    '`item_type` AS `item_type`',
    '`item_description` AS `item_description`',
    '`pack_description` AS `pack_description`',
    '`quantity_requested` AS `quantity_requested`',
    '`quantity_original` AS `quantity_original`',
    '`quantity_delivered` AS `quantity_delivered`',
    '`order_unit` AS `order_unit`',
    '`label_directions` AS `label_directions`',
    '`nfd_reason` AS `nfd_reason`',
    '`supply_start_date` AS `supply_start_date`',
    '`supply_interval` AS `supply_interval`',
    '`supply_period` AS `supply_period`',
    '`request_item_count` AS `request_item_count`',
    '`request_complete_item_count` AS `request_complete_item_count`',
    '`request_released_date` AS `request_released_date`',
    '`location_name` AS `location_name`',
    '`cost_centre_name` AS `cost_centre_name`',
    '`indication` AS `indication`',
    '`clinic` AS `clinic`',
    '`dmd_vtm_concept_id` AS `dmd_vtm_concept_id`',
    '`dmd_vtm_code` AS `dmd_vtm_code`',
    '`dmd_vtm_name` AS `dmd_vtm_name`',
    '`drug_mapping_method` AS `drug_mapping_method`',
    '`care_site_cd` AS `care_site_cd`',
    '`care_site_match_method` AS `care_site_match_method`',
    '`lnkpid` AS `lnkpid`',
    '`name_key` AS `name_key`',
    '`mrn_candidates` AS `mrn_candidates`',
    '`nhs_candidates` AS `nhs_candidates`',
    '`person_match_method` AS `person_match_method`',
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

# contract v2: QC/batch inputs come from internal _clinical_medication_supply_metadata; source history stays on the main research table.
CLINICAL_MEDICATION_SUPPLY_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 271,725 rows of 271,752 that
    # are current and attributable; identity_status = 'resolved' keeps the 269,250 rows of
    # 271,752 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_MEDICATION_SUPPLY_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 2,502 of 271,752 at the profile.
    "gold.clinical.medication_supply.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 27 of 271,752 at the profile.
    "gold.clinical.medication_supply.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 489 of 271,752 rows (0.18%) when profiled on 2026-08-24.
    "gold.clinical.medication_supply.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",
}

CLINICAL_MEDICATION_SUPPLY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "homecare_request_item_id": "JAC HOMECARE_REQUEST_ITEM_ID; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "JAC internal patient link key; not a direct patient identifier.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Source request-created date.",
    "event_end_datetime": "Source request-completed date.",
    "source_coding_system": "Matched dm+d VTM SNOMED CT identifier.",
    "source_code": "Matched dm+d VTM SNOMED CT identifier.",
    "source_display": "Matched dm+d VTM preferred name.",
    "request_key": "JAC homecare request identifier from lnkrb.",
    "item_seq": "Item sequence within the request; NULL for an itemless header.",
    "request_status_code": "Raw JAC header status code.",
    "request_status_display": "COMPLETE, RELEASED, IN_PROGRESS, or provisional CANCELLED_OR_ERROR.",
    "item_status_code": "Raw JAC item status code.",
    "item_status_display": "COMPLETE, INCOMPLETE, or provisional CANCELLED_OR_ERROR.",
    "item_type": "Raw JAC homecare item type.",
    "item_description": "Source medicine or supply description.",
    "pack_description": "Source pack description.",
    "quantity_requested": "Requested quantity from the item.",
    "quantity_original": "Original requested quantity from the item.",
    "quantity_delivered": "Delivered quantity from the item.",
    "order_unit": "Source order unit.",
    "label_directions": "Source dispensing label directions.",
    "nfd_reason": "Source reason the item was not fulfilled.",
    "supply_start_date": "Requested supply start date.",
    "supply_interval": "Source supply interval.",
    "supply_period": "Source supply period.",
    "request_item_count": "Header-reported total item count.",
    "request_complete_item_count": "Header-reported completed item count.",
    "request_released_date": "Source request-released date.",
    "location_name": "Raw JAC homecare location name.",
    "cost_centre_name": "Raw JAC homecare cost-centre name.",
    "indication": "Clinical indication recorded on the request.",
    "clinic": "Clinic recorded on the request.",
    "dmd_vtm_concept_id": "Valid dm+d VTM OMOP concept_id from exact item-description matching.",
    "dmd_vtm_code": "Matched dm+d VTM SNOMED CT identifier.",
    "dmd_vtm_name": "Matched dm+d VTM preferred name.",
    "drug_mapping_method": "VTM_NAME, VTM_SYNONYM, UNMAPPED, DISABLED, or NULL for itemless headers.",
    "care_site_cd": "map_care_site key from a unique exact location-name match.",
    "care_site_match_method": "NAME_EXACT, UNMAPPED, or UNAVAILABLE.",
    "lnkpid": "Direct identifier published and IG-governed at serve time",
    "name_key": "Direct identifier published and IG-governed at serve time",
    "mrn_candidates": "Distinct active Cerner PERSON_ID candidates for the normalised MRN.",
    "nhs_candidates": "Distinct active Cerner PERSON_ID candidates for the checksum-valid NHS number.",
    "person_match_method": "MRN_NHS_AGREE, MRN_ONLY, NHS_ONLY, CONFLICT, AMBIGUOUS, or NONE.",
    "person_match_status": "MATCHED or the reason no PERSON_ID was assigned.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each medication supply record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each medication supply record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each medication supply record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each medication supply record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each medication supply record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_homecare_request.SOURCE_PRESENT_IND is false, otherwise active; missing presence defaults true. No superseded status is emitted. Request/item status, request completion and delivered quantity do not determine this label; usable-code filtering separately controls publication eligibility.",
    "record_status_effective_from": "Source request-created date.",
    "record_status_effective_to": "map_homecare_request.ADC_UPDT only when SOURCE_PRESENT_IND is false; otherwise null. This is the input-load-time end proxy, not REQUEST_COMPLETED or a clinical supply end; a missing ADC_UPDT remains null.",
    "source_update_timestamp": "First non-null map_homecare_request.ITEM_SOURCE_RECORD_UPDATED_DT then SOURCE_RECORD_UPDATED_DT. These are upstream mirror timestamps, with the item clock preferred even when the request clock is later; the expression is coalesce, not greatest, and has no ADC_UPDT or Silver refresh-time fallback.",
    "loaded_at": "map_homecare_request.ADC_UPDT carried unchanged through usable-code filtering and source/public projections. No parent clock is joined or aggregated. This input load clock is distinct from the preferred item/request mirror clock in source_update_timestamp, supply dates and Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_medication_supply"),
    comment="Internal quality-controlled twin of clinical_medication_supply: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_SUPPLY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_MEDICATION_SUPPLY_ADVISORY_RULES)
def _gold_qc_clinical_medication_supply():
    """Quality-controlled twin of journey_clinical.medication_supply."""
    df = _qc(
        "clinical_medication_supply",
        CLINICAL_MEDICATION_SUPPLY_SELECT,
        date_flags=["event_after_death_30d"],
    )
    return _with_comments(df, CLINICAL_MEDICATION_SUPPLY_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.medication_supply"),
    comment=(
        "One JAC homecare supply-request item; supply, never administration. Gold QC twin of "
        "the silver product: 0 columns are repaired or nulled, 1 rule(s) drop rows, 3 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_medication_supply():
    """Contract-v2 public twin of clinical_medication_supply; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_medication_supply")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`homecare_request_item_id` AS `homecare_request_item_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`request_key` AS `request_key`',
        '`item_seq` AS `item_seq`',
        '`request_status_code` AS `request_status_code`',
        '`request_status_display` AS `request_status_display`',
        '`item_status_code` AS `item_status_code`',
        '`item_status_display` AS `item_status_display`',
        '`item_type` AS `item_type`',
        '`item_description` AS `item_description`',
        '`pack_description` AS `pack_description`',
        '`quantity_requested` AS `quantity_requested`',
        '`quantity_original` AS `quantity_original`',
        '`quantity_delivered` AS `quantity_delivered`',
        '`order_unit` AS `order_unit`',
        '`label_directions` AS `label_directions`',
        '`nfd_reason` AS `nfd_reason`',
        '`supply_start_date` AS `supply_start_date`',
        '`supply_interval` AS `supply_interval`',
        '`supply_period` AS `supply_period`',
        '`request_item_count` AS `request_item_count`',
        '`request_complete_item_count` AS `request_complete_item_count`',
        '`request_released_date` AS `request_released_date`',
        '`location_name` AS `location_name`',
        '`cost_centre_name` AS `cost_centre_name`',
        '`indication` AS `indication`',
        '`clinic` AS `clinic`',
        '`dmd_vtm_concept_id` AS `dmd_vtm_concept_id`',
        '`dmd_vtm_code` AS `dmd_vtm_code`',
        '`dmd_vtm_name` AS `dmd_vtm_name`',
        '`drug_mapping_method` AS `drug_mapping_method`',
        '`care_site_cd` AS `care_site_cd`',
        '`care_site_match_method` AS `care_site_match_method`',
        '`lnkpid` AS `lnkpid`',
        '`name_key` AS `name_key`',
        '`mrn_candidates` AS `mrn_candidates`',
        '`nhs_candidates` AS `nhs_candidates`',
        '`person_match_method` AS `person_match_method`',
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
    return _with_comments(df, CLINICAL_MEDICATION_SUPPLY_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.microbiology_isolate ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_MICROBIOLOGY_ISOLATE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`microbiology_isolate_id` AS `microbiology_isolate_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`pathology_result_id` AS `pathology_result_id`',
    '`report_version_key` AS `report_version_key`',
    '`specimen_type_code` AS `specimen_type_code`',
    '`organism_text` AS `organism_text`',
    '`organism_snomed_code` AS `organism_snomed_code`',
    '`organism_omop_concept_id` AS `organism_omop_concept_id`',
    '`suspected_ind` AS `suspected_ind`',
    '`growth_grade` AS `growth_grade`',
    '`organism_code` AS `organism_code`',
    '`panel_code` AS `panel_code`',
    '`isolate_ordinal` AS `isolate_ordinal`',
    '`isolate_comment` AS `isolate_comment`',
    '`lims_no` AS `lims_no`',
    '`parse_status` AS `parse_status`',
    '`parser_version` AS `parser_version`',
    '`lifecycle_status` AS `lifecycle_status`',
    '`is_current` AS `is_current`',
    '`research_qi_only` AS `research_qi_only`',
    '`specimen_key` AS `specimen_key`',
    '`accession_identifier` AS `accession_identifier`',
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

# contract v2: QC/batch inputs come from internal _clinical_microbiology_isolate_metadata; source history stays on the main research table.
CLINICAL_MICROBIOLOGY_ISOLATE_MANDATORY_RULES = {
    # The research surface keeps current, attributable microbiology rows.
    # identity_status = 'resolved' requires a linked person and record_status = 'active' removes retracted/superseded rows.
    # and attributable. Superseded versions and rows whose identity was never resolved are
    # not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_MICROBIOLOGY_ISOLATE_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 0 at the profile.
    "gold.clinical.microbiology_isolate.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 0 at the profile.
    "gold.clinical.microbiology_isolate.record_status.default_view_active":
        "record_status = 'active'",
}

CLINICAL_MICROBIOLOGY_ISOLATE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "microbiology_isolate_id": "Pathology microbiology_isolate_id; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace selected for `subject_key`: Millennium person identifier first, then MRN, then NHS number; nosubject denotes the per-source-record fallback when none is present.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT through the shared pathology identity projection.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Accession sample/report/request fallback time.",
    "event_end_datetime": "Event end time.",
    "source_coding_system": "Organism coding namespace.",
    "source_code": "Organism text or code.",
    "source_display": "Approved SNOMED organism code.",
    "pathology_result_id": "Linked pathology-result fact.",
    "report_version_key": "Deterministic report-version key.",
    "specimen_type_code": "Source specimen type.",
    "organism_text": "Organism as reported.",
    "organism_snomed_code": "SNOMED organism code.",
    "organism_omop_concept_id": "Standard OMOP organism concept.",
    "suspected_ind": "Whether the organism is hedged or suspected.",
    "growth_grade": "Reported growth grade.",
    "organism_code": "WinPath organism code exactly as packed in the antibiogram (e.g. ESCOL).",
    "panel_code": "WinPath susceptibility panel code following the organism (e.g. GU5, 3P); null when blank.",
    "isolate_ordinal": "Zero-based position of this isolate block within the source result value.",
    "isolate_comment": "Text of the [~ annotation lines attached to this isolate, joined with single spaces; null when none.",
    "lims_no": "TFC/LIMS instance number the source row came from; dictionaries are keyed per instance.",
    "parse_status": "ok, or unterminated_block when this isolate block had no terminator before the next opener; row-level orphan_continuation is recorded in validation.",
    "parser_version": "pathology_antibiogram parser version that produced the row.",
    "lifecycle_status": "Inherited source lifecycle.",
    "is_current": "Whether the isolate remains current.",
    "research_qi_only": "Research/QI release flag.",
    "specimen_key": "Deterministic specimen key.",
    "accession_identifier": "Alias-resolved accession identifier.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each microbiology isolate record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each microbiology isolate record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Originating data feed responsible for the record for each microbiology isolate record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_pathology_microbiology_isolate.lifecycle_status is exactly cancelled or entered_in_error; otherwise superseded when is_current is false (null defaults true), otherwise active. Comparisons are case-sensitive without trimming; a null lifecycle_status does not select retraction. Retraction takes priority over supersession; clinical findings and joined identity/link fields are separate.",
    "record_status_effective_from": "Always null as a TIMESTAMP in the current microbiology-isolate projection. No source-status start is supplied here; the clinical event's report/sample/request date fallbacks and ADC_UPDT are not used for this field.",
    "record_status_effective_to": "Always null as a TIMESTAMP in the current microbiology-isolate projection, including when its status or source-current flag changes. No report, clinical-event or ingestion timestamp is substituted as a status end.",
    "source_update_timestamp": "map_pathology_microbiology_isolate.ADC_UPDT carried unchanged from the contributing row, identical to loaded_at. No independent native application-update clock is projected here, and Silver refresh time is not substituted.",
    "loaded_at": "map_pathology_microbiology_isolate.ADC_UPDT carried unchanged through its joins, usable-code filtering and source/public projections. Alias resolution and accession identity add no load clocks; neither joined report/parent timestamps nor the separately aggregated accession evidence clock replace this value. It is identical to source_update_timestamp, not the clinical event time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_microbiology_isolate"),
    comment="Internal quality-controlled twin of clinical_microbiology_isolate: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_MICROBIOLOGY_ISOLATE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_MICROBIOLOGY_ISOLATE_ADVISORY_RULES)
def _gold_qc_clinical_microbiology_isolate():
    """Quality-controlled twin of journey_clinical.microbiology_isolate."""
    df = _qc("clinical_microbiology_isolate", CLINICAL_MICROBIOLOGY_ISOLATE_SELECT)
    return _with_comments(df, CLINICAL_MICROBIOLOGY_ISOLATE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.microbiology_isolate"),
    comment=(
        "One organism/isolate finding per accession, parsed from the packed WinPath antibiogram in map_pathology (S2.5, parser 1.0.0). Person linkage follows the accession identity; unresolved rows stay in silver and drop at gold. Gold keeps identity_status='resolved' AND record_status='active'. Gold QC twin of the silver product: 0 columns are "
        "repaired or nulled, 1 rule(s) drop rows, 2 check(s) are advisory. Each rule states "
        "its reason in the pipeline notebook, and Lakeflow expectation metrics report what "
        "every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_microbiology_isolate():
    """Contract-v2 public twin of clinical_microbiology_isolate; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_microbiology_isolate")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`microbiology_isolate_id` AS `microbiology_isolate_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`pathology_result_id` AS `pathology_result_id`',
        '`report_version_key` AS `report_version_key`',
        '`specimen_type_code` AS `specimen_type_code`',
        '`organism_text` AS `organism_text`',
        '`organism_snomed_code` AS `organism_snomed_code`',
        '`organism_omop_concept_id` AS `organism_omop_concept_id`',
        '`suspected_ind` AS `suspected_ind`',
        '`growth_grade` AS `growth_grade`',
        '`organism_code` AS `organism_code`',
        '`panel_code` AS `panel_code`',
        '`isolate_ordinal` AS `isolate_ordinal`',
        '`isolate_comment` AS `isolate_comment`',
        '`lims_no` AS `lims_no`',
        '`parse_status` AS `parse_status`',
        '`parser_version` AS `parser_version`',
        '`lifecycle_status` AS `lifecycle_status`',
        '`is_current` AS `is_current`',
        '`research_qi_only` AS `research_qi_only`',
        '`specimen_key` AS `specimen_key`',
        '`accession_identifier` AS `accession_identifier`',
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
    return _with_comments(df, CLINICAL_MICROBIOLOGY_ISOLATE_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.pathology_order ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_PATHOLOGY_ORDER_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`source_object` AS `source_object`',
    '`wkg_code` AS `wkg_code`',
    "CASE WHEN UPPER(TRIM(CAST(`tlc_code` AS STRING))) = '0' OR UPPER(TRIM(CAST(`tlc_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `tlc_code` END AS `tlc_code`",
    '`order_id` AS `order_id`',
    '`order_mnemonic` AS `order_mnemonic`',
    '`raw_request_text` AS `raw_request_text`',
    '`test_description` AS `test_description`',
    '`test_snomed_code` AS `test_snomed_code`',
    '`test_omop_concept_id` AS `test_omop_concept_id`',
    '`mapping_status` AS `mapping_status`',
    '`request_ordinal` AS `request_ordinal`',
    '`specimen_key` AS `specimen_key`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_pathology_order_metadata; source history stays on the main research table.
CLINICAL_PATHOLOGY_ORDER_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 388,597,322 rows of
    # 388,597,322 that are current and attributable; identity_status = 'resolved' keeps the
    # 314,089,167 rows of 388,597,322 that are current and attributable. Superseded versions
    # and rows whose identity was never resolved are not research data, and a consumer who
    # wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_PATHOLOGY_ORDER_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 74,508,155 of 388,597,322 at the profile.
    "gold.clinical.pathology_order.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 388,597,322 at the profile.
    "gold.clinical.pathology_order.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 285,411 of 388,597,322 rows (0.0734%) when profiled on
    # 2026-08-24.
    "gold.clinical.pathology_order.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1,203,076 of 388,597,322 rows (0.31%) when profiled on
    # 2026-08-24.
    "gold.clinical.pathology_order.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_PATHOLOGY_ORDER_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic pathology-order event key minted from requested_test_occurrence_id.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Requested or source-validity timestamp.",
    "event_end_datetime": "Order event end when supplied.",
    "source_coding_system": "Native order catalogue coding system.",
    "source_code": "Native order catalogue code.",
    "source_display": "Native order description.",
    "source_object": "Source arm normalized from the existing source_system value.",
    "wkg_code": "TFC work-group code.",
    "tlc_code": "TFC test-level code.",
    "order_id": "CERNER ORDER_ID as BIGINT; NULL on the TFC_LIMS arm.",
    "order_mnemonic": "CERNER order mnemonic.",
    "raw_request_text": "Native request wording. Identifiable free text; ig_risk 4",
    "test_description": "Native requested-test description.",
    "test_snomed_code": "Source-provided requested-test SNOMED code.",
    "test_omop_concept_id": "Source-provided requested-test OMOP concept identifier.",
    "mapping_status": "'mapped' means a rule ran, not that codes landed — CERNER arm carries zero baked codes.",
    "request_ordinal": "Source request ordinal within the accession.",
    "specimen_key": "Alias-resolved accession-minted specimen key.",
    "confidentiality_code": "Source confidentiality code.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each pathology order record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "record_status": "Literal active for every retained requested-test occurrence from map_pathology_requested_test. This projection does not evaluate source presence, cancellation or result/report status; active is not evidence that an order has never been withdrawn.",
    "record_status_effective_from": "Always NULL as a TIMESTAMP in the pathology-order projection. No status-start clock is supplied, and accession request/sample times are not substituted.",
    "record_status_effective_to": "Always NULL as a TIMESTAMP in the pathology-order projection. No status-end clock is supplied, and result, report or ingestion timestamps are not substituted.",
    "source_update_timestamp": "Always NULL as a TIMESTAMP in the pathology-order projection. The contributing ADC_UPDT is published separately as loaded_at and is not copied into this field.",
    "loaded_at": "map_pathology_requested_test.ADC_UPDT carried unchanged for the requested-test occurrence. Accession alias and identity joins do not contribute another clock. This is ingestion provenance, not the request/sample event time or Silver refresh time; a missing timestamp remains null.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_pathology_order"),
    comment="Internal quality-controlled twin of clinical_pathology_order: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_PATHOLOGY_ORDER_MANDATORY_RULES)
@dp.expect_all(CLINICAL_PATHOLOGY_ORDER_ADVISORY_RULES)
def _gold_qc_clinical_pathology_order():
    """Quality-controlled twin of journey_clinical.pathology_order."""
    # 525 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "clinical_pathology_order",
        CLINICAL_PATHOLOGY_ORDER_SELECT,
        fk_columns=["person_id"],
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, CLINICAL_PATHOLOGY_ORDER_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.pathology_order"),
    comment=(
        "One requested-test occurrence across TFC_LIMS and CERNER; anchors accession-scoped "
        "order-to-report request threads. Gold QC twin of the silver product: 3 columns are "
        "repaired or nulled, 1 rule(s) drop rows, 4 check(s) are advisory. Each rule states "
        "its reason in the pipeline notebook, and Lakeflow expectation metrics report what "
        "every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_pathology_order():
    """Contract-v2 public twin of clinical_pathology_order; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_pathology_order")).selectExpr(
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
        '`source_object` AS `source_object`',
        '`wkg_code` AS `wkg_code`',
        '`tlc_code` AS `tlc_code`',
        '`order_id` AS `order_id`',
        '`order_mnemonic` AS `order_mnemonic`',
        '`raw_request_text` AS `raw_request_text`',
        '`test_description` AS `test_description`',
        '`test_snomed_code` AS `test_snomed_code`',
        '`test_omop_concept_id` AS `test_omop_concept_id`',
        '`mapping_status` AS `mapping_status`',
        '`request_ordinal` AS `request_ordinal`',
        '`specimen_key` AS `specimen_key`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_PATHOLOGY_ORDER_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.pathology_report ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_PATHOLOGY_REPORT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`report_code` AS `report_code`',
    '`report_role` AS `report_role`',
    '`discipline` AS `discipline`',
    '`report_section` AS `report_section`',
    '`lifecycle_status` AS `lifecycle_status`',
    '`version_ordinal` AS `version_ordinal`',
    '`version_count` AS `version_count`',
    '`report_version_key` AS `report_version_key`',
    '`supersedes_report_version_key` AS `supersedes_report_version_key`',
    '`is_current_present` AS `is_current_present`',
    '`document_key` AS `document_key`',
    'CASE WHEN CAST(`issued_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `issued_datetime` END AS `issued_datetime`',
    '`specimen_key` AS `specimen_key`',
    '`accession_identifier` AS `accession_identifier`',
    '`report_text_hash` AS `report_text_hash`',
    '`research_qi_only` AS `research_qi_only`',
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

# contract v2: QC/batch inputs come from internal _clinical_pathology_report_metadata; source history stays on the main research table.
CLINICAL_PATHOLOGY_REPORT_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 91,718,730 rows of 91,759,012
    # that are current and attributable; identity_status = 'resolved' keeps the 71,790,751
    # rows of 91,759,012 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_PATHOLOGY_REPORT_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 19,968,261 of 91,759,012 at the profile.
    "gold.clinical.pathology_report.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 40,282 of 91,759,012 at the profile.
    "gold.clinical.pathology_report.record_status.default_view_active":
        "record_status = 'active'",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 7 of 91,759,012
    # rows (7.63e-06%) when profiled on 2026-08-24.
    "gold.clinical.pathology_report.record_status_effective_from.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 167,974 of 91,759,012 rows (0.183%) when profiled on 2026-08-24.
    "gold.clinical.pathology_report.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 6,142 of 91,759,012 rows (0.00669%) when profiled on 2026-08-24.
    "gold.clinical.pathology_report.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_PATHOLOGY_REPORT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Report issue or result timestamp.",
    "event_end_datetime": "Report event end.",
    "source_coding_system": "Source test coding system.",
    "source_code": "Source test code.",
    "source_display": "Source test display.",
    "report_code": "Source and mapped report codings.",
    "report_role": "Latest-version report role.",
    "discipline": "Latest-version pathology discipline.",
    "report_section": "Latest-version report section.",
    "lifecycle_status": "Latest-version lifecycle status.",
    "version_ordinal": "Latest version ordinal in the report series.",
    "version_count": "Number of report versions in the series.",
    "report_version_key": "Deterministic report-version key.",
    "supersedes_report_version_key": "Deterministic prior report-version key.",
    "is_current_present": "True when the series has a current version row.",
    "document_key": "Deterministic text-document key.",
    "issued_datetime": "Report issue timestamp.",
    "specimen_key": "Deterministic specimen key.",
    "accession_identifier": "Alias-resolved canonical pathology accession identifier carried by the selected report-series version; not the source result-record key.",
    "report_text_hash": "Bronze hash for the latest report version",
    "research_qi_only": "Registry doctrine flag; true on 100% of rows today — describe-only",
    "confidentiality_code": "Source confidentiality classification attached to the record for each pathology report record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each pathology report record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Registered owning feed.",
    "record_status": "Derived from lifecycle_status of the selected map_pathology_report version: retracted for cancelled or entered_in_error, otherwise active, including a null lifecycle status. Selection is the grouped maximum full version struct, beginning with version_ordinal, report_version_id and source_record_key. This label is not the is_current_present flag and does not emit superseded.",
    "record_status_effective_from": "valid_from of the selected map_pathology_report version, carried unchanged as the report-series history start. The selected full version struct is ranked first by version_ordinal, report_version_id and source_record_key. This is not issued_datetime, a group-wide maximum validity timestamp or Silver refresh time; a missing value remains null.",
    "record_status_effective_to": "Always NULL as a TIMESTAMP in the pathology-report-series projection, including retracted reports. No cancellation, successor-version or ingestion timestamp is substituted as a status end.",
    "source_update_timestamp": "valid_from of the selected map_pathology_report version, carried unchanged. This field is a selected-version validity timestamp, not an independent source modification clock, the group-wide maximum ADC_UPDT or Silver refresh time; a missing value remains null.",
    "loaded_at": "Maximum map_pathology_report.ADC_UPDT across all contributing versions in the report_series_id group, not only the version selected for display. Accession aliases and identity/link lookups supply no additional load clock. This is ingestion provenance, not report issue time or Silver refresh time; null when all contributing clocks are null.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_pathology_report"),
    comment="Internal quality-controlled twin of clinical_pathology_report: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_PATHOLOGY_REPORT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_PATHOLOGY_REPORT_ADVISORY_RULES)
def _gold_qc_clinical_pathology_report():
    """Quality-controlled twin of journey_clinical.pathology_report."""
    df = _qc("clinical_pathology_report", CLINICAL_PATHOLOGY_REPORT_SELECT)
    return _with_comments(df, CLINICAL_PATHOLOGY_REPORT_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.pathology_report"),
    comment=(
        "One pathology report series keyed by report_series_id using the latest-version "
        "projection; per-version text lives in journey_text.document. Gold QC twin of the "
        "silver product: 8 columns are repaired or nulled, 1 rule(s) drop rows, 5 check(s) "
        "are advisory. Each rule states its reason in the pipeline notebook, and Lakeflow "
        "expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_pathology_report():
    """Contract-v2 public twin of clinical_pathology_report; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_pathology_report")).selectExpr(
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
        '`__gold_json_report_code` AS `report_code`',
        '`report_role` AS `report_role`',
        '`discipline` AS `discipline`',
        '`report_section` AS `report_section`',
        '`lifecycle_status` AS `lifecycle_status`',
        '`version_ordinal` AS `version_ordinal`',
        '`version_count` AS `version_count`',
        '`report_version_key` AS `report_version_key`',
        '`supersedes_report_version_key` AS `supersedes_report_version_key`',
        '`is_current_present` AS `is_current_present`',
        '`document_key` AS `document_key`',
        '`issued_datetime` AS `issued_datetime`',
        '`specimen_key` AS `specimen_key`',
        '`accession_identifier` AS `accession_identifier`',
        '`report_text_hash` AS `report_text_hash`',
        '`research_qi_only` AS `research_qi_only`',
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
    return _s3_flatten_gold_public(_with_comments(df, CLINICAL_PATHOLOGY_REPORT_COLUMN_COMMENTS), "clinical_pathology_report")

# COMMAND ----------

# ==== journey_clinical.pathology_result ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_PATHOLOGY_RESULT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    'CASE WHEN CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`result_code` AS `result_code`',
    '`pathology_report_key` AS `pathology_report_key`',
    '`specimen_key` AS `specimen_key`',
    '`equivalence_group_key` AS `equivalence_group_key`',
    '`representation_role` AS `representation_role`',
    '`preferred_result_ind` AS `preferred_result_ind`',
    '`person_projection_status` AS `person_projection_status`',
    'CASE WHEN ABS(CAST(`value_number` AS DOUBLE)) > 1e12 THEN NULL ELSE `value_number` END AS `value_number`',
    "CASE WHEN TRIM(CAST(`value_text` AS STRING)) = '' THEN NULL ELSE `value_text` END AS `value_text`",
    "CASE WHEN CAST(`value_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`value_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`value_datetime` AS DATE)) < 9999 OR CAST(`value_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`value_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `value_datetime` END END AS `value_datetime`",
    '`value_concept_id` AS `value_concept_id`',
    '`value_concept_display` AS `value_concept_display`',
    '`operator_concept_id` AS `operator_concept_id`',
    "CASE WHEN TRIM(CAST(`unit_source_value` AS STRING)) = '' THEN NULL ELSE `unit_source_value` END AS `unit_source_value`",
    '`ucum_code` AS `ucum_code`',
    '`unit_concept_id` AS `unit_concept_id`',
    '`reference_range_low` AS `reference_range_low`',
    '`reference_range_high` AS `reference_range_high`',
    '`interpretation_code` AS `interpretation_code`',
    '`polarity` AS `polarity`',
    '`finding_axis` AS `finding_axis`',
    '`result_status` AS `result_status`',
    "CASE WHEN UPPER(TRIM(CAST(`body_site_code` AS STRING))) = '0' THEN NULL ELSE `body_site_code` END AS `body_site_code`",
    '`clinician_code` AS `clinician_code`',
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

# contract v2: QC/batch inputs come from internal _clinical_pathology_result_metadata; source history stays on the main research table.
CLINICAL_PATHOLOGY_RESULT_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 1,497,882,424 rows of
    # 1,877,832,347 that are current and attributable; preferred_result_ind = TRUE keeps the
    # 0 rows of 1,877,832,347 that are current and attributable. Superseded versions and
    # rows whose identity was never resolved are not research data, and a consumer who wants
    # them has silver.
    "research_surface": "(identity_status = 'resolved') AND (preferred_result_ind = TRUE)",
}

CLINICAL_PATHOLOGY_RESULT_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 379,949,923 of 1,877,832,347 at the profile.
    "gold.clinical.pathology_result.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing preferred_result_ind = TRUE.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 1,877,832,347 of 1,877,832,347 at the profile.
    "gold.clinical.pathology_result.preferred_result_ind.default_view_preferred":
        "preferred_result_ind = TRUE",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1,240,568 of 1,877,832,347 rows (0.0661%) when profiled on
    # 2026-08-24.
    "gold.clinical.pathology_result.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 5,132,232 of 1,877,832,347 rows (0.273%) when profiled on
    # 2026-08-24.
    "gold.clinical.pathology_result.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",

    # Negative values here are signed by design -- credit lines in the costing feed and
    # below-zero readings on a calibrated scale -- so they are counted, not removed. Seen on
    # 4,908,621 of 1,877,832,347 rows (0.261%) when profiled on 2026-08-24.
    "gold.clinical.pathology_result.value_number.negative_mass_scale_owner":
        "NOT COALESCE((`value_number` < 0), FALSE)",
}

CLINICAL_PATHOLOGY_RESULT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date and time when the represented clinical or administrative event occurred for each pathology result record. It is derived from bronze field `measurement_datetime` in `4_prod.bronze.map_pathology`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "event_end_datetime": "Measurement end timestamp.",
    "source_coding_system": "Source test coding system.",
    "source_code": "Source test code.",
    "source_display": "Source test display.",
    "result_code": "Source and mapped test codings.",
    "pathology_report_key": "Deterministic pathology-report series key.",
    "specimen_key": "Deterministic specimen key.",
    "equivalence_group_key": "Derived cross-arm result-equivalence key.",
    "representation_role": "Result representation role within the equivalence group.",
    "preferred_result_ind": "Whether this is the preferred representation.",
    "person_projection_status": "Equivalence-layer person projection state.",
    "value_number": "Numeric result value.",
    "value_text": "Verbatim result value.",
    "value_datetime": "Datetime result value.",
    "value_concept_id": "Coded result concept identifier.",
    "value_concept_display": "Coded result concept display.",
    "operator_concept_id": "Result comparison operator concept.",
    "unit_source_value": "Verbatim source unit.",
    "ucum_code": "UCUM unit code.",
    "unit_concept_id": "OMOP unit concept identifier.",
    "reference_range_low": "Reference-range lower bound.",
    "reference_range_high": "Reference-range upper bound.",
    "interpretation_code": "Source interpretation or normalcy.",
    "polarity": "Source result polarity or growth grade.",
    "finding_axis": "Source finding/result axis.",
    "result_status": "Source result status.",
    "body_site_code": "Source body-site code.",
    "clinician_code": "Source clinician code.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each pathology result record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each pathology result record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Registered owning feed.",
    "record_status": "Superseded when map_pathology.valid_until_dt_tm is non-null and earlier than 2100-01-01; otherwise active. No retracted status is emitted. Result-status text, equivalence preference and parent linkage do not determine this label; the usable-code filter controls eligibility separately.",
    "record_status_effective_from": "Timestamp from which the normalised record status applies, when the source supplies one.",
    "record_status_effective_to": "map_pathology.valid_until_dt_tm only when non-null and earlier than 2100-01-01; otherwise null. The predicate is identical to the superseded-status test. Unlike event timestamps, this history end has no 1950 lower clamp; neither event_end_dt_tm nor ingestion time is substituted.",
    "source_update_timestamp": "Greatest contributing source update timestamp; never changed by mapping-only backfills.",
    "loaded_at": "map_pathology.ADC_UPDT carried unchanged through parent-link and result-equivalence joins, usable-code filtering and QC/public projections. Those joins select linkage/equivalence fields, not additional clocks; parent-link loaded_at is not propagated. This differs from source_adc_updt used for source_update_timestamp and from Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_pathology_result"),
    comment="Internal quality-controlled twin of clinical_pathology_result: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_PATHOLOGY_RESULT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_PATHOLOGY_RESULT_ADVISORY_RULES)
def _gold_qc_clinical_pathology_result():
    """Quality-controlled twin of journey_clinical.pathology_result."""
    df = _qc("clinical_pathology_result", CLINICAL_PATHOLOGY_RESULT_SELECT)
    return _with_comments(df, CLINICAL_PATHOLOGY_RESULT_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.pathology_result"),
    comment=(
        "One pathology result with typed value, units, range, interpretation and mapping "
        "evidence. Gold QC twin of the silver product: 11 columns are repaired or nulled, 1 "
        "rule(s) drop rows, 5 check(s) are advisory. Each rule states its reason in the "
        "pipeline notebook, and Lakeflow expectation metrics report what every rule matched "
        "on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_pathology_result():
    """Contract-v2 public twin of clinical_pathology_result; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_pathology_result")).selectExpr(
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
        '`__gold_json_result_code` AS `result_code`',
        '`pathology_report_key` AS `pathology_report_key`',
        '`specimen_key` AS `specimen_key`',
        '`equivalence_group_key` AS `equivalence_group_key`',
        '`representation_role` AS `representation_role`',
        '`preferred_result_ind` AS `preferred_result_ind`',
        '`person_projection_status` AS `person_projection_status`',
        '`value_number` AS `value_number`',
        '`value_text` AS `value_text`',
        '`value_datetime` AS `value_datetime`',
        '`value_concept_id` AS `value_concept_id`',
        '`value_concept_display` AS `value_concept_display`',
        '`operator_concept_id` AS `operator_concept_id`',
        '`unit_source_value` AS `unit_source_value`',
        '`ucum_code` AS `ucum_code`',
        '`unit_concept_id` AS `unit_concept_id`',
        '`reference_range_low` AS `reference_range_low`',
        '`reference_range_high` AS `reference_range_high`',
        '`interpretation_code` AS `interpretation_code`',
        '`polarity` AS `polarity`',
        '`finding_axis` AS `finding_axis`',
        '`result_status` AS `result_status`',
        '`body_site_code` AS `body_site_code`',
        '`clinician_code` AS `clinician_code`',
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
    return _s3_flatten_gold_public(_with_comments(df, CLINICAL_PATHOLOGY_RESULT_COLUMN_COMMENTS), "clinical_pathology_result")

# COMMAND ----------

# ==== journey_clinical.procedure ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_PROCEDURE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_patient_event_key` AS `source_patient_event_key`',
    '`source_object` AS `source_object`',
    '`procedure_id` AS `procedure_id`',
    '`endobase_exam_id` AS `endobase_exam_id`',
    '`implant_event_id` AS `implant_event_id`',
    '`implant_sequence` AS `implant_sequence`',
    '`surg_case_proc_id` AS `surg_case_proc_id`',
    '`source_row_hash` AS `source_row_hash`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    "CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS DATE) = DATE'1899-12-30' THEN NULL ELSE `event_datetime` END END AS `event_datetime`",
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` OR YEAR(CAST(`event_end_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    '`procedure_code` AS `procedure_code`',
    '`status_code` AS `status_code`',
    '`status_display` AS `status_display`',
    "CASE WHEN CAST(`performed_start` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`performed_start` AS DATE) = DATE'1899-12-30' THEN NULL ELSE `performed_start` END END AS `performed_start`",
    'CASE WHEN `performed_start` IS NOT NULL AND `performed_end` IS NOT NULL AND `performed_start` > `performed_end` OR YEAR(CAST(`performed_end` AS TIMESTAMP)) = 1970 THEN NULL ELSE `performed_end` END AS `performed_end`',
    '`body_site_code` AS `body_site_code`',
    '`body_site_display` AS `body_site_display`',
    "CASE WHEN UPPER(TRIM(CAST(`laterality_code` AS STRING))) = '0' THEN NULL ELSE `laterality_code` END AS `laterality_code`",
    '`laterality_display` AS `laterality_display`',
    '`performer_practitioner_id` AS `performer_practitioner_id`',
    "CASE WHEN UPPER(TRIM(CAST(`procedure_location_code` AS STRING))) = '0' THEN NULL ELSE `procedure_location_code` END AS `procedure_location_code`",
    '`procedure_location_display` AS `procedure_location_display`',
    '`procedure_note` AS `procedure_note`',
    '`implant_description` AS `implant_description`',
    '`device_code` AS `device_code`',
    '`device_display` AS `device_display`',
    '`manufacturer` AS `manufacturer`',
    '`serial_number` AS `serial_number`',
    '`batch_number` AS `batch_number`',
    '`udi_di` AS `udi_di`',
    '`udi_standard` AS `udi_standard`',
    'CASE WHEN ABS(CAST(`quantity` AS DOUBLE)) > 1e12 THEN NULL ELSE `quantity` END AS `quantity`',
    '`implant_attribute_count` AS `implant_attribute_count`',
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

# contract v2: QC/batch inputs come from internal _clinical_procedure_metadata; source history stays on the main research table.
CLINICAL_PROCEDURE_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 16,267,993 rows of
    # 16,307,178 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_PROCEDURE_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 39,185 of 16,307,178 at the profile.
    "gold.clinical.procedure.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 3,038 of
    # 16,307,178 rows (0.0186%) when profiled on 2026-08-24.
    "gold.clinical.procedure.record_status_effective_from.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1,009 of 16,307,178 rows (0.00619%) when profiled on 2026-08-24.
    "gold.clinical.procedure.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 41,288 of 16,307,178 rows (0.253%) when profiled on 2026-08-24.
    "gold.clinical.procedure.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_PROCEDURE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "source_patient_event_key": "Stable patient-event identity retained from the originating Silver row before source splitting.",
    "source_object": "Source arm: millennium_procedure, implant, theatre, endobase_exam, or nccmds.",
    "procedure_id": "Millennium PROCEDURE_ID; primary key for the millennium_procedure arm.",
    "endobase_exam_id": "Endobase ENDOBASE_EXAM_ID; primary key for the endobase_exam arm.",
    "implant_event_id": "Millennium implant EVENT_ID; first component of the implant-arm primary key.",
    "implant_sequence": "Millennium IMPLANT_SEQUENCE; second component of the implant-arm primary key.",
    "surg_case_proc_id": "SurgiNet SURG_CASE_PROC_ID; primary key for the theatre arm.",
    "source_row_hash": "Native ROW_HASH for the nccmds arm; NULL for other arms.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Primary performed timestamp.",
    "event_end_datetime": "Procedure end timestamp.",
    "source_coding_system": "Verbatim source coding system.",
    "source_code": "Source procedure code or implant label",
    "source_display": "Verbatim source procedure display.",
    "procedure_code": "Source and mapped procedure codings.",
    "status_code": "Source-derived FHIR procedure status.",
    "status_display": "Source status display.",
    "performed_start": "Procedure performed start.",
    "performed_end": "Procedure performed end.",
    "body_site_code": "Body-site code where supplied.",
    "body_site_display": "Body-site display where supplied.",
    "laterality_code": "Source laterality code.",
    "laterality_display": "Source laterality display.",
    "performer_practitioner_id": "Millennium personnel PERSON_ID as BIGINT when available.",
    "procedure_location_code": "Source procedure-location code retained without asserting a location-dimension FK.",
    "procedure_location_display": "Source procedure-location display.",
    "procedure_note": "Source procedure note.",
    "implant_description": "Implant description for implant-placement facts.",
    "device_code": "Device concept identifier for implant-placement facts.",
    "device_display": "Device concept display.",
    "manufacturer": "Implant manufacturer name",
    "serial_number": "Implant serial number.",
    "batch_number": "Implant batch number.",
    "udi_di": "Unique device identifier device identifier.",
    "udi_standard": "UDI issuing standard.",
    "quantity": "Parsed implant quantity.",
    "implant_attribute_count": "Number of retained implant attribute rows.",
    "confidentiality_code": "Source confidentiality code.",
    "vip_ind": "Source VIP indicator.",
    "withheld_identity_ind": "Identity-withholding status is not populated by the current source projections; null does not mean identity was not withheld.",
    "source_feed": "Registered source feed owning the fact.",
    "record_status": "Arm-specific label: Millennium procedures are superseded when lower(coalesce(active_status_desc, empty string)) contains the substring inactive, otherwise active. Implants are always active. Theatre procedures are superseded for source presence false (null defaults true) or ACTIVE_IND cast to LONG zero (null defaults one), otherwise active. EndoBase exams are superseded for source presence false (null defaults true), otherwise active. NCCMDS procedures instead map source absence to retracted, otherwise active. Clinical completion/cancellation fields remain separate.",
    "record_status_effective_from": "Millennium: ACTIVE_STATUS_DT_TM. Implant: VALID_FROM_DT_TM. Theatre: first non-null procedure PROC_START_DT_TM then parent case SCHED_START_DT_TM, not the wider performed-time fallback. NCCMDS: OPCS_Proc_Dt_CLEAN. EndoBase: first non-null PERFORMED_TS_CLEAN, EXAM_TS_CLEAN, TRUE_START_TS_CLEAN then START_TS_CLEAN. These arm-specific values are carried unchanged; no ingestion-time or cross-arm fallback is added.",
    "record_status_effective_to": "Millennium: ACTIVE_STATUS_DT_TM only when inactive. Implant: always null. Theatre: first non-null procedure SOURCE_ABSENT_DETECTED_TS then procedure ADC_UPDT only when inactive; the parent case clock does not contribute. NCCMDS: own ADC_UPDT only when retracted. EndoBase: own ADC_UPDT only when source-absent/superseded. All other rows have null ends; clinical procedure/exam end timestamps are not substituted.",
    "source_update_timestamp": "Source-arm-specific timestamp; interpret with source_object. Cerner procedures carry map_procedure.UPDT_DT_TM; implants carry map_implant_details.CLINSIG_UPDT_DT_TM; theatre carries the greatest SOURCE_ADC_UPDT from its case and case-procedure rows. EndoBase exams and NCCMDS procedures carry their bronze PIPELINE_UPDT_DT_TM. This mixes source and processing clocks and is not a uniform clinical event time or native-system update time.",
    "loaded_at": "Millennium, NCCMDS and EndoBase arms use their own ADC_UPDT. Theatre uses greatest of procedure ADC_UPDT and joined case ADC_UPDT. Implant first takes coalesce(SOURCE_MAX_ADC_UPDT, ADC_UPDT, BASE_EVENT_ADC_UPDT), in that order, then greatest of this result and maximum ADC_UPDT from matching map_implant_detail_events grouped by patient_event_key. Attribute matching uses the base clinical-event ID or implant-form parent-event ID. The ordered implant fallback is not itself a maximum; the five-arm union and QC add no further clocks or Silver refresh timestamp.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_procedure"),
    comment="Internal quality-controlled twin of clinical_procedure: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_PROCEDURE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_PROCEDURE_ADVISORY_RULES)
def _gold_qc_clinical_procedure():
    """Quality-controlled twin of journey_clinical.procedure."""
    df = _qc("clinical_procedure", CLINICAL_PROCEDURE_SELECT)
    return _with_comments(df, CLINICAL_PROCEDURE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.procedure"),
    comment=(
        "One performed procedure or implant-placement source fact without cross-feed "
        "deduplication. Gold QC twin of the silver product: 13 columns are repaired or "
        "nulled, 1 rule(s) drop rows, 4 check(s) are advisory. Each rule states its reason in "
        "the pipeline notebook, and Lakeflow expectation metrics report what every rule "
        "matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_procedure():
    """Contract-v2 public twin of clinical_procedure; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_procedure")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_patient_event_key` AS `source_patient_event_key`',
        '`source_object` AS `source_object`',
        '`procedure_id` AS `procedure_id`',
        '`endobase_exam_id` AS `endobase_exam_id`',
        '`implant_event_id` AS `implant_event_id`',
        '`implant_sequence` AS `implant_sequence`',
        '`surg_case_proc_id` AS `surg_case_proc_id`',
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
        '`__gold_json_procedure_code` AS `procedure_code`',
        '`status_code` AS `status_code`',
        '`status_display` AS `status_display`',
        '`performed_start` AS `performed_start`',
        '`performed_end` AS `performed_end`',
        '`body_site_code` AS `body_site_code`',
        '`body_site_display` AS `body_site_display`',
        '`laterality_code` AS `laterality_code`',
        '`laterality_display` AS `laterality_display`',
        '`performer_practitioner_id` AS `performer_practitioner_id`',
        '`procedure_location_code` AS `procedure_location_code`',
        '`procedure_location_display` AS `procedure_location_display`',
        '`procedure_note` AS `procedure_note`',
        '`implant_description` AS `implant_description`',
        '`__gold_json_device_code` AS `device_code`',
        '`device_display` AS `device_display`',
        '`manufacturer` AS `manufacturer`',
        '`serial_number` AS `serial_number`',
        '`batch_number` AS `batch_number`',
        '`udi_di` AS `udi_di`',
        '`udi_standard` AS `udi_standard`',
        '`quantity` AS `quantity`',
        '`implant_attribute_count` AS `implant_attribute_count`',
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
    return _s3_flatten_gold_public(_with_comments(df, CLINICAL_PROCEDURE_COLUMN_COMMENTS), "clinical_procedure")

# COMMAND ----------

# ==== journey_clinical.research_enrollment ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_RESEARCH_ENROLLMENT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`pt_prot_reg_id` AS `pt_prot_reg_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`research_study_key` AS `research_study_key`',
    '`registration_id` AS `registration_id`',
    '`protocol_accession_number` AS `protocol_accession_number`',
    '`protocol_arm_id` AS `protocol_arm_id`',
    '`status_code` AS `status_code`',
    '`status_desc` AS `status_desc`',
    '`off_study_datetime` AS `off_study_datetime`',
    '`treatment_start_datetime` AS `treatment_start_datetime`',
    '`treatment_completion_datetime` AS `treatment_completion_datetime`',
    '`removal_reason_desc` AS `removal_reason_desc`',
    "CASE WHEN TRIM(CAST(`removal_reason_text` AS STRING)) = '' THEN NULL ELSE `removal_reason_text` END AS `removal_reason_text`",
    '`off_treatment_reason_desc` AS `off_treatment_reason_desc`',
    "CASE WHEN TRIM(CAST(`off_treatment_reason_text` AS STRING)) = '' THEN NULL ELSE `off_treatment_reason_text` END AS `off_treatment_reason_text`",
    '`source_episode_id` AS `source_episode_id`',
    '`enrolling_organization_id` AS `enrolling_organization_id`',
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

# contract v2: QC/batch inputs come from internal _clinical_research_enrollment_metadata; source history stays on the main research table.
CLINICAL_RESEARCH_ENROLLMENT_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 34,354 rows of 34,354 that
    # are current and attributable; identity_status = 'resolved' keeps the 34,354 rows of
    # 34,354 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_RESEARCH_ENROLLMENT_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 34,354 at the profile.
    "gold.clinical.research_enrollment.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 34,354 at the profile.
    "gold.clinical.research_enrollment.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 24 of 34,354 rows (0.0699%) when profiled on 2026-08-24.
    "gold.clinical.research_enrollment.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 2 of 34,354 rows (0.00582%) when profiled on 2026-08-24.
    "gold.clinical.research_enrollment.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_RESEARCH_ENROLLMENT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "pt_prot_reg_id": "Cerner PT_PROT_REG_ID; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace used to interpret the subject identifier on this record for each research enrollment record. It is derived from bronze field `PERSON_ID` in `4_prod.bronze.map_research_subject`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date and time when the represented clinical or administrative event occurred for each research enrollment record. It is derived from bronze field `ON_STUDY_DT_TM_CLEAN` in `4_prod.bronze.map_research_subject`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each research enrollment record. It is derived from bronze field `OFF_STUDY_DT_TM_CLEAN` in `4_prod.bronze.map_research_subject`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each research enrollment record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "Number to indicate the current status of the enrolled patient(1: On Study,2:OnTreatment, 3:Off Treatment,4-OnFollowup,5-OffStudy)",
    "source_display": "Human-readable label supplied by the source system for the source code for each research enrollment record. It is derived from bronze field `STATUS_DESC` in `4_prod.bronze.map_research_subject`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "research_study_key": "Deterministic SHA-256 key for the related research study.",
    "registration_id": "Source-system identifier for the registration associated with each research enrollment record. It is derived from bronze field `REG_ID` in `4_prod.bronze.map_research_subject`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "protocol_accession_number": "This field contains the patient's accession number (order of enrollment) for this protocol.",
    "protocol_arm_id": "Source-system identifier for the protocol arm associated with each research enrollment record. It is derived from bronze field `PROT_ARM_ID` in `4_prod.bronze.map_research_subject`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "status_code": "Number to indicate the current status of the enrolled patient(1: On Study,2:OnTreatment, 3:Off Treatment,4-OnFollowup,5-OffStudy)",
    "status_desc": "Value describing status desc for the research enrollment record. It is carried from bronze field `STATUS_DESC` in `4_prod.bronze.map_research_subject`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "off_study_datetime": "Date and time associated with off study for the research enrollment record. It is derived from bronze field `OFF_STUDY_DT_TM_CLEAN` in `4_prod.bronze.map_research_subject`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "treatment_start_datetime": "Date and time associated with treatment start for the research enrollment record. It is derived from bronze field `TX_START_DT_TM_CLEAN` in `4_prod.bronze.map_research_subject`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "treatment_completion_datetime": "Date and time associated with treatment completion for the research enrollment record. It is derived from bronze field `TX_COMPLETION_DT_TM_CLEAN` in `4_prod.bronze.map_research_subject`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "removal_reason_desc": "The description for the code value",
    "removal_reason_text": "Textual reason patient is taken off study.",
    "off_treatment_reason_desc": "The description for the code value",
    "off_treatment_reason_text": "Textual reason patient is taken off treatment.",
    "source_episode_id": "Source-system identifier for the source episode associated with each research enrollment record. It is derived from bronze field `EPISODE_ID` in `4_prod.bronze.map_research_subject`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "enrolling_organization_id": "Source-system identifier for the enrolling organization associated with each research enrollment record. It is carried from bronze field `ENROLLING_ORGANIZATION_ID` in `4_prod.bronze.map_research_subject`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each research enrollment record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each research enrollment record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each research enrollment record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each research enrollment record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each research enrollment record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Always active in the current map_research_subject projection. No superseded or retracted status is emitted; STATUS_ENUM, off-study/treatment dates and removal reasons remain separate clinical fields and do not change this literal label.",
    "record_status_effective_from": "map_research_subject.ON_STUDY_DT_TM_CLEAN carried unchanged. This study-entry-time proxy is not an independently observed source-status transition; there is no registration, treatment-start or ingestion-time fallback.",
    "record_status_effective_to": "Always null as a TIMESTAMP in the current research-enrollment projection. OFF_STUDY_DT_TM_CLEAN and treatment/removal fields are not used as history ends; a null here must not be interpreted as evidence that study participation is ongoing.",
    "source_update_timestamp": "Bronze pipeline processing timestamp carried from `PIPELINE_UPDT_DT_TM` in `4_prod.bronze.map_research_subject`. It records when bronze processed the source row, not a native clinical-system update timestamp, clinical event time, or the current Silver refresh time.",
    "loaded_at": "map_research_subject.ADC_UPDT carried unchanged for the contributing research-subject row. The study key is derived without joining a study load clock. This input load provenance is distinct from PIPELINE_UPDT_DT_TM published as source_update_timestamp and from Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_research_enrollment"),
    comment="Internal quality-controlled twin of clinical_research_enrollment: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_RESEARCH_ENROLLMENT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_RESEARCH_ENROLLMENT_ADVISORY_RULES)
def _gold_qc_clinical_research_enrollment():
    """Quality-controlled twin of journey_clinical.research_enrollment."""
    df = _qc(
        "clinical_research_enrollment",
        CLINICAL_RESEARCH_ENROLLMENT_SELECT,
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, CLINICAL_RESEARCH_ENROLLMENT_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.research_enrollment"),
    comment=(
        "One Cerner research-subject registration using sentinel-safe clean dates. Gold QC "
        "twin of the silver product: 2 columns are repaired or nulled, 1 rule(s) drop rows, 4 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_research_enrollment():
    """Contract-v2 public twin of clinical_research_enrollment; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_research_enrollment")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`pt_prot_reg_id` AS `pt_prot_reg_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`research_study_key` AS `research_study_key`',
        '`registration_id` AS `registration_id`',
        '`protocol_accession_number` AS `protocol_accession_number`',
        '`protocol_arm_id` AS `protocol_arm_id`',
        '`status_code` AS `status_code`',
        '`status_desc` AS `status_desc`',
        '`off_study_datetime` AS `off_study_datetime`',
        '`treatment_start_datetime` AS `treatment_start_datetime`',
        '`treatment_completion_datetime` AS `treatment_completion_datetime`',
        '`removal_reason_desc` AS `removal_reason_desc`',
        '`removal_reason_text` AS `removal_reason_text`',
        '`off_treatment_reason_desc` AS `off_treatment_reason_desc`',
        '`off_treatment_reason_text` AS `off_treatment_reason_text`',
        '`source_episode_id` AS `source_episode_id`',
        '`enrolling_organization_id` AS `enrolling_organization_id`',
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
    return _with_comments(df, CLINICAL_RESEARCH_ENROLLMENT_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.specimen ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_SPECIMEN_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = '0' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    '`specimen_type` AS `specimen_type`',
    '`accession_identifier` AS `accession_identifier`',
    '`resolved_accession_key` AS `resolved_accession_key`',
    '`normalized_lab_no` AS `normalized_lab_no`',
    '`canonical_accession_status` AS `canonical_accession_status`',
    '`person_resolution_status` AS `person_resolution_status`',
    '`lab_series` AS `lab_series`',
    '`discipline` AS `discipline`',
    '`urgent_flag` AS `urgent_flag`',
    '`research_qi_only` AS `research_qi_only`',
    '`clinical_details` AS `clinical_details`',
    '`tlcs_requested` AS `tlcs_requested`',
    '`conditions` AS `conditions`',
    '`reason` AS `reason`',
    "CASE WHEN UPPER(TRIM(CAST(`body_site_code` AS STRING))) = '0' THEN NULL ELSE `body_site_code` END AS `body_site_code`",
    '`body_site_snomed_code` AS `body_site_snomed_code`',
    "CASE WHEN UPPER(TRIM(CAST(`specimen_type_code` AS STRING))) = '0' THEN NULL ELSE `specimen_type_code` END AS `specimen_type_code`",
    '`specimen_type_snomed_code` AS `specimen_type_snomed_code`',
    'CASE WHEN CAST(`sample_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `sample_datetime` END AS `sample_datetime`',
    'CASE WHEN CAST(`request_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `request_datetime` END AS `request_datetime`',
    'CASE WHEN CAST(`report_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS OR YEAR(CAST(`report_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `report_datetime` END AS `report_datetime`',
    '`source_history_row_count` AS `source_history_row_count`',
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

# contract v2: QC/batch inputs come from internal _clinical_specimen_metadata; source history stays on the main research table.
CLINICAL_SPECIMEN_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 172,077,158 rows of
    # 172,077,158 that are current and attributable; identity_status = 'resolved' keeps the
    # 150,927,313 rows of 172,077,158 that are current and attributable. Superseded versions
    # and rows whose identity was never resolved are not research data, and a consumer who
    # wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_SPECIMEN_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 21,149,845 of 172,077,158 at the profile.
    "gold.clinical.specimen.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 172,077,158 at the profile.
    "gold.clinical.specimen.record_status.default_view_active": "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 88,413 of 172,077,158 rows (0.0514%) when profiled on
    # 2026-08-24.
    "gold.clinical.specimen.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1,203,382 of 172,077,158 rows (0.699%) when profiled on
    # 2026-08-24.
    "gold.clinical.specimen.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_SPECIMEN_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Collection or receipt timestamp.",
    "event_end_datetime": "Event end when supplied.",
    "source_coding_system": "Source specimen type system.",
    "source_code": "Source specimen type code.",
    "source_display": "Source specimen type display.",
    "specimen_type": "Source specimen type CodeableConcept.",
    "accession_identifier": "Canonical accession identifier (pathology_accession_id); never LabNo-derived.",
    "resolved_accession_key": "Alias-resolved accession key retained as derivation evidence.",
    "normalized_lab_no": "Matching evidence only — reused lab numbers reach 3",
    "canonical_accession_status": "Canonical accession-link state.",
    "person_resolution_status": "Person-projection eligibility state.",
    "lab_series": "Source laboratory series.",
    "discipline": "Source pathology discipline.",
    "urgent_flag": "Source urgent-request indicator code (Y",
    "research_qi_only": "Registry doctrine flag; true on 100% of rows today — describe-only",
    "clinical_details": "Source clinical details. Identifiable free text; ig_risk 4",
    "tlcs_requested": "Requested TLC context. Identifiable free text; ig_risk 4",
    "conditions": "Source request conditions. Identifiable free text; ig_risk 4",
    "reason": "Source request reason. Identifiable free text; ig_risk 4",
    "body_site_code": "Source collection body-site code.",
    "body_site_snomed_code": "Source-provided SNOMED body-site code.",
    "specimen_type_code": "Native specimen-type code.",
    "specimen_type_snomed_code": "Source-provided SNOMED specimen-type code.",
    "sample_datetime": "Clamped source sample timestamp.",
    "request_datetime": "Clamped source request timestamp.",
    "report_datetime": "Clamped source report timestamp.",
    "source_history_row_count": "accession_source rows behind this accession.",
    "confidentiality_code": "Source confidentiality code.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each specimen record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Registered owning feed.",
    "record_status": "Always active in the current specimen projection of surviving pathology accessions. The input is restricted to accessions whose ID equals the alias-resolved survivor ID; canonical_accession_status and lifecycle_status do not change this literal label.",
    "record_status_effective_from": "Time this immutable accession identifier was first assigned.",
    "record_status_effective_to": "Always null as a TIMESTAMP in the current specimen projection. Sample, request and report dates and accession lifecycle fields are not used as status ends.",
    "source_update_timestamp": "Time this immutable accession identifier was first assigned.",
    "loaded_at": "ADC_UPDT of the surviving map_pathology_accession row, carried unchanged through identity and QC/public projections. The separately aggregated maximum evidence_loaded_at from alias-resolved map_pathology_accession_source rows is not used here. This is the accession row's load clock, not created_at, specimen collection time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_specimen"),
    comment="Internal quality-controlled twin of clinical_specimen: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_SPECIMEN_MANDATORY_RULES)
@dp.expect_all(CLINICAL_SPECIMEN_ADVISORY_RULES)
def _gold_qc_clinical_specimen():
    """Quality-controlled twin of journey_clinical.specimen."""
    df = _qc("clinical_specimen", CLINICAL_SPECIMEN_SELECT)
    return _with_comments(df, CLINICAL_SPECIMEN_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.specimen"),
    comment=(
        "One canonical pathology accession with collection and request evidence. Gold QC twin "
        "of the silver product: 10 columns are repaired or nulled, 1 rule(s) drop rows, 4 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_specimen():
    """Contract-v2 public twin of clinical_specimen; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_specimen")).selectExpr(
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
        '`__gold_json_specimen_type` AS `specimen_type`',
        '`accession_identifier` AS `accession_identifier`',
        '`resolved_accession_key` AS `resolved_accession_key`',
        '`normalized_lab_no` AS `normalized_lab_no`',
        '`canonical_accession_status` AS `canonical_accession_status`',
        '`person_resolution_status` AS `person_resolution_status`',
        '`lab_series` AS `lab_series`',
        '`discipline` AS `discipline`',
        '`urgent_flag` AS `urgent_flag`',
        '`research_qi_only` AS `research_qi_only`',
        '`clinical_details` AS `clinical_details`',
        '`tlcs_requested` AS `tlcs_requested`',
        '`conditions` AS `conditions`',
        '`reason` AS `reason`',
        '`body_site_code` AS `body_site_code`',
        '`body_site_snomed_code` AS `body_site_snomed_code`',
        '`specimen_type_code` AS `specimen_type_code`',
        '`specimen_type_snomed_code` AS `specimen_type_snomed_code`',
        '`sample_datetime` AS `sample_datetime`',
        '`request_datetime` AS `request_datetime`',
        '`report_datetime` AS `report_datetime`',
        '`source_history_row_count` AS `source_history_row_count`',
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
    return _s3_flatten_gold_public(_with_comments(df, CLINICAL_SPECIMEN_COLUMN_COMMENTS), "clinical_specimen")

# COMMAND ----------

# ==== journey_clinical.susceptibility_result ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_SUSCEPTIBILITY_RESULT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`susceptibility_result_id` AS `susceptibility_result_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`microbiology_isolate_id` AS `microbiology_isolate_id`',
    '`isolate_event_key` AS `isolate_event_key`',
    '`pathology_result_id` AS `pathology_result_id`',
    '`link_status` AS `link_status`',
    '`antimicrobial_text` AS `antimicrobial_text`',
    '`antimicrobial_code` AS `antimicrobial_code`',
    '`antimicrobial_omop_concept_id` AS `antimicrobial_omop_concept_id`',
    '`interpretation_raw` AS `interpretation_raw`',
    '`interpretation` AS `interpretation`',
    '`mic_raw` AS `mic_raw`',
    '`mic` AS `mic`',
    '`unit_source_value` AS `unit_source_value`',
    '`method` AS `method`',
    '`token_class` AS `token_class`',
    '`token_ordinal` AS `token_ordinal`',
    '`parser_version` AS `parser_version`',
    '`lifecycle_status` AS `lifecycle_status`',
    '`is_current` AS `is_current`',
    '`research_qi_only` AS `research_qi_only`',
    '`specimen_key` AS `specimen_key`',
    '`accession_identifier` AS `accession_identifier`',
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

# contract v2: QC/batch inputs come from internal _clinical_susceptibility_result_metadata; source history stays on the main research table.
CLINICAL_SUSCEPTIBILITY_RESULT_MANDATORY_RULES = {
    # The research surface keeps current, attributable susceptibility rows.
    # identity_status = 'resolved' requires a linked person and record_status = 'active' removes retracted/superseded rows.
    # and attributable. Superseded versions and rows whose identity was never resolved are
    # not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_SUSCEPTIBILITY_RESULT_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 0 at the profile.
    "gold.clinical.susceptibility_result.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 0 at the profile.
    "gold.clinical.susceptibility_result.record_status.default_view_active":
        "record_status = 'active'",
}

CLINICAL_SUSCEPTIBILITY_RESULT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "susceptibility_result_id": "Pathology susceptibility_result_id; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace selected for `subject_key`: Millennium person identifier first, then MRN, then NHS number; nosubject denotes the per-source-record fallback when none is present.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT through the shared pathology identity projection.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Accession sample/report/request fallback time.",
    "event_end_datetime": "Event end time.",
    "source_coding_system": "Antimicrobial coding namespace.",
    "source_code": "Antimicrobial code or text.",
    "source_display": "Source antimicrobial result/test code.",
    "microbiology_isolate_id": "Native bronze microbiology isolate identifier; foreign key to clinical_microbiology_isolate.microbiology_isolate_id.",
    "isolate_event_key": "Deterministic event key for the linked microbiology isolate.",
    "pathology_result_id": "Linked pathology-result fact.",
    "link_status": "Isolate-link resolution state.",
    "antimicrobial_text": "Antimicrobial as reported.",
    "antimicrobial_code": "Source antimicrobial code.",
    "antimicrobial_omop_concept_id": "Standard OMOP antimicrobial concept.",
    "interpretation_raw": "Raw susceptibility result.",
    "interpretation": "S, I, R or indeterminate for antimicrobials; positive, negative or indeterminate for mechanism markers. Case of the source flag is preserved in interpretation_raw and is not interpreted.",
    "mic_raw": "Raw MIC text.",
    "mic": "Parsed MIC value.",
    "unit_source_value": "Raw MIC unit.",
    "method": "Reported susceptibility method.",
    "token_class": "antimicrobial, mechanism or unparsed token class assigned by pathology_antibiogram.",
    "token_ordinal": "Zero-based position of this token within its isolate block.",
    "parser_version": "pathology_antibiogram parser version that produced the row.",
    "lifecycle_status": "Inherited source lifecycle.",
    "is_current": "Whether the result remains current.",
    "research_qi_only": "Research/QI release flag.",
    "specimen_key": "Deterministic specimen key.",
    "accession_identifier": "Alias-resolved accession identifier.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each susceptibility result record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each susceptibility result record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Originating data feed responsible for the record for each susceptibility result record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_pathology_antimicrobial_susceptibility.lifecycle_status is exactly cancelled or entered_in_error; otherwise superseded when is_current is false (null defaults true), otherwise active. Comparisons are case-sensitive without trimming; a null lifecycle_status does not select retraction. Retraction takes priority over supersession; clinical findings and joined identity/link fields are separate.",
    "record_status_effective_from": "Always null as a TIMESTAMP in the current susceptibility-result projection. No source-status start is supplied here; the clinical event's report/sample/request date fallbacks and ADC_UPDT are not used for this field.",
    "record_status_effective_to": "Always null as a TIMESTAMP in the current susceptibility-result projection, including when its status or source-current flag changes. No report, clinical-event or ingestion timestamp is substituted as a status end.",
    "source_update_timestamp": "map_pathology_antimicrobial_susceptibility.ADC_UPDT carried unchanged from the contributing row, identical to loaded_at. No independent native application-update clock is projected here, and Silver refresh time is not substituted.",
    "loaded_at": "map_pathology_antimicrobial_susceptibility.ADC_UPDT carried unchanged through its joins, usable-code filtering and source/public projections. Alias resolution and accession identity add no load clocks; neither joined report/parent timestamps nor the separately aggregated accession evidence clock replace this value. It is identical to source_update_timestamp, not the clinical event time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_susceptibility_result"),
    comment="Internal quality-controlled twin of clinical_susceptibility_result: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_SUSCEPTIBILITY_RESULT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_SUSCEPTIBILITY_RESULT_ADVISORY_RULES)
def _gold_qc_clinical_susceptibility_result():
    """Quality-controlled twin of journey_clinical.susceptibility_result."""
    df = _qc("clinical_susceptibility_result", CLINICAL_SUSCEPTIBILITY_RESULT_SELECT)
    return _with_comments(df, CLINICAL_SUSCEPTIBILITY_RESULT_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.susceptibility_result"),
    comment=(
        "One antimicrobial, mechanism-marker or unparsed token per isolate, parsed from the packed WinPath antibiogram (S2.5, parser 1.0.0). Categorical S/I/R only; MIC columns are null by source. Gold keeps identity_status='resolved' AND record_status='active'. Gold QC twin of the silver product: "
        "0 columns are repaired or nulled, 1 rule(s) drop rows, 2 check(s) are advisory. Each "
        "rule states its reason in the pipeline notebook, and Lakeflow expectation metrics "
        "report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_susceptibility_result():
    """Contract-v2 public twin of clinical_susceptibility_result; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_susceptibility_result")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`susceptibility_result_id` AS `susceptibility_result_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`microbiology_isolate_id` AS `microbiology_isolate_id`',
        '`isolate_event_key` AS `isolate_event_key`',
        '`pathology_result_id` AS `pathology_result_id`',
        '`link_status` AS `link_status`',
        '`antimicrobial_text` AS `antimicrobial_text`',
        '`antimicrobial_code` AS `antimicrobial_code`',
        '`antimicrobial_omop_concept_id` AS `antimicrobial_omop_concept_id`',
        '`interpretation_raw` AS `interpretation_raw`',
        '`interpretation` AS `interpretation`',
        '`mic_raw` AS `mic_raw`',
        '`mic` AS `mic`',
        '`unit_source_value` AS `unit_source_value`',
        '`method` AS `method`',
        '`token_class` AS `token_class`',
        '`token_ordinal` AS `token_ordinal`',
        '`parser_version` AS `parser_version`',
        '`lifecycle_status` AS `lifecycle_status`',
        '`is_current` AS `is_current`',
        '`research_qi_only` AS `research_qi_only`',
        '`specimen_key` AS `specimen_key`',
        '`accession_identifier` AS `accession_identifier`',
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
    return _with_comments(df, CLINICAL_SUSCEPTIBILITY_RESULT_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.transfusion ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_TRANSFUSION_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`transfusion_key` AS `transfusion_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`begin_datetime` AS `begin_datetime`',
    'CASE WHEN `begin_datetime` IS NOT NULL AND `end_datetime` IS NOT NULL AND `begin_datetime` > `end_datetime` THEN NULL ELSE `end_datetime` END AS `end_datetime`',
    '`transfusion_status` AS `transfusion_status`',
    '`elapsed_minutes` AS `elapsed_minutes`',
    '`unit_number` AS `unit_number`',
    '`blood_product_group` AS `blood_product_group`',
    '`blood_unit_group` AS `blood_unit_group`',
    '`patient_blood_group` AS `patient_blood_group`',
    '`quantity_value` AS `quantity_value`',
    '`quantity_raw` AS `quantity_raw`',
    '`begin_location` AS `begin_location`',
    '`end_location` AS `end_location`',
    '`unit_is_irradiated` AS `unit_is_irradiated`',
    '`unit_is_cmv_neg` AS `unit_is_cmv_neg`',
    '`requires_irradiated` AS `requires_irradiated`',
    '`requires_cmv_neg` AS `requires_cmv_neg`',
    '`ambiguity_ind` AS `ambiguity_ind`',
    '`product_concept_id` AS `product_concept_id`',
    '`product_concept_name` AS `product_concept_name`',
    '`unit_group_concept_id` AS `unit_group_concept_id`',
    '`patient_group_concept_id` AS `patient_group_concept_id`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_transfusion_metadata; source history stays on the main research table.
CLINICAL_TRANSFUSION_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 127,986 rows of 127,986 that
    # are current and attributable; identity_status = 'resolved' keeps the 127,955 rows of
    # 127,986 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_TRANSFUSION_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 31 of 127,986 at the profile.
    "gold.clinical.transfusion.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 127,986 at the profile.
    "gold.clinical.transfusion.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1,523 of 127,986 rows (1.19%) when profiled on 2026-08-24.
    "gold.clinical.transfusion.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",
}

CLINICAL_TRANSFUSION_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "transfusion_key": "BloodTrack TRANSFUSION_KEY; native primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID when resolved; native person foreign key.",
    "encounter_id": "Nullable Millennium ENCNTR_ID; BloodTrack transfusion rows do not supply encounter context.",
    "event_datetime": "Transfusion begin timestamp falling back to end.",
    "event_end_datetime": "Transfusion end timestamp.",
    "source_coding_system": "BloodTrack product coding system.",
    "source_code": "ISBT product code with product display fallback.",
    "source_display": "Blood product description.",
    "begin_datetime": "Paired transfusion begin timestamp.",
    "end_datetime": "Paired transfusion end timestamp.",
    "transfusion_status": "Pairing and clock-quality outcome.",
    "elapsed_minutes": "Elapsed transfusion minutes when calculable.",
    "unit_number": "Blood unit number.",
    "blood_product_group": "Normalized blood product group.",
    "blood_unit_group": "Blood group recorded on the unit.",
    "patient_blood_group": "Patient blood group at transfusion.",
    "quantity_value": "Parsed transfused quantity.",
    "quantity_raw": "Verbatim quantity text.",
    "begin_location": "Begin workflow location.",
    "end_location": "End workflow location.",
    "unit_is_irradiated": "Unit irradiation flag.",
    "unit_is_cmv_neg": "Unit CMV-negative flag.",
    "requires_irradiated": "Patient requires irradiated product.",
    "requires_cmv_neg": "Patient requires CMV-negative product.",
    "ambiguity_ind": "Pairing ambiguity indicator.",
    "product_concept_id": "Mapped SNOMED device concept identifier.",
    "product_concept_name": "Mapped product concept display.",
    "unit_group_concept_id": "Mapped unit blood-group concept identifier.",
    "patient_group_concept_id": "Mapped patient blood-group concept identifier.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Identity-withholding status is not populated by the current source projections; null does not mean identity was not withheld.",
    "record_status": "Superseded when map_bloodtrack_transfusion.IS_CURRENT_IN_SOURCE is false, otherwise active; a missing flag defaults true. No retracted status is emitted. TRANSFUSION_STATUS and ambiguity indicators are separate, while usable-code filtering controls publication eligibility.",
    "record_status_effective_from": "First non-null map_bloodtrack_transfusion.BEGIN_TS then END_TS, also used as this row's clinical event time. This is a begin/end-time proxy, not an independently observed status transition; no event-time clamp is applied.",
    "record_status_effective_to": "map_bloodtrack_transfusion.PIPELINE_LOADED_AT when IS_CURRENT_IN_SOURCE is false; otherwise null. A missing processing timestamp remains null; neither BEGIN_TS nor END_TS is used as a fallback.",
    "source_update_timestamp": "Always null as a TIMESTAMP in the current transfusion projection. PIPELINE_LOADED_AT is published separately as loaded_at and is not copied into this field; no native update, clinical-event or Silver refresh timestamp is substituted.",
    "loaded_at": "Timestamp when the pipeline wrote this row to the bronze table.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_transfusion"),
    comment="Internal quality-controlled twin of clinical_transfusion: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_TRANSFUSION_MANDATORY_RULES)
@dp.expect_all(CLINICAL_TRANSFUSION_ADVISORY_RULES)
def _gold_qc_clinical_transfusion():
    """Quality-controlled twin of journey_clinical.transfusion."""
    df = _qc(
        "clinical_transfusion",
        CLINICAL_TRANSFUSION_SELECT,
        date_flags=["event_after_death_30d"],
    )
    return _with_comments(df, CLINICAL_TRANSFUSION_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.transfusion"),
    comment=(
        "One BloodTrack unit-recipient transfusion episode with paired begin/end evidence and "
        "mapped product and blood-group concepts. Gold QC twin of the silver product: 2 "
        "columns are repaired or nulled, 1 rule(s) drop rows, 3 check(s) are advisory. Each "
        "rule states its reason in the pipeline notebook, and Lakeflow expectation metrics "
        "report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_transfusion():
    """Contract-v2 public twin of clinical_transfusion; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_transfusion")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`transfusion_key` AS `transfusion_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`begin_datetime` AS `begin_datetime`',
        '`end_datetime` AS `end_datetime`',
        '`transfusion_status` AS `transfusion_status`',
        '`elapsed_minutes` AS `elapsed_minutes`',
        '`unit_number` AS `unit_number`',
        '`blood_product_group` AS `blood_product_group`',
        '`blood_unit_group` AS `blood_unit_group`',
        '`patient_blood_group` AS `patient_blood_group`',
        '`quantity_value` AS `quantity_value`',
        '`quantity_raw` AS `quantity_raw`',
        '`begin_location` AS `begin_location`',
        '`end_location` AS `end_location`',
        '`unit_is_irradiated` AS `unit_is_irradiated`',
        '`unit_is_cmv_neg` AS `unit_is_cmv_neg`',
        '`requires_irradiated` AS `requires_irradiated`',
        '`requires_cmv_neg` AS `requires_cmv_neg`',
        '`ambiguity_ind` AS `ambiguity_ind`',
        '`product_concept_id` AS `product_concept_id`',
        '`product_concept_name` AS `product_concept_name`',
        '`unit_group_concept_id` AS `unit_group_concept_id`',
        '`patient_group_concept_id` AS `patient_group_concept_id`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_TRANSFUSION_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.transfusion_event ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_TRANSFUSION_EVENT_SELECT = [
    '`transfusion_event_key` AS `transfusion_event_key`',
    '`bloodtrack_transaction_key` AS `bloodtrack_transaction_key`',
    '`person_id` AS `person_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`event_datetime` AS `event_datetime`',
    '`workflow_step` AS `workflow_step`',
    '`transaction_success_ind` AS `transaction_success_ind`',
    '`unit_number` AS `unit_number`',
    '`product_code` AS `product_code`',
    '`product_description` AS `product_description`',
    '`bloodtrack_unit_id` AS `bloodtrack_unit_id`',
    '`device_name` AS `device_name`',
    '`source_location` AS `source_location`',
    '`linkage_status` AS `linkage_status`',
    "CASE WHEN UPPER(TRIM(CAST(`response_code` AS STRING))) = '0' THEN NULL ELSE `response_code` END AS `response_code`",
    '`response_text` AS `response_text`',
    '`blood_unit_state` AS `blood_unit_state`',
    '`blood_unit_fate` AS `blood_unit_fate`',
    '`alert_present_ind` AS `alert_present_ind`',
    '`comment_present_ind` AS `comment_present_ind`',
    '`source_table` AS `source_table`',
    '`source_row_key` AS `source_row_key`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_transfusion_event_metadata; source history stays on the main research table.
CLINICAL_TRANSFUSION_EVENT_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 228,205 rows of 228,205 that
    # are current and attributable; identity_status = 'resolved' keeps the 227,703 rows of
    # 228,205 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_TRANSFUSION_EVENT_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 502 of 228,205 at the profile.
    "gold.clinical.transfusion_event.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 228,205 at the profile.
    "gold.clinical.transfusion_event.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 2,924 of 228,205 rows (1.28%) when profiled on 2026-08-24.
    "gold.clinical.transfusion_event.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",
}

CLINICAL_TRANSFUSION_EVENT_COLUMN_COMMENTS = {
    "transfusion_event_key": "Deterministic SHA-256 BloodTrack scan-event key.",
    "bloodtrack_transaction_key": "BloodTrack transaction key; native primary key of this table.",
    "person_id": "Millennium PERSON_ID when resolved; native person foreign key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "event_datetime": "Effective transaction timestamp.",
    "workflow_step": "BloodTrack workflow step.",
    "transaction_success_ind": "Whether the transaction succeeded.",
    "unit_number": "Blood unit number.",
    "product_code": "Product code scanned.",
    "product_description": "Source BloodTrack textual description of the blood component product.",
    "bloodtrack_unit_id": "BloodTrack unit identifier.",
    "device_name": "Scanning device name.",
    "source_location": "Source workflow location name.",
    "linkage_status": "Patient linkage status.",
    "response_code": "Device or workflow response code.",
    "response_text": "Device or workflow response text.",
    "blood_unit_state": "Source BloodTrack status of the blood unit within the storage and issue workflow at the time of the transaction.",
    "blood_unit_fate": "Source BloodTrack final fate recorded for the blood unit, such as confirmed use or non-use.",
    "alert_present_ind": "Alert presence indicator.",
    "comment_present_ind": "Comment presence indicator.",
    "source_table": "Registered source table.",
    "source_row_key": "Verbatim BloodTrack transaction key retained as source-row provenance.",
    "record_status": "Superseded when map_bloodtrack_transaction.IS_CURRENT_IN_SOURCE is false, otherwise active; a missing flag defaults true. No retracted status is emitted. Workflow step, transaction success, response and blood-unit state/fate do not set this label.",
    "loaded_at": "map_bloodtrack_transaction.ADC_UPDT carried unchanged for the transaction row. No join to the transfusion aggregate contributes its PIPELINE_LOADED_AT; device/event times and the current Silver refresh time do not replace this value.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_transfusion_event"),
    comment="Internal quality-controlled twin of clinical_transfusion_event: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_TRANSFUSION_EVENT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_TRANSFUSION_EVENT_ADVISORY_RULES)
def _gold_qc_clinical_transfusion_event():
    """Quality-controlled twin of journey_clinical.transfusion_event."""
    df = _qc(
        "clinical_transfusion_event",
        CLINICAL_TRANSFUSION_EVENT_SELECT,
        date_flags=["event_after_death_30d"],
    )
    return _with_comments(df, CLINICAL_TRANSFUSION_EVENT_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.transfusion_event"),
    comment=(
        "One BloodTrack scan-grain workflow event, including failed attempts and safety "
        "checks that do not count as transfusions. Gold QC twin of the silver product: 1 "
        "columns are repaired or nulled, 1 rule(s) drop rows, 3 check(s) are advisory. Each "
        "rule states its reason in the pipeline notebook, and Lakeflow expectation metrics "
        "report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_transfusion_event():
    """Contract-v2 public twin of clinical_transfusion_event; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_transfusion_event")).selectExpr(
        '`transfusion_event_key` AS `transfusion_event_key`',
        '`bloodtrack_transaction_key` AS `bloodtrack_transaction_key`',
        '`person_id` AS `person_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`event_datetime` AS `event_datetime`',
        '`workflow_step` AS `workflow_step`',
        '`transaction_success_ind` AS `transaction_success_ind`',
        '`unit_number` AS `unit_number`',
        '`product_code` AS `product_code`',
        '`product_description` AS `product_description`',
        '`bloodtrack_unit_id` AS `bloodtrack_unit_id`',
        '`device_name` AS `device_name`',
        '`source_location` AS `source_location`',
        '`linkage_status` AS `linkage_status`',
        '`response_code` AS `response_code`',
        '`response_text` AS `response_text`',
        '`blood_unit_state` AS `blood_unit_state`',
        '`blood_unit_fate` AS `blood_unit_fate`',
        '`alert_present_ind` AS `alert_present_ind`',
        '`comment_present_ind` AS `comment_present_ind`',
        '`source_table` AS `source_table`',
        '`source_row_key` AS `source_row_key`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_TRANSFUSION_EVENT_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.vital_sign ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_VITAL_SIGN_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`event_id` AS `event_id`',
    '`doc_response_key` AS `doc_response_key`',
    '`source_dcp_forms_activity_id` AS `source_dcp_forms_activity_id`',
    '`source_object` AS `source_object`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    "CASE WHEN YEAR(CAST(`event_datetime` AS TIMESTAMP)) = 1970 AND timestamp_seconds(CAST(unix_timestamp(`event_datetime`) AS BIGINT) * 1000) BETWEEN TIMESTAMP'1990-01-01 00:00:00' AND `loaded_at` THEN timestamp_seconds(CAST(unix_timestamp(`event_datetime`) AS BIGINT) * 1000) WHEN YEAR(CAST(`event_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `event_datetime` END AS `event_datetime`",
    'CASE WHEN CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`vital_code` AS `vital_code`',
    'CASE WHEN ABS(CAST(`value_number` AS DOUBLE)) > 1e12 OR `value_number` < 0 THEN NULL ELSE `value_number` END AS `value_number`',
    '`value_text` AS `value_text`',
    '`unit_source_value` AS `unit_source_value`',
    '`unit_concept_id` AS `unit_concept_id`',
    '`reference_range_low` AS `reference_range_low`',
    '`reference_range_high` AS `reference_range_high`',
    '`method_code` AS `method_code`',
    '`method_display` AS `method_display`',
    '`body_site_code` AS `body_site_code`',
    '`body_site_display` AS `body_site_display`',
    "CASE WHEN UPPER(TRIM(CAST(`interpretation_code` AS STRING))) = '0' THEN NULL ELSE `interpretation_code` END AS `interpretation_code`",
    '`interpretation_display` AS `interpretation_display`',
    '`result_status_code` AS `result_status_code`',
    '`result_status_display` AS `result_status_display`',
    '`performer_practitioner_id` AS `performer_practitioner_id`',
    '`source_form_key` AS `source_form_key`',
    '`promotion_rule_id` AS `promotion_rule_id`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
    '`registry_field_id` AS `registry_field_id`',
    '`event_datetime_status` AS `event_datetime_status`',
]
for _s3b_axis in ("unit", "method", "body_site", "interpretation"):
    CLINICAL_VITAL_SIGN_SELECT.extend(
        [f"`{name}` AS `{name}`" for name in _s3_axis_columns(_s3b_axis)]
    )

# contract v2: QC/batch inputs come from internal _clinical_vital_sign_metadata; source history stays on the main research table.
CLINICAL_VITAL_SIGN_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 283,503,962 rows of
    # 283,505,704 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_VITAL_SIGN_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 1,742 of 283,505,704 at the profile.
    "gold.clinical.vital_sign.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 31,497 of 283,505,704 rows (0.0111%) when profiled on
    # 2026-08-24.
    "gold.clinical.vital_sign.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 6,504,311 of 283,505,704 rows (2.29%) when profiled on
    # 2026-08-24.
    "gold.clinical.vital_sign.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",

    # Left as a warning because it fires on 213,158,920 of 283,505,704 rows (75.2%) when
    # profiled on 2026-08-24 -- at that rate the rule's assumption about what event_datetime
    # and event_end_datetime mean is the thing in doubt, not the data. The inverted gaps are
    # mostly minutes, which reads as two clocks rather than two events in the wrong order.
    "gold.clinical.vital_sign.table.ordering_violation_event_datetime_event_end_datetime":
        "NOT COALESCE((`event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime`), FALSE)",
}

CLINICAL_VITAL_SIGN_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "event_id": "Millennium numeric EVENT_ID; native key for the numeric_event arm.",
    "doc_response_key": "PowerForm DOC_RESPONSE_KEY; native key for the form_promotion arm.",
    "source_dcp_forms_activity_id": "Millennium DCP_FORMS_ACTIVITY_ID of the source form for promoted rows.",
    "source_object": "Native source arm: numeric_event or form_promotion.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "event_datetime": "Clinical start date and time of the event as recorded on the Millennium CLINICAL_EVENT row.",
    "event_end_datetime": "Measurement end timestamp.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each vital sign record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "Source vital code.",
    "source_display": "Source vital display.",
    "vital_code": "Source and mapped vital CodeableConcept.",
    "value_number": "Numeric vital value.",
    "value_text": "Verbatim result text.",
    "unit_source_value": "Source result unit.",
    "unit_concept_id": "Mapped unit concept identifier.",
    "reference_range_low": "Reference-range lower bound.",
    "reference_range_high": "Reference-range upper bound.",
    "method_code": "Source measurement method code.",
    "method_display": "Source measurement method display.",
    "body_site_code": "Body-site code when supplied.",
    "body_site_display": "Body-site display when supplied.",
    "interpretation_code": "Source interpretation code.",
    "interpretation_display": "Source interpretation display.",
    "result_status_code": "Source result status code.",
    "result_status_display": "Source result status display.",
    "performer_practitioner_id": "Millennium personnel PERSON_ID for the performer.",
    "source_form_key": "Deterministic SHA-256 key of the source PowerForm instance for promoted rows.",
    "promotion_rule_id": "Governed promotion-rule identifier.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Registered source route.",
    "record_status": "Arm-specific row status. Numeric events are retracted when map_numeric_events.SOURCE_DELETED_IND is true (null defaults false), otherwise superseded when CLINICAL_EVENT_VALID_UNTIL_DT_TM is before 2100-01-01, otherwise active. Form-promoted rows are retracted when CANONICAL_SOURCE_DELETED_IND is true or SOURCE_PRESENT_IND is false or missing; otherwise superseded when ACTIVE_IND is zero or missing, otherwise active. Retraction has priority over supersession; this is not the clinical result-status code.",
    "record_status_effective_from": "map_numeric_events.CLINICAL_EVENT_VALID_FROM_DT_TM on the numeric-event arm; map_powerform_assessment_item.FIRST_DOCUMENTED_DT_TM on the form-promotion arm. Each is carried unchanged, with no performed/response/ingestion-time fallback; a missing value remains null.",
    "record_status_effective_to": "Numeric events return CLINICAL_EVENT_VALID_UNTIL_DT_TM when deleted or ended, otherwise null; deletion can therefore retain a far-future end value. Form-promoted rows return LAST_DOCUMENTED_DT_TM when deleted, absent or inactive, otherwise null. No missing-end fallback is applied, and the form documentation end is not an independently observed status-transition time.",
    "source_update_timestamp": "For numeric events, greatest of map_numeric_events.STRING_RESULT_UPDT_DT_TM, CLINICAL_EVENT_UPDT_DT_TM and ADC_UPDT. For form-promoted rows, map_powerform_assessment_item.ADC_UPDT directly. This mixes source and ingestion provenance rather than uniformly representing a native modification clock; the promotion lookup adds no clock and Silver refresh time is not substituted.",
    "loaded_at": "map_numeric_events.ADC_UPDT for numeric-event rows, or map_powerform_assessment_item.ADC_UPDT for form-promoted rows. Each arm's bronze load clock is carried unchanged through its staging, union and quality wrappers. No form-parent or promotion-configuration clock is added; missing values remain null and this is not Silver refresh time.",
    "registry_field_id": "Governed SHA-256 registry product and field identifier for registry observations; null on native rows.",
    "event_datetime_status": "registry_dated or registry_undated for registry observations; null on native rows.",
}
for _s3b_axis in ("unit", "method", "body_site", "interpretation"):
    CLINICAL_VITAL_SIGN_COLUMN_COMMENTS.update({
        name: f"Governed S3b {_s3b_axis.replace('_', ' ')} mapping provenance."
        for name in _s3_axis_columns(_s3b_axis)
    })

@dp.materialized_view(
    name=_n("gold_qc._clinical_vital_sign"),
    comment="Internal quality-controlled twin of clinical_vital_sign: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_VITAL_SIGN_MANDATORY_RULES)
@dp.expect_all(CLINICAL_VITAL_SIGN_ADVISORY_RULES)
def _gold_qc_clinical_vital_sign():
    """Quality-controlled twin of journey_clinical.vital_sign."""
    df = _qc("clinical_vital_sign", CLINICAL_VITAL_SIGN_SELECT)
    return _with_comments(df, CLINICAL_VITAL_SIGN_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.vital_sign"),
    comment=(
        "One native or form-promoted vital measurement. Gold QC twin of the silver product: 8 "
        "columns are repaired or nulled, 1 rule(s) drop rows, 4 check(s) are advisory. Each "
        "rule states its reason in the pipeline notebook, and Lakeflow expectation metrics "
        "report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_vital_sign():
    """Contract-v2 public twin of clinical_vital_sign; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_vital_sign")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`event_id` AS `event_id`',
        '`doc_response_key` AS `doc_response_key`',
        '`source_dcp_forms_activity_id` AS `source_dcp_forms_activity_id`',
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
        '`__gold_json_vital_code` AS `vital_code`',
        '`value_number` AS `value_number`',
        '`value_text` AS `value_text`',
        '`unit_source_value` AS `unit_source_value`',
        '`unit_concept_id` AS `unit_concept_id`',
        '`reference_range_low` AS `reference_range_low`',
        '`reference_range_high` AS `reference_range_high`',
        '`method_code` AS `method_code`',
        '`method_display` AS `method_display`',
        '`body_site_code` AS `body_site_code`',
        '`body_site_display` AS `body_site_display`',
        '`interpretation_code` AS `interpretation_code`',
        '`interpretation_display` AS `interpretation_display`',
        '`result_status_code` AS `result_status_code`',
        '`result_status_display` AS `result_status_display`',
        '`performer_practitioner_id` AS `performer_practitioner_id`',
        '`source_form_key` AS `source_form_key`',
        '`promotion_rule_id` AS `promotion_rule_id`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
        '`registry_field_id` AS `registry_field_id`',
        '`event_datetime_status` AS `event_datetime_status`',
        *[
            f"`{name}` AS `{name}`"
            for axis in ("unit", "method", "body_site", "interpretation")
            for name in _s3_axis_columns(axis)
        ],
        *[
            f"`_{axis}_gate_pass` AS `_{axis}_gate_pass`"
            for axis in ("unit", "method", "body_site", "interpretation")
        ],
        *[
            f"`_{axis}_policy_id` AS `_{axis}_policy_id`"
            for axis in ("unit", "method", "body_site", "interpretation")
        ],
    )
    df = _s3_flatten_gold_public(_with_comments(df, CLINICAL_VITAL_SIGN_COLUMN_COMMENTS), "clinical_vital_sign")
    return (_s3_gate_direct_public(df, "clinical_vital_sign")
            .drop(*[f"_{axis}_{suffix}" for axis in ("unit", "method", "body_site", "interpretation")
                    for suffix in ("gate_pass", "policy_id")]))

# COMMAND ----------

# ==== journey_events.patient_event ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
EVENTS_PATIENT_EVENT_SELECT = [
    '`patient_event_row_key` AS `patient_event_row_key`',
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    "CASE WHEN CAST(`event_datetime` AS DATE) = DATE'1899-12-30' OR CAST(`event_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_datetime` AS DATE)) < 9999 OR CAST(`event_datetime` AS DATE) = DATE'1900-01-01' OR CAST(`event_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`event_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `event_datetime` END AS `event_datetime`",
    "CASE WHEN CAST(`event_end_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_end_datetime` AS DATE)) < 9999 THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`",
    '`event_type` AS `event_type`',
    '`fact_category` AS `fact_category`',
    '`fact_table` AS `fact_table`',
    '`fact_row_key` AS `fact_row_key`',
    '`source_system` AS `source_system`',
    '`source_object` AS `source_object`',
    '`source_row_key` AS `source_row_key`',
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = '0' OR UPPER(TRIM(CAST(`source_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`mapped_coding_system` AS `mapped_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`mapped_code` AS STRING))) = '0' THEN NULL ELSE `mapped_code` END AS `mapped_code`",
    '`mapped_display` AS `mapped_display`',
    '`target_domain` AS `target_domain`',
    '`map_source` AS `map_source`',
    '`map_version` AS `map_version`',
    '`map_method` AS `map_method`',
    '`map_confidence` AS `map_confidence`',
    '`map_rule_id` AS `map_rule_id`',
    '`map_candidate_count` AS `map_candidate_count`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _events_patient_event_metadata; source history stays on the main research table.
EVENTS_PATIENT_EVENT_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 13,235,874,738 rows of
    # 14,356,906,440 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}

EVENTS_PATIENT_EVENT_ADVISORY_RULES = {
    # Counted rather than nulled because the event index carries every booked appointment
    # forward, so a future stamp here is the booking and not an error. Seen on 656,600 of
    # 14,356,906,440 rows (0.00457%) when profiled on 2026-08-24.
    "gold.events.patient_event.event_datetime.future_owner":
        "NOT COALESCE((CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counted rather than nulled because the event index carries every booked appointment
    # forward, so a future stamp here is the booking and not an error. Seen on 490,000 of
    # 14,356,906,440 rows (0.00341%) when profiled on 2026-08-24.
    "gold.events.patient_event.event_end_datetime.future_owner":
        "NOT COALESCE((CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Kept as a regression guard on the accepted set ('resolved', 'provisional',
    # 'unresolved'): the research-surface filter already removes every row that fails it, so
    # this expectation should read zero forever and is worth watching for the day it does
    # not. Measured at 203 of 14,356,906,440 rows (1.41e-06%) when profiled on 2026-08-24.
    "gold.events.patient_event.identity_status.accepted_values":
        "NOT COALESCE((`identity_status` IS NOT NULL AND `identity_status` NOT IN ('resolved', 'provisional', 'unresolved')), FALSE)",

    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 1,121,031,702 of 14,356,906,440 at the profile.
    "gold.events.patient_event.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 13,288,735 of 14,356,906,440 rows (0.0926%) when profiled on
    # 2026-08-24.
    "gold.events.patient_event.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 58,507,245 of 14,356,906,440 rows (0.408%) when profiled on
    # 2026-08-24.
    "gold.events.patient_event.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",

    # Left as a warning because it fires on 507,133,196 of 14,356,906,440 rows (3.53%) when
    # profiled on 2026-08-24 -- at that rate the rule's assumption about what event_datetime
    # and event_end_datetime mean is the thing in doubt, not the data. The inverted gaps are
    # mostly minutes, which reads as two clocks rather than two events in the wrong order.
    "gold.events.patient_event.table.ordering_violation_event_datetime_event_end_datetime":
        "NOT COALESCE((`event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime`), FALSE)",
}

EVENTS_PATIENT_EVENT_COLUMN_COMMENTS = {
    "patient_event_row_key": "Deterministic row key (the mapping id for mapped rows; a namespaced unmapped id otherwise).",
    "patient_event_key": "Immutable deterministic fact-grain event key shared across a mapped event's N rows.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system behind subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT when available.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Clinical or administrative event time.",
    "event_end_datetime": "Event end when the source supplies one.",
    "event_type": "Fact kind (condition",
    "fact_category": "Clinical versus administrative fact.",
    "fact_table": "Typed fact table holding the event's values.",
    "fact_row_key": "Deterministic row key inside fact_table.",
    "source_system": "Plain-language source system (Millennium",
    "source_object": "Native source entity in plain language",
    "source_row_key": "Native source record key (BIGINT ids rendered as text); the typed native id lives on the fact table.",
    "source_coding_system": "The description for the code value",
    "source_code": "Source code as recorded.",
    "source_display": "Source display as recorded.",
    "confidentiality_code": "Source confidentiality label.",
    "vip_ind": "Source VIP indicator carried as data.",
    "withheld_identity_ind": "Source withheld-identity indicator.",
    "mapped_coding_system": "Standard coding system for this mapping row; null on the unmapped row.",
    "mapped_code": "Unique valid standard OMOP concept reached from exact ICD code semantics; NULL when absent or ambiguous.",
    "mapped_display": "Standard display or definition.",
    "target_domain": "Downstream routing domain for this mapping.",
    "map_source": "Governed mapping product that produced the row.",
    "map_version": "Mapping content version.",
    "map_method": "How the mapping was reached: source_native or rule for legacy lanes; S3 lookup lanes retain their explicit method.",
    "map_confidence": "Mapping confidence in [0,1] where the lane measured one.",
    "map_rule_id": "Rule or lane identifier that produced this candidate.",
    "map_candidate_count": "Number of candidates for the event, coding system and mapping source.",
    "record_status": "Status label carried unchanged from the contributing canonical arm through internal_event_index and the public event-to-mapping left join. It is not a Boolean mirror of IS_CURRENT_IN_SOURCE or a verbatim closure flag: each arm supplies its own active, superseded or retracted recipe. Every mapping row for an event inherits the event-side status; mapping fields do not replace it. The input is the canonical event-index row, not a promise of equality with a later typed public product's history or latest-version selection.",
    "loaded_at": "Per-event loaded_at carried unchanged from the contributing canonical arm through internal_event_index, then selected from the event side of the public mapping join. Source arms have different clock expressions, including typed nulls; this is not uniformly one bronze row's write time. Mapping clocks do not contribute, and mapping fan-out does not calculate a new maximum. Later typed-product enrichment, such as implant attribute clocks or additional document-history rows, is not folded back into this event-index clock. No current Silver refresh timestamp is substituted.",
}

@dp.materialized_view(
    name=_n("gold_qc._events_patient_event"),
    comment="Internal quality-controlled twin of events_patient_event: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(EVENTS_PATIENT_EVENT_MANDATORY_RULES)
@dp.expect_all(EVENTS_PATIENT_EVENT_ADVISORY_RULES)
def _gold_qc_events_patient_event():
    """Quality-controlled twin of journey_events.patient_event."""
    # 6,302,313 rows point at a encounter_id the spine does not have. The pointer is nulled
    # so it cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    # 10,710 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "events_patient_event",
        EVENTS_PATIENT_EVENT_SELECT,
        fk_columns=["encounter_id", "person_id"],
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, EVENTS_PATIENT_EVENT_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_events.patient_event"),
    comment=(
        "One row per admitted event x standard mapping; unmapped admitted events carry "
        "exactly one row with a null mapping block. Values live in the typed fact tables. "
        "Gold QC twin of the silver product: 6 columns are repaired or nulled, 1 rule(s) drop "
        "rows, 7 check(s) are advisory. Each rule states its reason in the pipeline notebook, "
        "and Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_events_patient_event():
    """Contract-v2 public twin of events_patient_event; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._events_patient_event")).selectExpr(
        '`patient_event_row_key` AS `patient_event_row_key`',
        '`patient_event_key` AS `patient_event_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`event_type` AS `event_type`',
        '`fact_category` AS `fact_category`',
        '`fact_table` AS `fact_table`',
        '`fact_row_key` AS `fact_row_key`',
        '`source_system` AS `source_system`',
        '`source_object` AS `source_object`',
        '`source_row_key` AS `source_row_key`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`mapped_coding_system` AS `mapped_coding_system`',
        '`mapped_code` AS `mapped_code`',
        '`mapped_display` AS `mapped_display`',
        '`target_domain` AS `target_domain`',
        '`map_source` AS `map_source`',
        '`map_version` AS `map_version`',
        '`map_method` AS `map_method`',
        '`map_confidence` AS `map_confidence`',
        '`map_rule_id` AS `map_rule_id`',
        '`map_candidate_count` AS `map_candidate_count`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, EVENTS_PATIENT_EVENT_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.clinical_score_component ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_clinical_score applies the parent Gold drop rule.
CLINICAL_CLINICAL_SCORE_COMPONENT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`event_id` AS `event_id`',
    '`doc_response_key` AS `doc_response_key`',
    '`source_object` AS `source_object`',
    '`sequence` AS `sequence`',
    '`component_id` AS `component_id`',
    '`coding_system` AS `coding_system`',
    '`coding_code` AS `coding_code`',
    '`coding_display` AS `coding_display`',
    *[f'`{name}` AS `{name}`' for name in _s3_axis_columns("component")],
    '`value_number` AS `value_number`',
    '`value_text` AS `value_text`',
    '`unit` AS `unit`',
    '`loaded_at` AS `loaded_at`',
]

CLINICAL_CLINICAL_SCORE_COMPONENT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic key of the parent clinical_clinical_score row.",
    "event_id": "Millennium numeric EVENT_ID for the numeric-event parent arm.",
    "doc_response_key": "PowerForm DOC_RESPONSE_KEY for the form-promotion parent arm.",
    "source_object": "Parent source arm: numeric_event or form_promotion.",
    "sequence": "Component sequence within the parent score.",
    "component_id": "Native source component identifier.",
    "coding_system": "Source coding-system namespace for the component.",
    "coding_code": "Source component code.",
    "coding_display": "Source component display.",
    **{
        name: "Governed S3b clinical-score component mapping field carried from Silver."
        for name in _s3_axis_columns("component")
    },
    "value_number": "Numeric component value when supplied.",
    "value_text": "Verbatim component text when supplied.",
    "unit": "Source component unit when supplied.",
    "loaded_at": "Bronze load timestamp inherited from the parent source row.",
}

CLINICAL_CLINICAL_SCORE_COMPONENT_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_clinical_score_component.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_clinical_score_component"),
    comment="Internal quality-controlled twin of clinical_clinical_score_component with the S3b admission decision.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_CLINICAL_SCORE_COMPONENT_MANDATORY_RULES)
def _gold_qc_clinical_clinical_score_component():
    """Prepare internal QC data for clinical_clinical_score_component; native rules run on the returned rows."""
    parent = spark.read.table(_n("gold_clinical.clinical_score")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_clinical_score_component"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_CLINICAL_SCORE_COMPONENT_SELECT, "__gold_parent_present AS __gold_parent_present")
    axis = "component"
    required = _s3_axis_columns(axis)
    item = {name[len(axis)+1:]: F.col(name) for name in required}
    gate, policy_id = _s3_axis_gate_and_policy(item)
    has_target = item["target_code"].isNotNull() | item["snomed_code"].isNotNull() | item["omop_concept_id"].isNotNull()
    return (df.withColumn("_component_gate_pass", F.when(has_target, gate))
            .withColumn("_component_policy_id", F.when(has_target, policy_id)))

@dp.materialized_view(
    name=_n("gold_clinical.clinical_score_component"),
    comment='One ordered component per native or governed promoted clinical score.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_clinical_score_component():
    """Task 3 Gold child with governed component targets and retained provenance."""
    df = spark.read.table(_n("gold_qc._clinical_clinical_score_component"))
    public = (_s3_gate_direct_public(df, "clinical_clinical_score_component")
              .drop("_component_gate_pass", "_component_policy_id", "__gold_parent_present"))
    return _with_comments(public, CLINICAL_CLINICAL_SCORE_COMPONENT_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.condition_revision ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_condition applies the parent Gold drop rule.
CLINICAL_CONDITION_REVISION_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`problem_id` AS `problem_id`',
    '`sequence` AS `sequence`',
    '`revision_rank` AS `revision_rank`',
    '`is_current_revision` AS `is_current_revision`',
    '`clinical_status_code` AS `clinical_status_code`',
    '`clinical_status_display` AS `clinical_status_display`',
    '`clinical_status_datetime` AS `clinical_status_datetime`',
    '`verification_status_code` AS `verification_status_code`',
    '`verification_status_display` AS `verification_status_display`',
    '`display` AS `display`',
    '`severity_code` AS `severity_code`',
    '`severity_display` AS `severity_display`',
    '`onset_datetime` AS `onset_datetime`',
    '`asserted_datetime` AS `asserted_datetime`',
    '`effective_start` AS `effective_start`',
    '`effective_end` AS `effective_end`',
    '`source_tombstone_ind` AS `source_tombstone_ind`',
    '`loaded_at` AS `loaded_at`',
    '`source_update_timestamp` AS `source_update_timestamp`',
]

CLINICAL_CONDITION_REVISION_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic key of the parent clinical_condition row.",
    "problem_id": "Millennium PROBLEM_ID of the parent problem condition.",
    "sequence": "Problem revision sequence, defaulted to zero when the source rank is null.",
    "revision_rank": "Source problem revision rank.",
    "is_current_revision": "Whether the source marks this as the current problem revision.",
    "clinical_status_code": "Source lifecycle-status code for this revision.",
    "clinical_status_display": "Source lifecycle-status display for this revision.",
    "clinical_status_datetime": "Source lifecycle-status timestamp for this revision.",
    "verification_status_code": "Source confirmation-status code for this revision.",
    "verification_status_display": "Source confirmation-status display for this revision.",
    "display": "Best available source problem display for this revision.",
    "severity_code": "Source severity code for this revision.",
    "severity_display": "Source severity display for this revision.",
    "onset_datetime": "Source onset timestamp carried by this revision.",
    "asserted_datetime": "Source assertion timestamp carried by this revision.",
    "effective_start": "Source revision effective-start timestamp.",
    "effective_end": "Source revision effective-end timestamp.",
    "source_tombstone_ind": "Whether the source revision is marked deleted.",
    "loaded_at": "Bronze load timestamp of the problem-history row.",
    "source_update_timestamp": "Source application update datetime.",
}

CLINICAL_CONDITION_REVISION_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_condition_revision.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_condition_revision"),
    comment="Internal QC of clinical_condition_revision; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_CONDITION_REVISION_MANDATORY_RULES)
def _gold_qc_clinical_condition_revision():
    """Task 3 Gold child of clinical_condition; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.condition")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_condition_revision"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_CONDITION_REVISION_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_CONDITION_REVISION_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.condition_revision"),
    comment='One Millennium problem-history revision per parent clinical_condition problem row.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_condition_revision():
    """Publish clinical_condition_revision without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_condition_revision")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.medication_admin_ingredient ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_medication_admin applies the parent Gold drop rule.
CLINICAL_MEDICATION_ADMIN_INGREDIENT_SELECT = [
    '`event_id` AS `event_id`',
    '`action_sequence` AS `action_sequence`',
    '`component_sequence` AS `component_sequence`',
    '`component_id` AS `component_id`',
    '`coding_system` AS `coding_system`',
    '`coding_code` AS `coding_code`',
    '`coding_display` AS `coding_display`',
    '`strength_value` AS `strength_value`',
    '`strength_unit` AS `strength_unit`',
    '`volume_value` AS `volume_value`',
    '`volume_unit` AS `volume_unit`',
    '`dose_value` AS `dose_value`',
    '`dose_unit` AS `dose_unit`',
    '`rate_value` AS `rate_value`',
    '`rate_unit` AS `rate_unit`',
    '`concentration_value` AS `concentration_value`',
    '`concentration_unit` AS `concentration_unit`',
    '`ingredient_type_code` AS `ingredient_type_code`',
    '`clinically_significant_ind` AS `clinically_significant_ind`',
    '`include_in_total_volume_ind` AS `include_in_total_volume_ind`',
    '`freetext_dose` AS `freetext_dose`',
    '`source_present_ind` AS `source_present_ind`',
    '`loaded_at` AS `loaded_at`',
]

CLINICAL_MEDICATION_ADMIN_INGREDIENT_SELECT += [
    '`ingredient_source_system` AS `ingredient_source_system`',
    '`ingredient_source_code` AS `ingredient_source_code`',
    '`ingredient_source_display` AS `ingredient_source_display`',
    '`ingredient_snomed_code` AS `ingredient_snomed_code`',
    '`ingredient_snomed_display` AS `ingredient_snomed_display`',
    '`ingredient_omop_concept_id` AS `ingredient_omop_concept_id`',
    '`ingredient_map_method` AS `ingredient_map_method`',
    '`ingredient_map_version` AS `ingredient_map_version`',
    '`ingredient_map_confidence` AS `ingredient_map_confidence`',
    '`ingredient_map_rule_id` AS `ingredient_map_rule_id`',
    '`ingredient_map_candidate_count` AS `ingredient_map_candidate_count`',
    '`ingredient_target_system` AS `ingredient_target_system`',
    '`ingredient_target_code` AS `ingredient_target_code`',
    '`ingredient_target_display` AS `ingredient_target_display`',
    '`ingredient_omop_source_concept_id` AS `ingredient_omop_source_concept_id`',
    '`ingredient_map_cosine` AS `ingredient_map_cosine`',
    '`ingredient_map_scoring_model` AS `ingredient_map_scoring_model`',
    '`ingredient_map_status` AS `ingredient_map_status`',
    '`ingredient_map_status_reason` AS `ingredient_map_status_reason`',
    '`ingredient_map_competing_count` AS `ingredient_map_competing_count`',
    '`ingredient_map_candidate_set_id` AS `ingredient_map_candidate_set_id`',
    '`ingredient_map_component_index` AS `ingredient_map_component_index`',
    '`ingredient_map_component_count` AS `ingredient_map_component_count`',
    '`ingredient_map_rollup_levels` AS `ingredient_map_rollup_levels`',
]

CLINICAL_MEDICATION_ADMIN_INGREDIENT_COLUMN_COMMENTS = {
    "event_id": "Millennium EVENT_ID of the parent medication administration.",
    "action_sequence": "Administration action sequence; zero when absent.",
    "component_sequence": "Ingredient component sequence within the action; zero when absent.",
    "component_id": "Source composite INGREDIENT_ORDER_ID:ACTION_SEQUENCE:COMP_SEQUENCE.",
    "coding_system": "Cerner synonym identifier namespace.",
    "coding_code": "Millennium SYNONYM_ID of the ingredient.",
    "coding_display": "Ordered-as, supplied-as, order, or HNA mnemonic; first available.",
    "strength_value": "Numeric strength value of the ingredient component, interpreted with STRENGTH_UNIT_CD.",
    "strength_unit": "Source strength-unit code.",
    "volume_value": "Numeric volume value for the ingredient component, interpreted with VOLUME_UNIT_CD.",
    "volume_unit": "Source volume-unit code.",
    "dose_value": "Ordered dose, falling back to dose quantity.",
    "dose_unit": "Ordered-dose unit code, falling back to dose-quantity unit code.",
    "rate_value": "Administration or infusion rate for this ingredient component normalised by the source system, interpreted with NORMALIZED_RATE_UNIT_CD.",
    "rate_unit": "Normalized rate-unit code.",
    "concentration_value": "Concentration value of this ingredient component within the order, interpreted with CONCENTRATION_UNIT_CD.",
    "concentration_unit": "Source concentration-unit code.",
    "ingredient_type_code": "Millennium ingredient type flag.",
    "clinically_significant_ind": "Whether the source flags the ingredient as clinically significant.",
    "include_in_total_volume_ind": "Whether the ingredient counts toward total volume.",
    "freetext_dose": "Free-text dose when supplied.",
    "source_present_ind": "True for rows retained by the current administration ingredient source.",
    "loaded_at": "Bronze load timestamp of the ingredient row.",
    "ingredient_source_system": "Governed S3c ingredient coding-axis field.",
    "ingredient_source_code": "Governed S3c ingredient coding-axis field.",
    "ingredient_source_display": "Governed S3c ingredient coding-axis field.",
    "ingredient_snomed_code": "Governed S3c ingredient coding-axis field.",
    "ingredient_snomed_display": "Governed S3c ingredient coding-axis field.",
    "ingredient_omop_concept_id": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_method": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_version": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_confidence": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_rule_id": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_candidate_count": "Governed S3c ingredient coding-axis field.",
    "ingredient_target_system": "Governed S3c ingredient coding-axis field.",
    "ingredient_target_code": "Governed S3c ingredient coding-axis field.",
    "ingredient_target_display": "Governed S3c ingredient coding-axis field.",
    "ingredient_omop_source_concept_id": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_cosine": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_scoring_model": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_status": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_status_reason": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_competing_count": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_candidate_set_id": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_component_index": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_component_count": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_rollup_levels": "Governed S3c ingredient coding-axis field.",
}

CLINICAL_MEDICATION_ADMIN_INGREDIENT_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_medication_admin_ingredient.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_medication_admin_ingredient"),
    comment="Internal QC of clinical_medication_admin_ingredient; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_ADMIN_INGREDIENT_MANDATORY_RULES)
def _gold_qc_clinical_medication_admin_ingredient():
    """Task 3 Gold child of clinical_medication_admin; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.medication_admin")).select(
        'event_id',
    ).dropDuplicates(['event_id'])
    child = _qc("clinical_medication_admin_ingredient", CLINICAL_MEDICATION_ADMIN_INGREDIENT_SELECT)
    df = _with_parent_status(child, parent, ['event_id'])
    public = (_s3_gate_direct_public(df, "clinical_medication_admin_ingredient")
              .drop("_ingredient_gate_pass", "_ingredient_policy_id"))
    return _with_comments(public, CLINICAL_MEDICATION_ADMIN_INGREDIENT_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.medication_admin_ingredient"),
    comment="One ingredient component per medication administration, retaining source component order and its parent event key.",
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_medication_admin_ingredient():
    """Publish clinical_medication_admin_ingredient without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_medication_admin_ingredient")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.medication_admin_status ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_medication_admin applies the parent Gold drop rule.
CLINICAL_MEDICATION_ADMIN_STATUS_SELECT = [
    '`event_id` AS `event_id`',
    '`sequence` AS `sequence`',
    '`status_kind` AS `status_kind`',
    '`status_code` AS `status_code`',
    '`status_display` AS `status_display`',
    '`effective_datetime` AS `effective_datetime`',
    '`practitioner_key` AS `practitioner_key`',
    '`source_action_sequence` AS `source_action_sequence`',
    '`source_present_ind` AS `source_present_ind`',
    '`loaded_at` AS `loaded_at`',
]

CLINICAL_MEDICATION_ADMIN_STATUS_COLUMN_COMMENTS = {
    "event_id": "Millennium EVENT_ID of the parent medication administration.",
    "sequence": "Deterministic status milestone sequence from the parent expression.",
    "status_kind": "Milestone kind such as scheduled, result, verified, or discontinued.",
    "status_code": "Source status code synthesized by the existing parent expression.",
    "status_display": "Source status display synthesized by the existing parent expression.",
    "effective_datetime": "Effective timestamp of the status milestone.",
    "practitioner_key": "Deterministic practitioner key when the milestone identifies personnel.",
    "source_action_sequence": "Source action sequence when supplied; null for synthesized milestones.",
    "source_present_ind": "True for status rows synthesized from a retained administration row.",
    "loaded_at": "Bronze load timestamp inherited from the parent source row.",
}

CLINICAL_MEDICATION_ADMIN_STATUS_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_medication_admin_status.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_medication_admin_status"),
    comment="Internal QC of clinical_medication_admin_status; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_ADMIN_STATUS_MANDATORY_RULES)
def _gold_qc_clinical_medication_admin_status():
    """Task 3 Gold child of clinical_medication_admin; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.medication_admin")).select(
        'event_id',
    ).dropDuplicates(['event_id'])
    child = spark.read.table(_src("clinical_medication_admin_status"))
    df = _with_parent_status(child, parent, ['event_id']).selectExpr(*CLINICAL_MEDICATION_ADMIN_STATUS_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_MEDICATION_ADMIN_STATUS_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.medication_admin_status"),
    comment="One status milestone per medication administration, retaining source sequence and the parent event key.",
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_medication_admin_status():
    """Publish clinical_medication_admin_status without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_medication_admin_status")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.medication_order_detail ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_medication_order applies the parent Gold drop rule.
CLINICAL_MEDICATION_ORDER_DETAIL_SELECT = [
    '`order_id` AS `order_id`',
    '`action_sequence` AS `action_sequence`',
    '`detail_sequence` AS `detail_sequence`',
    '`field_id` AS `field_id`',
    '`field_meaning` AS `field_meaning`',
    '`value_text` AS `value_text`',
    '`value_number` AS `value_number`',
    '`value_datetime` AS `value_datetime`',
    '`history_contract` AS `history_contract`',
    '`parent_action_sequence` AS `parent_action_sequence`',
    '`last_action_sequence` AS `last_action_sequence`',
    '`source_present_ind` AS `source_present_ind`',
    '`loaded_at` AS `loaded_at`',
]

CLINICAL_MEDICATION_ORDER_DETAIL_COLUMN_COMMENTS = {
    "order_id": "Millennium ORDER_ID of the parent medication order.",
    "action_sequence": "Order action sequence the detail belongs to; zero when absent.",
    "detail_sequence": "Detail sequence within the action; zero when absent.",
    "field_id": "Millennium order-entry field identifier.",
    "field_meaning": "Source order-entry field meaning.",
    "value_text": "Extended display value, falling back to display value.",
    "value_number": "Numeric order-detail value when supplied.",
    "value_datetime": "Datetime order-detail value when supplied.",
    "history_contract": "Source detail-history contract marker.",
    "parent_action_sequence": "Source parent action sequence.",
    "last_action_sequence": "Source last action sequence.",
    "source_present_ind": "Whether the source row remains present.",
    "loaded_at": "Bronze load timestamp of the order-detail row.",
}

CLINICAL_MEDICATION_ORDER_DETAIL_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_medication_order_detail.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_medication_order_detail"),
    comment="Internal QC of clinical_medication_order_detail; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_ORDER_DETAIL_MANDATORY_RULES)
def _gold_qc_clinical_medication_order_detail():
    """Task 3 Gold child of clinical_medication_order; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.medication_order")).select(
        'order_id',
    ).dropDuplicates(['order_id'])
    child = spark.read.table(_src("clinical_medication_order_detail"))
    df = _with_parent_status(child, parent, ['order_id']).selectExpr(*CLINICAL_MEDICATION_ORDER_DETAIL_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_MEDICATION_ORDER_DETAIL_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.medication_order_detail"),
    comment='One latest-action detail row per medication order field.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_medication_order_detail():
    """Publish clinical_medication_order_detail without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_medication_order_detail")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.medication_order_ingredient ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_medication_order applies the parent Gold drop rule.
CLINICAL_MEDICATION_ORDER_INGREDIENT_SELECT = [
    '`order_id` AS `order_id`',
    '`action_sequence` AS `action_sequence`',
    '`component_sequence` AS `component_sequence`',
    '`component_id` AS `component_id`',
    '`coding_system` AS `coding_system`',
    '`coding_code` AS `coding_code`',
    '`coding_display` AS `coding_display`',
    '`strength_value` AS `strength_value`',
    '`strength_unit` AS `strength_unit`',
    '`volume_value` AS `volume_value`',
    '`volume_unit` AS `volume_unit`',
    '`dose_value` AS `dose_value`',
    '`dose_unit` AS `dose_unit`',
    '`rate_value` AS `rate_value`',
    '`rate_unit` AS `rate_unit`',
    '`concentration_value` AS `concentration_value`',
    '`concentration_unit` AS `concentration_unit`',
    '`ingredient_type_code` AS `ingredient_type_code`',
    '`clinically_significant_ind` AS `clinically_significant_ind`',
    '`include_in_total_volume_ind` AS `include_in_total_volume_ind`',
    '`freetext_dose` AS `freetext_dose`',
    '`source_present_ind` AS `source_present_ind`',
    '`loaded_at` AS `loaded_at`',
]

CLINICAL_MEDICATION_ORDER_INGREDIENT_SELECT += [
    '`ingredient_source_system` AS `ingredient_source_system`',
    '`ingredient_source_code` AS `ingredient_source_code`',
    '`ingredient_source_display` AS `ingredient_source_display`',
    '`ingredient_snomed_code` AS `ingredient_snomed_code`',
    '`ingredient_snomed_display` AS `ingredient_snomed_display`',
    '`ingredient_omop_concept_id` AS `ingredient_omop_concept_id`',
    '`ingredient_map_method` AS `ingredient_map_method`',
    '`ingredient_map_version` AS `ingredient_map_version`',
    '`ingredient_map_confidence` AS `ingredient_map_confidence`',
    '`ingredient_map_rule_id` AS `ingredient_map_rule_id`',
    '`ingredient_map_candidate_count` AS `ingredient_map_candidate_count`',
    '`ingredient_target_system` AS `ingredient_target_system`',
    '`ingredient_target_code` AS `ingredient_target_code`',
    '`ingredient_target_display` AS `ingredient_target_display`',
    '`ingredient_omop_source_concept_id` AS `ingredient_omop_source_concept_id`',
    '`ingredient_map_cosine` AS `ingredient_map_cosine`',
    '`ingredient_map_scoring_model` AS `ingredient_map_scoring_model`',
    '`ingredient_map_status` AS `ingredient_map_status`',
    '`ingredient_map_status_reason` AS `ingredient_map_status_reason`',
    '`ingredient_map_competing_count` AS `ingredient_map_competing_count`',
    '`ingredient_map_candidate_set_id` AS `ingredient_map_candidate_set_id`',
    '`ingredient_map_component_index` AS `ingredient_map_component_index`',
    '`ingredient_map_component_count` AS `ingredient_map_component_count`',
    '`ingredient_map_rollup_levels` AS `ingredient_map_rollup_levels`',
]

CLINICAL_MEDICATION_ORDER_INGREDIENT_COLUMN_COMMENTS = {
    "order_id": "Millennium ORDER_ID of the parent medication order.",
    "action_sequence": "Order action sequence the ingredient row belongs to; zero when absent.",
    "component_sequence": "Ingredient component sequence within the action; zero when absent.",
    "component_id": "Source composite ORDER_ID:ACTION_SEQUENCE:COMP_SEQUENCE.",
    "coding_system": "Cerner synonym identifier namespace.",
    "coding_code": "Millennium SYNONYM_ID of the ingredient.",
    "coding_display": "Ordered-as, supplied-as, order, or HNA mnemonic; first available.",
    "strength_value": "Ingredient strength as ordered.",
    "strength_unit": "Strength unit description.",
    "volume_value": "Ingredient volume as ordered.",
    "volume_unit": "Volume unit description.",
    "dose_value": "Ordered dose, falling back to dose quantity.",
    "dose_unit": "Ordered-dose unit, falling back to dose-quantity unit.",
    "rate_value": "S10 medication_order medication/dosage; retained as a source-faithful bronze input or Journey standard-block field.",
    "rate_unit": "Normalized rate unit description.",
    "concentration_value": "Concentration as ordered.",
    "concentration_unit": "Concentration unit description.",
    "ingredient_type_code": "Millennium ingredient type flag.",
    "clinically_significant_ind": "Whether the source flags the ingredient as clinically significant.",
    "include_in_total_volume_ind": "Whether the ingredient counts toward total volume.",
    "freetext_dose": "Free-text dose when supplied.",
    "source_present_ind": "Whether the source row remains present.",
    "loaded_at": "Bronze load timestamp of the ingredient row.",
    "ingredient_source_system": "Governed S3c ingredient coding-axis field.",
    "ingredient_source_code": "Governed S3c ingredient coding-axis field.",
    "ingredient_source_display": "Governed S3c ingredient coding-axis field.",
    "ingredient_snomed_code": "Governed S3c ingredient coding-axis field.",
    "ingredient_snomed_display": "Governed S3c ingredient coding-axis field.",
    "ingredient_omop_concept_id": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_method": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_version": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_confidence": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_rule_id": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_candidate_count": "Governed S3c ingredient coding-axis field.",
    "ingredient_target_system": "Governed S3c ingredient coding-axis field.",
    "ingredient_target_code": "Governed S3c ingredient coding-axis field.",
    "ingredient_target_display": "Governed S3c ingredient coding-axis field.",
    "ingredient_omop_source_concept_id": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_cosine": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_scoring_model": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_status": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_status_reason": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_competing_count": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_candidate_set_id": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_component_index": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_component_count": "Governed S3c ingredient coding-axis field.",
    "ingredient_map_rollup_levels": "Governed S3c ingredient coding-axis field.",
}

CLINICAL_MEDICATION_ORDER_INGREDIENT_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_medication_order_ingredient.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_medication_order_ingredient"),
    comment="Internal QC of clinical_medication_order_ingredient; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_ORDER_INGREDIENT_MANDATORY_RULES)
def _gold_qc_clinical_medication_order_ingredient():
    """Task 3 Gold child of clinical_medication_order; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.medication_order")).select(
        'order_id',
    ).dropDuplicates(['order_id'])
    child = _qc("clinical_medication_order_ingredient", CLINICAL_MEDICATION_ORDER_INGREDIENT_SELECT)
    df = _with_parent_status(child, parent, ['order_id'])
    public = (_s3_gate_direct_public(df, "clinical_medication_order_ingredient")
              .drop("_ingredient_gate_pass", "_ingredient_policy_id"))
    return _with_comments(public, CLINICAL_MEDICATION_ORDER_INGREDIENT_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.medication_order_ingredient"),
    comment="One ordered ingredient component per medication order, linked to its parent by the retained event key.",
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_medication_order_ingredient():
    """Publish clinical_medication_order_ingredient without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_medication_order_ingredient")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.medication_order_status ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_medication_order applies the parent Gold drop rule.
CLINICAL_MEDICATION_ORDER_STATUS_SELECT = [
    '`order_id` AS `order_id`',
    '`sequence` AS `sequence`',
    '`status_kind` AS `status_kind`',
    '`status_code` AS `status_code`',
    '`status_display` AS `status_display`',
    '`effective_datetime` AS `effective_datetime`',
    '`practitioner_key` AS `practitioner_key`',
    '`source_action_sequence` AS `source_action_sequence`',
    '`action_type_code` AS `action_type_code`',
    '`action_type_display` AS `action_type_display`',
    '`action_qualifier_code` AS `action_qualifier_code`',
    '`action_qualifier_display` AS `action_qualifier_display`',
    '`action_rejected_ind` AS `action_rejected_ind`',
    '`historical_feed_ind` AS `historical_feed_ind`',
    '`source_present_ind` AS `source_present_ind`',
    '`loaded_at` AS `loaded_at`',
]

CLINICAL_MEDICATION_ORDER_STATUS_COLUMN_COMMENTS = {
    "order_id": "Millennium ORDER_ID of the parent medication order.",
    "sequence": "Deterministic action sequence, defaulted to zero when absent.",
    "status_kind": "Source action type display, falling back to its code.",
    "status_code": "Source order-status code at this action.",
    "status_display": "Source order-status display, falling back to action type.",
    "effective_datetime": "Action, effective, or order timestamp; first available.",
    "practitioner_key": "Deterministic practitioner key derived from ACTION_PERSONNEL_ID.",
    "source_action_sequence": "Verbatim source action sequence.",
    "action_type_code": "Source action-type code.",
    "action_type_display": "Source action-type display.",
    "action_qualifier_code": "Source action-qualifier code.",
    "action_qualifier_display": "Source action-qualifier display.",
    "action_rejected_ind": "Whether the source marks the action rejected.",
    "historical_feed_ind": "Whether the action came from the historical feed.",
    "source_present_ind": "Whether the source row remains present.",
    "loaded_at": "Bronze load timestamp of the action row.",
}

CLINICAL_MEDICATION_ORDER_STATUS_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_medication_order_status.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_medication_order_status"),
    comment="Internal QC of clinical_medication_order_status; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_ORDER_STATUS_MANDATORY_RULES)
def _gold_qc_clinical_medication_order_status():
    """Task 3 Gold child of clinical_medication_order; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.medication_order")).select(
        'order_id',
    ).dropDuplicates(['order_id'])
    child = spark.read.table(_src("clinical_medication_order_status"))
    df = _with_parent_status(child, parent, ['order_id']).selectExpr(*CLINICAL_MEDICATION_ORDER_STATUS_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_MEDICATION_ORDER_STATUS_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.medication_order_status"),
    comment='One ordered source action or status row per medication order.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_medication_order_status():
    """Publish clinical_medication_order_status without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_medication_order_status")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.procedure_implant_attribute ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_procedure applies the parent Gold drop rule.
CLINICAL_PROCEDURE_IMPLANT_ATTRIBUTE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`implant_event_id` AS `implant_event_id`',
    '`implant_sequence` AS `implant_sequence`',
    '`sequence` AS `sequence`',
    '`clinical_event_id` AS `clinical_event_id`',
    '`attribute_name` AS `attribute_name`',
    '`attribute_value` AS `attribute_value`',
    '`event_tag` AS `event_tag`',
    '`event_title` AS `event_title`',
    '`result_value` AS `result_value`',
    '`value_source` AS `value_source`',
    '`value_conflict_ind` AS `value_conflict_ind`',
    '`performed_datetime` AS `performed_datetime`',
    '`valid_from` AS `valid_from`',
    '`loaded_at` AS `loaded_at`',
]

CLINICAL_PROCEDURE_IMPLANT_ATTRIBUTE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic key of the parent implant clinical_procedure row.",
    "implant_event_id": "Millennium implant EVENT_ID; first native parent-key component.",
    "implant_sequence": "Millennium IMPLANT_SEQUENCE; second native parent-key component.",
    "sequence": "Deterministic source ordering value derived from performed or event-start time.",
    "clinical_event_id": "Source clinical-event identifier for the implant attribute.",
    "attribute_name": "Source implant attribute name.",
    "attribute_value": "Source implant attribute value.",
    "event_tag": "Source clinical-event tag.",
    "event_title": "Source clinical-event title.",
    "result_value": "Source result value.",
    "value_source": "Source route that supplied the retained value.",
    "value_conflict_ind": "Whether source evidence for the value conflicts.",
    "performed_datetime": "Source performed timestamp.",
    "valid_from": "Source validity-start timestamp.",
    "loaded_at": "Bronze load timestamp of the implant attribute row.",
}

CLINICAL_PROCEDURE_IMPLANT_ATTRIBUTE_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_procedure_implant_attribute.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_procedure_implant_attribute"),
    comment="Internal QC of clinical_procedure_implant_attribute; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_PROCEDURE_IMPLANT_ATTRIBUTE_MANDATORY_RULES)
def _gold_qc_clinical_procedure_implant_attribute():
    """Task 3 Gold child of clinical_procedure; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.procedure")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_procedure_implant_attribute"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_PROCEDURE_IMPLANT_ATTRIBUTE_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_PROCEDURE_IMPLANT_ATTRIBUTE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.procedure_implant_attribute"),
    comment='One source implant attribute per parent implant clinical_procedure row.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_procedure_implant_attribute():
    """Publish clinical_procedure_implant_attribute without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_procedure_implant_attribute")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.presenting_complaint (S3b) ====

PRESENTING_COMPLAINT_COLUMNS = [
    "patient_event_key", "source_patient_event_key", "encounter_key", "encounter_id",
    "subject_key", "subject_id_system", "person_id", "source_object", "source_event_id",
    "statement_sequence", "event_datetime", "encounter_class", "source_coding_system",
    "source_code", "source_display", "complaint_text_normalised", "complaint_group_display",
    *_s3_axis_columns("complaint"),
    "record_status", "record_status_effective_from", "record_status_effective_to",
    "confidentiality_code", "vip_ind", "withheld_identity_ind",
    "source_update_timestamp", "loaded_at",
]
PRESENTING_COMPLAINT_SELECT = [f"`{name}` AS `{name}`" for name in PRESENTING_COMPLAINT_COLUMNS]
PRESENTING_COMPLAINT_MANDATORY_RULES = {"research_surface": "identity_status = 'resolved'"}
PRESENTING_COMPLAINT_ADVISORY_RULES = {
    "gold.clinical.presenting_complaint.after_death_30d": "NOT COALESCE(event_after_death_30d, FALSE)",
    "gold.clinical.presenting_complaint.before_birth": "NOT COALESCE(event_before_birth, FALSE)",
}
PRESENTING_COMPLAINT_COMMENTS = {
    name: ("Cosine similarity between the normalised source text and the selected target."
           if name == "complaint_map_cosine"
           else "S3b presenting-complaint field carried from the governed Silver product.")
    for name in PRESENTING_COMPLAINT_COLUMNS
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_presenting_complaint"),
    comment="Internal quality-controlled twin of clinical_presenting_complaint with the S3b admission decision.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(PRESENTING_COMPLAINT_MANDATORY_RULES)
@dp.expect_all(PRESENTING_COMPLAINT_ADVISORY_RULES)
def _gold_qc_clinical_presenting_complaint():
    """Prepare internal QC data for clinical_presenting_complaint; native rules run on the returned rows."""
    df = _qc(
        "clinical_presenting_complaint", PRESENTING_COMPLAINT_SELECT,
        fk_columns=("person_id", "encounter_id"),
        date_flags=("event_after_death_30d", "event_before_birth"),
    )
    return _with_comments(df, PRESENTING_COMPLAINT_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.presenting_complaint"),
    comment="Research-facing presenting complaints from encounter RFV, ED pick-list and bounded ED text sources.",
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_presenting_complaint():
    """Publish clinical_presenting_complaint with its existing research columns and quality policy."""
    qc = spark.read.table(_n("gold_qc._clinical_presenting_complaint"))
    selected = qc.select(
        *PRESENTING_COMPLAINT_COLUMNS,
        "_complaint_gate_pass", "_complaint_policy_id",
    )
    return (_s3_gate_direct_public(selected, "clinical_presenting_complaint")
            .drop("_complaint_gate_pass", "_complaint_policy_id"))

# COMMAND ----------

