"""Gold clinical.condition: 4 dataset(s).

- datasets: _qc_clinical_condition, clinical_condition, _qc_clinical_condition_stage, clinical_condition_stage
- moved verbatim from: 2_gold_clinical_events:1001, 2_gold_clinical_events:1039, 2_gold_clinical_events:1051, 2_gold_clinical_events:1058, 2_gold_clinical_events:1137, 2_gold_clinical_events:1141, 2_gold_clinical_events:1154, 2_gold_clinical_events:712

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


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
    # ENCOUNTER_SLICE_V1: a slice belongs to its encounter, so it is nulled wherever encounter_id is.
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_slice_id` END AS `encounter_slice_id`',
    "CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS DATE) = DATE'1900-01-01' OR CAST(`event_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`event_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `event_datetime` END END AS `event_datetime`",
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    "CASE WHEN `event_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS DATE) = DATE'1900-01-01' OR CAST(`event_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`event_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `event_datetime` END END) IS NULL THEN NULL ELSE `event_datetime_local` END AS `event_datetime_local`",  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`abatement_datetime_local` AS `abatement_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    '`condition_code` AS `condition_code`',
    '`condition_source_system` AS `condition_source_system`',  # SRC_LINEAGE_V1
    "CASE WHEN UPPER(TRIM(CAST(`condition_source_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `condition_source_code` END AS `condition_source_code`",  # SRC_LINEAGE_V1
    '`condition_source_display` AS `condition_source_display`',  # SRC_LINEAGE_V1
    "CASE WHEN UPPER(TRIM(CAST(`category_code` AS STRING))) = '0' THEN NULL ELSE `category_code` END AS `category_code`",
    '`category_display` AS `category_display`',
    # DIAG_PRIORITY_V1: Millennium diagnosis ranking. 0 means "not ranked" in both priority fields and in RANKING_CD, so
    # it is nulled here (the same treatment as category_code '0'); silver keeps the verbatim value.
    "CASE WHEN `diag_priority` = 0 THEN NULL ELSE `diag_priority` END AS `diag_priority`",
    "CASE WHEN `clinical_diag_priority` = 0 THEN NULL ELSE `clinical_diag_priority` END AS `clinical_diag_priority`",
    "CASE WHEN UPPER(TRIM(CAST(`ranking_code` AS STRING))) = '0' THEN NULL ELSE `ranking_code` END AS `ranking_code`",
    '`ranking_display` AS `ranking_display`',
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
    # INSR_W3 W3.06: Gold: NULL where the Cerner system user id 1 (J5; silver publishes the flag).
    "CASE WHEN CAST(`recorder_practitioner_id` AS BIGINT) = 1 THEN NULL ELSE `recorder_practitioner_id` END AS `recorder_practitioner_id`",
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


CLINICAL_CONDITION_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`clinical_status_effective_datetime` AS `clinical_status_effective_datetime`',
    '`onset_precision_flag` AS `onset_precision_flag`',
    '`onset_precision_code` AS `onset_precision_code`',
    '`condition_omop_map_rule_id` AS `condition_omop_map_rule_id`',
    '`condition_omop_map_version` AS `condition_omop_map_version`',
    '`recorder_system_ind` AS `recorder_system_ind`',
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
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.condition.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # NDC_DISPLAY_SUFFIX_V1 regression guard: Millennium's retired-term suffix ("... 07-SEP-2022 18:17:27<$>")
    # is stripped upstream in Nomenclature_Pipeline, so this should read zero forever.
    "gold.clinical.condition.source_display.no_retired_term_suffix":
        "NOT COALESCE(`source_display` LIKE '%<$>', FALSE)",

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

    # DQ4 G2. Left as a warning because either the code or the person's recorded sex may be
    # the wrong side. Seen on 1,191 of 50,978,755 rows when profiled on 2026-09-26.
    "gold.clinical.condition.table.sex_implausible_concept":
        "NOT COALESCE(event_sex_implausible, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_CONDITION_PERSON_SCOPE_RULES = {
    "gold.clinical.condition.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_condition"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_CONDITION_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CONDITION_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_CONDITION_PERSON_SCOPE_RULES)
def _gold_qc_clinical_condition():
    """Quality-controlled twin of journey_clinical.condition."""
    df = _qc("clinical_condition", CLINICAL_CONDITION_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.condition"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
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
        '`encounter_slice_id` AS `encounter_slice_id`',  # ENCOUNTER_SLICE_V1
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`abatement_datetime_local` AS `abatement_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`__gold_json_condition_code` AS `condition_code`',
        '`condition_source_system` AS `condition_source_system`',  # SRC_LINEAGE_V1
        '`condition_source_code` AS `condition_source_code`',  # SRC_LINEAGE_V1
        '`condition_source_display` AS `condition_source_display`',  # SRC_LINEAGE_V1
        '`category_code` AS `category_code`',
        '`category_display` AS `category_display`',
        # DIAG_PRIORITY_V1: diagnosis ranking.
        '`diag_priority` AS `diag_priority`',
        '`clinical_diag_priority` AS `clinical_diag_priority`',
        '`ranking_code` AS `ranking_code`',
        '`ranking_display` AS `ranking_display`',
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
        '`clinical_status_effective_datetime` AS `clinical_status_effective_datetime`',  # INSR_W3 W3.12
        '`onset_precision_flag` AS `onset_precision_flag`',  # INSR_W3 W3.12
        '`onset_precision_code` AS `onset_precision_code`',  # INSR_W3 W3.12
        '`condition_omop_map_rule_id` AS `condition_omop_map_rule_id`',  # INSR_W3 W3.12
        '`condition_omop_map_version` AS `condition_omop_map_version`',  # INSR_W3 W3.12
        '`recorder_system_ind` AS `recorder_system_ind`',  # INSR_W3 W3.12
    )
    return _s3_flatten_gold_public(df, "clinical_condition")


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
    '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`stage_code` AS `stage_code`',
    '`stage_diagnosis_source_system` AS `stage_diagnosis_source_system`',  # SRC_LINEAGE_V1
    '`stage_diagnosis_source_code` AS `stage_diagnosis_source_code`',  # SRC_LINEAGE_V1
    '`stage_diagnosis_source_display` AS `stage_diagnosis_source_display`',  # SRC_LINEAGE_V1
    '`stage_group_source_system` AS `stage_group_source_system`',  # SRC_LINEAGE_V1
    '`stage_group_source_code` AS `stage_group_source_code`',  # SRC_LINEAGE_V1
    '`stage_group_source_display` AS `stage_group_source_display`',  # SRC_LINEAGE_V1
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


CLINICAL_CONDITION_STAGE_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`stage_source_kind` AS `stage_source_kind`',
    '`stage_source_id` AS `stage_source_id`',
    '`stage_prefix` AS `stage_prefix`',
    '`t_stage` AS `t_stage`',
    '`n_stage` AS `n_stage`',
    '`m_stage` AS `m_stage`',
    '`stage_text` AS `stage_text`',
    '`stage_rule_id` AS `stage_rule_id`',
]


# contract v2: QC/batch inputs come from internal _clinical_condition_stage_metadata; source history stays on the main research table.
CLINICAL_CONDITION_STAGE_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 130,441 rows of 133,086
    # that are current and attributable. Superseded versions and rows whose identity was
    # never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved')",
}


CLINICAL_CONDITION_STAGE_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.condition_stage.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
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


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_CONDITION_STAGE_PERSON_SCOPE_RULES = {
    "gold.clinical.condition_stage.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_condition_stage"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_CONDITION_STAGE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CONDITION_STAGE_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_CONDITION_STAGE_PERSON_SCOPE_RULES)
def _gold_qc_clinical_condition_stage():
    """Quality-controlled twin of journey_clinical.condition_stage."""
    df = _qc("clinical_condition_stage", CLINICAL_CONDITION_STAGE_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.condition_stage"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
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
        '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`__gold_json_stage_code` AS `stage_code`',
        '`stage_diagnosis_source_system` AS `stage_diagnosis_source_system`',  # SRC_LINEAGE_V1
        '`stage_diagnosis_source_code` AS `stage_diagnosis_source_code`',  # SRC_LINEAGE_V1
        '`stage_diagnosis_source_display` AS `stage_diagnosis_source_display`',  # SRC_LINEAGE_V1
        '`stage_group_source_system` AS `stage_group_source_system`',  # SRC_LINEAGE_V1
        '`stage_group_source_code` AS `stage_group_source_code`',  # SRC_LINEAGE_V1
        '`stage_group_source_display` AS `stage_group_source_display`',  # SRC_LINEAGE_V1
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
        '`stage_source_kind` AS `stage_source_kind`',  # INSR_W3 W3.12
        '`stage_source_id` AS `stage_source_id`',  # INSR_W3 W3.12
        '`stage_prefix` AS `stage_prefix`',  # INSR_W3 W3.12
        '`t_stage` AS `t_stage`',  # INSR_W3 W3.12
        '`n_stage` AS `n_stage`',  # INSR_W3 W3.12
        '`m_stage` AS `m_stage`',  # INSR_W3 W3.12
        '`stage_text` AS `stage_text`',  # INSR_W3 W3.12
        '`stage_rule_id` AS `stage_rule_id`',  # INSR_W3 W3.12
    )
    return _s3_flatten_gold_public(df, "clinical_condition_stage")


def _public_steps_clinical_condition(df):
    """clinical_condition: the public body after its projection (GoldRules.public_select), moved from gold_clinical_condition; df is the projected twin frame."""
    return _s3_flatten_gold_public(df, 'clinical_condition')


def _public_steps_clinical_condition_stage(df):
    """clinical_condition_stage: the public body after its projection (GoldRules.public_select), moved from gold_clinical_condition_stage; df is the projected twin frame."""
    return _s3_flatten_gold_public(df, 'clinical_condition_stage')


RULES = {
    'clinical_condition': GoldRules(
        table='clinical_condition',
        select=CLINICAL_CONDITION_SELECT,
        mandatory=CLINICAL_CONDITION_MANDATORY_RULES,
        advisory=CLINICAL_CONDITION_ADVISORY_RULES,
        person_scope=CLINICAL_CONDITION_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'person_id_resolved',
                             'encounter_id_resolved',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth'),
        sex_plausibility='condition_omop_concept_id',
        variant_columns=('condition_code',),
        s3_axis_specs={'condition_code': [('condition', False)]},
        value_rules={'gold.clinical.condition.asserter_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                              'column': 'asserter_practitioner_id',
                                                                              'companions': [],
                                                                              'match': '`asserter_practitioner_id` '
                                                                                       '= 0',
                                                                              'table': 'clinical_condition'},
         'gold.clinical.condition.condition_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                 'column': 'condition_snomed_code',
                                                                                 'companions': ['condition_snomed_display',
                                                                                                'condition_target_code',
                                                                                                'condition_target_display'],
                                                                                 'match': '`condition_snomed_code` '
                                                                                          'IS NOT NULL AND '
                                                                                          '(NOT '
                                                                                          '`condition_snomed_code` '
                                                                                          'RLIKE '
                                                                                          "'^[1-9][0-9]{5,17}$' "
                                                                                          'OR '
                                                                                          'substring(`condition_snomed_code`, '
                                                                                          'length(`condition_snomed_code`) '
                                                                                          '- 2, 2) NOT IN '
                                                                                          "('00', '10') OR "
                                                                                          '(substring(`condition_snomed_code`, '
                                                                                          'length(`condition_snomed_code`) '
                                                                                          "- 2, 2) = '10' "
                                                                                          'AND '
                                                                                          'length(`condition_snomed_code`) '
                                                                                          '< 11))',
                                                                                 'table': 'clinical_condition'},
         'gold.clinical.condition.condition_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                 'column': 'condition_target_code',
                                                                                 'companions': ['condition_target_display',
                                                                                                'condition_snomed_code',
                                                                                                'condition_snomed_display'],
                                                                                 'match': '`condition_target_system` '
                                                                                          '= '
                                                                                          "'http://snomed.info/sct' "
                                                                                          'AND '
                                                                                          '`condition_target_code` '
                                                                                          'IS NOT NULL AND '
                                                                                          '(NOT '
                                                                                          '`condition_target_code` '
                                                                                          'RLIKE '
                                                                                          "'^[1-9][0-9]{5,17}$' "
                                                                                          'OR '
                                                                                          'substring(`condition_target_code`, '
                                                                                          'length(`condition_target_code`) '
                                                                                          '- 2, 2) NOT IN '
                                                                                          "('00', '10') OR "
                                                                                          '(substring(`condition_target_code`, '
                                                                                          'length(`condition_target_code`) '
                                                                                          "- 2, 2) = '10' "
                                                                                          'AND '
                                                                                          'length(`condition_target_code`) '
                                                                                          '< 11))',
                                                                                 'table': 'clinical_condition'},
         'gold.clinical.condition.encounter_slice_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                        'column': 'encounter_slice_id',
                                                                        'companions': [],
                                                                        'match': '`encounter_slice_id` = 0',
                                                                        'table': 'clinical_condition'},
         'gold.clinical.condition.recorder_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                              'column': 'recorder_practitioner_id',
                                                                              'companions': [],
                                                                              'match': '`recorder_practitioner_id` '
                                                                                       '= 0',
                                                                              'table': 'clinical_condition'}},
        public_select=[
            '`patient_event_key` AS `patient_event_key`',
            '`source_patient_event_key` AS `source_patient_event_key`',
            '`source_object` AS `source_object`',
            '`diagnosis_id` AS `diagnosis_id`',
            '`problem_id` AS `problem_id`',
            '`subject_key` AS `subject_key`',
            '`subject_id_system` AS `subject_id_system`',
            '`person_id` AS `person_id`',
            '`encounter_id` AS `encounter_id`',
            '`encounter_slice_id` AS `encounter_slice_id`',
            '`event_datetime` AS `event_datetime`',
            '`event_end_datetime` AS `event_end_datetime`',
            '`event_datetime_local` AS `event_datetime_local`',
            '`event_end_datetime_local` AS `event_end_datetime_local`',
            '`abatement_datetime_local` AS `abatement_datetime_local`',
            '`source_coding_system` AS `source_coding_system`',
            '`source_code` AS `source_code`',
            '`source_display` AS `source_display`',
            '`__gold_json_condition_code` AS `condition_code`',
            '`condition_source_system` AS `condition_source_system`',  # SRC_LINEAGE_V1
            '`condition_source_code` AS `condition_source_code`',  # SRC_LINEAGE_V1
            '`condition_source_display` AS `condition_source_display`',  # SRC_LINEAGE_V1
            '`category_code` AS `category_code`',
            '`category_display` AS `category_display`',
            '`diag_priority` AS `diag_priority`',
            '`clinical_diag_priority` AS `clinical_diag_priority`',
            '`ranking_code` AS `ranking_code`',
            '`ranking_display` AS `ranking_display`',
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
            '`clinical_status_effective_datetime` AS `clinical_status_effective_datetime`',
            '`onset_precision_flag` AS `onset_precision_flag`',
            '`onset_precision_code` AS `onset_precision_code`',
            '`condition_omop_map_rule_id` AS `condition_omop_map_rule_id`',
            '`condition_omop_map_version` AS `condition_omop_map_version`',
            '`recorder_system_ind` AS `recorder_system_ind`',
        ],
        public_steps=(_public_steps_clinical_condition,)),
    'clinical_condition_stage': GoldRules(
        table='clinical_condition_stage',
        select=CLINICAL_CONDITION_STAGE_SELECT,
        mandatory=CLINICAL_CONDITION_STAGE_MANDATORY_RULES,
        advisory=CLINICAL_CONDITION_STAGE_ADVISORY_RULES,
        person_scope=CLINICAL_CONDITION_STAGE_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth'),
        variant_columns=('stage_code',),
        s3_axis_specs={'stage_code': [('stage_diagnosis', False), ('stage_group', False)]},
        value_rules={'gold.clinical.condition_stage.stage_diagnosis_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                             'column': 'stage_diagnosis_snomed_code',
                                                                                             'companions': ['stage_diagnosis_snomed_display',
                                                                                                            'stage_diagnosis_target_code',
                                                                                                            'stage_diagnosis_target_display'],
                                                                                             'match': '`stage_diagnosis_snomed_code` '
                                                                                                      'IS '
                                                                                                      'NOT '
                                                                                                      'NULL '
                                                                                                      'AND '
                                                                                                      '(NOT '
                                                                                                      '`stage_diagnosis_snomed_code` '
                                                                                                      'RLIKE '
                                                                                                      "'^[1-9][0-9]{5,17}$' "
                                                                                                      'OR '
                                                                                                      'substring(`stage_diagnosis_snomed_code`, '
                                                                                                      'length(`stage_diagnosis_snomed_code`) '
                                                                                                      '- '
                                                                                                      '2, '
                                                                                                      '2) '
                                                                                                      'NOT '
                                                                                                      'IN '
                                                                                                      "('00', "
                                                                                                      "'10') "
                                                                                                      'OR '
                                                                                                      '(substring(`stage_diagnosis_snomed_code`, '
                                                                                                      'length(`stage_diagnosis_snomed_code`) '
                                                                                                      '- '
                                                                                                      '2, '
                                                                                                      '2) '
                                                                                                      '= '
                                                                                                      "'10' "
                                                                                                      'AND '
                                                                                                      'length(`stage_diagnosis_snomed_code`) '
                                                                                                      '< '
                                                                                                      '11))',
                                                                                             'table': 'clinical_condition_stage'},
         'gold.clinical.condition_stage.stage_diagnosis_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                             'column': 'stage_diagnosis_target_code',
                                                                                             'companions': ['stage_diagnosis_target_display',
                                                                                                            'stage_diagnosis_snomed_code',
                                                                                                            'stage_diagnosis_snomed_display'],
                                                                                             'match': '`stage_diagnosis_target_system` '
                                                                                                      '= '
                                                                                                      "'http://snomed.info/sct' "
                                                                                                      'AND '
                                                                                                      '`stage_diagnosis_target_code` '
                                                                                                      'IS '
                                                                                                      'NOT '
                                                                                                      'NULL '
                                                                                                      'AND '
                                                                                                      '(NOT '
                                                                                                      '`stage_diagnosis_target_code` '
                                                                                                      'RLIKE '
                                                                                                      "'^[1-9][0-9]{5,17}$' "
                                                                                                      'OR '
                                                                                                      'substring(`stage_diagnosis_target_code`, '
                                                                                                      'length(`stage_diagnosis_target_code`) '
                                                                                                      '- '
                                                                                                      '2, '
                                                                                                      '2) '
                                                                                                      'NOT '
                                                                                                      'IN '
                                                                                                      "('00', "
                                                                                                      "'10') "
                                                                                                      'OR '
                                                                                                      '(substring(`stage_diagnosis_target_code`, '
                                                                                                      'length(`stage_diagnosis_target_code`) '
                                                                                                      '- '
                                                                                                      '2, '
                                                                                                      '2) '
                                                                                                      '= '
                                                                                                      "'10' "
                                                                                                      'AND '
                                                                                                      'length(`stage_diagnosis_target_code`) '
                                                                                                      '< '
                                                                                                      '11))',
                                                                                             'table': 'clinical_condition_stage'},
         'gold.clinical.condition_stage.stage_group_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                         'column': 'stage_group_snomed_code',
                                                                                         'companions': ['stage_group_snomed_display',
                                                                                                        'stage_group_target_code',
                                                                                                        'stage_group_target_display'],
                                                                                         'match': '`stage_group_snomed_code` '
                                                                                                  'IS NOT '
                                                                                                  'NULL '
                                                                                                  'AND '
                                                                                                  '(NOT '
                                                                                                  '`stage_group_snomed_code` '
                                                                                                  'RLIKE '
                                                                                                  "'^[1-9][0-9]{5,17}$' "
                                                                                                  'OR '
                                                                                                  'substring(`stage_group_snomed_code`, '
                                                                                                  'length(`stage_group_snomed_code`) '
                                                                                                  '- 2, 2) '
                                                                                                  'NOT IN '
                                                                                                  "('00', "
                                                                                                  "'10') "
                                                                                                  'OR '
                                                                                                  '(substring(`stage_group_snomed_code`, '
                                                                                                  'length(`stage_group_snomed_code`) '
                                                                                                  '- 2, 2) '
                                                                                                  "= '10' "
                                                                                                  'AND '
                                                                                                  'length(`stage_group_snomed_code`) '
                                                                                                  '< 11))',
                                                                                         'table': 'clinical_condition_stage'},
         'gold.clinical.condition_stage.stage_group_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                         'column': 'stage_group_target_code',
                                                                                         'companions': ['stage_group_target_display',
                                                                                                        'stage_group_snomed_code',
                                                                                                        'stage_group_snomed_display'],
                                                                                         'match': '`stage_group_target_system` '
                                                                                                  '= '
                                                                                                  "'http://snomed.info/sct' "
                                                                                                  'AND '
                                                                                                  '`stage_group_target_code` '
                                                                                                  'IS NOT '
                                                                                                  'NULL '
                                                                                                  'AND '
                                                                                                  '(NOT '
                                                                                                  '`stage_group_target_code` '
                                                                                                  'RLIKE '
                                                                                                  "'^[1-9][0-9]{5,17}$' "
                                                                                                  'OR '
                                                                                                  'substring(`stage_group_target_code`, '
                                                                                                  'length(`stage_group_target_code`) '
                                                                                                  '- 2, 2) '
                                                                                                  'NOT IN '
                                                                                                  "('00', "
                                                                                                  "'10') "
                                                                                                  'OR '
                                                                                                  '(substring(`stage_group_target_code`, '
                                                                                                  'length(`stage_group_target_code`) '
                                                                                                  '- 2, 2) '
                                                                                                  "= '10' "
                                                                                                  'AND '
                                                                                                  'length(`stage_group_target_code`) '
                                                                                                  '< 11))',
                                                                                         'table': 'clinical_condition_stage'}},
        public_select=[
            '`patient_event_key` AS `patient_event_key`',
            '`aria_pt_id` AS `aria_pt_id`',
            '`aria_dx_id` AS `aria_dx_id`',
            '`subject_key` AS `subject_key`',
            '`subject_id_system` AS `subject_id_system`',
            '`person_id` AS `person_id`',
            '`encounter_id` AS `encounter_id`',
            '`event_datetime` AS `event_datetime`',
            '`event_end_datetime` AS `event_end_datetime`',
            '`event_datetime_local` AS `event_datetime_local`',
            '`event_end_datetime_local` AS `event_end_datetime_local`',
            '`source_coding_system` AS `source_coding_system`',
            '`source_code` AS `source_code`',
            '`source_display` AS `source_display`',
            '`__gold_json_stage_code` AS `stage_code`',
            '`stage_diagnosis_source_system` AS `stage_diagnosis_source_system`',  # SRC_LINEAGE_V1
            '`stage_diagnosis_source_code` AS `stage_diagnosis_source_code`',  # SRC_LINEAGE_V1
            '`stage_diagnosis_source_display` AS `stage_diagnosis_source_display`',  # SRC_LINEAGE_V1
            '`stage_group_source_system` AS `stage_group_source_system`',  # SRC_LINEAGE_V1
            '`stage_group_source_code` AS `stage_group_source_code`',  # SRC_LINEAGE_V1
            '`stage_group_source_display` AS `stage_group_source_display`',  # SRC_LINEAGE_V1
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
            '`stage_source_kind` AS `stage_source_kind`',
            '`stage_source_id` AS `stage_source_id`',
            '`stage_prefix` AS `stage_prefix`',
            '`t_stage` AS `t_stage`',
            '`n_stage` AS `n_stage`',
            '`m_stage` AS `m_stage`',
            '`stage_text` AS `stage_text`',
            '`stage_rule_id` AS `stage_rule_id`',
        ],
        public_steps=(_public_steps_clinical_condition_stage,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_flatten_gold_public = _rules._s3_flatten_gold_public


finalise()


COMMENTS = {'_qc_clinical_condition': {'columns': {'abatement_datetime': 'Source abatement or effective-end timestamp.',
                                        'abatement_datetime_local': 'Source abatement or effective-end '
                                                                    'timestamp for the condition expressed '
                                                                    'in local time.',
                                        'asserted_datetime': 'Source assertion timestamp.',
                                        'asserter_practitioner_id': 'Millennium personnel PERSON_ID as '
                                                                    'BIGINT when available.',
                                        'body_site_code': 'Body-site code where supplied.',
                                        'body_site_display': 'Body-site display where supplied.',
                                        'category_code': 'Source condition category code.',
                                        'category_display': 'Source condition category display.',
                                        'clinical_diag_priority': 'Millennium '
                                                                  'DIAGNOSIS.CLINICAL_DIAG_PRIORITY, '
                                                                  "verbatim: the clinician's ordering of "
                                                                  'diagnoses recorded at the point of care, '
                                                                  '1 = first listed; Gold nulls 0 (not '
                                                                  'ranked). Set mainly on clinician-entered '
                                                                  'Discharge, Working and Admitting '
                                                                  'diagnoses; null on coded Final and '
                                                                  'Principal diagnoses and on the problem '
                                                                  'and maternity arms.',
                                        'clinical_status_code': 'Source clinical-status code.',
                                        'clinical_status_display': 'Source clinical-status display.',
                                        'clinical_status_effective_datetime': 'When the problem took its '
                                                                              'current life-cycle status '
                                                                              '(map_problem '
                                                                              'LIFE_CYCLE_DT_TM, UTC '
                                                                              'instant); NULL on diagnosis '
                                                                              'and maternity rows. INSR_W2 '
                                                                              'W2.19 (L931).',
                                        'condition_code': 'Source and mapped condition codings.',
                                        'condition_omop_map_rule_id': 'Rule that filled '
                                                                      'condition_omop_concept_id / '
                                                                      'condition_omop_source_concept_id from '
                                                                      "the OMOP 'Maps to' lookup "
                                                                      '(omop:maps_to:<vocabulary>); NULL '
                                                                      "when those ids came with the row's "
                                                                      'own mapping. INSR_W2 W2.03.',
                                        'condition_omop_map_version': 'Vocabulary release '
                                                                      '(<vocabulary>|<version>) of the '
                                                                      'lookup that filled the condition OMOP '
                                                                      'ids; NULL when the lane filled '
                                                                      'nothing. INSR_W2 W2.03.',
                                        'confidentiality_code': 'Source confidentiality code.',
                                        'diag_priority': 'Millennium DIAGNOSIS.DIAG_PRIORITY, verbatim: the '
                                                         "clinical coder's sequence number for the diagnosis "
                                                         'within its consultant episode '
                                                         '(encounter_slice_id), 1 = primary diagnosis of '
                                                         'that episode; Gold nulls 0 (not ranked). Set on '
                                                         'coded Final and Principal ICD-10 diagnoses. A '
                                                         'spell with several consultant episodes has one '
                                                         'rank-1 diagnosis per episode. Null on the problem '
                                                         'and maternity arms.',
                                        'diagnosis_id': 'Millennium DIAGNOSIS_ID; primary key for the '
                                                        'diagnosis arm.',
                                        'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                        'encounter_slice_id': 'Millennium ENCNTR_SLICE_ID as BIGINT: the '
                                                              'encounter slice (consultant episode) the '
                                                              'diagnosis was coded against. Clinical coders '
                                                              'record one ranked diagnosis list per slice, '
                                                              'so diag_priority is ranked within it. '
                                                              'Diagnosis arm only; null on the problem and '
                                                              'maternity arms. Gold: NULL wherever Gold '
                                                              'nulls encounter_id.',
                                        'event_datetime': 'Primary assertion timestamp.',
                                        'event_datetime_local': 'Primary condition assertion timestamp '
                                                                'expressed in local time. Gold: NULL '
                                                                'wherever Gold nulls event_datetime.',
                                        'event_end_datetime': 'Source effective end where supplied.',
                                        'event_end_datetime_local': 'Source effective end timestamp for the '
                                                                    'condition assertion expressed in local '
                                                                    'time, where supplied. Gold: NULL '
                                                                    'wherever Gold nulls event_end_datetime.',
                                        'laterality_code': 'Source laterality code.',
                                        'laterality_display': 'Source laterality display.',
                                        'loaded_at': 'Diagnosis and maternity arms carry their own ADC_UPDT. '
                                                     'The problem arm takes greatest(map_problem.ADC_UPDT, '
                                                     'maximum map_problem_history.ADC_UPDT for that '
                                                     'PROBLEM_ID), across all retained revisions rather than '
                                                     'only the latest. Problem-history UPDT_DT_TM is not '
                                                     "added to the parent's source_update_timestamp, which "
                                                     'remains its own UPDT_DT_TM; diagnosis uses its own '
                                                     'UPDT_DT_TM and maternity uses RECORD_UPDATED_DT. The '
                                                     'maternity identity join adds no pregnancy-spine clock, '
                                                     'and the union adds no cross-arm timestamp maximum.',
                                        'onset_datetime': 'Source onset timestamp.',
                                        'onset_precision_code': 'Millennium ONSET_DT_CD (code value '
                                                                'qualifying the onset date; 0, the no-code '
                                                                'value, is NULL; set on about 0.5% of active '
                                                                'problems). NULL on diagnosis and maternity '
                                                                'rows. INSR_W2 (L4261).',
                                        'onset_precision_flag': 'Millennium ONSET_DT_FLAG as recorded: the '
                                                                'precision indicator for onset_datetime (0 = '
                                                                'full date and time; a non-zero value means '
                                                                'reduced precision, so the day or month of '
                                                                'onset_datetime may be a placeholder). Set '
                                                                'on every dated problem onset; NULL on '
                                                                'diagnosis and maternity rows. INSR_W2 '
                                                                '(L4261).',
                                        'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                             'cross-feed join key.',
                                        'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                        'problem_id': 'Millennium PROBLEM_ID; primary key for the problem '
                                                      'arm.',
                                        'ranking_code': 'Millennium RANKING_CD, verbatim (code set 12034: '
                                                        '3310 Primary, 3311 Secondary, 684907 Tertiary); '
                                                        "Gold nulls '0' (not ranked). Rarely set on "
                                                        'diagnoses (about 68,000 of 46.6 million rows); set '
                                                        'on about 119,000 problems. Null on the maternity '
                                                        'arm.',
                                        'ranking_display': 'Display for ranking_code from bronze '
                                                           'map_problem.ranking_desc. Null on the diagnosis '
                                                           'arm, because bronze map_diagnosis carries no '
                                                           'ranking description, and on the maternity arm.',
                                        'record_status': 'Always active. Inactive, deleted and end-dated '
                                                         '(superseded) diagnosis and problem rows are not '
                                                         'brought into silver (ACTIVE_ROWS_V1, '
                                                         'silver_active_rows.py); the maternity-diagnosis '
                                                         'arm is always active.',
                                        'record_status_effective_from': 'ACTIVE_STATUS_DT_TM from '
                                                                        'map_diagnosis or map_problem on the '
                                                                        'corresponding Millennium arm; '
                                                                        'DIAGDATE_CLEAN from '
                                                                        'map_maternity_diagnosis on the '
                                                                        'maternity arm. Values are carried '
                                                                        'unchanged without a fallback. The '
                                                                        'maternity value is a diagnosis-date '
                                                                        'proxy rather than an independently '
                                                                        'observed row-status change.',
                                        'record_status_effective_to': 'Always null: end-dated diagnosis and '
                                                                      'problem rows are not brought into '
                                                                      'silver (ACTIVE_ROWS_V1).',
                                        'recorder_practitioner_id': 'Millennium personnel PERSON_ID as '
                                                                    'BIGINT when available.',
                                        'recorder_system_ind': 'True when the source '
                                                               'recorder_practitioner_id was 1, the Cerner '
                                                               'system user (J5); the id is then published '
                                                               'as NULL. NULL when the source id was NULL. '
                                                               'INSR_W2.',
                                        'revision_history_count': 'Number of retained problem revision rows.',
                                        'severity_code': 'Source severity code.',
                                        'severity_display': 'Source severity display.',
                                        'source_code': 'Source condition code',
                                        'source_coding_system': 'Verbatim source coding system.',
                                        'source_display': 'Verbatim source condition display.',
                                        'source_feed': 'Registered source feed owning the assertion.',
                                        'source_object': 'Source arm: diagnosis, problem, or maternity.',
                                        'source_patient_event_key': 'Stable patient-event identity retained '
                                                                    'from the originating Silver row before '
                                                                    'source splitting.',
                                        'source_update_timestamp': 'Source application update datetime.',
                                        'subject_id_system': 'Identifier system used for subject_key.',
                                        'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                       'identifier. Not salted, not secret; a stable join '
                                                       'key across feeds where person_id is unresolved.',
                                        'verification_status_code': 'Source verification-status code.',
                                        'verification_status_display': 'Source verification-status display.',
                                        'vip_ind': 'Source VIP indicator.',
                                        'withheld_identity_ind': 'Identity-withholding status is not '
                                                                 'populated by the current source '
                                                                 'projections; null does not mean identity '
                                                                 'was not withheld.'},
                            'quantity_override': True,
                            'table': 'Internal quality-controlled twin of clinical_condition: the Gold '
                                     'repairs, nulls and expectations are applied here and the published '
                                     'Gold MV reads this object. Materialized rather than temporary so the '
                                     'published MV can refresh incrementally.'},
 '_qc_clinical_condition_stage': {'columns': {'aria_dx_id': 'ARIA diagnosis identifier; second component of '
                                                            'the primary key.',
                                              'aria_pt_id': 'ARIA patient identifier; first component of the '
                                                            'primary key.',
                                              'cause_of_death_ind': 'True when the ARIA cause-of-death '
                                                                    'indicator `CS_OF_DTH_IND` is Y after '
                                                                    'uppercasing; null source values and '
                                                                    'other values become false.',
                                              'clinical_description': 'Clinical diagnosis description.',
                                              'confidentiality_code': 'Security classification when '
                                                                      'supplied.',
                                              'current_entry_ind': 'True when the ARIA current-entry '
                                                                   'indicator `CUR_ENTRY_IND` is Y after '
                                                                   'uppercasing; null source values and '
                                                                   'other values become false.',
                                              'dx_comment': 'Free-text comment recorded against the '
                                                            'diagnosis entry.',
                                              'dx_confirmed': 'Diagnosis confirmation state.',
                                              'dx_method': 'Coded value recording the method by which the '
                                                           'diagnosis was made.',
                                              'dx_type': 'Code indicating the type or category of the '
                                                         'diagnosis entry.',
                                              'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                              'available.',
                                              'event_datetime': 'Sentinel-cleaned diagnosis onset timestamp.',
                                              'event_datetime_local': 'event_datetime in Europe/London '
                                                                      'wall-clock time (GMT/BST).',
                                              'event_end_datetime': 'Sentinel-cleaned resolution timestamp.',
                                              'event_end_datetime_local': 'event_end_datetime in '
                                                                          'Europe/London wall-clock time '
                                                                          '(GMT/BST). Gold: NULL wherever '
                                                                          'Gold nulls event_end_datetime.',
                                              'history_ind': 'Indicator showing whether the entry represents '
                                                             'a history of the condition rather than an '
                                                             'active diagnosis.',
                                              'loaded_at': 'map_aria_diagnosis_staging.ADC_UPDT carried '
                                                           'unchanged through code filtering and '
                                                           'quality/public projections. This is the '
                                                           'contributing bronze load clock, distinct from '
                                                           'PIPELINE_UPDT_DT_TM published as '
                                                           'source_update_timestamp; it is not disease '
                                                           'onset/resolution or Silver refresh time.',
                                              'm_stage': 'M category as recorded. NULL on ARIA rows (bronze '
                                                         'map_cancer_stage_mill.M_STAGE, INSR W1.14). '
                                                         'INSR_W2 W2.20.',
                                              'n_stage': 'N category as recorded. NULL on ARIA rows (bronze '
                                                         'map_cancer_stage_mill.N_STAGE, INSR W1.14). '
                                                         'INSR_W2 W2.20.',
                                              'onset_datetime': 'Source onset timestamp.',
                                              'patient_event_key': 'Deterministic event key derived from '
                                                                   'source identifiers; retained as a stable '
                                                                   'join key.',
                                              'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                           'resolved.',
                                              'person_link_status': 'Bronze person-link status.',
                                              'record_status': "Superseded when coalesce(CUR_ENTRY_IND,'Y') "
                                                               'differs from the exact case-sensitive value '
                                                               'Y, without trimming, or SOURCE_PRESENT_IND '
                                                               'is false (null defaults true); otherwise '
                                                               'active. No retracted status is emitted. This '
                                                               'differs from current_entry_ind, which '
                                                               'uppercases CUR_ENTRY_IND and defaults a '
                                                               'missing value to N.',
                                              'record_status_effective_from': 'map_aria_diagnosis_staging.EVLV_TSTAMP_CLEAN '
                                                                              'carried unchanged as the '
                                                                              'history start. It is the '
                                                                              'source-supplied cleaned '
                                                                              'timestamp, not a fallback to '
                                                                              'diagnosis onset, resolution '
                                                                              'or ingestion time; a missing '
                                                                              'value remains null.',
                                              'record_status_effective_to': 'map_aria_diagnosis_staging.ADC_UPDT '
                                                                            'only when the row is '
                                                                            'superseded; otherwise null. '
                                                                            'This is an ingestion-time '
                                                                            'proxy, not '
                                                                            'RESOLUTION_DATE_CLEAN or an '
                                                                            'independently observed '
                                                                            'status-transition timestamp; a '
                                                                            'missing ADC_UPDT remains null.',
                                              'resolution_datetime': 'Source resolution timestamp.',
                                              'source_code': 'ICD diagnosis code with diagnosis-name '
                                                             'fallback.',
                                              'source_coding_system': 'ICD-10 coding system.',
                                              'source_display': 'Diagnosis name or description.',
                                              'source_update_timestamp': 'Bronze pipeline processing '
                                                                         'timestamp carried from '
                                                                         '`PIPELINE_UPDT_DT_TM` in '
                                                                         '`4_prod.bronze.map_aria_diagnosis_staging`. '
                                                                         'It records when bronze processed '
                                                                         'the source row, not a native '
                                                                         'clinical-system update timestamp, '
                                                                         'clinical event time, or the '
                                                                         'current Silver refresh time.',
                                              'stage_code': 'Source ICD and mapped OMOP diagnosis codings.',
                                              'stage_criteria': 'Staging criteria description.',
                                              'stage_of_disease': 'Source disease stage.',
                                              'stage_prefix': 'p, c, yp or yc when stated; NULL otherwise. '
                                                              'NULL on ARIA rows (bronze '
                                                              'map_cancer_stage_mill.STAGE_PREFIX, INSR '
                                                              'W1.14). INSR_W2 W2.20.',
                                              'stage_rule_id': 'URO_TNM_EVENTCD_V1 or PTNM_REGEX_V1. NULL on '
                                                               'ARIA rows (bronze '
                                                               'map_cancer_stage_mill.RULE_ID, INSR W1.14). '
                                                               'INSR_W2 W2.20.',
                                              'stage_source_id': 'Clinical EVENT_ID, or pathology '
                                                                 'report_version_id. NULL on ARIA rows '
                                                                 '(bronze map_cancer_stage_mill.SOURCE_ID, '
                                                                 'INSR W1.14). INSR_W2 W2.20.',
                                              'stage_source_kind': 'URO_TNM_EVENT or PATH_REPORT_PTNM. NULL '
                                                                   'on ARIA rows (bronze '
                                                                   'map_cancer_stage_mill.SOURCE_KIND, INSR '
                                                                   'W1.14). INSR_W2 W2.20.',
                                              'stage_text': 'The matched statement or the event result text. '
                                                            'NULL on ARIA rows (bronze '
                                                            'map_cancer_stage_mill.STAGE_TEXT, INSR W1.14). '
                                                            'INSR_W2 W2.20.',
                                              'subject_id_system': 'Identifier system used for subject_key.',
                                              'subject_key': 'Deterministic SHA-256 over the strongest '
                                                             'source identifier. Not salted, not secret; a '
                                                             'stable join key across feeds where person_id '
                                                             'is unresolved.',
                                              't_stage': 'T category as recorded (e.g. T2a, Tis, Tx). NULL '
                                                         'on ARIA rows (bronze '
                                                         'map_cancer_stage_mill.T_STAGE, INSR W1.14). '
                                                         'INSR_W2 W2.20.',
                                              'vip_ind': 'VIP indicator when supplied.',
                                              'withheld_identity_ind': 'Identity-withholding status is not '
                                                                       'populated by the current source '
                                                                       'projections; null does not mean '
                                                                       'identity was not withheld.'},
                                  'quantity_override': True,
                                  'table': 'Internal quality-controlled twin of clinical_condition_stage: '
                                           'the Gold repairs, nulls and expectations are applied here and '
                                           'the published Gold MV reads this object. Materialized rather '
                                           'than temporary so the published MV can refresh incrementally.'},
 'clinical_condition': {'columns': {'abatement_datetime': 'Source abatement or effective-end timestamp.',
                                    'abatement_datetime_local': 'Source abatement or effective-end timestamp '
                                                                'for the condition expressed in local time.',
                                    'asserted_datetime': 'Source assertion timestamp.',
                                    'asserter_practitioner_id': 'Millennium personnel PERSON_ID as BIGINT '
                                                                'when available.',
                                    'body_site_code': 'Body-site code where supplied.',
                                    'body_site_display': 'Body-site display where supplied.',
                                    'category_code': 'Source condition category code.',
                                    'category_display': 'Source condition category display.',
                                    'clinical_diag_priority': 'Millennium DIAGNOSIS.CLINICAL_DIAG_PRIORITY, '
                                                              "verbatim: the clinician's ordering of "
                                                              'diagnoses recorded at the point of care, 1 = '
                                                              'first listed; Gold nulls 0 (not ranked). Set '
                                                              'mainly on clinician-entered Discharge, '
                                                              'Working and Admitting diagnoses; null on '
                                                              'coded Final and Principal diagnoses and on '
                                                              'the problem and maternity arms.',
                                    'clinical_status_code': 'Source clinical-status code.',
                                    'clinical_status_display': 'Source clinical-status display.',
                                    'clinical_status_effective_datetime': 'When the problem took its current '
                                                                          'life-cycle status (map_problem '
                                                                          'LIFE_CYCLE_DT_TM, UTC instant); '
                                                                          'NULL on diagnosis and maternity '
                                                                          'rows. INSR_W2 W2.19 (L931).',
                                    'condition_code': 'Source and mapped condition codings.',
                                    'condition_omop_map_rule_id': 'Rule that filled '
                                                                  'condition_omop_concept_id / '
                                                                  'condition_omop_source_concept_id from the '
                                                                  "OMOP 'Maps to' lookup "
                                                                  '(omop:maps_to:<vocabulary>); NULL when '
                                                                  "those ids came with the row's own "
                                                                  'mapping. INSR_W2 W2.03.',
                                    'condition_omop_map_version': 'Vocabulary release '
                                                                  '(<vocabulary>|<version>) of the lookup '
                                                                  'that filled the condition OMOP ids; NULL '
                                                                  'when the lane filled nothing. INSR_W2 '
                                                                  'W2.03.',
                                    'confidentiality_code': 'Source confidentiality code.',
                                    'diag_priority': 'Millennium DIAGNOSIS.DIAG_PRIORITY, verbatim: the '
                                                     "clinical coder's sequence number for the diagnosis "
                                                     'within its encounter, 1 = primary diagnosis; 0 = not '
                                                     'ranked. Set on coded Final and Principal ICD-10 '
                                                     'diagnoses; null on the problem and maternity arms. '
                                                     'About 13% of coded encounters have more than one '
                                                     'diagnosis at 1 (the same code repeated, or one primary '
                                                     'per consultant episode). Rows are diagnosis_id x map '
                                                     'component, so a primary diagnosis that maps to several '
                                                     'concepts appears on several rows: count primaries with '
                                                     'COUNT(DISTINCT diagnosis_id) WHERE diag_priority = 1.',
                                    'diagnosis_id': 'Millennium DIAGNOSIS_ID; primary key for the diagnosis '
                                                    'arm.',
                                    'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                    'encounter_slice_id': 'Millennium ENCNTR_SLICE_ID as BIGINT: the '
                                                          'encounter slice (consultant episode) the '
                                                          'diagnosis was coded against. Clinical coders '
                                                          'record one ranked diagnosis list per slice, so '
                                                          'diag_priority is ranked within it. Diagnosis arm '
                                                          'only; null on the problem and maternity arms. '
                                                          'Gold: NULL wherever Gold nulls encounter_id.',
                                    'event_datetime': 'Primary assertion timestamp.',
                                    'event_datetime_local': 'Primary condition assertion timestamp expressed '
                                                            'in local time. Gold: NULL wherever Gold nulls '
                                                            'event_datetime.',
                                    'event_end_datetime': 'Source effective end where supplied.',
                                    'event_end_datetime_local': 'Source effective end timestamp for the '
                                                                'condition assertion expressed in local '
                                                                'time, where supplied. Gold: NULL wherever '
                                                                'Gold nulls event_end_datetime.',
                                    'laterality_code': 'Source laterality code.',
                                    'laterality_display': 'Source laterality display.',
                                    'loaded_at': 'Diagnosis and maternity arms carry their own ADC_UPDT. The '
                                                 'problem arm takes greatest(map_problem.ADC_UPDT, maximum '
                                                 'map_problem_history.ADC_UPDT for that PROBLEM_ID), across '
                                                 'all retained revisions rather than only the latest. '
                                                 "Problem-history UPDT_DT_TM is not added to the parent's "
                                                 'source_update_timestamp, which remains its own UPDT_DT_TM; '
                                                 'diagnosis uses its own UPDT_DT_TM and maternity uses '
                                                 'RECORD_UPDATED_DT. The maternity identity join adds no '
                                                 'pregnancy-spine clock, and the union adds no cross-arm '
                                                 'timestamp maximum.',
                                    'onset_datetime': 'Source onset timestamp.',
                                    'onset_precision_code': 'Millennium ONSET_DT_CD (code value qualifying '
                                                            'the onset date; 0, the no-code value, is NULL; '
                                                            'set on about 0.5% of active problems). NULL on '
                                                            'diagnosis and maternity rows. INSR_W2 (L4261).',
                                    'onset_precision_flag': 'Millennium ONSET_DT_FLAG as recorded: the '
                                                            'precision indicator for onset_datetime (0 = '
                                                            'full date and time; a non-zero value means '
                                                            'reduced precision, so the day or month of '
                                                            'onset_datetime may be a placeholder). Set on '
                                                            'every dated problem onset; NULL on diagnosis '
                                                            'and maternity rows. INSR_W2 (L4261).',
                                    'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                         'cross-feed join key.',
                                    'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                    'problem_id': 'Millennium PROBLEM_ID; primary key for the problem arm.',
                                    'ranking_code': 'Millennium RANKING_CD, verbatim (code set 12034: 3310 '
                                                    'Primary, 3311 Secondary, 684907 Tertiary); Gold nulls '
                                                    "'0' (not ranked). Rarely set on diagnoses (about 68,000 "
                                                    'of 46.6 million rows); set on about 119,000 problems. '
                                                    'Null on the maternity arm.',
                                    'ranking_display': 'Display for ranking_code from bronze '
                                                       'map_problem.ranking_desc. Null on the diagnosis arm, '
                                                       'because bronze map_diagnosis carries no ranking '
                                                       'description, and on the maternity arm.',
                                    'record_status': 'Always active. Inactive, deleted and end-dated '
                                                     '(superseded) diagnosis and problem rows are not '
                                                     'brought into silver (ACTIVE_ROWS_V1, '
                                                     'silver_active_rows.py); the maternity-diagnosis arm is '
                                                     'always active.',
                                    'record_status_effective_from': 'ACTIVE_STATUS_DT_TM from map_diagnosis '
                                                                    'or map_problem on the corresponding '
                                                                    'Millennium arm; DIAGDATE_CLEAN from '
                                                                    'map_maternity_diagnosis on the '
                                                                    'maternity arm. Values are carried '
                                                                    'unchanged without a fallback. The '
                                                                    'maternity value is a diagnosis-date '
                                                                    'proxy rather than an independently '
                                                                    'observed row-status change.',
                                    'record_status_effective_to': 'Always null: end-dated diagnosis and '
                                                                  'problem rows are not brought into silver '
                                                                  '(ACTIVE_ROWS_V1).',
                                    'recorder_practitioner_id': 'Millennium personnel PERSON_ID as BIGINT '
                                                                'when available.',
                                    'recorder_system_ind': 'True when the source recorder_practitioner_id '
                                                           'was 1, the Cerner system user (J5); the id is '
                                                           'then published as NULL. NULL when the source id '
                                                           'was NULL. INSR_W2.',
                                    'revision_history_count': 'Number of retained problem revision rows.',
                                    'severity_code': 'Source severity code.',
                                    'severity_display': 'Source severity display.',
                                    'source_code': 'Source condition code',
                                    'source_coding_system': 'Verbatim source coding system.',
                                    'source_display': 'Verbatim source condition display.',
                                    'source_feed': 'Registered source feed owning the assertion.',
                                    'source_object': 'Source arm: diagnosis, problem, or maternity.',
                                    'source_patient_event_key': 'Stable patient-event identity retained from '
                                                                'the originating Silver row before source '
                                                                'splitting.',
                                    'source_update_timestamp': 'Source application update datetime.',
                                    'subject_id_system': 'Identifier system used for subject_key.',
                                    'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                   'identifier. Not salted, not secret; a stable join key '
                                                   'across feeds where person_id is unresolved.',
                                    'verification_status_code': 'Source verification-status code.',
                                    'verification_status_display': 'Source verification-status display.',
                                    'vip_ind': 'Source VIP indicator.',
                                    'withheld_identity_ind': 'Identity-withholding status is not populated '
                                                             'by the current source projections; null does '
                                                             'not mean identity was not withheld.'},
                        'quantity_override': False,
                        'table': 'One row per diagnosis (or problem / maternity condition) and map '
                                 'component: a diagnosis whose source code maps to several SNOMED concepts '
                                 'has one row per component (condition_map_component_index). Count primary '
                                 'diagnoses with COUNT(DISTINCT diagnosis_id) WHERE diag_priority = 1, not '
                                 'count(*). Declared key: patient_event_key, condition_map_component_index. '
                                 'Coverage: Diagnoses and procedures dated March-April 2026 are about half '
                                 'the January/May volume (gold ratio 0.50 diagnoses, 0.54 procedures); gold '
                                 'holds what raw holds for those months (missing ids 2 of 119,823 and 186 of '
                                 '92,312), so the shortfall is at source and was not re-landed; Problem '
                                 'assertion times are captured only from October 2024 at source; Diagnosis '
                                 'onset has no precision column at source, so a year-only onset appears as 1 '
                                 'January; problem onsets carry their precision flag; The most recent two to '
                                 "three months are incomplete while clinical coding catches up; each table's "
                                 'complete-through month says where the data is complete; complete-through '
                                 'month per release: 6_mgmt.data_quality.release_finding (finding_code '
                                 'F2_complete_through).'},
 'clinical_condition_stage': {'columns': {'aria_dx_id': 'ARIA diagnosis identifier; second component of the '
                                                        'primary key.',
                                          'aria_pt_id': 'ARIA patient identifier; first component of the '
                                                        'primary key.',
                                          'cause_of_death_ind': 'True when the ARIA cause-of-death indicator '
                                                                '`CS_OF_DTH_IND` is Y after uppercasing; '
                                                                'null source values and other values become '
                                                                'false.',
                                          'clinical_description': 'Clinical diagnosis description.',
                                          'confidentiality_code': 'Security classification when supplied.',
                                          'current_entry_ind': 'True when the ARIA current-entry indicator '
                                                               '`CUR_ENTRY_IND` is Y after uppercasing; null '
                                                               'source values and other values become false.',
                                          'dx_comment': 'Free-text comment recorded against the diagnosis '
                                                        'entry.',
                                          'dx_confirmed': 'Diagnosis confirmation state.',
                                          'dx_method': 'Coded value recording the method by which the '
                                                       'diagnosis was made.',
                                          'dx_type': 'Code indicating the type or category of the diagnosis '
                                                     'entry.',
                                          'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                          'available.',
                                          'event_datetime': 'Sentinel-cleaned diagnosis onset timestamp.',
                                          'event_datetime_local': 'event_datetime in Europe/London '
                                                                  'wall-clock time (GMT/BST).',
                                          'event_end_datetime': 'Sentinel-cleaned resolution timestamp.',
                                          'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                      'wall-clock time (GMT/BST). Gold: NULL '
                                                                      'wherever Gold nulls '
                                                                      'event_end_datetime.',
                                          'history_ind': 'Indicator showing whether the entry represents a '
                                                         'history of the condition rather than an active '
                                                         'diagnosis.',
                                          'loaded_at': 'map_aria_diagnosis_staging.ADC_UPDT carried '
                                                       'unchanged through code filtering and quality/public '
                                                       'projections. This is the contributing bronze load '
                                                       'clock, distinct from PIPELINE_UPDT_DT_TM published '
                                                       'as source_update_timestamp; it is not disease '
                                                       'onset/resolution or Silver refresh time.',
                                          'm_stage': 'M category as recorded. NULL on ARIA rows (bronze '
                                                     'map_cancer_stage_mill.M_STAGE, INSR W1.14). INSR_W2 '
                                                     'W2.20.',
                                          'n_stage': 'N category as recorded. NULL on ARIA rows (bronze '
                                                     'map_cancer_stage_mill.N_STAGE, INSR W1.14). INSR_W2 '
                                                     'W2.20.',
                                          'onset_datetime': 'Source onset timestamp.',
                                          'patient_event_key': 'Deterministic event key derived from source '
                                                               'identifiers; retained as a stable join key.',
                                          'person_id': 'Native Millennium PERSON_ID as BIGINT when resolved.',
                                          'person_link_status': 'Bronze person-link status.',
                                          'record_status': "Superseded when coalesce(CUR_ENTRY_IND,'Y') "
                                                           'differs from the exact case-sensitive value Y, '
                                                           'without trimming, or SOURCE_PRESENT_IND is false '
                                                           '(null defaults true); otherwise active. No '
                                                           'retracted status is emitted. This differs from '
                                                           'current_entry_ind, which uppercases '
                                                           'CUR_ENTRY_IND and defaults a missing value to N.',
                                          'record_status_effective_from': 'Stage history start; the ARIA '
                                                                          'source records it on very few '
                                                                          'rows (EVLV_TSTAMP on 0.03% of raw '
                                                                          'rows), so it is mostly NULL.',
                                          'record_status_effective_to': 'map_aria_diagnosis_staging.ADC_UPDT '
                                                                        'only when the row is superseded; '
                                                                        'otherwise null. This is an '
                                                                        'ingestion-time proxy, not '
                                                                        'RESOLUTION_DATE_CLEAN or an '
                                                                        'independently observed '
                                                                        'status-transition timestamp; a '
                                                                        'missing ADC_UPDT remains null.',
                                          'resolution_datetime': 'Source resolution timestamp.',
                                          'source_code': 'ICD diagnosis code with diagnosis-name fallback.',
                                          'source_coding_system': 'ICD-10 coding system.',
                                          'source_display': 'Diagnosis name or description.',
                                          'source_update_timestamp': 'Bronze pipeline processing timestamp '
                                                                     'carried from `PIPELINE_UPDT_DT_TM` in '
                                                                     '`4_prod.bronze.map_aria_diagnosis_staging`. '
                                                                     'It records when bronze processed the '
                                                                     'source row, not a native '
                                                                     'clinical-system update timestamp, '
                                                                     'clinical event time, or the current '
                                                                     'Silver refresh time.',
                                          'stage_code': 'Source ICD and mapped OMOP diagnosis codings.',
                                          'stage_criteria': 'Staging criteria description.',
                                          'stage_of_disease': 'Source disease stage.',
                                          'stage_prefix': 'p, c, yp or yc when stated; NULL otherwise. NULL '
                                                          'on ARIA rows (bronze '
                                                          'map_cancer_stage_mill.STAGE_PREFIX, INSR W1.14). '
                                                          'INSR_W2 W2.20.',
                                          'stage_rule_id': 'URO_TNM_EVENTCD_V1 or PTNM_REGEX_V1. NULL on '
                                                           'ARIA rows (bronze map_cancer_stage_mill.RULE_ID, '
                                                           'INSR W1.14). INSR_W2 W2.20.',
                                          'stage_source_id': 'Clinical EVENT_ID, or pathology '
                                                             'report_version_id. NULL on ARIA rows (bronze '
                                                             'map_cancer_stage_mill.SOURCE_ID, INSR W1.14). '
                                                             'INSR_W2 W2.20.',
                                          'stage_source_kind': 'URO_TNM_EVENT or PATH_REPORT_PTNM. NULL on '
                                                               'ARIA rows (bronze '
                                                               'map_cancer_stage_mill.SOURCE_KIND, INSR '
                                                               'W1.14). INSR_W2 W2.20.',
                                          'stage_text': 'The matched statement or the event result text. '
                                                        'NULL on ARIA rows (bronze '
                                                        'map_cancer_stage_mill.STAGE_TEXT, INSR W1.14). '
                                                        'INSR_W2 W2.20.',
                                          'subject_id_system': 'Identifier system used for subject_key.',
                                          'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                         'identifier. Not salted, not secret; a stable join '
                                                         'key across feeds where person_id is unresolved.',
                                          't_stage': 'T category as recorded (e.g. T2a, Tis, Tx). NULL on '
                                                     'ARIA rows (bronze map_cancer_stage_mill.T_STAGE, INSR '
                                                     'W1.14). INSR_W2 W2.20.',
                                          'vip_ind': 'VIP indicator when supplied.',
                                          'withheld_identity_ind': 'Identity-withholding status is not '
                                                                   'populated by the current source '
                                                                   'projections; null does not mean identity '
                                                                   'was not withheld.'},
                              'quantity_override': False,
                              'table': 'One frozen ARIA diagnosis-and-staging assertion with ICD source '
                                       'coding and mapped OMOP diagnosis evidence. Gold QC twin of the '
                                       'silver product: 2 columns are repaired or nulled, 1 rule(s) drop '
                                       'rows, 3 check(s) are advisory. Each rule states its reason in the '
                                       'pipeline notebook, and Lakeflow expectation metrics report what '
                                       'every rule matched on each update. Coverage: Frozen feeds with no '
                                       'rows in the last 12 months: condition stage to 2023-10, costed '
                                       'activity and cost line items to 2022-03, device to 2020-01; '
                                       'complete-through month per release: '
                                       '6_mgmt.data_quality.release_finding (finding_code '
                                       'F2_complete_through).'}}
