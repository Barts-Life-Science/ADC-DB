"""Gold pathways.waiting_list_entry: 2 dataset(s).

- datasets: _qc_clinical_waiting_list_entry, clinical_waiting_list_entry
- moved verbatim from: 4_gold_care_pathways:3318, 4_gold_care_pathways:3400, 4_gold_care_pathways:3406, 4_gold_care_pathways:3414, 4_gold_care_pathways:3533, 4_gold_care_pathways:3538, 4_gold_care_pathways:3549, 4_gold_care_pathways:3553

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.waiting_list_entry ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_WAITING_LIST_ENTRY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    # INSR_W3 W3.06: Gold: NULL where a 1914-08 WL placeholder day (P3f).
    "CASE WHEN CAST(`event_datetime` AS DATE) BETWEEN DATE'1914-08-01' AND DATE'1914-08-31' THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_datetime` AS DATE)) < 9999 THEN NULL ELSE `event_datetime` END END END AS `event_datetime`",
    "CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` OR CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_end_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_end_datetime` AS DATE)) < 9999 OR CAST(`event_end_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`event_end_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `event_end_datetime` END END AS `event_end_datetime`",
    "CASE WHEN `event_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_datetime` AS DATE)) < 9999 THEN NULL ELSE `event_datetime` END END) IS NULL THEN NULL ELSE `event_datetime_local` END AS `event_datetime_local`",  # GOLD_COLUMN_PARITY_V1
    "CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` OR CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_end_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`event_end_datetime` AS DATE)) < 9999 OR CAST(`event_end_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`event_end_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `event_end_datetime` END END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`",  # GOLD_COLUMN_PARITY_V1
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
    # INSR_W3 W3.06: Gold: NULL where a 1914-08 WL placeholder day (P3f).
    "CASE WHEN CAST(`recommend_datetime` AS DATE) BETWEEN DATE'1914-08-01' AND DATE'1914-08-31' THEN NULL ELSE CASE WHEN CAST(`recommend_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `recommend_datetime` END END AS `recommend_datetime`",
    # INSR_W3 W3.06: Gold: NULL where a 1914-08 WL placeholder day (P3f).
    "CASE WHEN CAST(`referral_datetime` AS DATE) BETWEEN DATE'1914-08-01' AND DATE'1914-08-31' THEN NULL ELSE CASE WHEN CAST(`referral_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`referral_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`referral_datetime` AS DATE)) < 9999 THEN NULL ELSE `referral_datetime` END END END AS `referral_datetime`",
    "CASE WHEN CAST(`original_request_received_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`original_request_received_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`original_request_received_datetime` AS DATE)) < 9999 THEN NULL ELSE `original_request_received_datetime` END END AS `original_request_received_datetime`",
    # INSR_W3 W3.06: Gold: NULL where a 1914-08 WL placeholder day (P3f).
    "CASE WHEN CAST(`admit_decision_datetime` AS DATE) BETWEEN DATE'1914-08-01' AND DATE'1914-08-31' THEN NULL ELSE CASE WHEN CAST(`admit_decision_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `admit_decision_datetime` END END AS `admit_decision_datetime`",
    "CASE WHEN CAST(`admit_guaranteed_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`admit_guaranteed_datetime` AS DATE)) < 9999 OR CAST(`admit_guaranteed_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`admit_guaranteed_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `admit_guaranteed_datetime` END AS `admit_guaranteed_datetime`",
    # INSR_W3 W3.06: Gold: NULL where the 2050-01-01 WL placeholder (P3f).
    "CASE WHEN CAST(`provisional_admit_datetime` AS DATE) IN (DATE'2050-01-01') THEN NULL ELSE CASE WHEN CAST(`provisional_admit_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`provisional_admit_datetime` AS DATE)) < 9999 THEN NULL ELSE `provisional_admit_datetime` END END AS `provisional_admit_datetime`",
    # INSR_W3 W3.06: Gold: NULL where the 2050-01-01 WL placeholder (P3f).
    "CASE WHEN CAST(`previous_provisional_admit_datetime` AS DATE) IN (DATE'2050-01-01') THEN NULL ELSE CASE WHEN CAST(`previous_provisional_admit_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`previous_provisional_admit_datetime` AS DATE)) < 9999 THEN NULL ELSE `previous_provisional_admit_datetime` END END AS `previous_provisional_admit_datetime`",
    # INSR_W3 W3.06: Gold: NULL where a 1914-08 WL placeholder day (P3f).
    "CASE WHEN CAST(`adjusted_waiting_start_datetime` AS DATE) BETWEEN DATE'1914-08-01' AND DATE'1914-08-31' THEN NULL ELSE CASE WHEN CAST(`adjusted_waiting_start_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`adjusted_waiting_start_datetime` AS DATE)) < 9999 THEN NULL ELSE `adjusted_waiting_start_datetime` END END AS `adjusted_waiting_start_datetime`",
    '`scheduled_datetime` AS `scheduled_datetime`',
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN YEAR(CAST(`requested_datetime` AS DATE)) >= 2100 THEN NULL ELSE `requested_datetime` END AS `requested_datetime`",
    "CASE WHEN CAST(`removal_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`removal_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`removal_datetime` AS DATE)) < 9999 OR CAST(`removal_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`removal_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `removal_datetime` END END AS `removal_datetime`",
    # INSR_W3 W3.06: Gold: NULL where the last did-not-attend is later than the load (J6: a DNA is past by definition).
    "CASE WHEN CAST(`last_dna_datetime` AS TIMESTAMP) > `loaded_at` THEN NULL ELSE CASE WHEN CAST(`last_dna_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`last_dna_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`last_dna_datetime` AS DATE)) < 9999 THEN NULL ELSE `last_dna_datetime` END END END AS `last_dna_datetime`",
    '`status_datetime` AS `status_datetime`',
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN (CAST(`status_end_datetime` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`status_end_datetime` AS DATE)) < 9999) THEN NULL ELSE `status_end_datetime` END AS `status_end_datetime`",
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
    # INSR_W3 W3.05: Gold: NULL where an open-end marker in [2099-12-31, 9999).
    "CASE WHEN (CAST(`record_status_effective_to` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`record_status_effective_to` AS DATE)) < 9999) THEN NULL ELSE `record_status_effective_to` END AS `record_status_effective_to`",
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_WAITING_LIST_ENTRY_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`waiting_start_derived` AS `waiting_start_derived`',
    '`waiting_start_basis` AS `waiting_start_basis`',
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
    # INSR_W4 W4.05 date_typo (estate 2026-10-29): Waiting starts before 1990 predate the PAS (L3499 floor).
    'gold.clinical.waiting_list_entry.adjusted_waiting_start_datetime.insr_f6_wl_start_floor':
        "NOT COALESCE((`adjusted_waiting_start_datetime` < TIMESTAMP'1990-01-01 00:00:00'), FALSE)",
    # INSR_W4 W4.05 birth_death (measured in W4.05 Step 8): A waiting-list date before the person's birth (L3499, F5(g)).
    'gold.clinical.waiting_list_entry.admit_decision_datetime.insr_f5g_before_birth':
        'NOT COALESCE((`insr_w4_before_birth_admit_decision_datetime`), FALSE)',
    # INSR_W4 W4.05 birth_death (measured in W4.05 Step 8): A waiting-list date before the person's birth (L3499, F5(g)).
    'gold.clinical.waiting_list_entry.recommend_datetime.insr_f5g_before_birth':
        'NOT COALESCE((`insr_w4_before_birth_recommend_datetime`), FALSE)',
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.waiting_list_entry.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # INSR_W4 W4.05 birth_death (measured in W4.05 Step 8): A waiting-list date before the person's birth (L3499, F5(g)).
    'gold.clinical.waiting_list_entry.referral_datetime.insr_f5g_before_birth':
        'NOT COALESCE((`insr_w4_before_birth_referral_datetime`), FALSE)',
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


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_WAITING_LIST_ENTRY_PERSON_SCOPE_RULES = {
    "gold.clinical.waiting_list_entry.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.06: warn_row rules (J4); rows are kept.
CLINICAL_WAITING_LIST_ENTRY_W3_06_ADVISORY_RULES = {
    "gold.clinical.waiting_list_entry.referral_datetime.pre_2007": "NOT COALESCE(CAST(`referral_datetime` AS DATE) < DATE'2007-01-01', FALSE)",
    "gold.clinical.waiting_list_entry.admit_decision_datetime.pre_2007": "NOT COALESCE(CAST(`admit_decision_datetime` AS DATE) < DATE'2007-01-01', FALSE)",
    "gold.clinical.waiting_list_entry.recommend_datetime.pre_2007": "NOT COALESCE(CAST(`recommend_datetime` AS DATE) < DATE'2007-01-01', FALSE)",
    "gold.clinical.waiting_list_entry.event_datetime.pre_2007": "NOT COALESCE(CAST(`event_datetime` AS DATE) < DATE'2007-01-01', FALSE)",
    "gold.clinical.waiting_list_entry.adjusted_waiting_start_datetime.pre_2007": "NOT COALESCE(CAST(`adjusted_waiting_start_datetime` AS DATE) < DATE'2007-01-01', FALSE)",
    "gold.clinical.waiting_list_entry.provisional_admit_datetime.pre_2007": "NOT COALESCE(CAST(`provisional_admit_datetime` AS DATE) < DATE'2007-01-01', FALSE)",
    "gold.clinical.waiting_list_entry.admit_guaranteed_datetime.pre_2007": "NOT COALESCE(CAST(`admit_guaranteed_datetime` AS DATE) < DATE'2007-01-01', FALSE)",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
CLINICAL_WAITING_LIST_ENTRY_ORPHAN_RULES = {
    "gold.clinical.waiting_list_entry.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_waiting_list_entry"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_WAITING_LIST_ENTRY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_WAITING_LIST_ENTRY_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_WAITING_LIST_ENTRY_PERSON_SCOPE_RULES)
@dp.expect_all(CLINICAL_WAITING_LIST_ENTRY_W3_06_ADVISORY_RULES)
@dp.expect_all(CLINICAL_WAITING_LIST_ENTRY_ORPHAN_RULES)
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
    return df


@dp.materialized_view(
    name=_n("gold_clinical.waiting_list_entry"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
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
        '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
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
        '`waiting_start_derived` AS `waiting_start_derived`',  # INSR_W3 W3.12
        '`waiting_start_basis` AS `waiting_start_basis`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'clinical_waiting_list_entry': GoldRules(
        table='clinical_waiting_list_entry',
        select=CLINICAL_WAITING_LIST_ENTRY_SELECT,
        mandatory=CLINICAL_WAITING_LIST_ENTRY_MANDATORY_RULES,
        advisory=CLINICAL_WAITING_LIST_ENTRY_ADVISORY_RULES,
        person_scope=CLINICAL_WAITING_LIST_ENTRY_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key', 'row_source', 'source_version_id'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth'),
        value_rules={'gold.clinical.waiting_list_entry.location_code.insr_f7_zero_code': {'action': 'normalize',
                                                                              'column': 'location_code',
                                                                              'companions': [],
                                                                              'match': 'TRIM(`location_code`) '
                                                                                       "= '0' AND "
                                                                                       '`location_display` '
                                                                                       'IS NULL',
                                                                              'table': 'clinical_waiting_list_entry'},
         'gold.clinical.waiting_list_entry.sch_event_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                           'column': 'sch_event_id',
                                                                           'companions': [],
                                                                           'match': '`sch_event_id` = 0',
                                                                           'table': 'clinical_waiting_list_entry'}},
        birth_flag_columns=('admit_decision_datetime', 'recommend_datetime', 'referral_datetime')),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_waiting_list_entry': {'columns': {'active_status_display': 'Source active-status display.',
                                                 'adjusted_waiting_start_datetime': 'Adjusted waiting-start '
                                                                                    'timestamp.',
                                                 'admit_booking_display': 'Source admission-booking display.',
                                                 'admit_category_display': 'Source admission-category '
                                                                           'display.',
                                                 'admit_decision_datetime': 'Source decision-to-admit '
                                                                            'timestamp.',
                                                 'admit_guaranteed_datetime': 'Source guaranteed-admission '
                                                                              'timestamp.',
                                                 'admit_offer_outcome_display': 'Source admission-offer '
                                                                                'outcome display.',
                                                 'admit_type_display': 'Source admission-type display.',
                                                 'anesthetic_display': 'Source anesthetic display.',
                                                 'attendance_display': 'Source attendance display.',
                                                 'confidentiality_code': 'Source confidentiality code when '
                                                                         'supplied.',
                                                 'encounter_id': 'Millennium ENCNTR_ID when supplied; native '
                                                                 'encounter foreign key.',
                                                 'event_datetime': 'Recorded waiting-period start timestamp '
                                                                   'carried from `WAITING_START_DT_TM`.',
                                                 'event_datetime_local': 'Waiting-start timestamp expressed '
                                                                         'in local time, when available. '
                                                                         'Gold: NULL wherever Gold nulls '
                                                                         'event_datetime.',
                                                 'event_end_datetime': 'Recorded waiting-period end '
                                                                       'timestamp carried from '
                                                                       '`WAITING_END_DT_TM`.',
                                                 'event_end_datetime_local': 'Waiting-end timestamp '
                                                                             'expressed in local time, when '
                                                                             'available. Gold: NULL wherever '
                                                                             'Gold nulls event_end_datetime.',
                                                 'facility_display': 'Source facility display.',
                                                 'fact_category': 'Whether this fact is clinical or '
                                                                  'administrative in the v2 plane merge.',
                                                 'from_ed_ind': 'Raw source indicator for origin in the '
                                                                'emergency department.',
                                                 'hist_action': 'Source history action when the row is '
                                                                'historical.',
                                                 'is_current': 'Whether this is the current physical '
                                                               'version.',
                                                 'last_dna_datetime': 'Source last did-not-attend timestamp.',
                                                 'loaded_at': 'map_waiting_list.ADC_UPDT carried unchanged '
                                                              'for the physical CURRENT or HIST version. '
                                                              'This is ingestion provenance, not '
                                                              'waiting-start/end, descriptive VERSION_DT_TM '
                                                              'or Silver refresh time; a missing source '
                                                              'timestamp remains null.',
                                                 'location_code': 'Millennium nurse-unit code.',
                                                 'location_display': 'Source nurse-unit display.',
                                                 'management_display': 'Source management display.',
                                                 'original_request_received_datetime': 'Original '
                                                                                       'request-received '
                                                                                       'timestamp.',
                                                 'patient_event_key': 'Deterministic SHA-256 waiting-list '
                                                                      'entry identity shared by physical '
                                                                      'versions.',
                                                 'person_id': 'Millennium PERSON_ID; native person foreign '
                                                              'key.',
                                                 'planned_procedure_code': 'Source planned-procedure code.',
                                                 'planned_procedure_display': 'Source planned-procedure '
                                                                              'display.',
                                                 'pm_wait_list_id': 'Native waiting-list entry identifier.',
                                                 'previous_provisional_admit_datetime': 'Previous '
                                                                                        'provisional-admission '
                                                                                        'timestamp.',
                                                 'provisional_admit_datetime': 'Source provisional-admission '
                                                                               'timestamp.',
                                                 'reason_for_change_display': 'Source reason-for-change '
                                                                              'display.',
                                                 'reason_for_removal_display': 'Source reason-for-removal '
                                                                               'display.',
                                                 'recommend_datetime': 'Source recommendation timestamp.',
                                                 'record_status': 'Derived map_waiting_list version status: '
                                                                  'superseded when SOURCE_PRESENT_IND cast '
                                                                  'to Boolean is false or IS_CURRENT is '
                                                                  'false; otherwise active. Null presence '
                                                                  'defaults to true and null IS_CURRENT '
                                                                  'defaults to false. Source absence '
                                                                  'therefore maps to superseded, not '
                                                                  'retracted; this is distinct from the '
                                                                  'clinical waiting-list status code.',
                                                 'record_status_effective_from': 'map_waiting_list.BEG_EFFECTIVE_DT_TM '
                                                                                 'carried unchanged for the '
                                                                                 'physical CURRENT or HIST '
                                                                                 'version. VERSION_DT_TM and '
                                                                                 'waiting/request dates are '
                                                                                 'not used as fallbacks; a '
                                                                                 'missing value remains '
                                                                                 'null.',
                                                 'record_status_effective_to': 'For a superseded '
                                                                               'waiting-list version, first '
                                                                               'non-null '
                                                                               'map_waiting_list.SOURCE_ABSENT_DETECTED_TS, '
                                                                               'END_EFFECTIVE_DT_TM, then '
                                                                               'ADC_UPDT. Null for active '
                                                                               'versions or when all three '
                                                                               'are missing. This expression '
                                                                               'does not filter far-future '
                                                                               'END_EFFECTIVE_DT_TM '
                                                                               'sentinels and can use '
                                                                               'ingestion time as its last '
                                                                               'fallback.',
                                                 'referral_datetime': 'Source referral timestamp.',
                                                 'referral_source_display': 'Source referral-source display.',
                                                 'referral_type_display': 'Source referral-type display.',
                                                 'removal_datetime': 'Source removal timestamp.',
                                                 'requested_datetime': 'Source requested timestamp.',
                                                 'row_source': 'Physical bronze row source.',
                                                 'sch_event_id': 'Raw scheduling event linkage evidence.',
                                                 'scheduled_datetime': 'Source scheduled timestamp.',
                                                 'service_type_requested_display': 'Source '
                                                                                   'requested-service-type '
                                                                                   'display.',
                                                 'source_code': 'Verbatim planned-procedure code.',
                                                 'source_coding_system': 'Verbatim Millennium '
                                                                         'planned-procedure coding system.',
                                                 'source_display': 'Verbatim planned-procedure display.',
                                                 'source_feed': 'Registered feed owning the fact.',
                                                 'source_update_timestamp': 'map_waiting_list.SOURCE_ADC_UPDT '
                                                                            'carried unchanged for the '
                                                                            'physical CURRENT or HIST '
                                                                            'version. It is separate from '
                                                                            'ADC_UPDT ingestion provenance '
                                                                            'and is not VERSION_DT_TM, the '
                                                                            'clinical status date or Silver '
                                                                            'refresh time; a missing '
                                                                            'timestamp remains null.',
                                                 'source_version_id': 'Native source version identifier; '
                                                                      'CURRENT rows carry -1.',
                                                 'stand_by_display': 'Source stand-by display.',
                                                 'status_code': 'Source waiting-list status code.',
                                                 'status_datetime': 'Source status timestamp.',
                                                 'status_display': 'Source waiting-list status display.',
                                                 'status_end_datetime': 'Source status-end timestamp.',
                                                 'sub_status_display': 'Source waiting-list sub-status '
                                                                       'display.',
                                                 'subject_id_system': 'Identifier system used for '
                                                                      'subject_key.',
                                                 'subject_key': 'Deterministic SHA-256 over the strongest '
                                                                'source identifier. Not salted, not secret; '
                                                                'a stable join key across feeds where '
                                                                'person_id is unresolved.',
                                                 'suspended_days': 'Source count of suspended days.',
                                                 'updt_cnt': 'Native source update counter.',
                                                 'urgency_display': 'Source urgency display.',
                                                 'version_datetime': 'Descriptive fallback version timestamp '
                                                                     'only; it defines no effective '
                                                                     'interval.',
                                                 'vip_ind': 'Source VIP indicator when supplied.',
                                                 'waiting_start_basis': 'Which column supplied '
                                                                        'waiting_start_derived; NULL iff it '
                                                                        'is NULL. INSR_W2 W2.10.',
                                                 'waiting_start_derived': 'Waiting start: the source '
                                                                          'WAITING_START_DT_TM, else '
                                                                          'referral, else decision to admit, '
                                                                          'else requested (precedence by '
                                                                          'measured fill, P7_L3465b). The '
                                                                          'source column is unchanged. '
                                                                          'INSR_W2 W2.10 (J22, J38).',
                                                 'withheld_identity_ind': 'Withheld-identity indicator when '
                                                                          'supplied.'},
                                     'quantity_override': True,
                                     'table': 'Internal quality-controlled twin of '
                                              'clinical_waiting_list_entry: the Gold repairs, nulls and '
                                              'expectations are applied here and the published Gold MV reads '
                                              'this object. Materialized rather than temporary so the '
                                              'published MV can refresh incrementally.'},
 'clinical_waiting_list_entry': {'columns': {'active_status_display': 'Source active-status display.',
                                             'adjusted_waiting_start_datetime': 'Adjusted waiting-start '
                                                                                'timestamp.',
                                             'admit_booking_display': 'Source admission-booking display.',
                                             'admit_category_display': 'Source admission-category display.',
                                             'admit_decision_datetime': 'Source decision-to-admit timestamp.',
                                             'admit_guaranteed_datetime': 'Source guaranteed-admission '
                                                                          'timestamp.',
                                             'admit_offer_outcome_display': 'Source admission-offer outcome '
                                                                            'display.',
                                             'admit_type_display': 'Source admission-type display.',
                                             'anesthetic_display': 'Source anesthetic display.',
                                             'attendance_display': 'Source attendance display.',
                                             'confidentiality_code': 'Source confidentiality code when '
                                                                     'supplied.',
                                             'encounter_id': 'Millennium ENCNTR_ID when supplied; native '
                                                             'encounter foreign key.',
                                             'event_datetime': 'Recorded waiting-period start timestamp '
                                                               'carried from `WAITING_START_DT_TM`.',
                                             'event_datetime_local': 'Waiting-start timestamp expressed in '
                                                                     'local time, when available. Gold: NULL '
                                                                     'wherever Gold nulls event_datetime.',
                                             'event_end_datetime': 'Recorded waiting-period end timestamp '
                                                                   'carried from `WAITING_END_DT_TM`.',
                                             'event_end_datetime_local': 'Waiting-end timestamp expressed in '
                                                                         'local time, when available. Gold: '
                                                                         'NULL wherever Gold nulls '
                                                                         'event_end_datetime.',
                                             'facility_display': 'Source facility display.',
                                             'fact_category': 'Whether this fact is clinical or '
                                                              'administrative in the v2 plane merge.',
                                             'from_ed_ind': 'Raw source indicator for origin in the '
                                                            'emergency department.',
                                             'hist_action': 'Source history action when the row is '
                                                            'historical.',
                                             'is_current': 'Whether this is the current physical version.',
                                             'last_dna_datetime': 'Source last did-not-attend timestamp.',
                                             'loaded_at': 'map_waiting_list.ADC_UPDT carried unchanged for '
                                                          'the physical CURRENT or HIST version. This is '
                                                          'ingestion provenance, not waiting-start/end, '
                                                          'descriptive VERSION_DT_TM or Silver refresh time; '
                                                          'a missing source timestamp remains null.',
                                             'location_code': 'Millennium nurse-unit code.',
                                             'location_display': 'Source nurse-unit display.',
                                             'management_display': 'Source management display.',
                                             'original_request_received_datetime': 'Original '
                                                                                   'request-received '
                                                                                   'timestamp.',
                                             'patient_event_key': 'Deterministic SHA-256 waiting-list entry '
                                                                  'identity shared by physical versions.',
                                             'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                             'planned_procedure_code': 'Source planned-procedure code.',
                                             'planned_procedure_display': 'Source planned-procedure display.',
                                             'pm_wait_list_id': 'Native waiting-list entry identifier.',
                                             'previous_provisional_admit_datetime': 'Previous '
                                                                                    'provisional-admission '
                                                                                    'timestamp.',
                                             'provisional_admit_datetime': 'Source provisional-admission '
                                                                           'timestamp.',
                                             'reason_for_change_display': 'Source reason-for-change display.',
                                             'reason_for_removal_display': 'Source reason-for-removal '
                                                                           'display.',
                                             'recommend_datetime': 'Source recommendation timestamp.',
                                             'record_status': 'Derived map_waiting_list version status: '
                                                              'superseded when SOURCE_PRESENT_IND cast to '
                                                              'Boolean is false or IS_CURRENT is false; '
                                                              'otherwise active. Null presence defaults to '
                                                              'true and null IS_CURRENT defaults to false. '
                                                              'Source absence therefore maps to superseded, '
                                                              'not retracted; this is distinct from the '
                                                              'clinical waiting-list status code.',
                                             'record_status_effective_from': 'map_waiting_list.BEG_EFFECTIVE_DT_TM '
                                                                             'carried unchanged for the '
                                                                             'physical CURRENT or HIST '
                                                                             'version. VERSION_DT_TM and '
                                                                             'waiting/request dates are not '
                                                                             'used as fallbacks; a missing '
                                                                             'value remains null.',
                                             'record_status_effective_to': 'For a superseded waiting-list '
                                                                           'version, first non-null '
                                                                           'map_waiting_list.SOURCE_ABSENT_DETECTED_TS, '
                                                                           'END_EFFECTIVE_DT_TM, then '
                                                                           'ADC_UPDT. Null for active '
                                                                           'versions or when all three are '
                                                                           'missing. This expression does '
                                                                           'not filter far-future '
                                                                           'END_EFFECTIVE_DT_TM sentinels '
                                                                           'and can use ingestion time as '
                                                                           'its last fallback.',
                                             'referral_datetime': 'Source referral timestamp.',
                                             'referral_source_display': 'Source referral-source display.',
                                             'referral_type_display': 'Source referral-type display.',
                                             'removal_datetime': 'Source removal timestamp.',
                                             'requested_datetime': 'Source requested timestamp.',
                                             'row_source': 'Physical bronze row source.',
                                             'sch_event_id': 'Raw scheduling event linkage evidence.',
                                             'scheduled_datetime': 'Source scheduled timestamp.',
                                             'service_type_requested_display': 'Source '
                                                                               'requested-service-type '
                                                                               'display.',
                                             'source_code': 'Verbatim planned-procedure code.',
                                             'source_coding_system': 'Verbatim Millennium planned-procedure '
                                                                     'coding system.',
                                             'source_display': 'Verbatim planned-procedure display.',
                                             'source_feed': 'Registered feed owning the fact.',
                                             'source_update_timestamp': 'map_waiting_list.SOURCE_ADC_UPDT '
                                                                        'carried unchanged for the physical '
                                                                        'CURRENT or HIST version. It is '
                                                                        'separate from ADC_UPDT ingestion '
                                                                        'provenance and is not '
                                                                        'VERSION_DT_TM, the clinical status '
                                                                        'date or Silver refresh time; a '
                                                                        'missing timestamp remains null.',
                                             'source_version_id': 'Native source version identifier; CURRENT '
                                                                  'rows carry -1.',
                                             'stand_by_display': 'Source stand-by display.',
                                             'status_code': 'Source waiting-list status code.',
                                             'status_datetime': 'Source status timestamp.',
                                             'status_display': 'Source waiting-list status display.',
                                             'status_end_datetime': 'Source status-end timestamp.',
                                             'sub_status_display': 'Source waiting-list sub-status display.',
                                             'subject_id_system': 'Identifier system used for subject_key.',
                                             'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                            'identifier. Not salted, not secret; a stable '
                                                            'join key across feeds where person_id is '
                                                            'unresolved.',
                                             'suspended_days': 'Source count of suspended days.',
                                             'updt_cnt': 'Native source update counter.',
                                             'urgency_display': 'Source urgency display.',
                                             'version_datetime': 'Descriptive fallback version timestamp '
                                                                 'only; it defines no effective interval.',
                                             'vip_ind': 'Source VIP indicator when supplied.',
                                             'waiting_start_basis': 'Which column supplied '
                                                                    'waiting_start_derived; NULL iff it is '
                                                                    'NULL. INSR_W2 W2.10.',
                                             'waiting_start_derived': 'Waiting start: the source '
                                                                      'WAITING_START_DT_TM, else referral, '
                                                                      'else decision to admit, else '
                                                                      'requested (precedence by measured '
                                                                      'fill, P7_L3465b). The source column '
                                                                      'is unchanged. INSR_W2 W2.10 (J22, '
                                                                      'J38).',
                                             'withheld_identity_ind': 'Withheld-identity indicator when '
                                                                      'supplied.'},
                                 'quantity_override': False,
                                 'table': 'One row per version of a Millennium waiting-list entry '
                                          '(PM_WAIT_LIST); record_status_effective_from/to bound each '
                                          'version and the current version has a NULL effective_to (the '
                                          'source 2100-12-31 open marker is normalised to NULL, J17). '
                                          'Coverage: Waiting-list referral dates are recorded on 56% as many '
                                          'entries in 2019 as in 2018 at source; complete-through month per '
                                          'release: 6_mgmt.data_quality.release_finding (finding_code '
                                          'F2_complete_through).'}}
