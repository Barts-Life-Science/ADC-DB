"""Gold pathways.waiting_list_snapshot: 2 dataset(s).

- datasets: _qc_clinical_waiting_list_snapshot, clinical_waiting_list_snapshot
- moved verbatim from: 4_gold_care_pathways:3671, 4_gold_care_pathways:3729, 4_gold_care_pathways:3734, 4_gold_care_pathways:3742, 4_gold_care_pathways:3824, 4_gold_care_pathways:3829, 4_gold_care_pathways:3838, 4_gold_care_pathways:3842

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


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
    # INSR_W3 W3.06: Gold: NULL where a 1914-08 WL placeholder day (P3f).
    "CASE WHEN CAST(`referral_datetime` AS DATE) BETWEEN DATE'1914-08-01' AND DATE'1914-08-31' THEN NULL ELSE CASE WHEN CAST(`referral_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `referral_datetime` END END AS `referral_datetime`",
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
    # INSR_W3 W3.06: Gold: NULL where the last did-not-attend is later than the load (J6: a DNA is past by definition).
    "CASE WHEN CAST(`last_dna_datetime` AS TIMESTAMP) > `loaded_at` THEN NULL ELSE CASE WHEN CAST(`last_dna_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `last_dna_datetime` END END AS `last_dna_datetime`",
    '`status_datetime` AS `status_datetime`',
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN (CAST(`status_end_datetime` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`status_end_datetime` AS DATE)) < 9999) THEN NULL ELSE `status_end_datetime` END AS `status_end_datetime`",
    '`location_code` AS `location_code`',
    '`location_display` AS `location_display`',
    '`facility_display` AS `facility_display`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_WAITING_LIST_SNAPSHOT_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`plausibly_open_ind` AS `plausibly_open_ind`',
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
    # INSR_W4 W4.05 birth_death (measured in W4.05 Step 8): A waiting-list date before the person's birth (L3499, F5(g)).
    'gold.clinical.waiting_list_snapshot.admit_decision_datetime.insr_f5g_before_birth':
        'NOT COALESCE((`insr_w4_before_birth_admit_decision_datetime`), FALSE)',
    # INSR_W4 W4.05 birth_death (measured in W4.05 Step 8): A waiting-list date before the person's birth (L3499, F5(g)).
    'gold.clinical.waiting_list_snapshot.recommend_datetime.insr_f5g_before_birth':
        'NOT COALESCE((`insr_w4_before_birth_recommend_datetime`), FALSE)',
    # INSR_W4 W4.05 birth_death (measured in W4.05 Step 8): A waiting-list date before the person's birth (L3499, F5(g)).
    'gold.clinical.waiting_list_snapshot.referral_datetime.insr_f5g_before_birth':
        'NOT COALESCE((`insr_w4_before_birth_referral_datetime`), FALSE)',
    # INSR_W4 W4.05 date_typo (measured in W4.05 Step 8): Waiting starts before 1990 predate the PAS (L3499 floor).
    'gold.clinical.waiting_list_snapshot.waiting_start_datetime.insr_f6_wl_start_floor':
        "NOT COALESCE((`waiting_start_datetime` < TIMESTAMP'1990-01-01 00:00:00'), FALSE)",
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 0 of 4,771,446 at the profile.
    "gold.clinical.waiting_list_snapshot.identity_status.default_view_resolved":
        "identity_status = 'resolved'",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_WAITING_LIST_SNAPSHOT_PERSON_SCOPE_RULES = {
    "gold.clinical.waiting_list_snapshot.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.06: warn_row rules (J4); rows are kept.
CLINICAL_WAITING_LIST_SNAPSHOT_W3_06_ADVISORY_RULES = {
    "gold.clinical.waiting_list_snapshot.referral_datetime.pre_2007": "NOT COALESCE(CAST(`referral_datetime` AS DATE) < DATE'2007-01-01', FALSE)",
    "gold.clinical.waiting_list_snapshot.waiting_start_datetime.pre_2007": "NOT COALESCE(CAST(`waiting_start_datetime` AS DATE) < DATE'2007-01-01', FALSE)",
    "gold.clinical.waiting_list_snapshot.adjusted_waiting_start_datetime.pre_2007": "NOT COALESCE(CAST(`adjusted_waiting_start_datetime` AS DATE) < DATE'2007-01-01', FALSE)",
    "gold.clinical.waiting_list_snapshot.provisional_admit_datetime.pre_2007": "NOT COALESCE(CAST(`provisional_admit_datetime` AS DATE) < DATE'2007-01-01', FALSE)",
    "gold.clinical.waiting_list_snapshot.admit_guaranteed_datetime.pre_2007": "NOT COALESCE(CAST(`admit_guaranteed_datetime` AS DATE) < DATE'2007-01-01', FALSE)",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
CLINICAL_WAITING_LIST_SNAPSHOT_ORPHAN_RULES = {
    "gold.clinical.waiting_list_snapshot.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_waiting_list_snapshot"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_WAITING_LIST_SNAPSHOT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_WAITING_LIST_SNAPSHOT_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_WAITING_LIST_SNAPSHOT_PERSON_SCOPE_RULES)
@dp.expect_all(CLINICAL_WAITING_LIST_SNAPSHOT_W3_06_ADVISORY_RULES)
@dp.expect_all(CLINICAL_WAITING_LIST_SNAPSHOT_ORPHAN_RULES)
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
    return df


@dp.materialized_view(
    name=_n("gold_clinical.waiting_list_snapshot"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
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
        '`plausibly_open_ind` AS `plausibly_open_ind`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'clinical_waiting_list_snapshot': GoldRules(
        table='clinical_waiting_list_snapshot',
        select=CLINICAL_WAITING_LIST_SNAPSHOT_SELECT,
        mandatory=CLINICAL_WAITING_LIST_SNAPSHOT_MANDATORY_RULES,
        advisory=CLINICAL_WAITING_LIST_SNAPSHOT_ADVISORY_RULES,
        person_scope=CLINICAL_WAITING_LIST_SNAPSHOT_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['waiting_list_snapshot_key'], 'support_columns': ['identity_status', 'loaded_at']},
        value_rules={'gold.clinical.waiting_list_snapshot.location_code.insr_f7_zero_code': {'action': 'normalize',
                                                                                 'column': 'location_code',
                                                                                 'companions': [],
                                                                                 'match': 'TRIM(`location_code`) '
                                                                                          "= '0' AND "
                                                                                          '`location_display` '
                                                                                          'IS NULL',
                                                                                 'table': 'clinical_waiting_list_snapshot'},
         'gold.clinical.waiting_list_snapshot.sch_event_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                              'column': 'sch_event_id',
                                                                              'companions': [],
                                                                              'match': '`sch_event_id` = 0',
                                                                              'table': 'clinical_waiting_list_snapshot'}},
        birth_flag_columns=('admit_decision_datetime', 'recommend_datetime', 'referral_datetime')),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_waiting_list_snapshot': {'columns': {'active_status_display': 'Source active-status display.',
                                                    'adjusted_waiting_start_datetime': 'Adjusted '
                                                                                       'waiting-start '
                                                                                       'timestamp.',
                                                    'admit_booking_display': 'Source admission-booking '
                                                                             'display.',
                                                    'admit_category_display': 'Source admission-category '
                                                                              'display.',
                                                    'admit_decision_datetime': 'Source decision-to-admit '
                                                                               'timestamp.',
                                                    'admit_guaranteed_datetime': 'Source '
                                                                                 'guaranteed-admission '
                                                                                 'timestamp.',
                                                    'admit_offer_outcome_display': 'Source admission-offer '
                                                                                   'outcome display.',
                                                    'admit_type_display': 'Source admission-type display.',
                                                    'anesthetic_display': 'Source anesthetic display.',
                                                    'attendance_display': 'Source attendance display.',
                                                    'encounter_id': 'Millennium ENCNTR_ID when supplied; '
                                                                    'native encounter foreign key.',
                                                    'facility_display': 'Source facility display.',
                                                    'fact_category': 'Whether this fact is clinical or '
                                                                     'administrative in the v2 plane merge.',
                                                    'from_ed_ind': 'Raw source indicator for origin in the '
                                                                   'emergency department.',
                                                    'last_dna_datetime': 'Source last did-not-attend '
                                                                         'timestamp.',
                                                    'loaded_at': 'Single immutable UTC cutoff timestamp '
                                                                 'shared by every row for SNAPSHOT_DATE.',
                                                    'location_code': 'Millennium nurse-unit code.',
                                                    'location_display': 'Source nurse-unit display.',
                                                    'management_display': 'Source management display.',
                                                    'original_request_received_datetime': 'Original '
                                                                                          'request-received '
                                                                                          'timestamp.',
                                                    'person_id': 'Millennium PERSON_ID; native person '
                                                                 'foreign key.',
                                                    'planned_procedure_code': 'Source planned-procedure '
                                                                              'code.',
                                                    'planned_procedure_display': 'Source planned-procedure '
                                                                                 'display.',
                                                    'plausibly_open_ind': 'Census row plausibly still open '
                                                                          'at the cut-off (W1.09). (bronze '
                                                                          'PLAUSIBLY_OPEN_IND, INSR W1). '
                                                                          'INSR_W2 W2.20.',
                                                    'pm_wait_list_id': 'Native waiting-list entry '
                                                                       'identifier.',
                                                    'previous_provisional_admit_datetime': 'Previous '
                                                                                           'provisional-admission '
                                                                                           'timestamp.',
                                                    'provisional_admit_datetime': 'Source '
                                                                                  'provisional-admission '
                                                                                  'timestamp.',
                                                    'reason_for_change_display': 'Source reason-for-change '
                                                                                 'display.',
                                                    'reason_for_removal_display': 'Source reason-for-removal '
                                                                                  'display.',
                                                    'recommend_datetime': 'Source recommendation timestamp.',
                                                    'referral_datetime': 'Source referral timestamp.',
                                                    'referral_source_display': 'Source referral-source '
                                                                               'display.',
                                                    'referral_type_display': 'Source referral-type display.',
                                                    'removal_datetime': 'Source removal timestamp.',
                                                    'requested_datetime': 'Source requested timestamp.',
                                                    'sch_event_id': 'Raw scheduling event linkage evidence.',
                                                    'scheduled_datetime': 'Source scheduled timestamp.',
                                                    'service_type_requested_display': 'Source '
                                                                                      'requested-service-type '
                                                                                      'display.',
                                                    'snapshot_cutoff_ts': 'Immutable census cutoff '
                                                                          'timestamp.',
                                                    'snapshot_date': 'Census snapshot date.',
                                                    'source_feed': 'Registered feed owning the census row.',
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
                                                                   'source identifier. Not salted, not '
                                                                   'secret; a stable join key across feeds '
                                                                   'where person_id is unresolved.',
                                                    'suspended_days': 'Source count of suspended days.',
                                                    'urgency_display': 'Source urgency display.',
                                                    'waiting_end_datetime': 'Source waiting-end timestamp.',
                                                    'waiting_list_snapshot_key': 'Deterministic SHA-256 '
                                                                                 'census-row key.',
                                                    'waiting_start_datetime': 'Source waiting-start '
                                                                              'timestamp.'},
                                        'quantity_override': True,
                                        'table': 'Internal quality-controlled twin of '
                                                 'clinical_waiting_list_snapshot: the Gold repairs, nulls '
                                                 'and expectations are applied here and the published Gold '
                                                 'MV reads this object. Materialized rather than temporary '
                                                 'so the published MV can refresh incrementally.'},
 'clinical_waiting_list_snapshot': {'columns': {'active_status_display': 'Source active-status display.',
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
                                                'encounter_id': 'Millennium ENCNTR_ID when supplied; native '
                                                                'encounter foreign key.',
                                                'facility_display': 'Source facility display.',
                                                'fact_category': 'Whether this fact is clinical or '
                                                                 'administrative in the v2 plane merge.',
                                                'from_ed_ind': 'Raw source indicator for origin in the '
                                                               'emergency department.',
                                                'last_dna_datetime': 'Source last did-not-attend timestamp.',
                                                'loaded_at': 'Single immutable UTC cutoff timestamp shared '
                                                             'by every row for SNAPSHOT_DATE.',
                                                'location_code': 'Millennium nurse-unit code.',
                                                'location_display': 'Source nurse-unit display.',
                                                'management_display': 'Source management display.',
                                                'original_request_received_datetime': 'Original '
                                                                                      'request-received '
                                                                                      'timestamp.',
                                                'person_id': 'Millennium PERSON_ID; native person foreign '
                                                             'key.',
                                                'planned_procedure_code': 'Source planned-procedure code.',
                                                'planned_procedure_display': 'Source planned-procedure '
                                                                             'display.',
                                                'plausibly_open_ind': 'Census row plausibly still open at '
                                                                      'the cut-off (W1.09). (bronze '
                                                                      'PLAUSIBLY_OPEN_IND, INSR W1). INSR_W2 '
                                                                      'W2.20.',
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
                                                'referral_datetime': 'Source referral timestamp.',
                                                'referral_source_display': 'Source referral-source display.',
                                                'referral_type_display': 'Source referral-type display.',
                                                'removal_datetime': 'Source removal timestamp.',
                                                'requested_datetime': 'Source requested timestamp.',
                                                'sch_event_id': 'Raw scheduling event linkage evidence.',
                                                'scheduled_datetime': 'Source scheduled timestamp.',
                                                'service_type_requested_display': 'Source '
                                                                                  'requested-service-type '
                                                                                  'display.',
                                                'snapshot_cutoff_ts': 'Immutable census cutoff timestamp.',
                                                'snapshot_date': 'Census snapshot date.',
                                                'source_feed': 'Registered feed owning the census row.',
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
                                                               'source identifier. Not salted, not secret; a '
                                                               'stable join key across feeds where person_id '
                                                               'is unresolved.',
                                                'suspended_days': 'Source count of suspended days.',
                                                'urgency_display': 'Source urgency display.',
                                                'waiting_end_datetime': 'Source waiting-end timestamp.',
                                                'waiting_list_snapshot_key': 'Deterministic SHA-256 '
                                                                             'census-row key.',
                                                'waiting_start_datetime': 'Source waiting-start timestamp.'},
                                    'quantity_override': False,
                                    'table': 'Immutable Millennium waiting-list census activated as a '
                                             'recorded contract change from a zero-row stub with no consumer '
                                             'break. Gold QC twin of the silver product: 8 columns are '
                                             'repaired or nulled, 1 rule(s) drop rows, 1 check(s) are '
                                             'advisory. Each rule states its reason in the pipeline '
                                             'notebook, and Lakeflow expectation metrics report what every '
                                             'rule matched on each update. Coverage: Waiting-list snapshots '
                                             'are weekly census captures (unique per snapshot date, list id '
                                             'and fact category); some weeks have no census and some have '
                                             'two; Waiting-list attributes captured at the census cut-off '
                                             'are sparse at source and are not back-filled from other '
                                             'versions; complete-through month per release: '
                                             '6_mgmt.data_quality.release_finding (finding_code '
                                             'F2_complete_through).'}}
