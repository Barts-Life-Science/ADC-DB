"""Gold pathways.elective_access_entry: 2 dataset(s).

- datasets: _qc_clinical_elective_access_entry, clinical_elective_access_entry
- moved verbatim from: 4_gold_care_pathways:1515, 4_gold_care_pathways:1590, 4_gold_care_pathways:1596, 4_gold_care_pathways:1604, 4_gold_care_pathways:1724, 4_gold_care_pathways:1729, 4_gold_care_pathways:1733, 4_gold_care_pathways:1755

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


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
    '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
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


CLINICAL_ELECTIVE_ACCESS_ENTRY_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`event_datetime_derived` AS `event_datetime_derived`',
    '`event_datetime_basis` AS `event_datetime_basis`',
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
    # INSR_W4 W4.05 date_typo (estate 2026-10-30): Elective access starts before 1990 predate the PAS (L3499 floor).
    'gold.clinical.elective_access_entry.event_datetime.insr_f6_wl_start_floor':
        "NOT COALESCE((`event_datetime` < TIMESTAMP'1990-01-01 00:00:00'), FALSE)",
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.elective_access_entry.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
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


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_ELECTIVE_ACCESS_ENTRY_PERSON_SCOPE_RULES = {
    "gold.clinical.elective_access_entry.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
CLINICAL_ELECTIVE_ACCESS_ENTRY_ORPHAN_RULES = {
    "gold.clinical.elective_access_entry.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_elective_access_entry"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_ELECTIVE_ACCESS_ENTRY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_ELECTIVE_ACCESS_ENTRY_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_ELECTIVE_ACCESS_ENTRY_PERSON_SCOPE_RULES)
@dp.expect_all(CLINICAL_ELECTIVE_ACCESS_ENTRY_ORPHAN_RULES)
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
    return df


@dp.materialized_view(
    name=_n("gold_clinical.elective_access_entry"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
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
        '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
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
        '`event_datetime_derived` AS `event_datetime_derived`',  # INSR_W3 W3.12
        '`event_datetime_basis` AS `event_datetime_basis`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'clinical_elective_access_entry': GoldRules(
        table='clinical_elective_access_entry',
        select=CLINICAL_ELECTIVE_ACCESS_ENTRY_SELECT,
        mandatory=CLINICAL_ELECTIVE_ACCESS_ENTRY_MANDATORY_RULES,
        advisory=CLINICAL_ELECTIVE_ACCESS_ENTRY_ADVISORY_RULES,
        person_scope=CLINICAL_ELECTIVE_ACCESS_ENTRY_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth')),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_elective_access_entry': {'columns': {'active_ind': 'Source closure state retained verbatim; '
                                                                  'false does not mean source absence.',
                                                    'actual_guaranteed_activity_datetime': 'B3 bronze field '
                                                                                           'ACTUAL_GUARANTEED_ACTIVITY_DT_TM_CLEAN; '
                                                                                           'source value '
                                                                                           'retained unless '
                                                                                           'documented as '
                                                                                           'derived.',
                                                    'admin_category_rvid': 'B3 bronze field '
                                                                           'ADMIN_CATEGORY_RVID; source '
                                                                           'value retained unless documented '
                                                                           'as derived.',
                                                    'admit_datetime': 'B3 bronze field ADMIT_DT_TM_CLEAN; '
                                                                      'source value retained unless '
                                                                      'documented as derived.',
                                                    'admit_method_rvid': 'B3 bronze field ADMIT_METHOD_RVID; '
                                                                         'source value retained unless '
                                                                         'documented as derived.',
                                                    'admit_offer_outcome_rvid': 'B3 bronze field '
                                                                                'ADMIT_OFFER_OUTCOME_RVID; '
                                                                                'source value retained '
                                                                                'unless documented as '
                                                                                'derived.',
                                                    'business_unit': 'B3 bronze field BUSINESS_UNIT; source '
                                                                     'value retained unless documented as '
                                                                     'derived.',
                                                    'clinical_priority_code': 'B3 bronze field '
                                                                              'FIELD_VALUE_VAR; source value '
                                                                              'retained unless documented as '
                                                                              'derived.',
                                                    'clinical_priority_recorded_datetime': 'B3 bronze field '
                                                                                           'FIELD_VALUE_DATE_CLEAN; '
                                                                                           'source value '
                                                                                           'retained unless '
                                                                                           'documented as '
                                                                                           'derived.',
                                                    'comments': 'Direct identifier published and IG-governed '
                                                                'at serve time',
                                                    'confidentiality_code': 'Source confidentiality '
                                                                            'classification attached to the '
                                                                            'record for each elective access '
                                                                            'entry record. It is produced by '
                                                                            'the silver transformation and '
                                                                            'has no direct bronze-column '
                                                                            'lineage entry. Source code '
                                                                            'meanings and sentinel values '
                                                                            'are retained unless the '
                                                                            'pipeline explicitly maps them; '
                                                                            'null means no code was '
                                                                            'supplied.',
                                                    'created_by_prid': 'B3 bronze field CREATED_BY_PRID; '
                                                                       'source value retained unless '
                                                                       'documented as derived.',
                                                    'created_datetime': 'B3 bronze field CREATED_DT_TM; '
                                                                        'source value retained unless '
                                                                        'documented as derived.',
                                                    'decided_to_admit_datetime': 'B3 bronze field '
                                                                                 'DECIDED_TO_ADMIT_DT_TM_CLEAN; '
                                                                                 'source value retained '
                                                                                 'unless documented as '
                                                                                 'derived.',
                                                    'department': 'B3 bronze field DEPARTMENT; source value '
                                                                  'retained unless documented as derived.',
                                                    'division': 'B3 bronze field DIVISION; source value '
                                                                'retained unless documented as derived.',
                                                    'division_rvid': 'B3 bronze field DIVISION_RVID; source '
                                                                     'value retained unless documented as '
                                                                     'derived.',
                                                    'earliest_reasonable_offer_datetime': 'B3 bronze field '
                                                                                          'EARLIEST_REASONABLE_OFFER_DT_TM_CLEAN; '
                                                                                          'source value '
                                                                                          'retained unless '
                                                                                          'documented as '
                                                                                          'derived.',
                                                    'elective_admission_type_rvid': 'B3 bronze field '
                                                                                    'ELECTIVE_ADMISSION_TYPE_RVID; '
                                                                                    'source value retained '
                                                                                    'unless documented as '
                                                                                    'derived.',
                                                    'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT '
                                                                    'when available.',
                                                    'encounter_type_rvid': 'B3 bronze field '
                                                                           'ENCOUNTER_TYPE_RVID; source '
                                                                           'value retained unless documented '
                                                                           'as derived.',
                                                    'event_datetime': 'B3 bronze field CREATED_DT_TM; source '
                                                                      'value retained unless documented as '
                                                                      'derived.',
                                                    'event_datetime_basis': 'decided_to_admit or created; '
                                                                            'NULL iff event_datetime_derived '
                                                                            'is NULL. INSR_W2 W2.10.',
                                                    'event_datetime_derived': 'Clinical event time for the '
                                                                              'entry: '
                                                                              'decided_to_admit_datetime, '
                                                                              'else created_datetime '
                                                                              '(event_datetime is the LUNA '
                                                                              'creation time, L2637). '
                                                                              'INSR_W2 W2.10 (J22).',
                                                    'event_datetime_local': 'Event start date and time for '
                                                                            'the elective-access entry, '
                                                                            'expressed in local time.',
                                                    'event_end_datetime': 'B3 bronze field '
                                                                          'ADMIT_DT_TM_CLEAN; source value '
                                                                          'retained unless documented as '
                                                                          'derived.',
                                                    'event_end_datetime_local': 'Event end date and time for '
                                                                                'the elective-access entry, '
                                                                                'expressed in local time.',
                                                    'fact_category': 'Contract category used to group the '
                                                                     'record with comparable clinical facts '
                                                                     'for each elective access entry record. '
                                                                     'It is produced by the silver '
                                                                     'transformation and has no direct '
                                                                     'bronze-column lineage entry. '
                                                                     'Whitespace and source sentinel text '
                                                                     'are retained unless the pipeline '
                                                                     'explicitly normalizes them; null means '
                                                                     'no value was supplied.',
                                                    'guaranteed_activity_datetime': 'B3 bronze field '
                                                                                    'GUARANTEED_ACTIVITY_DT_TM_CLEAN; '
                                                                                    'source value retained '
                                                                                    'unless documented as '
                                                                                    'derived.',
                                                    'identifier_link_status': 'B3 bronze field '
                                                                              'IDENTIFIER_LINK_STATUS; '
                                                                              'source value retained unless '
                                                                              'documented as derived.',
                                                    'intended_management_rvid': 'B3 bronze field '
                                                                                'INTENDED_MANAGEMENT_RVID; '
                                                                                'source value retained '
                                                                                'unless documented as '
                                                                                'derived.',
                                                    'lead_clinician_prid': 'B3 bronze field '
                                                                           'LEAD_CLINICIAN_PRID; source '
                                                                           'value retained unless documented '
                                                                           'as derived.',
                                                    'legacy_waiting_list_id': 'B3 bronze field '
                                                                              'LEGACY_WAITING_LIST_ID; '
                                                                              'source value retained unless '
                                                                              'documented as derived.',
                                                    'linkage_fallback_conflict_ind': 'B3 bronze field '
                                                                                     'LINKAGE_FALLBACK_CONFLICT_IND; '
                                                                                     'source value retained '
                                                                                     'unless documented as '
                                                                                     'derived.',
                                                    'linkage_historical_fallback_ind': 'B3 bronze field '
                                                                                       'LINKAGE_HISTORICAL_FALLBACK_IND; '
                                                                                       'source value '
                                                                                       'retained unless '
                                                                                       'documented as '
                                                                                       'derived.',
                                                    'loaded_at': 'map_elective_access_list.ADC_UPDT carried '
                                                                 'unchanged. Procedure and attribute '
                                                                 'children contribute clinical fields but '
                                                                 'their clocks are not selected or '
                                                                 'aggregated here; this is not the latest '
                                                                 'child load time or the current Silver '
                                                                 'refresh time.',
                                                    'modified_by_prid': 'B3 bronze field MODIFIED_BY_PRID; '
                                                                        'source value retained unless '
                                                                        'documented as derived.',
                                                    'modified_datetime': 'B3 bronze field MODIFIED_DT_TM; '
                                                                         'source value retained unless '
                                                                         'documented as derived.',
                                                    'nhs_number_valid_ind': 'B3 bronze field '
                                                                            'NHS_NUMBER_VALID_IND; source '
                                                                            'value retained unless '
                                                                            'documented as derived.',
                                                    'pathway_oid': 'B3 bronze field PATHWAY_OID; source '
                                                                   'value retained unless documented as '
                                                                   'derived.',
                                                    'patient_event_key': 'Deterministic event key derived '
                                                                         'from source identifiers; retained '
                                                                         'as a stable join key.',
                                                    'patient_oid': 'B3 bronze field PATIENT_OID; source '
                                                                   'value retained unless documented as '
                                                                   'derived.',
                                                    'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                                 'resolved.',
                                                    'person_link_method': 'B3 bronze field '
                                                                          'PERSON_LINK_METHOD; source value '
                                                                          'retained unless documented as '
                                                                          'derived.',
                                                    'person_link_status': 'Cross-arm consensus status; '
                                                                          'conflicts never publish '
                                                                          'PERSON_ID.',
                                                    'planned_datetime': 'B3 bronze field '
                                                                        'PLANNED_DT_TM_CLEAN; source value '
                                                                        'retained unless documented as '
                                                                        'derived.',
                                                    'priority_rvid': 'B3 bronze field '
                                                                     'WAITING_LIST_PRIORITY_RVID; source '
                                                                     'value retained unless documented as '
                                                                     'derived.',
                                                    'record_status': 'Retracted when '
                                                                     'map_elective_access_list.SOURCE_PRESENT_IND '
                                                                     'is false; otherwise superseded when '
                                                                     'ACTIVE_IND is false; otherwise active. '
                                                                     'Each missing flag defaults true, and '
                                                                     'retraction takes priority over '
                                                                     'supersession. This is a derived label, '
                                                                     'not the source closure flag.',
                                                    'record_status_effective_from': 'map_elective_access_list.CREATED_DT_TM '
                                                                                    'carried directly as the '
                                                                                    'record-status start, '
                                                                                    'without event-time '
                                                                                    'clamping. No admission, '
                                                                                    'procedure or '
                                                                                    'child-attribute date '
                                                                                    'replaces it.',
                                                    'record_status_effective_to': 'map_elective_access_list.SOURCE_ABSENT_DETECTED_TS '
                                                                                  'for a retracted row; '
                                                                                  'otherwise MODIFIED_DT_TM '
                                                                                  'for a superseded row; '
                                                                                  'otherwise null. '
                                                                                  'Retraction takes '
                                                                                  'priority. A missing '
                                                                                  'timestamp in the selected '
                                                                                  'branch remains null: '
                                                                                  'there is no fallback to '
                                                                                  'the other branch or '
                                                                                  'ADC_UPDT.',
                                                    'referral_oid': 'B3 bronze field REFERRAL_OID; source '
                                                                    'value retained unless documented as '
                                                                    'derived.',
                                                    'removal_reason_rvid': 'B3 bronze field '
                                                                           'REMOVAL_REASON_RVID; source '
                                                                           'value retained unless documented '
                                                                           'as derived.',
                                                    'site_rvid': 'B3 bronze field SITE_RVID; source value '
                                                                 'retained unless documented as derived.',
                                                    'source_code': 'B3 bronze field PROCEDURE_CODE; source '
                                                                   'value retained unless documented as '
                                                                   'derived.',
                                                    'source_coding_system': 'B3 bronze field PROCEDURE_CODE; '
                                                                            'source value retained unless '
                                                                            'documented as derived.',
                                                    'source_display': 'B3 bronze field PROCEDURE_DESC; '
                                                                      'source value retained unless '
                                                                      'documented as derived.',
                                                    'source_feed': 'Originating data feed responsible for '
                                                                   'the record for each elective access '
                                                                   'entry record. It is produced by the '
                                                                   'silver transformation and has no direct '
                                                                   'bronze-column lineage entry. Whitespace '
                                                                   'and source sentinel text are retained '
                                                                   'unless the pipeline explicitly '
                                                                   'normalizes them; null means no value was '
                                                                   'supplied.',
                                                    'source_system_oid': 'LUNA SOURCE_SYSTEM_OID as BIGINT; '
                                                                         'first component of the primary '
                                                                         'key.',
                                                    'source_update_timestamp': 'map_elective_access_list.MODIFIED_DT_TM '
                                                                               'carried unchanged, not a '
                                                                               'maximum over the joined '
                                                                               'procedure or attribute '
                                                                               'children. No ingestion or '
                                                                               'Silver refresh timestamp is '
                                                                               'substituted.',
                                                    'status_change_datetime': 'B3 bronze field '
                                                                              'WAITING_LIST_STATUS_CHANGE_DT_TM_CLEAN; '
                                                                              'source value retained unless '
                                                                              'documented as derived.',
                                                    'status_reason': 'B3 bronze field '
                                                                     'WAITING_LIST_STATUS_REASON; source '
                                                                     'value retained unless documented as '
                                                                     'derived.',
                                                    'status_rvid': 'B3 bronze field '
                                                                   'WAITING_LIST_STATUS_RVID; source value '
                                                                   'retained unless documented as derived.',
                                                    'subject_id_system': 'B3 bronze field PATIENT_OID; '
                                                                         'source value retained unless '
                                                                         'documented as derived.',
                                                    'subject_key': 'Deterministic SHA-256 over the strongest '
                                                                   'source identifier. Not salted, not '
                                                                   'secret; a stable join key across feeds '
                                                                   'where person_id is unresolved.',
                                                    'tci_created_datetime': 'B3 bronze field '
                                                                            'TCI_CREATED_DT_TM; source value '
                                                                            'retained unless documented as '
                                                                            'derived.',
                                                    'tci_datetime': 'B3 bronze field TCI_DT_TM_CLEAN; source '
                                                                    'value retained unless documented as '
                                                                    'derived.',
                                                    'tci_future_ind': 'B3 bronze field TCI_DT_TM_FUTURE_IND; '
                                                                      'source value retained unless '
                                                                      'documented as derived.',
                                                    'tci_location_rvid': 'B3 bronze field TCI_LOCATION_RVID; '
                                                                         'source value retained unless '
                                                                         'documented as derived.',
                                                    'treatment_function_rvid': 'B3 bronze field '
                                                                               'TREATMENT_FUNCTION_RVID; '
                                                                               'source value retained unless '
                                                                               'documented as derived.',
                                                    'vip_ind': 'Indicator that the source record carries VIP '
                                                               'handling status for each elective access '
                                                               'entry record. It is produced by the silver '
                                                               'transformation and has no direct '
                                                               'bronze-column lineage entry. Null means the '
                                                               'source did not state the indicator and must '
                                                               'not be interpreted as false.',
                                                    'waiting_list_code': 'B3 bronze field WAITING_LIST_CODE; '
                                                                         'source value retained unless '
                                                                         'documented as derived.',
                                                    'waiting_list_id': 'B3 bronze field WAITING_LIST_ID; '
                                                                       'source value retained unless '
                                                                       'documented as derived.',
                                                    'waiting_list_name': 'B3 bronze field WAITING_LIST_NAME; '
                                                                         'source value retained unless '
                                                                         'documented as derived.',
                                                    'waiting_list_oid': 'LUNA WAITING_LIST_OID as BIGINT; '
                                                                        'second component of the primary '
                                                                        'key.',
                                                    'withheld_identity_ind': 'Indicator that the source '
                                                                             'identity was deliberately '
                                                                             'withheld for each elective '
                                                                             'access entry record. It is '
                                                                             'produced by the silver '
                                                                             'transformation and has no '
                                                                             'direct bronze-column lineage '
                                                                             'entry. Null means the source '
                                                                             'did not state the indicator '
                                                                             'and must not be interpreted as '
                                                                             'false.'},
                                        'quantity_override': True,
                                        'table': 'Internal quality-controlled twin of '
                                                 'clinical_elective_access_entry: the Gold repairs, nulls '
                                                 'and expectations are applied here and the published Gold '
                                                 'MV reads this object. Materialized rather than temporary '
                                                 'so the published MV can refresh incrementally.'},
 'clinical_elective_access_entry': {'columns': {'active_ind': 'Source closure state retained verbatim; false '
                                                              'does not mean source absence.',
                                                'actual_guaranteed_activity_datetime': 'B3 bronze field '
                                                                                       'ACTUAL_GUARANTEED_ACTIVITY_DT_TM_CLEAN; '
                                                                                       'source value '
                                                                                       'retained unless '
                                                                                       'documented as '
                                                                                       'derived.',
                                                'admin_category_rvid': 'B3 bronze field ADMIN_CATEGORY_RVID; '
                                                                       'source value retained unless '
                                                                       'documented as derived.',
                                                'admit_datetime': 'B3 bronze field ADMIT_DT_TM_CLEAN; source '
                                                                  'value retained unless documented as '
                                                                  'derived.',
                                                'admit_method_rvid': 'B3 bronze field ADMIT_METHOD_RVID; '
                                                                     'source value retained unless '
                                                                     'documented as derived.',
                                                'admit_offer_outcome_rvid': 'B3 bronze field '
                                                                            'ADMIT_OFFER_OUTCOME_RVID; '
                                                                            'source value retained unless '
                                                                            'documented as derived.',
                                                'business_unit': 'B3 bronze field BUSINESS_UNIT; source '
                                                                 'value retained unless documented as '
                                                                 'derived.',
                                                'clinical_priority_code': 'B3 bronze field FIELD_VALUE_VAR; '
                                                                          'source value retained unless '
                                                                          'documented as derived.',
                                                'clinical_priority_recorded_datetime': 'B3 bronze field '
                                                                                       'FIELD_VALUE_DATE_CLEAN; '
                                                                                       'source value '
                                                                                       'retained unless '
                                                                                       'documented as '
                                                                                       'derived.',
                                                'comments': 'Direct identifier published and IG-governed at '
                                                            'serve time',
                                                'confidentiality_code': 'Source confidentiality '
                                                                        'classification attached to the '
                                                                        'record for each elective access '
                                                                        'entry record. It is produced by the '
                                                                        'silver transformation and has no '
                                                                        'direct bronze-column lineage entry. '
                                                                        'Source code meanings and sentinel '
                                                                        'values are retained unless the '
                                                                        'pipeline explicitly maps them; null '
                                                                        'means no code was supplied.',
                                                'created_by_prid': 'B3 bronze field CREATED_BY_PRID; source '
                                                                   'value retained unless documented as '
                                                                   'derived.',
                                                'created_datetime': 'B3 bronze field CREATED_DT_TM; source '
                                                                    'value retained unless documented as '
                                                                    'derived.',
                                                'decided_to_admit_datetime': 'B3 bronze field '
                                                                             'DECIDED_TO_ADMIT_DT_TM_CLEAN; '
                                                                             'source value retained unless '
                                                                             'documented as derived.',
                                                'department': 'B3 bronze field DEPARTMENT; source value '
                                                              'retained unless documented as derived.',
                                                'division': 'B3 bronze field DIVISION; source value retained '
                                                            'unless documented as derived.',
                                                'division_rvid': 'B3 bronze field DIVISION_RVID; source '
                                                                 'value retained unless documented as '
                                                                 'derived.',
                                                'earliest_reasonable_offer_datetime': 'B3 bronze field '
                                                                                      'EARLIEST_REASONABLE_OFFER_DT_TM_CLEAN; '
                                                                                      'source value retained '
                                                                                      'unless documented as '
                                                                                      'derived.',
                                                'elective_admission_type_rvid': 'B3 bronze field '
                                                                                'ELECTIVE_ADMISSION_TYPE_RVID; '
                                                                                'source value retained '
                                                                                'unless documented as '
                                                                                'derived.',
                                                'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                                'available.',
                                                'encounter_type_rvid': 'B3 bronze field ENCOUNTER_TYPE_RVID; '
                                                                       'source value retained unless '
                                                                       'documented as derived.',
                                                'event_datetime': 'B3 bronze field CREATED_DT_TM; source '
                                                                  'value retained unless documented as '
                                                                  'derived.',
                                                'event_datetime_basis': 'decided_to_admit or created; NULL '
                                                                        'iff event_datetime_derived is NULL. '
                                                                        'INSR_W2 W2.10.',
                                                'event_datetime_derived': 'Clinical event time for the '
                                                                          'entry: decided_to_admit_datetime, '
                                                                          'else created_datetime '
                                                                          '(event_datetime is the LUNA '
                                                                          'creation time, L2637). INSR_W2 '
                                                                          'W2.10 (J22).',
                                                'event_datetime_local': 'Event start date and time for the '
                                                                        'elective-access entry, expressed in '
                                                                        'local time.',
                                                'event_end_datetime': 'B3 bronze field ADMIT_DT_TM_CLEAN; '
                                                                      'source value retained unless '
                                                                      'documented as derived.',
                                                'event_end_datetime_local': 'Event end date and time for the '
                                                                            'elective-access entry, '
                                                                            'expressed in local time.',
                                                'fact_category': 'Contract category used to group the record '
                                                                 'with comparable clinical facts for each '
                                                                 'elective access entry record. It is '
                                                                 'produced by the silver transformation and '
                                                                 'has no direct bronze-column lineage entry. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                                'guaranteed_activity_datetime': 'B3 bronze field '
                                                                                'GUARANTEED_ACTIVITY_DT_TM_CLEAN; '
                                                                                'source value retained '
                                                                                'unless documented as '
                                                                                'derived.',
                                                'identifier_link_status': 'B3 bronze field '
                                                                          'IDENTIFIER_LINK_STATUS; source '
                                                                          'value retained unless documented '
                                                                          'as derived.',
                                                'intended_management_rvid': 'B3 bronze field '
                                                                            'INTENDED_MANAGEMENT_RVID; '
                                                                            'source value retained unless '
                                                                            'documented as derived.',
                                                'lead_clinician_prid': 'B3 bronze field LEAD_CLINICIAN_PRID; '
                                                                       'source value retained unless '
                                                                       'documented as derived.',
                                                'legacy_waiting_list_id': 'B3 bronze field '
                                                                          'LEGACY_WAITING_LIST_ID; source '
                                                                          'value retained unless documented '
                                                                          'as derived.',
                                                'linkage_fallback_conflict_ind': 'B3 bronze field '
                                                                                 'LINKAGE_FALLBACK_CONFLICT_IND; '
                                                                                 'source value retained '
                                                                                 'unless documented as '
                                                                                 'derived.',
                                                'linkage_historical_fallback_ind': 'B3 bronze field '
                                                                                   'LINKAGE_HISTORICAL_FALLBACK_IND; '
                                                                                   'source value retained '
                                                                                   'unless documented as '
                                                                                   'derived.',
                                                'loaded_at': 'map_elective_access_list.ADC_UPDT carried '
                                                             'unchanged. Procedure and attribute children '
                                                             'contribute clinical fields but their clocks '
                                                             'are not selected or aggregated here; this is '
                                                             'not the latest child load time or the current '
                                                             'Silver refresh time.',
                                                'modified_by_prid': 'B3 bronze field MODIFIED_BY_PRID; '
                                                                    'source value retained unless documented '
                                                                    'as derived.',
                                                'modified_datetime': 'B3 bronze field MODIFIED_DT_TM; source '
                                                                     'value retained unless documented as '
                                                                     'derived.',
                                                'nhs_number_valid_ind': 'B3 bronze field '
                                                                        'NHS_NUMBER_VALID_IND; source value '
                                                                        'retained unless documented as '
                                                                        'derived.',
                                                'pathway_oid': 'B3 bronze field PATHWAY_OID; source value '
                                                               'retained unless documented as derived.',
                                                'patient_event_key': 'Deterministic event key derived from '
                                                                     'source identifiers; retained as a '
                                                                     'stable join key.',
                                                'patient_oid': 'B3 bronze field PATIENT_OID; source value '
                                                               'retained unless documented as derived.',
                                                'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                             'resolved.',
                                                'person_link_method': 'B3 bronze field PERSON_LINK_METHOD; '
                                                                      'source value retained unless '
                                                                      'documented as derived.',
                                                'person_link_status': 'Cross-arm consensus status; conflicts '
                                                                      'never publish PERSON_ID.',
                                                'planned_datetime': 'B3 bronze field PLANNED_DT_TM_CLEAN; '
                                                                    'source value retained unless documented '
                                                                    'as derived.',
                                                'priority_rvid': 'B3 bronze field '
                                                                 'WAITING_LIST_PRIORITY_RVID; source value '
                                                                 'retained unless documented as derived.',
                                                'record_status': 'Retracted when '
                                                                 'map_elective_access_list.SOURCE_PRESENT_IND '
                                                                 'is false; otherwise superseded when '
                                                                 'ACTIVE_IND is false; otherwise active. '
                                                                 'Each missing flag defaults true, and '
                                                                 'retraction takes priority over '
                                                                 'supersession. This is a derived label, not '
                                                                 'the source closure flag.',
                                                'record_status_effective_from': 'map_elective_access_list.CREATED_DT_TM '
                                                                                'carried directly as the '
                                                                                'record-status start, '
                                                                                'without event-time '
                                                                                'clamping. No admission, '
                                                                                'procedure or '
                                                                                'child-attribute date '
                                                                                'replaces it.',
                                                'record_status_effective_to': 'map_elective_access_list.SOURCE_ABSENT_DETECTED_TS '
                                                                              'for a retracted row; '
                                                                              'otherwise MODIFIED_DT_TM for '
                                                                              'a superseded row; otherwise '
                                                                              'null. Retraction takes '
                                                                              'priority. A missing timestamp '
                                                                              'in the selected branch '
                                                                              'remains null: there is no '
                                                                              'fallback to the other branch '
                                                                              'or ADC_UPDT.',
                                                'referral_oid': 'B3 bronze field REFERRAL_OID; source value '
                                                                'retained unless documented as derived.',
                                                'removal_reason_rvid': 'B3 bronze field REMOVAL_REASON_RVID; '
                                                                       'source value retained unless '
                                                                       'documented as derived.',
                                                'site_rvid': 'B3 bronze field SITE_RVID; source value '
                                                             'retained unless documented as derived.',
                                                'source_code': 'B3 bronze field PROCEDURE_CODE; source value '
                                                               'retained unless documented as derived.',
                                                'source_coding_system': 'B3 bronze field PROCEDURE_CODE; '
                                                                        'source value retained unless '
                                                                        'documented as derived.',
                                                'source_display': 'B3 bronze field PROCEDURE_DESC; source '
                                                                  'value retained unless documented as '
                                                                  'derived.',
                                                'source_feed': 'Originating data feed responsible for the '
                                                               'record for each elective access entry '
                                                               'record. It is produced by the silver '
                                                               'transformation and has no direct '
                                                               'bronze-column lineage entry. Whitespace and '
                                                               'source sentinel text are retained unless the '
                                                               'pipeline explicitly normalizes them; null '
                                                               'means no value was supplied.',
                                                'source_system_oid': 'LUNA SOURCE_SYSTEM_OID as BIGINT; '
                                                                     'first component of the primary key.',
                                                'source_update_timestamp': 'map_elective_access_list.MODIFIED_DT_TM '
                                                                           'carried unchanged, not a maximum '
                                                                           'over the joined procedure or '
                                                                           'attribute children. No ingestion '
                                                                           'or Silver refresh timestamp is '
                                                                           'substituted.',
                                                'status_change_datetime': 'B3 bronze field '
                                                                          'WAITING_LIST_STATUS_CHANGE_DT_TM_CLEAN; '
                                                                          'source value retained unless '
                                                                          'documented as derived.',
                                                'status_reason': 'B3 bronze field '
                                                                 'WAITING_LIST_STATUS_REASON; source value '
                                                                 'retained unless documented as derived.',
                                                'status_rvid': 'B3 bronze field WAITING_LIST_STATUS_RVID; '
                                                               'source value retained unless documented as '
                                                               'derived.',
                                                'subject_id_system': 'B3 bronze field PATIENT_OID; source '
                                                                     'value retained unless documented as '
                                                                     'derived.',
                                                'subject_key': 'Deterministic SHA-256 over the strongest '
                                                               'source identifier. Not salted, not secret; a '
                                                               'stable join key across feeds where person_id '
                                                               'is unresolved.',
                                                'tci_created_datetime': 'B3 bronze field TCI_CREATED_DT_TM; '
                                                                        'source value retained unless '
                                                                        'documented as derived.',
                                                'tci_datetime': 'B3 bronze field TCI_DT_TM_CLEAN; source '
                                                                'value retained unless documented as '
                                                                'derived.',
                                                'tci_future_ind': 'B3 bronze field TCI_DT_TM_FUTURE_IND; '
                                                                  'source value retained unless documented '
                                                                  'as derived.',
                                                'tci_location_rvid': 'B3 bronze field TCI_LOCATION_RVID; '
                                                                     'source value retained unless '
                                                                     'documented as derived.',
                                                'treatment_function_rvid': 'B3 bronze field '
                                                                           'TREATMENT_FUNCTION_RVID; source '
                                                                           'value retained unless documented '
                                                                           'as derived.',
                                                'vip_ind': 'Indicator that the source record carries VIP '
                                                           'handling status for each elective access entry '
                                                           'record. It is produced by the silver '
                                                           'transformation and has no direct bronze-column '
                                                           'lineage entry. Null means the source did not '
                                                           'state the indicator and must not be interpreted '
                                                           'as false.',
                                                'waiting_list_code': 'B3 bronze field WAITING_LIST_CODE; '
                                                                     'source value retained unless '
                                                                     'documented as derived.',
                                                'waiting_list_id': 'B3 bronze field WAITING_LIST_ID; source '
                                                                   'value retained unless documented as '
                                                                   'derived.',
                                                'waiting_list_name': 'B3 bronze field WAITING_LIST_NAME; '
                                                                     'source value retained unless '
                                                                     'documented as derived.',
                                                'waiting_list_oid': 'LUNA WAITING_LIST_OID as BIGINT; second '
                                                                    'component of the primary key.',
                                                'withheld_identity_ind': 'Indicator that the source identity '
                                                                         'was deliberately withheld for each '
                                                                         'elective access entry record. It '
                                                                         'is produced by the silver '
                                                                         'transformation and has no direct '
                                                                         'bronze-column lineage entry. Null '
                                                                         'means the source did not state the '
                                                                         'indicator and must not be '
                                                                         'interpreted as false.'},
                                    'quantity_override': False,
                                    'table': 'One LUNA elective-access entry. Gold QC twin of the silver '
                                             'product: 1 columns are repaired or nulled, 1 rule(s) drop '
                                             'rows, 4 check(s) are advisory. Each rule states its reason in '
                                             'the pipeline notebook, and Lakeflow expectation metrics report '
                                             'what every rule matched on each update. Coverage: Planned and '
                                             'guaranteed admission dates are always equal at source, and the '
                                             'clinical priority code is empty on 85% of entries; '
                                             'complete-through month per release: '
                                             '6_mgmt.data_quality.release_finding (finding_code '
                                             'F2_complete_through).'}}
