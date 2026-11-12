"""Gold pathways.referral: 2 dataset(s).

- datasets: _qc_clinical_referral, clinical_referral
- moved verbatim from: 4_gold_care_pathways:2497, 4_gold_care_pathways:2559, 4_gold_care_pathways:2582, 4_gold_care_pathways:2590, 4_gold_care_pathways:2704, 4_gold_care_pathways:2709, 4_gold_care_pathways:2713, 4_gold_care_pathways:2735

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


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
    '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
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


CLINICAL_REFERRAL_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`referred_by_prid` AS `referred_by_prid`',
    '`local_patient_id` AS `local_patient_id`',
    '`referral_reason` AS `referral_reason`',
    '`referral_comments` AS `referral_comments`',
    '`referred_by_organisation` AS `referred_by_organisation`',
    '`ers_local_patient_id` AS `ers_local_patient_id`',
    '`ers_nhs_number` AS `ers_nhs_number`',
    '`ers_patient_name` AS `ers_patient_name`',
    '`ers_referral_reason` AS `ers_referral_reason`',
    '`ers_referral_comments` AS `ers_referral_comments`',
    '`ers_referred_by_organisation` AS `ers_referred_by_organisation`',
    '`ers_referred_by_team_name` AS `ers_referred_by_team_name`',
    '`ers_referred_to_team_name` AS `ers_referred_to_team_name`',
    '`ers_lead_clinician_forename` AS `ers_lead_clinician_forename`',
    '`ers_lead_clinician_surname` AS `ers_lead_clinician_surname`',
    '`ers_referred_by_care_provider_forename` AS `ers_referred_by_care_provider_forename`',
    '`ers_referred_by_care_provider_surname` AS `ers_referred_by_care_provider_surname`',
    '`ers_referred_to_care_provider_forename` AS `ers_referred_to_care_provider_forename`',
    '`ers_referred_to_care_provider_surname` AS `ers_referred_to_care_provider_surname`',
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
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.referral.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
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


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_REFERRAL_PERSON_SCOPE_RULES = {
    "gold.clinical.referral.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
CLINICAL_REFERRAL_ORPHAN_RULES = {
    "gold.clinical.referral.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_referral"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_REFERRAL_MANDATORY_RULES)
@dp.expect_all(CLINICAL_REFERRAL_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_REFERRAL_PERSON_SCOPE_RULES)
@dp.expect_all(CLINICAL_REFERRAL_ORPHAN_RULES)
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
    return df


@dp.materialized_view(
    name=_n("gold_clinical.referral"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
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
        '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
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
        '`referred_by_prid` AS `referred_by_prid`',  # INSR_W3 W3.12
        '`local_patient_id` AS `local_patient_id`',  # INSR_W3 W3.12
        '`referral_reason` AS `referral_reason`',  # INSR_W3 W3.12
        '`referral_comments` AS `referral_comments`',  # INSR_W3 W3.12
        '`referred_by_organisation` AS `referred_by_organisation`',  # INSR_W3 W3.12
        '`ers_local_patient_id` AS `ers_local_patient_id`',  # INSR_W3 W3.12
        '`ers_nhs_number` AS `ers_nhs_number`',  # INSR_W3 W3.12
        '`ers_patient_name` AS `ers_patient_name`',  # INSR_W3 W3.12
        '`ers_referral_reason` AS `ers_referral_reason`',  # INSR_W3 W3.12
        '`ers_referral_comments` AS `ers_referral_comments`',  # INSR_W3 W3.12
        '`ers_referred_by_organisation` AS `ers_referred_by_organisation`',  # INSR_W3 W3.12
        '`ers_referred_by_team_name` AS `ers_referred_by_team_name`',  # INSR_W3 W3.12
        '`ers_referred_to_team_name` AS `ers_referred_to_team_name`',  # INSR_W3 W3.12
        '`ers_lead_clinician_forename` AS `ers_lead_clinician_forename`',  # INSR_W3 W3.12
        '`ers_lead_clinician_surname` AS `ers_lead_clinician_surname`',  # INSR_W3 W3.12
        '`ers_referred_by_care_provider_forename` AS `ers_referred_by_care_provider_forename`',  # INSR_W3 W3.12
        '`ers_referred_by_care_provider_surname` AS `ers_referred_by_care_provider_surname`',  # INSR_W3 W3.12
        '`ers_referred_to_care_provider_forename` AS `ers_referred_to_care_provider_forename`',  # INSR_W3 W3.12
        '`ers_referred_to_care_provider_surname` AS `ers_referred_to_care_provider_surname`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'clinical_referral': GoldRules(
        table='clinical_referral',
        select=CLINICAL_REFERRAL_SELECT,
        mandatory=CLINICAL_REFERRAL_MANDATORY_RULES,
        advisory=CLINICAL_REFERRAL_ADVISORY_RULES,
        person_scope=CLINICAL_REFERRAL_PERSON_SCOPE_RULES,
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


COMMENTS = {'_qc_clinical_referral': {'columns': {'admin_category_code': 'Source administrative-category code.',
                                       'admin_category_display': 'Source administrative-category display.',
                                       'booking_type_code': 'Source booking-type code.',
                                       'booking_type_display': 'Source booking-type display.',
                                       'business_unit': 'Source business unit.',
                                       'confidentiality_code': 'Source confidentiality code when supplied.',
                                       'division': 'Name of the clinical division responsible for the '
                                                   'referral within the organisational hierarchy.',
                                       'encounter_id': 'Nullable Millennium ENCNTR_ID; the LUNA referral '
                                                       'feed supplies no encounter.',
                                       'encounter_type_code': 'Source encounter-type code.',
                                       'encounter_type_display': 'Source encounter-type display.',
                                       'ers_lead_clinician_forename': 'e-RS lead clinician forename. '
                                                                      'Identifier, published and tagged. '
                                                                      'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                      'ERS_LEAD_CLINICIAN_FORENAME, INSR '
                                                                      'W1). INSR_W2 W2.20.',
                                       'ers_lead_clinician_surname': 'e-RS lead clinician surname. '
                                                                     'Identifier, published and tagged. '
                                                                     'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                     'ERS_LEAD_CLINICIAN_SURNAME, INSR W1). '
                                                                     'INSR_W2 W2.20.',
                                       'ers_local_patient_id': 'e-RS LocalPatientID (MRN as sent by e-RS). '
                                                               'Identifier, published and tagged. '
                                                               'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                               'ERS_LOCAL_PATIENT_ID, INSR W1). INSR_W2 '
                                                               'W2.20.',
                                       'ers_nhs_number': 'e-RS NHSNumber. Identifier, published and tagged. '
                                                         'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                         'ERS_NHS_NUMBER, INSR W1). INSR_W2 W2.20.',
                                       'ers_pathway_start': 'e-Referral pathway start date.',
                                       'ers_patient_name': 'e-RS patient name. Identifier, published and '
                                                           'tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                           'ERS_PATIENT_NAME, INSR W1). INSR_W2 W2.20.',
                                       'ers_referral_comments': 'e-RS free-text referral comments. '
                                                                'Identifier, published and tagged. '
                                                                'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                'ERS_REFERRAL_COMMENTS, INSR W1). INSR_W2 '
                                                                'W2.20.',
                                       'ers_referral_reason': 'e-RS free-text referral reason. Identifier, '
                                                              'published and tagged. '
                                                              'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                              'ERS_REFERRAL_REASON, INSR W1). INSR_W2 W2.20.',
                                       'ers_referred_by_care_provider_forename': 'e-RS referring care '
                                                                                 'provider forename. '
                                                                                 'Identifier, published and '
                                                                                 'tagged. '
                                                                                 'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                                 '(bronze '
                                                                                 'ERS_REFERRED_BY_CARE_PROVIDER_FORENAME, '
                                                                                 'INSR W1). INSR_W2 W2.20.',
                                       'ers_referred_by_care_provider_surname': 'e-RS referring care '
                                                                                'provider surname. '
                                                                                'Identifier, published and '
                                                                                'tagged. '
                                                                                'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                                '(bronze '
                                                                                'ERS_REFERRED_BY_CARE_PROVIDER_SURNAME, '
                                                                                'INSR W1). INSR_W2 W2.20.',
                                       'ers_referred_by_organisation': 'e-RS referring organisation. '
                                                                       'Identifier, published and tagged. '
                                                                       'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                       'ERS_REFERRED_BY_ORGANISATION, INSR '
                                                                       'W1). INSR_W2 W2.20.',
                                       'ers_referred_by_team_name': 'e-RS referring team name. Identifier, '
                                                                    'published and tagged. '
                                                                    'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                    'ERS_REFERRED_BY_TEAM_NAME, INSR W1). '
                                                                    'INSR_W2 W2.20.',
                                       'ers_referred_to_care_provider_forename': 'e-RS receiving care '
                                                                                 'provider forename. '
                                                                                 'Identifier, published and '
                                                                                 'tagged. '
                                                                                 'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                                 '(bronze '
                                                                                 'ERS_REFERRED_TO_CARE_PROVIDER_FORENAME, '
                                                                                 'INSR W1). INSR_W2 W2.20.',
                                       'ers_referred_to_care_provider_surname': 'e-RS receiving care '
                                                                                'provider surname. '
                                                                                'Identifier, published and '
                                                                                'tagged. '
                                                                                'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                                '(bronze '
                                                                                'ERS_REFERRED_TO_CARE_PROVIDER_SURNAME, '
                                                                                'INSR W1). INSR_W2 W2.20.',
                                       'ers_referred_to_team_name': 'e-RS receiving team name. Identifier, '
                                                                    'published and tagged. '
                                                                    'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                    'ERS_REFERRED_TO_TEAM_NAME, INSR W1). '
                                                                    'INSR_W2 W2.20.',
                                       'ers_service_name': 'e-Referral service name.',
                                       'ers_specialty': 'e-Referral specialty recorded in the source '
                                                        'referral field `ERS_SPECIALTY`.',
                                       'ers_ubrn_received': 'e-Referral UBRN received date.',
                                       'event_datetime': 'Referral received timestamp.',
                                       'event_datetime_local': 'Referral received timestamp expressed in '
                                                               'local time, the local-time counterpart of '
                                                               'event_datetime.',
                                       'event_end_datetime': 'Event end timestamp; not supplied by LUNA '
                                                             'referrals.',
                                       'event_end_datetime_local': 'Local-time counterpart of '
                                                                   'event_end_datetime; not supplied by LUNA '
                                                                   'referrals, so expected to be null.',
                                       'fact_category': 'Whether this fact is clinical or administrative in '
                                                        'the v2 plane merge.',
                                       'loaded_at': 'map_referral.ADC_UPDT carried unchanged through the '
                                                    'source and public projections. No parent or other '
                                                    'source clock is joined into this value; it differs from '
                                                    'SOURCE_ADC_UPDT used for source_update_timestamp and '
                                                    'from the current Silver refresh time.',
                                       'local_patient_id': 'LUNA LocalPatientID (Barts MRN) of the '
                                                           "referral's patient. Identifier, published and "
                                                           'tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                           'LOCAL_PATIENT_ID, INSR W1). INSR_W2 W2.20.',
                                       'original_received_datetime': 'Original referral received timestamp.',
                                       'pathway_oid': 'Raw LUNA pathway object identifier.',
                                       'patient_event_key': 'Deterministic SHA-256 referral identity.',
                                       'person_id': 'Millennium PERSON_ID when resolved; native person '
                                                    'foreign key.',
                                       'record_status': 'Superseded when map_referral.SOURCE_PRESENT_IND or '
                                                        'ACTIVE_IND is false; each missing flag defaults '
                                                        'true. Otherwise active. Source absence and '
                                                        'inactivity both map to superseded, not retracted; '
                                                        'the clinical status code does not set this '
                                                        'row-history label.',
                                       'record_status_effective_from': 'Date and time the referral record '
                                                                       'was created in the source system.',
                                       'record_status_effective_to': 'For a superseded referral row, the '
                                                                     'first non-null '
                                                                     'map_referral.SOURCE_ABSENT_DETECTED_TS, '
                                                                     'MODIFIED_DATETIME or ADC_UPDT, in that '
                                                                     'order; otherwise null. The same '
                                                                     'source-presence/inactivity predicate '
                                                                     'sets record_status. No event-time '
                                                                     'clamp or clinical-event timestamp is '
                                                                     'substituted.',
                                       'referral_comments': 'LUNA free-text referral comments. Identifier, '
                                                            'published and tagged. '
                                                            'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                            'REFERRAL_COMMENTS, INSR W1). INSR_W2 W2.20.',
                                       'referral_oid': 'LUNA REFERRAL_OID; component of the native primary '
                                                       'key.',
                                       'referral_priority_code': 'Source referral-priority code.',
                                       'referral_priority_display': 'Source referral-priority display.',
                                       'referral_reason': 'LUNA free-text referral reason. Identifier, '
                                                          'published and tagged. '
                                                          'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                          'REFERRAL_REASON, INSR W1). INSR_W2 W2.20.',
                                       'referral_source_code': 'Source referral-source code.',
                                       'referral_source_display': 'Source referral-source display.',
                                       'referred_by_org_id': 'Raw referring organization identifier.',
                                       'referred_by_organisation': 'LUNA referring organisation text. '
                                                                   'Identifier, published and tagged. '
                                                                   'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                   'REFERRED_BY_ORGANISATION, INSR W1). '
                                                                   'INSR_W2 W2.20.',
                                       'referred_by_prid': 'LUNA ReferredBy_PRID: referring professional id '
                                                           '(identifier). (bronze REFERRED_BY_PRID, INSR '
                                                           'W1). INSR_W2 W2.20.',
                                       'referring_facility_code': 'Source referring-facility code.',
                                       'referring_facility_display': 'Source referring-facility display.',
                                       'service_type_requested_code': 'Source requested-service-type code.',
                                       'service_type_requested_display': 'Source requested-service-type '
                                                                         'display.',
                                       'site_code': 'Source site code.',
                                       'site_display': 'Source site display.',
                                       'source_code': 'Verbatim treatment-function code.',
                                       'source_coding_system': 'Verbatim LUNA treatment-function coding '
                                                               'system.',
                                       'source_display': 'Verbatim treatment-function display.',
                                       'source_feed': 'Registered feed owning the fact.',
                                       'source_system_oid': 'LUNA SOURCE_SYSTEM_OID; component of the native '
                                                            'primary key.',
                                       'source_update_timestamp': 'map_referral.SOURCE_ADC_UPDT carried '
                                                                  'unchanged to the research row, distinct '
                                                                  'from ADC_UPDT used for loaded_at. It is '
                                                                  'not the clinical event timestamp or the '
                                                                  'current Silver refresh time.',
                                       'status_change_datetime': 'Source referral status-change timestamp.',
                                       'status_change_reason_code': 'Source referral status-change reason '
                                                                    'code.',
                                       'status_change_reason_display': 'Source referral status-change reason '
                                                                       'display.',
                                       'status_code': 'Source referral-status code.',
                                       'status_display': 'Source referral-status display.',
                                       'subject_id_system': 'Identifier system used for subject_key.',
                                       'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                      'identifier. Not salted, not secret; a stable join key '
                                                      'across feeds where person_id is unresolved.',
                                       'suspected_cancer_site_code': 'Source suspected-cancer-site code.',
                                       'suspected_cancer_site_display': 'Source suspected-cancer-site '
                                                                        'display.',
                                       'treatment_function_code': 'Source treatment-function code.',
                                       'treatment_function_display': 'Source treatment-function display.',
                                       'ubrn': 'Referral linkage evidence only; never a join key.',
                                       'vip_ind': 'Source VIP indicator when supplied.',
                                       'waiting_list_oid': 'Raw LUNA waiting-list object identifier.',
                                       'withheld_identity_ind': 'Withheld-identity indicator when supplied.'},
                           'quantity_override': True,
                           'table': 'Internal quality-controlled twin of clinical_referral: the Gold '
                                    'repairs, nulls and expectations are applied here and the published Gold '
                                    'MV reads this object. Materialized rather than temporary so the '
                                    'published MV can refresh incrementally.'},
 'clinical_referral': {'columns': {'admin_category_code': 'Source administrative-category code.',
                                   'admin_category_display': 'Source administrative-category display.',
                                   'booking_type_code': 'Source booking-type code.',
                                   'booking_type_display': 'Source booking-type display.',
                                   'business_unit': 'Source business unit.',
                                   'confidentiality_code': 'Source confidentiality code when supplied.',
                                   'division': 'Name of the clinical division responsible for the referral '
                                               'within the organisational hierarchy.',
                                   'encounter_id': 'Nullable Millennium ENCNTR_ID; the LUNA referral feed '
                                                   'supplies no encounter.',
                                   'encounter_type_code': 'Source encounter-type code.',
                                   'encounter_type_display': 'Source encounter-type display.',
                                   'ers_lead_clinician_forename': 'e-RS lead clinician forename. Identifier, '
                                                                  'published and tagged. '
                                                                  'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                  'ERS_LEAD_CLINICIAN_FORENAME, INSR W1). '
                                                                  'INSR_W2 W2.20.',
                                   'ers_lead_clinician_surname': 'e-RS lead clinician surname. Identifier, '
                                                                 'published and tagged. '
                                                                 'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                 'ERS_LEAD_CLINICIAN_SURNAME, INSR W1). '
                                                                 'INSR_W2 W2.20.',
                                   'ers_local_patient_id': 'e-RS LocalPatientID (MRN as sent by e-RS). '
                                                           'Identifier, published and tagged. '
                                                           'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                           'ERS_LOCAL_PATIENT_ID, INSR W1). INSR_W2 W2.20.',
                                   'ers_nhs_number': 'e-RS NHSNumber. Identifier, published and tagged. '
                                                     'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze ERS_NHS_NUMBER, '
                                                     'INSR W1). INSR_W2 W2.20.',
                                   'ers_pathway_start': 'e-Referral pathway start date.',
                                   'ers_patient_name': 'e-RS patient name. Identifier, published and tagged. '
                                                       'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                       'ERS_PATIENT_NAME, INSR W1). INSR_W2 W2.20.',
                                   'ers_referral_comments': 'e-RS free-text referral comments. Identifier, '
                                                            'published and tagged. '
                                                            'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                            'ERS_REFERRAL_COMMENTS, INSR W1). INSR_W2 W2.20.',
                                   'ers_referral_reason': 'e-RS free-text referral reason. Identifier, '
                                                          'published and tagged. '
                                                          'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                          'ERS_REFERRAL_REASON, INSR W1). INSR_W2 W2.20.',
                                   'ers_referred_by_care_provider_forename': 'e-RS referring care provider '
                                                                             'forename. Identifier, '
                                                                             'published and tagged. '
                                                                             'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                             '(bronze '
                                                                             'ERS_REFERRED_BY_CARE_PROVIDER_FORENAME, '
                                                                             'INSR W1). INSR_W2 W2.20.',
                                   'ers_referred_by_care_provider_surname': 'e-RS referring care provider '
                                                                            'surname. Identifier, published '
                                                                            'and tagged. '
                                                                            'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                            '(bronze '
                                                                            'ERS_REFERRED_BY_CARE_PROVIDER_SURNAME, '
                                                                            'INSR W1). INSR_W2 W2.20.',
                                   'ers_referred_by_organisation': 'e-RS referring organisation. Identifier, '
                                                                   'published and tagged. '
                                                                   'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                   'ERS_REFERRED_BY_ORGANISATION, INSR W1). '
                                                                   'INSR_W2 W2.20.',
                                   'ers_referred_by_team_name': 'e-RS referring team name. Identifier, '
                                                                'published and tagged. '
                                                                'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                'ERS_REFERRED_BY_TEAM_NAME, INSR W1). '
                                                                'INSR_W2 W2.20.',
                                   'ers_referred_to_care_provider_forename': 'e-RS receiving care provider '
                                                                             'forename. Identifier, '
                                                                             'published and tagged. '
                                                                             'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                             '(bronze '
                                                                             'ERS_REFERRED_TO_CARE_PROVIDER_FORENAME, '
                                                                             'INSR W1). INSR_W2 W2.20.',
                                   'ers_referred_to_care_provider_surname': 'e-RS receiving care provider '
                                                                            'surname. Identifier, published '
                                                                            'and tagged. '
                                                                            'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                            '(bronze '
                                                                            'ERS_REFERRED_TO_CARE_PROVIDER_SURNAME, '
                                                                            'INSR W1). INSR_W2 W2.20.',
                                   'ers_referred_to_team_name': 'e-RS receiving team name. Identifier, '
                                                                'published and tagged. '
                                                                'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                'ERS_REFERRED_TO_TEAM_NAME, INSR W1). '
                                                                'INSR_W2 W2.20.',
                                   'ers_service_name': 'e-Referral service name.',
                                   'ers_specialty': 'e-Referral specialty recorded in the source referral '
                                                    'field `ERS_SPECIALTY`.',
                                   'ers_ubrn_received': 'e-Referral UBRN received date.',
                                   'event_datetime': 'Referral received timestamp.',
                                   'event_datetime_local': 'Referral received timestamp expressed in local '
                                                           'time, the local-time counterpart of '
                                                           'event_datetime.',
                                   'event_end_datetime': 'Event end timestamp; not supplied by LUNA '
                                                         'referrals.',
                                   'event_end_datetime_local': 'Local-time counterpart of '
                                                               'event_end_datetime; not supplied by LUNA '
                                                               'referrals, so expected to be null.',
                                   'fact_category': 'Whether this fact is clinical or administrative in the '
                                                    'v2 plane merge.',
                                   'loaded_at': 'map_referral.ADC_UPDT carried unchanged through the source '
                                                'and public projections. No parent or other source clock is '
                                                'joined into this value; it differs from SOURCE_ADC_UPDT '
                                                'used for source_update_timestamp and from the current '
                                                'Silver refresh time.',
                                   'local_patient_id': "LUNA LocalPatientID (Barts MRN) of the referral's "
                                                       'patient. Identifier, published and tagged. '
                                                       'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                       'LOCAL_PATIENT_ID, INSR W1). INSR_W2 W2.20.',
                                   'original_received_datetime': 'Original referral received timestamp.',
                                   'pathway_oid': 'Raw LUNA pathway object identifier.',
                                   'patient_event_key': 'Deterministic SHA-256 referral identity.',
                                   'person_id': 'Millennium PERSON_ID when resolved; native person foreign '
                                                'key.',
                                   'record_status': 'Superseded when map_referral.SOURCE_PRESENT_IND or '
                                                    'ACTIVE_IND is false; each missing flag defaults true. '
                                                    'Otherwise active. Source absence and inactivity both '
                                                    'map to superseded, not retracted; the clinical status '
                                                    'code does not set this row-history label.',
                                   'record_status_effective_from': 'Date and time the referral record was '
                                                                   'created in the source system.',
                                   'record_status_effective_to': 'For a superseded referral row, the first '
                                                                 'non-null '
                                                                 'map_referral.SOURCE_ABSENT_DETECTED_TS, '
                                                                 'MODIFIED_DATETIME or ADC_UPDT, in that '
                                                                 'order; otherwise null. The same '
                                                                 'source-presence/inactivity predicate sets '
                                                                 'record_status. No event-time clamp or '
                                                                 'clinical-event timestamp is substituted.',
                                   'referral_comments': 'LUNA free-text referral comments. Identifier, '
                                                        'published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                        '(bronze REFERRAL_COMMENTS, INSR W1). INSR_W2 W2.20.',
                                   'referral_oid': 'LUNA REFERRAL_OID; component of the native primary key.',
                                   'referral_priority_code': 'Source referral-priority code.',
                                   'referral_priority_display': 'Source referral-priority display.',
                                   'referral_reason': 'LUNA free-text referral reason. Identifier, published '
                                                      'and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                      'REFERRAL_REASON, INSR W1). INSR_W2 W2.20.',
                                   'referral_source_code': 'Source referral-source code.',
                                   'referral_source_display': 'Source referral-source display.',
                                   'referred_by_org_id': 'Raw referring organization identifier.',
                                   'referred_by_organisation': 'LUNA referring organisation text. '
                                                               'Identifier, published and tagged. '
                                                               'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                               'REFERRED_BY_ORGANISATION, INSR W1). INSR_W2 '
                                                               'W2.20.',
                                   'referred_by_prid': 'LUNA ReferredBy_PRID: referring professional id '
                                                       '(identifier). (bronze REFERRED_BY_PRID, INSR W1). '
                                                       'INSR_W2 W2.20.',
                                   'referring_facility_code': 'Source referring-facility code.',
                                   'referring_facility_display': 'Source referring-facility display.',
                                   'service_type_requested_code': 'Source requested-service-type code.',
                                   'service_type_requested_display': 'Source requested-service-type display.',
                                   'site_code': 'Source site code.',
                                   'site_display': 'Source site display.',
                                   'source_code': 'Verbatim treatment-function code.',
                                   'source_coding_system': 'Verbatim LUNA treatment-function coding system.',
                                   'source_display': 'Verbatim treatment-function display.',
                                   'source_feed': 'Registered feed owning the fact.',
                                   'source_system_oid': 'LUNA SOURCE_SYSTEM_OID; component of the native '
                                                        'primary key.',
                                   'source_update_timestamp': 'map_referral.SOURCE_ADC_UPDT carried '
                                                              'unchanged to the research row, distinct from '
                                                              'ADC_UPDT used for loaded_at. It is not the '
                                                              'clinical event timestamp or the current '
                                                              'Silver refresh time.',
                                   'status_change_datetime': 'Source referral status-change timestamp.',
                                   'status_change_reason_code': 'Source referral status-change reason code.',
                                   'status_change_reason_display': 'Source referral status-change reason '
                                                                   'display.',
                                   'status_code': 'Source referral-status code.',
                                   'status_display': 'Source referral-status display.',
                                   'subject_id_system': 'Identifier system used for subject_key.',
                                   'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                  'identifier. Not salted, not secret; a stable join key '
                                                  'across feeds where person_id is unresolved.',
                                   'suspected_cancer_site_code': 'Source suspected-cancer-site code.',
                                   'suspected_cancer_site_display': 'Source suspected-cancer-site display.',
                                   'treatment_function_code': 'Source treatment-function code.',
                                   'treatment_function_display': 'Source treatment-function display.',
                                   'ubrn': 'Referral linkage evidence only; never a join key.',
                                   'vip_ind': 'Source VIP indicator when supplied.',
                                   'waiting_list_oid': 'Raw LUNA waiting-list object identifier.',
                                   'withheld_identity_ind': 'Withheld-identity indicator when supplied.'},
                       'quantity_override': False,
                       'table': 'One row per LUNA referral (REFERRAL_OID). Referral source and priority are '
                                'LUNA local codes with their displays; ubrn is e-Referral linkage evidence, '
                                'not a join key. e-RS rows created from 2024-12 with 10-digit referral OIDs '
                                'are not yet linked (see the coverage register). Coverage: e-RS fields are '
                                'filled only where the e-RS row joins a LUNA referral; e-RS rows created '
                                'from December 2024 use 10-digit referral ids that do not join (74,204 '
                                'rows), so recent e-RS detail is missing; complete-through month per '
                                'release: 6_mgmt.data_quality.release_finding (finding_code '
                                'F2_complete_through).'}}
