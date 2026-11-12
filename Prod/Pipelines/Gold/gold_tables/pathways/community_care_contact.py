"""Gold pathways.community_care_contact: 2 dataset(s).

- datasets: _qc_clinical_community_care_contact, clinical_community_care_contact
- moved verbatim from: 4_gold_care_pathways:536, 4_gold_care_pathways:586, 4_gold_care_pathways:593, 4_gold_care_pathways:602, 4_gold_care_pathways:694, 4_gold_care_pathways:698, 4_gold_care_pathways:715

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


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
    '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
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


CLINICAL_COMMUNITY_CARE_CONTACT_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`commissioner_ods_code_resolved` AS `commissioner_ods_code_resolved`',
    '`commissioner_parent_icb_ods_code` AS `commissioner_parent_icb_ods_code`',
    '`commissioner_resolution_method` AS `commissioner_resolution_method`',
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
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.community_care_contact.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
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


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_COMMUNITY_CARE_CONTACT_PERSON_SCOPE_RULES = {
    "gold.clinical.community_care_contact.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_community_care_contact"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_COMMUNITY_CARE_CONTACT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_COMMUNITY_CARE_CONTACT_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_COMMUNITY_CARE_CONTACT_PERSON_SCOPE_RULES)
def _gold_qc_clinical_community_care_contact():
    """Quality-controlled twin of journey_clinical.community_care_contact."""
    df = _qc(
        "clinical_community_care_contact",
        CLINICAL_COMMUNITY_CARE_CONTACT_SELECT,
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.community_care_contact"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
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
        '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
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
        '`commissioner_ods_code_resolved` AS `commissioner_ods_code_resolved`',  # INSR_W3 W3.12
        '`commissioner_parent_icb_ods_code` AS `commissioner_parent_icb_ods_code`',  # INSR_W3 W3.12
        '`commissioner_resolution_method` AS `commissioner_resolution_method`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'clinical_community_care_contact': GoldRules(
        table='clinical_community_care_contact',
        select=CLINICAL_COMMUNITY_CARE_CONTACT_SELECT,
        mandatory=CLINICAL_COMMUNITY_CARE_CONTACT_MANDATORY_RULES,
        advisory=CLINICAL_COMMUNITY_CARE_CONTACT_ADVISORY_RULES,
        person_scope=CLINICAL_COMMUNITY_CARE_CONTACT_PERSON_SCOPE_RULES,
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


COMMENTS = {'_qc_clinical_community_care_contact': {'columns': {'care_contact_date': 'Date of the care contact as '
                                                                          'submitted in the CSDS CYP201 '
                                                                          'record.',
                                                     'care_contact_id': 'Source CYP201 Care Contact '
                                                                        'Identifier; not unique by itself.',
                                                     'care_contact_id_variant_count': 'Rows sharing '
                                                                                      'source_database_id '
                                                                                      'and care_contact_id.',
                                                     'commissioner_ods_code': 'Submitted commissioner ODS '
                                                                              'code.',
                                                     'commissioner_ods_code_resolved': 'Commissioner ODS '
                                                                                       'code with the '
                                                                                       '3-character form A3A '
                                                                                       'resolved to its '
                                                                                       'sub-ICB location '
                                                                                       '(ODS ORD API, '
                                                                                       'RO98/RO319); other '
                                                                                       'codes unchanged. '
                                                                                       'INSR_W2 W2.11 '
                                                                                       '(L2513).',
                                                     'commissioner_organization_id': 'Internal organisation '
                                                                                     'identifier for the '
                                                                                     'commissioner resolved '
                                                                                     'by matching the '
                                                                                     'submitted ODS code to '
                                                                                     'organisation alias '
                                                                                     'records.',
                                                     'commissioner_organization_name': 'Name of the '
                                                                                       'commissioner '
                                                                                       'organisation '
                                                                                       'resolved from the '
                                                                                       'organisation '
                                                                                       'reference data for '
                                                                                       'the submitted '
                                                                                       'commissioner ODS '
                                                                                       'code.',
                                                     'commissioner_parent_icb_ods_code': 'Parent ICB of the '
                                                                                         'sub-ICB location '
                                                                                         'resolved from A3A '
                                                                                         '(ODS RE5 '
                                                                                         'relationship); '
                                                                                         'NULL for other '
                                                                                         'codes. INSR_W2 '
                                                                                         'W2.11.',
                                                     'commissioner_resolution_method': 'submitted, or '
                                                                                       'ods:sub_icb_location_prefix:A3A '
                                                                                       'when resolved from '
                                                                                       'the 3-character '
                                                                                       'form. INSR_W2 W2.11.',
                                                     'community_care_contact_key': 'Source-preserved '
                                                                                   'community care contact '
                                                                                   'key; primary key of this '
                                                                                   'table.',
                                                     'community_database_id': 'Source community system '
                                                                              'database identifier (CDB) '
                                                                              'from the CSDS CYP201 care '
                                                                              'contact extract, forming part '
                                                                              'of the record grain.',
                                                     'community_patient_key': 'Pseudonymised patient key '
                                                                              'derived from the source '
                                                                              'community system patient '
                                                                              'number and its registering '
                                                                              'database identifier.',
                                                     'confidentiality_code': 'Source confidentiality '
                                                                             'classification attached to the '
                                                                             'record for each community care '
                                                                             'contact record. It is produced '
                                                                             'by the silver transformation '
                                                                             'and has no direct '
                                                                             'bronze-column lineage entry. '
                                                                             'Source code meanings and '
                                                                             'sentinel values are retained '
                                                                             'unless the pipeline explicitly '
                                                                             'maps them; null means no code '
                                                                             'was supplied.',
                                                     'consultation_mechanism_code': 'Consultation mechanism '
                                                                                    '(medium of contact) '
                                                                                    'code standardised by '
                                                                                    'the pipeline from the '
                                                                                    'submitted source value.',
                                                     'consultation_mechanism_display': 'Description '
                                                                                       'corresponding to the '
                                                                                       'standardised '
                                                                                       'consultation '
                                                                                       'mechanism code.',
                                                     'duration_minutes': 'Clinical contact duration in '
                                                                         'minutes as submitted in the CSDS '
                                                                         'CYP201 record.',
                                                     'duration_quality_status': 'Derived data-quality status '
                                                                                'describing the completeness '
                                                                                'or plausibility of the '
                                                                                'submitted clinical contact '
                                                                                'duration.',
                                                     'earliest_clinically_appropriate_date': 'Earliest '
                                                                                             'clinically '
                                                                                             'appropriate '
                                                                                             'date submitted '
                                                                                             'with the CSDS '
                                                                                             'CYP201 care '
                                                                                             'contact '
                                                                                             'record.',
                                                     'earliest_reasonable_offer_date': 'Earliest reasonable '
                                                                                       'offer date submitted '
                                                                                       'with the CSDS CYP201 '
                                                                                       'care contact record.',
                                                     'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT '
                                                                     'when available.',
                                                     'event_datetime': 'Date of the care contact as '
                                                                       'submitted in the CSDS CYP201 record.',
                                                     'event_datetime_local': 'Local-time timestamp at which '
                                                                             'the community care contact '
                                                                             'event started.',
                                                     'event_end_datetime': 'Date and time when the '
                                                                           'represented clinical or '
                                                                           'administrative event ended for '
                                                                           'each community care contact '
                                                                           'record. It is produced by the '
                                                                           'silver transformation and has no '
                                                                           'direct bronze-column lineage '
                                                                           'entry. Source precision and '
                                                                           'timezone handling follow the '
                                                                           'pipeline expression; null means '
                                                                           'the time was unavailable.',
                                                     'event_end_datetime_local': 'Local-time timestamp at '
                                                                                 'which the community care '
                                                                                 'contact event ended.',
                                                     'fact_category': 'Contract category used to group the '
                                                                      'record with comparable clinical facts '
                                                                      'for each community care contact '
                                                                      'record. It is produced by the silver '
                                                                      'transformation and has no direct '
                                                                      'bronze-column lineage entry. '
                                                                      'Whitespace and source sentinel text '
                                                                      'are retained unless the pipeline '
                                                                      'explicitly normalizes them; null '
                                                                      'means no value was supplied.',
                                                     'loaded_at': 'Always null as a TIMESTAMP in the current '
                                                                  'community-care contact projection. No '
                                                                  'contributing load timestamp is supplied; '
                                                                  'neither the contact date nor Silver '
                                                                  'refresh time is substituted.',
                                                     'location_type_code': 'CSDS activity location type '
                                                                           '(site type) code submitted on '
                                                                           'the CYP201 care contact, derived '
                                                                           'from the source location code '
                                                                           'field.',
                                                     'location_type_display': 'Descriptive label for the '
                                                                              'CSDS activity location type '
                                                                              'code recorded on the care '
                                                                              'contact.',
                                                     'patient_event_key': 'Deterministic event key derived '
                                                                          'from source identifiers; retained '
                                                                          'as a stable join key.',
                                                     'person_id': 'Native Millennium PERSON_ID as BIGINT '
                                                                  'when resolved.',
                                                     'person_match_status': 'Status flag indicating the '
                                                                            'outcome of matching the '
                                                                            'community patient record to a '
                                                                            'curated person record.',
                                                     'record_status': 'Retracted when '
                                                                      'map_community_care_contact.SOURCE_PRESENT_IND '
                                                                      'is false, otherwise active; missing '
                                                                      'presence defaults true. No superseded '
                                                                      'status is emitted. This describes '
                                                                      'source-row presence, not attendance, '
                                                                      'consultation outcome or '
                                                                      'identity-match status.',
                                                     'record_status_effective_from': 'First non-null '
                                                                                     'care_contact_datetime_local '
                                                                                     'cast to TIMESTAMP then '
                                                                                     'care_contact_date cast '
                                                                                     'to TIMESTAMP from '
                                                                                     'map_community_care_contact. '
                                                                                     'This contact-time '
                                                                                     'proxy is not an '
                                                                                     'independent status '
                                                                                     'transition. Unlike '
                                                                                     'event_datetime, this '
                                                                                     'history start is not '
                                                                                     'clamped to the '
                                                                                     '[1950,2100) range, and '
                                                                                     'has no ingestion-time '
                                                                                     'fallback.',
                                                     'record_status_effective_to': 'map_community_care_contact.SOURCE_ABSENT_DETECTED_TS '
                                                                                   'only when '
                                                                                   'SOURCE_PRESENT_IND is '
                                                                                   'false; otherwise null. A '
                                                                                   'missing absence '
                                                                                   'timestamp stays null, '
                                                                                   'with no ingestion-time '
                                                                                   'fallback. This is the '
                                                                                   'row-absence end proxy, '
                                                                                   'not the clinical contact '
                                                                                   'end.',
                                                     'service_id': 'Identifier of the registering service or '
                                                                   'organisation recorded in the source '
                                                                   'community system patient lookup.',
                                                     'service_name': 'Name of the registering service or '
                                                                     'organisation recorded in the source '
                                                                     'community system patient lookup.',
                                                     'service_request_id': 'Source CYP201 Service Request '
                                                                           'Identifier.',
                                                     'service_team_type_code': 'CSDS team type code '
                                                                               'identifying the type of '
                                                                               'service team delivering the '
                                                                               'care contact.',
                                                     'service_team_type_display': 'Descriptive label for the '
                                                                                  'CSDS service team type '
                                                                                  'code recorded on the care '
                                                                                  'contact.',
                                                     'service_team_type_mapping_status': 'Derived status '
                                                                                         'flag indicating '
                                                                                         'whether the '
                                                                                         'submitted CSDS '
                                                                                         'team type value '
                                                                                         'was successfully '
                                                                                         'resolved to a '
                                                                                         'recognised service '
                                                                                         'team type '
                                                                                         'reference code.',
                                                     'source_code': 'Coded consultation type submitted with '
                                                                    'the CSDS CYP201 care contact record.',
                                                     'source_coding_system': 'Coded consultation type '
                                                                             'submitted with the CSDS CYP201 '
                                                                             'care contact record.',
                                                     'source_consultation_term': 'Original consultation '
                                                                                 'mechanism term as recorded '
                                                                                 'in the source system '
                                                                                 'before normalisation.',
                                                     'source_display': 'Description corresponding to the '
                                                                       'coded consultation type submitted '
                                                                       'with the care contact record.',
                                                     'source_feed': 'Originating data feed responsible for '
                                                                    'the record for each community care '
                                                                    'contact record. It is produced by the '
                                                                    'silver transformation and has no direct '
                                                                    'bronze-column lineage entry. Whitespace '
                                                                    'and source sentinel text are retained '
                                                                    'unless the pipeline explicitly '
                                                                    'normalizes them; null means no value '
                                                                    'was supplied.',
                                                     'source_update_timestamp': 'Always null as a TIMESTAMP '
                                                                                'in the current '
                                                                                'community-care contact '
                                                                                'projection. No native '
                                                                                'application-update or '
                                                                                'ingestion clock is supplied '
                                                                                'for this field; neither the '
                                                                                'contact date nor Silver '
                                                                                'refresh time is '
                                                                                'substituted.',
                                                     'subject_id_system': 'Pseudonymised patient key derived '
                                                                          'from the source community system '
                                                                          'patient number and its '
                                                                          'registering database identifier.',
                                                     'subject_key': 'Deterministic SHA-256 over the '
                                                                    'strongest source identifier. Not '
                                                                    'salted, not secret; a stable join key '
                                                                    'across feeds where person_id is '
                                                                    'unresolved.',
                                                     'team_id': 'Local team identifier submitted with the '
                                                                'CSDS CYP201 care contact record.',
                                                     'vip_ind': 'Indicator that the source record carries '
                                                                'VIP handling status for each community care '
                                                                'contact record. It is produced by the '
                                                                'silver transformation and has no direct '
                                                                'bronze-column lineage entry. Null means the '
                                                                'source did not state the indicator and must '
                                                                'not be interpreted as false.',
                                                     'withheld_identity_ind': 'Indicator that the source '
                                                                              'identity was deliberately '
                                                                              'withheld for each community '
                                                                              'care contact record. It is '
                                                                              'produced by the silver '
                                                                              'transformation and has no '
                                                                              'direct bronze-column lineage '
                                                                              'entry. Null means the source '
                                                                              'did not state the indicator '
                                                                              'and must not be interpreted '
                                                                              'as false.'},
                                         'quantity_override': True,
                                         'table': 'Internal quality-controlled twin of '
                                                  'clinical_community_care_contact: the Gold repairs, nulls '
                                                  'and expectations are applied here and the published Gold '
                                                  'MV reads this object. Materialized rather than temporary '
                                                  'so the published MV can refresh incrementally.'},
 'clinical_community_care_contact': {'columns': {'care_contact_date': 'Date of the care contact as submitted '
                                                                      'in the CSDS CYP201 record.',
                                                 'care_contact_id': 'Source CYP201 Care Contact Identifier; '
                                                                    'not unique by itself.',
                                                 'care_contact_id_variant_count': 'Rows sharing '
                                                                                  'source_database_id and '
                                                                                  'care_contact_id.',
                                                 'commissioner_ods_code': 'Submitted commissioner ODS code.',
                                                 'commissioner_ods_code_resolved': 'Commissioner ODS code '
                                                                                   'with the 3-character '
                                                                                   'form A3A resolved to its '
                                                                                   'sub-ICB location (ODS '
                                                                                   'ORD API, RO98/RO319); '
                                                                                   'other codes unchanged. '
                                                                                   'INSR_W2 W2.11 (L2513).',
                                                 'commissioner_organization_id': 'Internal organisation '
                                                                                 'identifier for the '
                                                                                 'commissioner resolved by '
                                                                                 'matching the submitted ODS '
                                                                                 'code to organisation alias '
                                                                                 'records.',
                                                 'commissioner_organization_name': 'Name of the commissioner '
                                                                                   'organisation resolved '
                                                                                   'from the organisation '
                                                                                   'reference data for the '
                                                                                   'submitted commissioner '
                                                                                   'ODS code.',
                                                 'commissioner_parent_icb_ods_code': 'Parent ICB of the '
                                                                                     'sub-ICB location '
                                                                                     'resolved from A3A (ODS '
                                                                                     'RE5 relationship); '
                                                                                     'NULL for other codes. '
                                                                                     'INSR_W2 W2.11.',
                                                 'commissioner_resolution_method': 'submitted, or '
                                                                                   'ods:sub_icb_location_prefix:A3A '
                                                                                   'when resolved from the '
                                                                                   '3-character form. '
                                                                                   'INSR_W2 W2.11.',
                                                 'community_care_contact_key': 'Source-preserved community '
                                                                               'care contact key; primary '
                                                                               'key of this table.',
                                                 'community_database_id': 'Source community system database '
                                                                          'identifier (CDB) from the CSDS '
                                                                          'CYP201 care contact extract, '
                                                                          'forming part of the record grain.',
                                                 'community_patient_key': 'Pseudonymised patient key derived '
                                                                          'from the source community system '
                                                                          'patient number and its '
                                                                          'registering database identifier.',
                                                 'confidentiality_code': 'Source confidentiality '
                                                                         'classification attached to the '
                                                                         'record for each community care '
                                                                         'contact record. It is produced by '
                                                                         'the silver transformation and has '
                                                                         'no direct bronze-column lineage '
                                                                         'entry. Source code meanings and '
                                                                         'sentinel values are retained '
                                                                         'unless the pipeline explicitly '
                                                                         'maps them; null means no code was '
                                                                         'supplied.',
                                                 'consultation_mechanism_code': 'Consultation mechanism '
                                                                                '(medium of contact) code '
                                                                                'standardised by the '
                                                                                'pipeline from the submitted '
                                                                                'source value.',
                                                 'consultation_mechanism_display': 'Description '
                                                                                   'corresponding to the '
                                                                                   'standardised '
                                                                                   'consultation mechanism '
                                                                                   'code.',
                                                 'duration_minutes': 'Clinical contact duration in minutes '
                                                                     'as submitted in the CSDS CYP201 '
                                                                     'record.',
                                                 'duration_quality_status': 'Derived data-quality status '
                                                                            'describing the completeness or '
                                                                            'plausibility of the submitted '
                                                                            'clinical contact duration.',
                                                 'earliest_clinically_appropriate_date': 'Earliest '
                                                                                         'clinically '
                                                                                         'appropriate date '
                                                                                         'submitted with the '
                                                                                         'CSDS CYP201 care '
                                                                                         'contact record.',
                                                 'earliest_reasonable_offer_date': 'Earliest reasonable '
                                                                                   'offer date submitted '
                                                                                   'with the CSDS CYP201 '
                                                                                   'care contact record.',
                                                 'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                                 'available.',
                                                 'event_datetime': 'Date of the care contact as submitted in '
                                                                   'the CSDS CYP201 record.',
                                                 'event_datetime_local': 'Local-time timestamp at which the '
                                                                         'community care contact event '
                                                                         'started.',
                                                 'event_end_datetime': 'Date and time when the represented '
                                                                       'clinical or administrative event '
                                                                       'ended for each community care '
                                                                       'contact record. It is produced by '
                                                                       'the silver transformation and has no '
                                                                       'direct bronze-column lineage entry. '
                                                                       'Source precision and timezone '
                                                                       'handling follow the pipeline '
                                                                       'expression; null means the time was '
                                                                       'unavailable.',
                                                 'event_end_datetime_local': 'Local-time timestamp at which '
                                                                             'the community care contact '
                                                                             'event ended.',
                                                 'fact_category': 'Contract category used to group the '
                                                                  'record with comparable clinical facts for '
                                                                  'each community care contact record. It is '
                                                                  'produced by the silver transformation and '
                                                                  'has no direct bronze-column lineage '
                                                                  'entry. Whitespace and source sentinel '
                                                                  'text are retained unless the pipeline '
                                                                  'explicitly normalizes them; null means no '
                                                                  'value was supplied.',
                                                 'loaded_at': 'Always null as a TIMESTAMP in the current '
                                                              'community-care contact projection. No '
                                                              'contributing load timestamp is supplied; '
                                                              'neither the contact date nor Silver refresh '
                                                              'time is substituted.',
                                                 'location_type_code': 'CSDS activity location type (site '
                                                                       'type) code submitted on the CYP201 '
                                                                       'care contact, derived from the '
                                                                       'source location code field.',
                                                 'location_type_display': 'Descriptive label for the CSDS '
                                                                          'activity location type code '
                                                                          'recorded on the care contact.',
                                                 'patient_event_key': 'Deterministic event key derived from '
                                                                      'source identifiers; retained as a '
                                                                      'stable join key.',
                                                 'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                              'resolved.',
                                                 'person_match_status': 'Status flag indicating the outcome '
                                                                        'of matching the community patient '
                                                                        'record to a curated person record.',
                                                 'record_status': 'Retracted when '
                                                                  'map_community_care_contact.SOURCE_PRESENT_IND '
                                                                  'is false, otherwise active; missing '
                                                                  'presence defaults true. No superseded '
                                                                  'status is emitted. This describes '
                                                                  'source-row presence, not attendance, '
                                                                  'consultation outcome or identity-match '
                                                                  'status.',
                                                 'record_status_effective_from': 'First non-null '
                                                                                 'care_contact_datetime_local '
                                                                                 'cast to TIMESTAMP then '
                                                                                 'care_contact_date cast to '
                                                                                 'TIMESTAMP from '
                                                                                 'map_community_care_contact. '
                                                                                 'This contact-time proxy is '
                                                                                 'not an independent status '
                                                                                 'transition. Unlike '
                                                                                 'event_datetime, this '
                                                                                 'history start is not '
                                                                                 'clamped to the [1950,2100) '
                                                                                 'range, and has no '
                                                                                 'ingestion-time fallback.',
                                                 'record_status_effective_to': 'map_community_care_contact.SOURCE_ABSENT_DETECTED_TS '
                                                                               'only when SOURCE_PRESENT_IND '
                                                                               'is false; otherwise null. A '
                                                                               'missing absence timestamp '
                                                                               'stays null, with no '
                                                                               'ingestion-time fallback. '
                                                                               'This is the row-absence end '
                                                                               'proxy, not the clinical '
                                                                               'contact end.',
                                                 'service_id': 'Identifier of the registering service or '
                                                               'organisation recorded in the source '
                                                               'community system patient lookup.',
                                                 'service_name': 'Name of the registering service or '
                                                                 'organisation recorded in the source '
                                                                 'community system patient lookup.',
                                                 'service_request_id': 'Source CYP201 Service Request '
                                                                       'Identifier.',
                                                 'service_team_type_code': 'CSDS team type code identifying '
                                                                           'the type of service team '
                                                                           'delivering the care contact.',
                                                 'service_team_type_display': 'Descriptive label for the '
                                                                              'CSDS service team type code '
                                                                              'recorded on the care contact.',
                                                 'service_team_type_mapping_status': 'Derived status flag '
                                                                                     'indicating whether the '
                                                                                     'submitted CSDS team '
                                                                                     'type value was '
                                                                                     'successfully resolved '
                                                                                     'to a recognised '
                                                                                     'service team type '
                                                                                     'reference code.',
                                                 'source_code': 'Coded consultation type submitted with the '
                                                                'CSDS CYP201 care contact record.',
                                                 'source_coding_system': 'Coded consultation type submitted '
                                                                         'with the CSDS CYP201 care contact '
                                                                         'record.',
                                                 'source_consultation_term': 'Original consultation '
                                                                             'mechanism term as recorded in '
                                                                             'the source system before '
                                                                             'normalisation.',
                                                 'source_display': 'Description corresponding to the coded '
                                                                   'consultation type submitted with the '
                                                                   'care contact record.',
                                                 'source_feed': 'Originating data feed responsible for the '
                                                                'record for each community care contact '
                                                                'record. It is produced by the silver '
                                                                'transformation and has no direct '
                                                                'bronze-column lineage entry. Whitespace and '
                                                                'source sentinel text are retained unless '
                                                                'the pipeline explicitly normalizes them; '
                                                                'null means no value was supplied.',
                                                 'source_update_timestamp': 'Always null as a TIMESTAMP in '
                                                                            'the current community-care '
                                                                            'contact projection. No native '
                                                                            'application-update or ingestion '
                                                                            'clock is supplied for this '
                                                                            'field; neither the contact date '
                                                                            'nor Silver refresh time is '
                                                                            'substituted.',
                                                 'subject_id_system': 'Pseudonymised patient key derived '
                                                                      'from the source community system '
                                                                      'patient number and its registering '
                                                                      'database identifier.',
                                                 'subject_key': 'Deterministic SHA-256 over the strongest '
                                                                'source identifier. Not salted, not secret; '
                                                                'a stable join key across feeds where '
                                                                'person_id is unresolved.',
                                                 'team_id': 'Local team identifier submitted with the CSDS '
                                                            'CYP201 care contact record.',
                                                 'vip_ind': 'Indicator that the source record carries VIP '
                                                            'handling status for each community care contact '
                                                            'record. It is produced by the silver '
                                                            'transformation and has no direct bronze-column '
                                                            'lineage entry. Null means the source did not '
                                                            'state the indicator and must not be interpreted '
                                                            'as false.',
                                                 'withheld_identity_ind': 'Indicator that the source '
                                                                          'identity was deliberately '
                                                                          'withheld for each community care '
                                                                          'contact record. It is produced by '
                                                                          'the silver transformation and has '
                                                                          'no direct bronze-column lineage '
                                                                          'entry. Null means the source did '
                                                                          'not state the indicator and must '
                                                                          'not be interpreted as false.'},
                                     'quantity_override': False,
                                     'table': 'One CSDS community-care contact. Gold QC twin of the silver '
                                              'product: 1 columns are repaired or nulled, 1 rule(s) drop '
                                              'rows, 4 check(s) are advisory. Each rule states its reason in '
                                              'the pipeline notebook, and Lakeflow expectation metrics '
                                              'report what every rule matched on each update.'}}
