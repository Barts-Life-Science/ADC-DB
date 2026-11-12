"""Gold safety.safety_incident_participant: 2 dataset(s).

- datasets: clinical_safety_incident_participant, _gold_qc_clinical_safety_incident_participant
- moved verbatim from: 8_gold_patient_safety:101, 8_gold_patient_safety:69, 8_gold_patient_safety:76, 8_gold_patient_safety:86

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import INTERNAL_SCHEMA, PUBLIC_SCHEMA
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== clinical_safety_incident_participant ====
CLINICAL_SAFETY_INCIDENT_PARTICIPANT_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_safety_incident_participant.active_record": "COALESCE(record_status = 'active', FALSE)",
    # Patient participants require resolution; non-patients retain their original treatment.
    # A null class passes only if the resolved-identity side makes the expression true.
    "gold.clinical_safety_incident_participant.patient_identity": "COALESCE(lower(participant_class) <> 'patient' OR identity_status = 'resolved', FALSE)",
}


CLINICAL_SAFETY_INCIDENT_PARTICIPANT_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.safety_incident_participant.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_safety_incident_participant.key_present": "safety_incident_participant_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_safety_incident_participant.load_time_present": "loaded_at IS NOT NULL",
}


@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_safety_incident_participant",
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_SAFETY_INCIDENT_PARTICIPANT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_SAFETY_INCIDENT_PARTICIPANT_ADVISORY_RULES)
def gold_clinical_safety_incident_participant():
    """Publish clinical_safety_incident_participant; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_safety_incident_participant")


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_safety_incident_participant",
    refresh_policy="auto",
)
def _gold_qc_clinical_safety_incident_participant():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_safety_incident_participant", "safety_incident_participant_key")


RULES = {
    'clinical_safety_incident_participant': GoldRules(
        table='clinical_safety_incident_participant',
        mandatory=CLINICAL_SAFETY_INCIDENT_PARTICIPANT_MANDATORY_RULES,
        advisory=CLINICAL_SAFETY_INCIDENT_PARTICIPANT_ADVISORY_RULES,
        tdx_value_repairs={'con_dob': "CAST(`con_dob` AS DATE) = DATE'1900-01-01'"}),
}
_rules = QcBinding(RULES)
_tdx_product = _rules._tdx_product
_tdx_qc = _rules._tdx_qc


finalise()


COMMENTS = {'_gold_qc_clinical_safety_incident_participant': {'columns': {'qc_checked_at': 'loaded_at of the admitted '
                                                                                'row (the QC evidence time).',
                                                               'qc_status': 'passed for every admitted row; '
                                                                            'advisory outcomes are in '
                                                                            'Lakeflow expectation metrics.',
                                                               'safety_incident_participant_key': 'Key of '
                                                                                                  'the '
                                                                                                  'admitted '
                                                                                                  'clinical_safety_incident_participant '
                                                                                                  'row.'},
                                                   'quantity_override': False,
                                                   'table': 'Compatibility evidence of admitted '
                                                            'clinical_safety_incident_participant rows; '
                                                            'advisory outcomes are in Lakeflow expectation '
                                                            'metrics.'},
 'clinical_safety_incident_participant': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                                      'con_address': 'Address of the contact.',
                                                      'con_clingroup': 'Clinical group code associated with '
                                                                       "the Datix contact's organisational "
                                                                       'placement.',
                                                      'con_directorate': 'Directorate code associated with '
                                                                         "the Datix contact's organisational "
                                                                         'placement.',
                                                      'con_disability': 'Disability status or details '
                                                                        'recorded for the Datix contact.',
                                                      'con_dob': 'Date of birth of the contact.',
                                                      'con_dod': 'Date of death recorded for the Datix '
                                                                 'contact.',
                                                      'con_email': 'Email address recorded for the Datix '
                                                                   'contact.',
                                                      'con_empl_grade': 'Employment grade recorded for the '
                                                                        'Datix contact.',
                                                      'con_ethnicity': 'Ethnicity of the contact.',
                                                      'con_fax': 'Fax number of the contact.',
                                                      'con_forenames': 'Forename(s) of the contact.',
                                                      'con_gender': 'Gender of the contact.',
                                                      'con_id': 'Identifier of the Datix contact record for '
                                                                'the patient associated with this LFPSE '
                                                                'submission.',
                                                      'con_jobtitle': 'Job title of the contact as free '
                                                                      'text.',
                                                      'con_language': 'Language recorded for the Datix '
                                                                      'contact.',
                                                      'con_locactual': 'Actual location code or codes '
                                                                       'associated with the Datix contact.',
                                                      'con_mrn': 'Medical record number recorded for the '
                                                                 'Datix contact participating in the safety '
                                                                 'incident, sourced from the Datix contact '
                                                                 'number field.',
                                                      'con_nhsno': 'NHS number recorded for the Datix '
                                                                   'contact.',
                                                      'con_orgcode': 'Organisation code associated with the '
                                                                     "Datix contact's organisational "
                                                                     'placement.',
                                                      'con_postcode': "Postcode of the contact's address.",
                                                      'con_religion': 'Religion or belief recorded for the '
                                                                      'contact.',
                                                      'con_sex_orientation': 'Sexual orientation recorded '
                                                                             'for the contact.',
                                                      'con_specialty': 'Specialty code associated with the '
                                                                       "Datix contact's organisational "
                                                                       'placement.',
                                                      'con_subtype': 'Code for the subtype of the Datix '
                                                                     'contact, refining the contact type.',
                                                      'con_surname': 'Surname of the contact.',
                                                      'con_tel1': 'Primary telephone number of the contact.',
                                                      'con_tel2': 'Secondary telephone number of the '
                                                                  'contact.',
                                                      'con_title': 'Title or honorific of the contact.',
                                                      'con_type': 'Code indicating the type of contact, such '
                                                                  'as patient or staff category.',
                                                      'con_unit': 'Unit code associated with the Datix '
                                                                  "contact's organisational placement.",
                                                      'dob_agrees_ind': 'Indicator of whether the date of '
                                                                        'birth recorded on the Datix contact '
                                                                        'agrees with the birth date of the '
                                                                        'resolved person record.',
                                                      'identity_status': 'resolved when person_id is '
                                                                         'present, unresolved when absent, '
                                                                         'not_applicable for tables without '
                                                                         'a patient.',
                                                      'inc_id': 'Identifier of the Datix incident record '
                                                                'associated with this LFPSE patient detail '
                                                                'record.',
                                                      'lfpse_age_at_time_of_incidents_days': "Patient's age "
                                                                                             'at the time of '
                                                                                             'the incident, '
                                                                                             'expressed in '
                                                                                             'days, as '
                                                                                             'recorded for '
                                                                                             'the LFPSE '
                                                                                             'submission.',
                                                      'lfpse_age_bracket': "Raw code for the patient's age "
                                                                           'bracket at the time of the '
                                                                           'incident, as recorded for the '
                                                                           'LFPSE submission.',
                                                      'lfpse_gender': "Raw code for the patient's gender as "
                                                                      'recorded for the LFPSE submission.',
                                                      'lfpse_id': 'Identifier of the Learn from Patient '
                                                                  'Safety Events (LFPSE) submission to which '
                                                                  'this patient detail record belongs.',
                                                      'lfpse_patient_ethnicity': "Raw code for the patient's "
                                                                                 'ethnicity as recorded for '
                                                                                 'the LFPSE submission.',
                                                      'lfpse_patient_recordid': 'Record identifier of the '
                                                                                'linked patient details '
                                                                                'record in the Datix LFPSE '
                                                                                'patient details source, '
                                                                                'where one applies.',
                                                      'lfpse_patient_sequence': 'Sequence number '
                                                                                'distinguishing individual '
                                                                                'patients within an LFPSE '
                                                                                'submission.',
                                                      'lfpse_physical_harm': 'Raw code for the level of '
                                                                             'physical harm to the patient '
                                                                             'recorded for the LFPSE '
                                                                             'submission.',
                                                      'lfpse_psychological_harm': 'Raw code for the level of '
                                                                                  'psychological harm to the '
                                                                                  'patient recorded for the '
                                                                                  'LFPSE submission.',
                                                      'lfpse_strength_of_association': 'Raw code for the '
                                                                                       'strength of '
                                                                                       'association between '
                                                                                       'the incident and the '
                                                                                       'patient harm '
                                                                                       'recorded for the '
                                                                                       'LFPSE submission.',
                                                      'link_abs_end': "End date of the contact's absence "
                                                                      'recorded against this link.',
                                                      'link_abs_start': "Start date of the contact's absence "
                                                                        'recorded against this link.',
                                                      'link_age': 'Age of the linked contact recorded '
                                                                  'against the link to the Datix record.',
                                                      'link_age_band': 'Age band of the linked contact '
                                                                       'recorded against this link.',
                                                      'link_attempted_assault': 'Indicator of whether the '
                                                                                'incident involving the '
                                                                                'linked contact involved an '
                                                                                'attempted assault.',
                                                      'link_become_unconscious': 'Indicator of whether the '
                                                                                 'linked contact became '
                                                                                 'unconscious as a result of '
                                                                                 'the incident.',
                                                      'link_bodypart1': 'Primary body part injured for the '
                                                                        'linked contact in relation to the '
                                                                        'linked Datix record.',
                                                      'link_clin_factors': 'Clinical factors recorded for '
                                                                           'the linked contact in relation '
                                                                           'to the incident.',
                                                      'link_date_admission': 'Date of admission recorded for '
                                                                             'the linked contact on the '
                                                                             'Datix record.',
                                                      'link_daysaway': 'Number of days the linked contact '
                                                                       'was away from work as a result of '
                                                                       'the event.',
                                                      'link_deceased': 'Indicator of whether the linked '
                                                                       'contact is deceased.',
                                                      'link_direct_indirect': 'Indicator of whether the '
                                                                              'linked contact was directly '
                                                                              'or indirectly involved in, or '
                                                                              'affected by, the incident.',
                                                      'link_discomfort_caused': 'Indicator of whether '
                                                                                'discomfort was caused to '
                                                                                'the linked contact in the '
                                                                                'incident.',
                                                      'link_dod': 'Date of death of the linked contact as '
                                                                  'recorded for this link.',
                                                      'link_harassment': 'Indicator of whether the incident '
                                                                         'involving the linked contact '
                                                                         'involved harassment.',
                                                      'link_hospital_24hours': 'Indicator of whether the '
                                                                               'linked contact was admitted '
                                                                               'to hospital for more than 24 '
                                                                               'hours following the '
                                                                               'incident.',
                                                      'link_injuries': 'Injury details recorded for the '
                                                                       'linked contact in relation to the '
                                                                       'linked Datix record.',
                                                      'link_injury1': 'Primary injury recorded for the '
                                                                      'linked contact in relation to the '
                                                                      'linked Datix record.',
                                                      'link_injury_caused': 'Indicator of whether injury was '
                                                                            'caused to the linked contact in '
                                                                            'the incident.',
                                                      'link_is_riddor': 'Indicator of whether the event is '
                                                                        'reportable under RIDDOR for the '
                                                                        'linked contact.',
                                                      'link_mh_observe': 'Mental health observation level '
                                                                         'recorded for the linked contact.',
                                                      'link_mhact': 'Indicator of whether the linked contact '
                                                                    'was detained under the Mental Health '
                                                                    'Act.',
                                                      'link_mhact_section': 'Mental Health Act section '
                                                                            'applicable to the linked '
                                                                            'contact.',
                                                      'link_mhcpa': 'Mental health Care Programme Approach '
                                                                    'status recorded for the linked contact.',
                                                      'link_npsa': 'NPSA-related classification recorded for '
                                                                   'the linked contact on the Datix record.',
                                                      'link_npsa_role': 'NPSA-related role of the linked '
                                                                        'contact in the Datix record.',
                                                      'link_patrelation': 'Relationship of the linked '
                                                                          'contact to the patient involved '
                                                                          'in the Datix record.',
                                                      'link_primary': 'Flag for whether this contact link is '
                                                                      'the primary link for the contact on '
                                                                      'the associated Datix record.',
                                                      'link_req_resuscitation': 'Indicator of whether the '
                                                                                'linked contact required '
                                                                                'resuscitation as a result '
                                                                                'of the incident.',
                                                      'link_riddor': 'RIDDOR reporting detail recorded for '
                                                                     'the contact on the linked Datix '
                                                                     'record.',
                                                      'link_role': 'Code indicating the role the contact '
                                                                   'plays in relation to the linked Datix '
                                                                   'record.',
                                                      'link_role_display': 'Descriptive label for the Datix '
                                                                           'link role code.',
                                                      'link_status': 'Status of the contact in relation to '
                                                                     'the linked Datix record.',
                                                      'link_treatment': 'Treatment received by the linked '
                                                                        'contact in connection with the '
                                                                        'Datix record.',
                                                      'link_type': 'Code indicating the type of link between '
                                                                   'the contact and the Datix record.',
                                                      'link_verbal_abuse': 'Indicator of whether the '
                                                                           'incident involving the linked '
                                                                           'contact involved verbal abuse.',
                                                      'link_worked_alone': 'Indicator of whether the linked '
                                                                           'contact was working alone at the '
                                                                           'time of the incident.',
                                                      'load_batch_id': 'Load batch label derived from '
                                                                       'loaded_at (yyyyMMddHHmmss); tdx-v21 '
                                                                       'when no load time exists.',
                                                      'loaded_at': 'When the row was last written to bronze '
                                                                   '(ADC_UPDT), else its source update time; '
                                                                   'the stable load clock.',
                                                      'login': 'Datix user account login name for the '
                                                               'contact.',
                                                      'participant_class': 'Classification of the incident '
                                                                           'participant derived from the '
                                                                           'Datix contact type and contact '
                                                                           'link role.',
                                                      'participant_row_id': 'Source record identifier for '
                                                                            'the participant link row, taken '
                                                                            'from the Datix contact link or '
                                                                            'LFPSE patient details record.',
                                                      'participant_source': 'Indicates the source from which '
                                                                            'the safety incident participant '
                                                                            'record was derived, carried '
                                                                            'from the bronze safety incident '
                                                                            'participant mapping.',
                                                      'person_id': 'Millennium PERSON_ID of the patient '
                                                                   '(identifier); NULL when the source row '
                                                                   'names no patient.',
                                                      'person_resolution_rule': 'Rule used to resolve the '
                                                                                'incident participant to a '
                                                                                'person record through the '
                                                                                'patient identifier mapping.',
                                                      'person_resolution_status': 'Outcome status of the '
                                                                                  'attempt to resolve the '
                                                                                  'incident participant to a '
                                                                                  'person record.',
                                                      'pipeline_run_id': 'Bronze pipeline run that last '
                                                                         'wrote the row.',
                                                      'record_status': 'active, or superseded when the row '
                                                                       'stopped appearing in the source '
                                                                       'snapshot (source_present_ind false).',
                                                      'record_status_effective_from': 'Start of the record '
                                                                                      'status; not supplied '
                                                                                      'by this source '
                                                                                      '(always NULL).',
                                                      'record_status_effective_to': 'When a superseded row '
                                                                                    'was first detected '
                                                                                    'absent from the source '
                                                                                    '(source_absent_detected_ts); '
                                                                                    'NULL while active.',
                                                      'row_hash': "SHA-256 of the row's payload columns, "
                                                                  'used by the bronze MERGE to detect '
                                                                  'changes.',
                                                      'safety_incident_participant_key': 'Deterministic '
                                                                                         'SHA-256 key of the '
                                                                                         'safety incident '
                                                                                         'participant row '
                                                                                         '(namespace plus '
                                                                                         'native source '
                                                                                         'key).',
                                                      'source_absent_detected_ts': 'When bronze first found '
                                                                                   'the row absent from the '
                                                                                   'source snapshot; NULL '
                                                                                   'while present.',
                                                      'source_adc_updt': 'When the source row was landed in '
                                                                         'raw (raw ADC_UPDT).',
                                                      'source_present_ind': 'True while the row is present '
                                                                            'in the latest full source '
                                                                            'snapshot.',
                                                      'source_row_id': 'Native source row identifier, as '
                                                                       'text.',
                                                      'source_table': 'Bronze or raw source table the row '
                                                                      'came from.',
                                                      'source_update_timestamp': 'Time the source row was '
                                                                                 'landed in the raw layer '
                                                                                 '(bronze SOURCE_ADC_UPDT), '
                                                                                 'not when it changed in the '
                                                                                 'source system; do not use '
                                                                                 'it for recency.',
                                                      'source_updt_dt_tm': 'Last-update time of the row in '
                                                                           'the source system (for Datix, '
                                                                           'updateddate); NULL when the '
                                                                           'source supplies none.',
                                                      'subject_id_system': 'System of subject_key: '
                                                                           'urn:cerner:person_id, or '
                                                                           'nosubject when the row names no '
                                                                           'patient.',
                                                      'subject_key': 'Deterministic subject key: hash of the '
                                                                     'Cerner person id, or of the source row '
                                                                     'when no person is recorded.',
                                                      'total_absence': 'Total absence recorded for the '
                                                                       'linked contact, likely related to '
                                                                       'the absence start and end dates; '
                                                                       'units are not documented.'},
                                          'quantity_override': False,
                                          'table': 'One row per person linked to a Datix incident (patient, '
                                                   'staff or other), keyed by the Datix link record id. Gold '
                                                   'QC twin of silver clinical_safety_incident_participant: '
                                                   'mandatory rules drop rows, advisory rules are reported '
                                                   'in Lakeflow expectation metrics.'}}
