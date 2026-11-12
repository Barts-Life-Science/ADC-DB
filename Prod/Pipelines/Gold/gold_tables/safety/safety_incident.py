"""Gold safety.safety_incident: 2 dataset(s).

- datasets: clinical_safety_incident, _gold_qc_clinical_safety_incident
- moved verbatim from: 8_gold_patient_safety:26, 8_gold_patient_safety:30, 8_gold_patient_safety:40, 8_gold_patient_safety:55

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import INTERNAL_SCHEMA, PUBLIC_SCHEMA
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== clinical_safety_incident ====
CLINICAL_SAFETY_INCIDENT_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_safety_incident.active_record": "COALESCE(record_status = 'active', FALSE)",
}


CLINICAL_SAFETY_INCIDENT_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.safety_incident.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_safety_incident.key_present": "safety_incident_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_safety_incident.load_time_present": "loaded_at IS NOT NULL",
}


@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_safety_incident",
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_SAFETY_INCIDENT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_SAFETY_INCIDENT_ADVISORY_RULES)
def gold_clinical_safety_incident():
    """Publish clinical_safety_incident; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_safety_incident")


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_safety_incident",
    refresh_policy="auto",
)
def _gold_qc_clinical_safety_incident():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_safety_incident", "safety_incident_key")


RULES = {
    'clinical_safety_incident': GoldRules(
        table='clinical_safety_incident',
        mandatory=CLINICAL_SAFETY_INCIDENT_MANDATORY_RULES,
        advisory=CLINICAL_SAFETY_INCIDENT_ADVISORY_RULES),
}
_rules = QcBinding(RULES)
_tdx_product = _rules._tdx_product
_tdx_qc = _rules._tdx_qc


finalise()


COMMENTS = {'_gold_qc_clinical_safety_incident': {'columns': {'qc_checked_at': 'loaded_at of the admitted row (the QC '
                                                                    'evidence time).',
                                                   'qc_status': 'passed for every admitted row; advisory '
                                                                'outcomes are in Lakeflow expectation '
                                                                'metrics.',
                                                   'safety_incident_key': 'Key of the admitted '
                                                                          'clinical_safety_incident row.'},
                                       'quantity_override': False,
                                       'table': 'Compatibility evidence of admitted clinical_safety_incident '
                                                'rows; advisory outcomes are in Lakeflow expectation '
                                                'metrics.'},
 'clinical_safety_incident': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                          'affected_patient_count': 'Count of patients recorded as affected '
                                                                    'by the clinical safety incident.',
                                          'context_fingerprint_current': "SHA-256 of the incident's current "
                                                                         'identifying context (names, birth '
                                                                         'date, postcode, reporter, '
                                                                         'investigator and participants); '
                                                                         'anonymised narrative text is '
                                                                         'released only while its '
                                                                         'anon_context_fingerprint matches '
                                                                         'this value.',
                                          'encounter_id': 'Millennium ENCNTR_ID of the encounter the row '
                                                          'belongs to (BIGINT); NULL when not recorded.',
                                          'encounter_link_confidence': 'HIGH when the linked encounter has '
                                                                       'HIGH arrival and departure '
                                                                       'confidence; LOW when an inpatient/ED '
                                                                       'departure was unresolved and bounded '
                                                                       'at arrival + 1 day; else MEDIUM.',
                                          'encounter_link_method': 'Method by which the clinical safety '
                                                                   'incident was linked to an encounter '
                                                                   'record.',
                                          'encounter_link_rule': 'Rule that set encounter_id (TDX-E2 '
                                                                 'incident-time overlap with Millennium '
                                                                 'encounter *_BEST bounds).',
                                          'identity_status': 'resolved when person_id is present, unresolved '
                                                             'when absent, not_applicable for tables without '
                                                             'a patient.',
                                          'inc_acctype': 'Type of accident recorded for the incident.',
                                          'inc_bodypart': 'Part of the body affected or injured in the '
                                                          'incident.',
                                          'inc_carestage': 'Code for the stage of care at which the incident '
                                                           'occurred, as recorded in Datix.',
                                          'inc_category': 'Coded category of the incident.',
                                          'inc_category_display': 'Descriptive label of the Datix incident '
                                                                  'category associated with the code.',
                                          'inc_cause': 'Recorded cause of the incident in Datix.',
                                          'inc_clingroup': 'Code for the clinical group or division '
                                                           'associated with the incident in Datix.',
                                          'inc_clinoutcome': 'Clinical outcome recorded for the incident in '
                                                             'Datix.',
                                          'inc_clinoutcome2': 'Secondary clinical outcome recorded for the '
                                                              'incident in Datix.',
                                          'inc_clintype': 'Code for the clinical incident type recorded in '
                                                          'Datix.',
                                          'inc_consequence': 'Coded consequence score used in the risk '
                                                             'grading of the incident.',
                                          'inc_directorate': 'Code for the directorate associated with the '
                                                             'incident.',
                                          'inc_dob': 'Date of birth of the person involved in the incident.',
                                          'inc_ethnicity': 'Ethnicity of the person affected by the '
                                                           'incident, as recorded in Datix.',
                                          'inc_further_inv': 'Datix incident field recording whether further '
                                                             'investigation of the incident is required.',
                                          'inc_gender': 'Gender of the person involved in the incident as '
                                                        'recorded in Datix.',
                                          'inc_grade': 'Overall risk grade of the incident derived from '
                                                       'consequence and likelihood.',
                                          'inc_grade_display': 'Descriptive label for the Datix incident '
                                                               'grade code.',
                                          'inc_head': 'Code identifying the head or senior manager '
                                                      'associated with the incident record in Datix.',
                                          'inc_impact': 'Impact classification of the incident.',
                                          'inc_injury': 'Type or nature of injury sustained in the incident.',
                                          'inc_investigator': 'Person assigned to investigate the incident, '
                                                              'as recorded in Datix.',
                                          'inc_is_riddor': 'Flag indicating whether the incident is '
                                                           'reportable under RIDDOR (Reporting of Injuries, '
                                                           'Diseases and Dangerous Occurrences Regulations).',
                                          'inc_likelihood': 'Coded likelihood of recurrence used in the risk '
                                                            'grading of the incident.',
                                          'inc_locactual': 'Code for the specific location where the '
                                                           'incident occurred.',
                                          'inc_location': 'Current location of the equipment involved in the '
                                                          'incident.',
                                          'inc_loctype': 'Coded type of location where the incident '
                                                         'occurred.',
                                          'inc_mgr': 'Code identifying the handler or manager responsible '
                                                     'for the incident record in Datix.',
                                          'inc_name': 'Name of the person affected by or associated with the '
                                                      'incident as recorded in Datix.',
                                          'inc_notify': 'Notification details recorded for the incident, '
                                                        'such as who was notified.',
                                          'inc_nspecialty': 'Specialty recorded for the incident, likely an '
                                                            'alternative or national specialty '
                                                            'classification in Datix.',
                                          'inc_organisation': 'Code for the organisation in which the '
                                                              'incident occurred, as recorded in Datix.',
                                          'inc_ourref': 'Local incident reference number assigned by the '
                                                        'organisation in Datix.',
                                          'inc_outcomecode': 'Code recording the outcome of the incident.',
                                          'inc_pat_type': 'Type of patient involved in the incident, as '
                                                          'recorded in Datix.',
                                          'inc_postcode': 'Postcode recorded against the incident.',
                                          'inc_rating': 'Numeric risk rating of the incident derived from '
                                                        'risk grading.',
                                          'inc_rc_required': 'Datix incident field recording whether a root '
                                                             'cause analysis is required for the incident.',
                                          'inc_repname': 'Name of the person who reported the incident.',
                                          'inc_reportedby': 'Role or staff category of the person who '
                                                            'reported the incident, as recorded in Datix.',
                                          'inc_result': 'Recorded result or outcome of the incident in '
                                                        'Datix.',
                                          'inc_riddor': 'RIDDOR-related classification recorded for the '
                                                        'incident.',
                                          'inc_riddorno': 'RIDDOR reference number assigned to the incident '
                                                          'report.',
                                          'inc_ridloc': 'RIDDOR location classification for where the '
                                                        'incident occurred.',
                                          'inc_severity': 'Coded severity or harm level assigned to the '
                                                          'incident.',
                                          'inc_severity_display': 'Descriptive label for the incident '
                                                                  'severity code shown to Datix users.',
                                          'inc_specialty': 'Code for the clinical specialty associated with '
                                                           'the incident.',
                                          'inc_subcategory': 'Coded subcategory of the incident within its '
                                                             'category.',
                                          'inc_subcategory_display': 'Descriptive label for the Datix '
                                                                     'incident subcategory code.',
                                          'inc_treatment': 'Treatment received as a result of the incident.',
                                          'inc_type': 'Coded incident type indicating who or what was '
                                                      'affected, such as patient or staff.',
                                          'inc_type_display': 'Descriptive label for the Datix incident type '
                                                              'code.',
                                          'inc_unit': 'Code for the organisational unit or site where the '
                                                      'incident occurred.',
                                          'inc_unit_type': 'Code recording the type of unit associated with '
                                                           'the Datix incident.',
                                          'incident_dt_tm': 'Date and time the clinical safety incident '
                                                            'occurred, derived from the Datix incident date '
                                                            'and incident time fields.',
                                          'incident_dt_tm_clean': 'Cleaned version of the date and time the '
                                                                  'clinical safety incident occurred, '
                                                                  'derived from the Datix incident date and '
                                                                  'incident time fields.',
                                          'incident_id': 'Datix record identifier for the clinical safety '
                                                         'incident.',
                                          'incident_time_valid_ind': 'Incident time was recorded (an undated '
                                                                     'incident time spans the whole incident '
                                                                     'day in TDX-E2).',
                                          'investigation_complete_dt_tm': 'Date and time the investigation '
                                                                          'of the clinical safety incident '
                                                                          'was completed, sourced from '
                                                                          'Datix.',
                                          'investigation_start_dt_tm': 'Date and time the investigation of '
                                                                       'the clinical safety incident was '
                                                                       'started, sourced from Datix.',
                                          'load_batch_id': 'Load batch label derived from loaded_at '
                                                           '(yyyyMMddHHmmss); tdx-v21 when no load time '
                                                           'exists.',
                                          'loaded_at': 'When the row was last written to bronze (ADC_UPDT), '
                                                       'else its source update time; the stable load clock.',
                                          'notified_dt_tm': 'Date and time of notification recorded against '
                                                            'the clinical safety incident in Datix, '
                                                            'populated for very few records.',
                                          'opened_dt_tm': 'Date and time the clinical safety incident record '
                                                          'was opened in Datix.',
                                          'person_id': 'Millennium PERSON_ID of the patient (identifier); '
                                                       'NULL when the source row names no patient.',
                                          'person_link_method': 'Method by which the clinical safety '
                                                                'incident was linked to a person record.',
                                          'pipeline_run_id': 'Bronze pipeline run that last wrote the row.',
                                          'record_status': 'active, or superseded when the row stopped '
                                                           'appearing in the source snapshot '
                                                           '(source_present_ind false).',
                                          'record_status_effective_from': 'Start of the record status; not '
                                                                          'supplied by this source (always '
                                                                          'NULL).',
                                          'record_status_effective_to': 'When a superseded row was first '
                                                                        'detected absent from the source '
                                                                        '(source_absent_detected_ts); NULL '
                                                                        'while active.',
                                          'rep_approved': 'Approval status code of the incident record '
                                                          'within the Datix review workflow.',
                                          'reported_dt_tm': 'Date and time the clinical safety incident was '
                                                            'reported, sourced from Datix.',
                                          'row_hash': "SHA-256 of the row's payload columns, used by the "
                                                      'bronze MERGE to detect changes.',
                                          'safety_incident_key': 'Deterministic SHA-256 key of the safety '
                                                                 'incident row (namespace plus native source '
                                                                 'key).',
                                          'secgroup': 'Security group associated with the incident record '
                                                      'for Datix access control.',
                                          'secgroup_display': 'Descriptive text for the Datix security group '
                                                              'code.',
                                          'seclevel': 'Security level assigned to the incident record for '
                                                      'Datix access control.',
                                          'source_absent_detected_ts': 'When bronze first found the row '
                                                                       'absent from the source snapshot; '
                                                                       'NULL while present.',
                                          'source_adc_updt': 'When the source row was landed in raw (raw '
                                                             'ADC_UPDT).',
                                          'source_present_ind': 'True while the row is present in the latest '
                                                                'full source snapshot.',
                                          'source_row_id': 'Native source row identifier, as text.',
                                          'source_table': 'Bronze or raw source table the row came from.',
                                          'source_update_timestamp': 'Time the source row was landed in the '
                                                                     'raw layer (bronze SOURCE_ADC_UPDT), '
                                                                     'not when it changed in the source '
                                                                     'system; do not use it for recency.',
                                          'source_updt_dt_tm': 'Last-update time of the row in the source '
                                                               'system (for Datix, updateddate); NULL when '
                                                               'the source supplies none.',
                                          'subject_id_system': 'System of subject_key: urn:cerner:person_id, '
                                                               'or nosubject when the row names no patient.',
                                          'subject_key': 'Deterministic subject key: hash of the Cerner '
                                                         'person id, or of the source row when no person is '
                                                         'recorded.'},
                              'quantity_override': False,
                              'table': 'One row per Datix patient-safety incident (incident reports, not '
                                       'incidence rates or proven causality), keyed by Datix RECORDID. Gold '
                                       'QC twin of silver clinical_safety_incident: mandatory rules drop '
                                       'rows, advisory rules are reported in Lakeflow expectation metrics.'}}
