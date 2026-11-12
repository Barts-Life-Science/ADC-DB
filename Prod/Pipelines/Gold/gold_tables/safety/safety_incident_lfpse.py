"""Gold safety.safety_incident_lfpse: 2 dataset(s).

- datasets: clinical_safety_incident_lfpse, _gold_qc_clinical_safety_incident_lfpse
- moved verbatim from: 8_gold_patient_safety:204, 8_gold_patient_safety:208, 8_gold_patient_safety:218, 8_gold_patient_safety:235

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import INTERNAL_SCHEMA, PUBLIC_SCHEMA
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== clinical_safety_incident_lfpse ====
CLINICAL_SAFETY_INCIDENT_LFPSE_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_safety_incident_lfpse.active_record": "COALESCE(record_status = 'active', FALSE)",
}


CLINICAL_SAFETY_INCIDENT_LFPSE_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.safety_incident_lfpse.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_safety_incident_lfpse.key_present": "safety_incident_lfpse_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_safety_incident_lfpse.load_time_present": "loaded_at IS NOT NULL",
}


@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_safety_incident_lfpse",
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_SAFETY_INCIDENT_LFPSE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_SAFETY_INCIDENT_LFPSE_ADVISORY_RULES)
def gold_clinical_safety_incident_lfpse():
    """Publish clinical_safety_incident_lfpse; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_safety_incident_lfpse")


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_safety_incident_lfpse",
    refresh_policy="auto",
)
def _gold_qc_clinical_safety_incident_lfpse():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_safety_incident_lfpse", "safety_incident_lfpse_key")


RULES = {
    'clinical_safety_incident_lfpse': GoldRules(
        table='clinical_safety_incident_lfpse',
        mandatory=CLINICAL_SAFETY_INCIDENT_LFPSE_MANDATORY_RULES,
        advisory=CLINICAL_SAFETY_INCIDENT_LFPSE_ADVISORY_RULES),
}
_rules = QcBinding(RULES)
_tdx_product = _rules._tdx_product
_tdx_qc = _rules._tdx_qc


finalise()


COMMENTS = {'_gold_qc_clinical_safety_incident_lfpse': {'columns': {'qc_checked_at': 'loaded_at of the admitted row '
                                                                          '(the QC evidence time).',
                                                         'qc_status': 'passed for every admitted row; '
                                                                      'advisory outcomes are in Lakeflow '
                                                                      'expectation metrics.',
                                                         'safety_incident_lfpse_key': 'Key of the admitted '
                                                                                      'clinical_safety_incident_lfpse '
                                                                                      'row.'},
                                             'quantity_override': False,
                                             'table': 'Compatibility evidence of admitted '
                                                      'clinical_safety_incident_lfpse rows; advisory '
                                                      'outcomes are in Lakeflow expectation metrics.'},
 'clinical_safety_incident_lfpse': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                                'con_id': 'Identifier of the Datix contact record that this '
                                                          'LFPSE row relates to, where applicable.',
                                                'identity_status': 'resolved when person_id is present, '
                                                                   'unresolved when absent, not_applicable '
                                                                   'for tables without a patient.',
                                                'inc_id': 'Identifier of the Datix incident record that this '
                                                          'LFPSE row relates to.',
                                                'lfpse_blood_involved': 'LFPSE response recording the blood '
                                                                        'involved in the reported event.',
                                                'lfpse_blood_problem': 'LFPSE response describing the '
                                                                       'problem with blood involved in the '
                                                                       'reported event.',
                                                'lfpse_blood_products_batch': 'LFPSE response recording the '
                                                                              'batch of blood products '
                                                                              'involved in the reported '
                                                                              'event.',
                                                'lfpse_blood_products_brand': 'LFPSE response recording the '
                                                                              'brand of blood products '
                                                                              'involved in the reported '
                                                                              'event.',
                                                'lfpse_blood_products_involved': 'LFPSE response recording '
                                                                                 'the blood products '
                                                                                 'involved in the reported '
                                                                                 'event.',
                                                'lfpse_blood_products_not_used': 'LFPSE response relating to '
                                                                                 'blood products not being '
                                                                                 'used when they should have '
                                                                                 'been in the reported '
                                                                                 'event.',
                                                'lfpse_blood_products_problem': 'LFPSE response describing '
                                                                                'the problem with blood '
                                                                                'products involved in the '
                                                                                'reported event.',
                                                'lfpse_blood_was_not_used': 'LFPSE response relating to '
                                                                            'blood not being used when it '
                                                                            'should have been in the '
                                                                            'reported event.',
                                                'lfpse_buildings_infrastructure': 'LFPSE response recording '
                                                                                  'the buildings or '
                                                                                  'infrastructure involved '
                                                                                  'in the reported event.',
                                                'lfpse_buildings_infrastructure_involved': 'LFPSE response '
                                                                                           'indicating '
                                                                                           'whether '
                                                                                           'buildings or '
                                                                                           'infrastructure '
                                                                                           'were involved in '
                                                                                           'the reported '
                                                                                           'event.',
                                                'lfpse_buildings_infrastructure_not_used': 'LFPSE response '
                                                                                           'relating to '
                                                                                           'buildings or '
                                                                                           'infrastructure '
                                                                                           'not being used '
                                                                                           'when they should '
                                                                                           'have been in the '
                                                                                           'reported event.',
                                                'lfpse_buildings_infrastructure_problem': 'LFPSE response '
                                                                                          'describing the '
                                                                                          'problem with '
                                                                                          'buildings or '
                                                                                          'infrastructure '
                                                                                          'involved in the '
                                                                                          'reported event.',
                                                'lfpse_cqc_criteria': 'LFPSE response recording the Care '
                                                                      'Quality Commission notification '
                                                                      'criteria applicable to the reported '
                                                                      'event.',
                                                'lfpse_cqc_notify': 'LFPSE response indicating whether the '
                                                                    'Care Quality Commission should be '
                                                                    'notified of the reported event.',
                                                'lfpse_damaged_blood': 'LFPSE response relating to damaged '
                                                                       'blood involved in the reported '
                                                                       'event.',
                                                'lfpse_damaged_blood_products': 'LFPSE response relating to '
                                                                                'damaged blood products '
                                                                                'involved in the reported '
                                                                                'event.',
                                                'lfpse_data_sharing_opt_out': 'LFPSE response relating to '
                                                                              'data-sharing opt-out for the '
                                                                              'reported event.',
                                                'lfpse_date': 'Date recorded for the LFPSE event.',
                                                'lfpse_deprivation_of_liberty': 'LFPSE response relating to '
                                                                                'deprivation of liberty for '
                                                                                'the reported event.',
                                                'lfpse_designations_mental_health': 'LFPSE field recording '
                                                                                    'mental health '
                                                                                    'designations relevant '
                                                                                    'to the reported event.',
                                                'lfpse_detection_point': 'LFPSE field recording the point at '
                                                                         'which the event was detected.',
                                                'lfpse_device_involvement_factors': 'LFPSE field recording '
                                                                                    'the factors describing '
                                                                                    'how a device was '
                                                                                    'involved in the event.',
                                                'lfpse_device_type': 'LFPSE field recording the type of '
                                                                     'device involved in the event.',
                                                'lfpse_device_used_unnecessarily': 'LFPSE field giving '
                                                                                   'details where a device '
                                                                                   'was used unnecessarily '
                                                                                   'in the event.',
                                                'lfpse_drug_involvement_factors': 'LFPSE field recording the '
                                                                                  'factors describing how '
                                                                                  'medication was involved '
                                                                                  'in the event.',
                                                'lfpse_drug_reaction': 'LFPSE field recording a drug '
                                                                       'reaction associated with the event.',
                                                'lfpse_drug_used_too_much': 'LFPSE field giving details '
                                                                            'where too much of a medication '
                                                                            'was involved in the event.',
                                                'lfpse_drugs_involved': 'LFPSE field recording the '
                                                                        'medications involved in the event.',
                                                'lfpse_duty_of_candour': 'LFPSE field recording whether duty '
                                                                         'of candour applies to the event.',
                                                'lfpse_estates_services': 'LFPSE field recording estates '
                                                                          'services relevant to the event.',
                                                'lfpse_estates_services_involved': 'LFPSE field recording '
                                                                                   'whether or which estates '
                                                                                   'services were involved '
                                                                                   'in the event.',
                                                'lfpse_estates_services_not_used': 'LFPSE field giving '
                                                                                   'details where estates '
                                                                                   'services were not used '
                                                                                   'in relation to the '
                                                                                   'event.',
                                                'lfpse_estates_services_problem': 'LFPSE field describing '
                                                                                  'the problem with estates '
                                                                                  'services involved in the '
                                                                                  'event.',
                                                'lfpse_estimated_time': 'LFPSE field recording the estimated '
                                                                        'time of the event, stored as a '
                                                                        'string.',
                                                'lfpse_from_online_forms': 'LFPSE field indicating whether '
                                                                           'the record originated from Datix '
                                                                           'online forms.',
                                                'lfpse_good_care_detection_factor': 'LFPSE field recording '
                                                                                    'the factor or factors '
                                                                                    'through which good care '
                                                                                    'was detected in the '
                                                                                    'reported event.',
                                                'lfpse_hsib_notify': 'LFPSE field indicating whether the '
                                                                     'event is notifiable to the Healthcare '
                                                                     'Safety Investigation Branch (HSIB).',
                                                'lfpse_id': 'Record identifier for the safety incident '
                                                            'record from the Learn From Patient Safety '
                                                            'Events (LFPSE) dataset, sourced from the Datix '
                                                            'LFPSE record ID.',
                                                'lfpse_identified_location': 'LFPSE field recording the '
                                                                             'location identified as where '
                                                                             'the reported event occurred.',
                                                'lfpse_incident_occurred_today': 'LFPSE field indicating '
                                                                                 'whether the reported '
                                                                                 'incident occurred on the '
                                                                                 'day of reporting.',
                                                'lfpse_involved_agents': 'LFPSE field recording the agents '
                                                                         'involved in the event, such as '
                                                                         'medication, devices, blood '
                                                                         'products, tissues and organs, '
                                                                         'estates, IT systems or people.',
                                                'lfpse_involved_persons_actions': 'LFPSE field recording the '
                                                                                  'actions of people '
                                                                                  'involved that contributed '
                                                                                  'to the reported event.',
                                                'lfpse_involved_processes': 'LFPSE field recording the care '
                                                                            'processes involved in the '
                                                                            'reported event.',
                                                'lfpse_is_anonymous': 'LFPSE field indicating whether the '
                                                                      'report was submitted anonymously.',
                                                'lfpse_it_systems_involvement_factors': 'LFPSE field '
                                                                                        'recording how IT '
                                                                                        'systems or software '
                                                                                        'were involved in '
                                                                                        'the reported event.',
                                                'lfpse_level_of_concern': 'LFPSE field recording the '
                                                                          "reporter's level of concern about "
                                                                          'the reported event or risk.',
                                                'lfpse_local_authority_safeguarding': 'LFPSE field '
                                                                                      'indicating whether '
                                                                                      'the event involves or '
                                                                                      'was referred for '
                                                                                      'local authority '
                                                                                      'safeguarding.',
                                                'lfpse_location_at_risk': 'LFPSE field recording the '
                                                                          'location affected by an '
                                                                          'identified risk.',
                                                'lfpse_location_known': 'LFPSE field indicating whether the '
                                                                        'location of the reported event is '
                                                                        'known.',
                                                'lfpse_location_within_service': 'LFPSE field recording the '
                                                                                 'location within the '
                                                                                 'service where the reported '
                                                                                 'event occurred.',
                                                'lfpse_manufacturer': 'LFPSE field recording the '
                                                                      'manufacturer of a product, such as a '
                                                                      'device or medication, involved in the '
                                                                      'reported event.',
                                                'lfpse_marvin_reference_number': 'LFPSE field holding an '
                                                                                 'external MARVIN reference '
                                                                                 'number associated with the '
                                                                                 'reported event.',
                                                'lfpse_medication_administered_incorrectly': 'LFPSE field '
                                                                                             'recording '
                                                                                             'details of '
                                                                                             'medication '
                                                                                             'administered '
                                                                                             'incorrectly in '
                                                                                             'the reported '
                                                                                             'event.',
                                                'lfpse_medication_administrator': 'LFPSE field recording who '
                                                                                  'administered the '
                                                                                  'medication involved in '
                                                                                  'the reported event.',
                                                'lfpse_model': 'LFPSE field recording the model of a device '
                                                               'or product involved in the reported event.',
                                                'lfpse_never_event': 'LFPSE field recorded in Datix '
                                                                     'indicating whether the patient safety '
                                                                     'event was considered a Never Event.',
                                                'lfpse_never_event_type': 'LFPSE field recorded in Datix '
                                                                          'giving the type or category of '
                                                                          'Never Event associated with the '
                                                                          'patient safety event.',
                                                'lfpse_nhsbt_report_number': 'External reference number of a '
                                                                             'related report made to NHS '
                                                                             'Blood and Transplant (NHSBT), '
                                                                             'recorded for the LFPSE event.',
                                                'lfpse_organisation': 'LFPSE field recording the '
                                                                      'organisation associated with the '
                                                                      'reported patient safety event.',
                                                'lfpse_outcome_type': 'LFPSE field recording the type of '
                                                                      'outcome of the reported patient '
                                                                      'safety event.',
                                                'lfpse_patient_event': 'LFPSE field indicating or describing '
                                                                       'whether the recorded event is a '
                                                                       'patient event.',
                                                'lfpse_patient_safety_incident_has_occurred': 'LFPSE field '
                                                                                              'indicating '
                                                                                              'whether a '
                                                                                              'patient '
                                                                                              'safety '
                                                                                              'incident has '
                                                                                              'occurred, as '
                                                                                              'opposed to a '
                                                                                              'risk or other '
                                                                                              'event type.',
                                                'lfpse_people_action_factors': 'LFPSE field recording the '
                                                                               "factors relating to people's "
                                                                               'actions that were involved '
                                                                               'in the event.',
                                                'lfpse_people_action_too_much': 'LFPSE field recording '
                                                                                'details where people '
                                                                                'involved in the event took '
                                                                                'too much of an action.',
                                                'lfpse_people_involvement_factor': 'LFPSE field recording '
                                                                                   'how people were involved '
                                                                                   'as a factor in the '
                                                                                   'event.',
                                                'lfpse_precise_time': 'LFPSE field recording the precise '
                                                                      'time at which the event occurred, '
                                                                      'stored as text.',
                                                'lfpse_problem_medication_packaging': 'LFPSE field recording '
                                                                                      'problems with '
                                                                                      'medication packaging '
                                                                                      'involved in the '
                                                                                      'event.',
                                                'lfpse_radiotherapy_incident_code': 'LFPSE field recording '
                                                                                    'the radiotherapy '
                                                                                    'incident classification '
                                                                                    'code for the event.',
                                                'lfpse_record_lvl_one': 'LFPSE level-one record field held '
                                                                        'on the Datix LFPSE record.',
                                                'lfpse_record_lvl_two': 'LFPSE level-two record field held '
                                                                        'on the Datix LFPSE record.',
                                                'lfpse_reference_number': 'LFPSE reference number assigned '
                                                                          'to the reported event.',
                                                'lfpse_reporter_involvement': 'LFPSE field recording how the '
                                                                              'reporter was involved in the '
                                                                              'event.',
                                                'lfpse_reporter_organisation': 'Organisation of the person '
                                                                               'reporting the LFPSE event as '
                                                                               'captured in Datix.',
                                                'lfpse_reporter_role': 'Role of the person reporting the '
                                                                       'LFPSE event as captured in Datix.',
                                                'lfpse_reporter_type': 'Type of reporter submitting the '
                                                                       'LFPSE event record as captured in '
                                                                       'Datix.',
                                                'lfpse_responsible_specialty': 'Clinical specialty '
                                                                               'responsible for the care '
                                                                               'associated with the LFPSE '
                                                                               'event.',
                                                'lfpse_risk_identified_location': 'Location where the '
                                                                                  'patient safety risk was '
                                                                                  'identified for an LFPSE '
                                                                                  'risk record.',
                                                'lfpse_risk_imminent': 'Indicator of whether the recorded '
                                                                       'patient safety risk is imminent.',
                                                'lfpse_risk_population': 'Population or patient group '
                                                                         'potentially affected by the '
                                                                         'recorded patient safety risk.',
                                                'lfpse_risk_service_area': 'Service area associated with the '
                                                                           'recorded patient safety risk.',
                                                'lfpse_risk_theme': 'Theme or category assigned to the '
                                                                    'recorded patient safety risk.',
                                                'lfpse_risk_timeframe': 'Timeframe within which the recorded '
                                                                        'patient safety risk is expected to '
                                                                        'materialise.',
                                                'lfpse_sabre_report_number': 'External SABRE report '
                                                                             'reference number linked to the '
                                                                             'LFPSE event.',
                                                'lfpse_safety_challenges': 'Safety challenges recorded as '
                                                                           'relevant to the LFPSE event.',
                                                'lfpse_serious_incident': 'Indicator of whether the LFPSE '
                                                                          'event is classed as a serious '
                                                                          'incident.',
                                                'lfpse_service_area': 'Service area in which the LFPSE event '
                                                                      'occurred as captured in Datix.',
                                                'lfpse_shot_report_number': 'External SHOT haemovigilance '
                                                                            'report reference number linked '
                                                                            'to the LFPSE event.',
                                                'lfpse_tissue_organs_involvement_factor': 'Factor describing '
                                                                                          'how tissues or '
                                                                                          'organs were '
                                                                                          'involved in the '
                                                                                          'LFPSE event.',
                                                'lfpse_tissue_organs_not_used': 'Details recorded where '
                                                                                'tissues or organs were not '
                                                                                'used when they should have '
                                                                                'been in the LFPSE event.',
                                                'lfpse_tissue_organs_used_too_much': 'Details recorded where '
                                                                                     'too much tissue or '
                                                                                     'organ material was '
                                                                                     'used in the LFPSE '
                                                                                     'event.',
                                                'lfpse_too_little_blood_products': 'LFPSE reporting field '
                                                                                   'capturing information on '
                                                                                   'too little of a blood '
                                                                                   'product being used in '
                                                                                   'the recorded event.',
                                                'lfpse_too_little_blood_used': 'LFPSE reporting field '
                                                                               'capturing information on too '
                                                                               'little blood being used in '
                                                                               'the recorded event.',
                                                'lfpse_too_much_blood_products': 'LFPSE reporting field '
                                                                                 'capturing information on '
                                                                                 'too much of a blood '
                                                                                 'product being used in the '
                                                                                 'recorded event.',
                                                'lfpse_too_much_blood_used': 'LFPSE reporting field '
                                                                             'capturing information on too '
                                                                             'much blood being used in the '
                                                                             'recorded event.',
                                                'lfpse_type': 'LFPSE event type recorded for the Datix '
                                                              'record.',
                                                'lfpse_wrong_blood': 'LFPSE reporting field capturing '
                                                                     'information on the wrong blood being '
                                                                     'involved in the recorded event.',
                                                'lfpse_wrong_blood_products': 'LFPSE reporting field '
                                                                              'capturing information on the '
                                                                              'wrong blood product being '
                                                                              'involved in the recorded '
                                                                              'event.',
                                                'lfpse_wrong_buildings_infrastructure': 'LFPSE reporting '
                                                                                        'field capturing '
                                                                                        'information on '
                                                                                        'wrong buildings or '
                                                                                        'infrastructure '
                                                                                        'involved in the '
                                                                                        'recorded event.',
                                                'lfpse_wrong_estates_services': 'LFPSE reporting field '
                                                                                'capturing information on '
                                                                                'wrong estates services '
                                                                                'involved in the recorded '
                                                                                'event.',
                                                'lfpse_yellow_card_reference': 'External Yellow Card '
                                                                               'reference number associated '
                                                                               'with the LFPSE record.',
                                                'load_batch_id': 'Load batch label derived from loaded_at '
                                                                 '(yyyyMMddHHmmss); tdx-v21 when no load '
                                                                 'time exists.',
                                                'loaded_at': 'When the row was last written to bronze '
                                                             '(ADC_UPDT), else its source update time; the '
                                                             'stable load clock.',
                                                'med_id': 'Identifier of the Datix medication record that '
                                                          'this LFPSE row relates to, where applicable.',
                                                'pipeline_run_id': 'Bronze pipeline run that last wrote the '
                                                                   'row.',
                                                'record_status': 'active, or superseded when the row stopped '
                                                                 'appearing in the source snapshot '
                                                                 '(source_present_ind false).',
                                                'record_status_effective_from': 'Start of the record status; '
                                                                                'not supplied by this source '
                                                                                '(always NULL).',
                                                'record_status_effective_to': 'When a superseded row was '
                                                                              'first detected absent from '
                                                                              'the source '
                                                                              '(source_absent_detected_ts); '
                                                                              'NULL while active.',
                                                'row_hash': "SHA-256 of the row's payload columns, used by "
                                                            'the bronze MERGE to detect changes.',
                                                'safety_incident_lfpse_key': 'Deterministic SHA-256 key of '
                                                                             'the safety incident lfpse row '
                                                                             '(namespace plus native source '
                                                                             'key).',
                                                'source_absent_detected_ts': 'When bronze first found the '
                                                                             'row absent from the source '
                                                                             'snapshot; NULL while present.',
                                                'source_adc_updt': 'When the source row was landed in raw '
                                                                   '(raw ADC_UPDT).',
                                                'source_present_ind': 'True while the row is present in the '
                                                                      'latest full source snapshot.',
                                                'source_row_id': 'Native source row identifier, as text.',
                                                'source_table': 'Bronze or raw source table the row came '
                                                                'from.',
                                                'source_update_timestamp': 'Time the source row was landed '
                                                                           'in the raw layer (bronze '
                                                                           'SOURCE_ADC_UPDT), not when it '
                                                                           'changed in the source system; do '
                                                                           'not use it for recency.',
                                                'source_updt_dt_tm': 'Last-update time of the row in the '
                                                                     'source system (for Datix, '
                                                                     'updateddate); NULL when the source '
                                                                     'supplies none.'},
                                    'quantity_override': False,
                                    'table': 'One row per LFPSE (Learn from Patient Safety Events) '
                                             'submission linked to a Datix incident, keyed by RECORDID. Gold '
                                             'QC twin of silver clinical_safety_incident_lfpse: mandatory '
                                             'rules drop rows, advisory rules are reported in Lakeflow '
                                             'expectation metrics. Some rows are stub LFPSE submissions that '
                                             'carry keys but no event content (no LFPSE date); filter on '
                                             'lfpse_date for analysis.'}}
