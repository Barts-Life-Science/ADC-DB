"""Gold specialist.registry_acs_transfer: 2 dataset(s).

- datasets: _qc_clinical_registry_acs_transfer, clinical_registry_acs_transfer
- moved verbatim from: 3_gold_specialist_care:2876, 3_gold_specialist_care:3071, 3_gold_specialist_care:3277, 3_gold_specialist_care:3283, 3_gold_specialist_care:3298

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.registry_acs_transfer ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_registry_entry applies the parent Gold drop rule.
CLINICAL_REGISTRY_ACS_TRANSFER_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entry_id` AS `entry_id`',
    '`mrn` AS `mrn`',
    '`gender` AS `gender`',
    '`date_of_death` AS `date_of_death`',
    '`ethnic_origin` AS `ethnic_origin`',
    '`admissiondate` AS `admissiondate`',
    '`patient_admin_status` AS `patient_admin_status`',
    '`admission_method` AS `admission_method`',
    '`referring_hospitaltxt` AS `referring_hospitaltxt`',
    '`admission_ward` AS `admission_ward`',
    '`admitting_consultant` AS `admitting_consultant`',
    '`pathway` AS `pathway`',
    '`symptom_onset` AS `symptom_onset`',
    '`call_for_help` AS `call_for_help`',
    '`first_responder` AS `first_responder`',
    '`arrival_of_ambulance` AS `arrival_of_ambulance`',
    '`arrive_dgh` AS `arrive_dgh`',
    '`arrive_here` AS `arrive_here`',
    '`time_of_diagnostic_ecg` AS `time_of_diagnostic_ecg`',
    '`angio_table_time` AS `angio_table_time`',
    '`reperfusion_date_time` AS `reperfusion_date_time`',
    '`local_intervention_date` AS `local_intervention_date`',
    '`referral_for_investigationintervention` AS `referral_for_investigationintervention`',
    '`critical_transfer_call` AS `critical_transfer_call`',
    '`time_left_dgh` AS `time_left_dgh`',
    '`ambulance_trust_codetxt` AS `ambulance_trust_codetxt`',
    '`cad` AS `cad`',
    '`transfer_cad` AS `transfer_cad`',
    '`previous_ami` AS `previous_ami`',
    '`previous_angina` AS `previous_angina`',
    '`hypertension` AS `hypertension`',
    '`hypercholesterolaemia` AS `hypercholesterolaemia`',
    '`peripheral_vascular_disease` AS `peripheral_vascular_disease`',
    '`cerebrovascular_disease` AS `cerebrovascular_disease`',
    '`asthma_copd` AS `asthma_copd`',
    '`chronic_renal_failure` AS `chronic_renal_failure`',
    '`heart_failure` AS `heart_failure`',
    '`previous_pci` AS `previous_pci`',
    '`previous_cabg` AS `previous_cabg`',
    '`family_history_of_chd` AS `family_history_of_chd`',
    '`smoking_status` AS `smoking_status`',
    '`diabetes` AS `diabetes`',
    '`beta_blocker_use` AS `beta_blocker_use`',
    '`ac_eor_ar_buse` AS `ac_eor_ar_buse`',
    '`statin_use` AS `statin_use`',
    '`thienopyridine_inhibitor_use` AS `thienopyridine_inhibitor_use`',
    '`initial_diagnosis` AS `initial_diagnosis`',
    '`high_risk_nstemi` AS `high_risk_nstemi`',
    '`killip_class` AS `killip_class`',
    '`place_first12_lead_ec_gperformed` AS `place_first12_lead_ec_gperformed`',
    '`ec_gdetermining_treatment` AS `ec_gdetermining_treatment`',
    '`ec_gsubcategoriestxt` AS `ec_gsubcategoriestxt`',
    '`qr_sduration` AS `qr_sduration`',
    '`where_was_aspirin_given` AS `where_was_aspirin_given`',
    '`patient_location_at_time_of_stemi` AS `patient_location_at_time_of_stemi`',
    '`patient_ventilated` AS `patient_ventilated`',
    '`cardiogenic_shock` AS `cardiogenic_shock`',
    '`grac_escore` AS `grac_escore`',
    '`serum_glucose` AS `serum_glucose`',
    '`serum_cholesterol` AS `serum_cholesterol`',
    '`creatinine` AS `creatinine`',
    '`haemoglobin` AS `haemoglobin`',
    '`peak_ck` AS `peak_ck`',
    '`peak_troponin` AS `peak_troponin`',
    '`troponin_assay` AS `troponin_assay`',
    '`cardiac_enzymes_raised` AS `cardiac_enzymes_raised`',
    '`systolic_bp` AS `systolic_bp`',
    '`heart_rate` AS `heart_rate`',
    '`lvef` AS `lvef`',
    '`height` AS `height`',
    '`weight` AS `weight`',
    '`assessment_at_non_interventional_hospital` AS `assessment_at_non_interventional_hospital`',
    '`cardiological_care` AS `cardiological_care`',
    '`initial_reperfusion` AS `initial_reperfusion`',
    '`where_was_initial_reperfusion_treatment_given` AS `where_was_initial_reperfusion_treatment_given`',
    '`reason_reperfusion_treatment_not_given` AS `reason_reperfusion_treatment_not_given`',
    '`delay_before_treatment` AS `delay_before_treatment`',
    '`additional_reperfusion` AS `additional_reperfusion`',
    '`assessment_at_interventional_centre` AS `assessment_at_interventional_centre`',
    '`intended_reperfusion_procedure` AS `intended_reperfusion_procedure`',
    '`procedure_performed` AS `procedure_performed`',
    '`why_was_no_angiogram_performed` AS `why_was_no_angiogram_performed`',
    '`why_was_no_intervention_performed` AS `why_was_no_intervention_performed`',
    '`bleeding_complications` AS `bleeding_complications`',
    '`site_of_infarction` AS `site_of_infarction`',
    '`coronary_angiography` AS `coronary_angiography`',
    '`coronary_intervention` AS `coronary_intervention`',
    '`delay_to_performance_of_angiogram` AS `delay_to_performance_of_angiogram`',
    '`exercise_test` AS `exercise_test`',
    '`echocardiography` AS `echocardiography`',
    '`radionuclide_study` AS `radionuclide_study`',
    '`stress_echo` AS `stress_echo`',
    '`cardiac_arrest` AS `cardiac_arrest`',
    '`cardiac_arrest_location` AS `cardiac_arrest_location`',
    '`arrest_presenting_rhythm` AS `arrest_presenting_rhythm`',
    '`outcome_of_arrest` AS `outcome_of_arrest`',
    '`unfractionated_heparin` AS `unfractionated_heparin`',
    '`low_molecular_weight_heparin` AS `low_molecular_weight_heparin`',
    '`thienopyridine_platelet_inhibitor` AS `thienopyridine_platelet_inhibitor`',
    '`iv2b3a_agent` AS `iv2b3a_agent`',
    '`iv_beta_blocker` AS `iv_beta_blocker`',
    '`calcium_channel_blocker` AS `calcium_channel_blocker`',
    '`i_vnitrate` AS `i_vnitrate`',
    '`oral_nitrate` AS `oral_nitrate`',
    '`potassium_channel_modulator` AS `potassium_channel_modulator`',
    '`warfarin` AS `warfarin`',
    '`ac_ei_arb` AS `ac_ei_arb`',
    '`thiazide_diuretic` AS `thiazide_diuretic`',
    '`loop_diuretic` AS `loop_diuretic`',
    '`oral_beta_blocker` AS `oral_beta_blocker`',
    '`aldosterone_antagonist` AS `aldosterone_antagonist`',
    '`fondaparinux` AS `fondaparinux`',
    '`bivalirudin` AS `bivalirudin`',
    '`thrombolytic_drug` AS `thrombolytic_drug`',
    '`inpatient_management_of_hyperglycaemia_diabetes` AS `inpatient_management_of_hyperglycaemia_diabetes`',
    '`start_of_insulin_infusion` AS `start_of_insulin_infusion`',
    '`beta_blockerdischarge` AS `beta_blockerdischarge`',
    '`ace_ior_ar_bdischarge` AS `ace_ior_ar_bdischarge`',
    '`statin_discharge` AS `statin_discharge`',
    '`aspirin_discharge` AS `aspirin_discharge`',
    '`thienopyridine_inhibitor_discharge` AS `thienopyridine_inhibitor_discharge`',
    '`aldosterone_antagonist_discharge` AS `aldosterone_antagonist_discharge`',
    '`ticagrelor_discharge` AS `ticagrelor_discharge`',
    '`diabetic_therapy_at_discharge` AS `diabetic_therapy_at_discharge`',
    '`discharge_diagnosis` AS `discharge_diagnosis`',
    '`other_discharge_diagnosistxt` AS `other_discharge_diagnosistxt`',
    '`discharge_destination` AS `discharge_destination`',
    '`reinfarction` AS `reinfarction`',
    '`death_in_hospital` AS `death_in_hospital`',
    '`followed_up_by` AS `followed_up_by`',
    '`cardiac_rehabilitation` AS `cardiac_rehabilitation`',
    '`smoking_cessation_advice_given` AS `smoking_cessation_advice_given`',
    '`dietary_advice_given` AS `dietary_advice_given`',
    '`date_of_return_to_referring_hospital` AS `date_of_return_to_referring_hospital`',
    '`what_procedure_was_performed_at_the_interventional_hospit` AS `what_procedure_was_performed_at_the_interventional_hospit`',
    '`date_of_discharge` AS `date_of_discharge`',
    '`stress_precipitanttxt` AS `stress_precipitanttxt`',
    '`m_stress_precipitant_other` AS `m_stress_precipitant_other`',
    '`datetime_of_stressful_precipitant` AS `datetime_of_stressful_precipitant`',
    '`regional_lv_dysfunction_distribution` AS `regional_lv_dysfunction_distribution`',
    '`m_regional_lv_dysfunction_distribution_other` AS `m_regional_lv_dysfunction_distribution_other`',
    '`previous_history_of_takotsubo` AS `previous_history_of_takotsubo`',
    '`family_history_of_takotsubo` AS `family_history_of_takotsubo`',
    '`thyroid_disease` AS `thyroid_disease`',
    '`psychiatric_disease` AS `psychiatric_disease`',
    '`hormonal_status` AS `hormonal_status`',
    '`hr_tor_oral_contraceptive` AS `hr_tor_oral_contraceptive`',
    '`androgen_modifying_therapy` AS `androgen_modifying_therapy`',
    '`beta_adrenergic_receptor_agonists` AS `beta_adrenergic_receptor_agonists`',
    '`qtc_interval` AS `qtc_interval`',
    '`atrial_tachycardia_or_fibrillation` AS `atrial_tachycardia_or_fibrillation`',
    '`ventricular_tachycardia_or_fibrillation` AS `ventricular_tachycardia_or_fibrillation`',
    '`left_ventricular_apical_thrombus` AS `left_ventricular_apical_thrombus`',
    '`left_ventricular_outflow_tract_obstruction` AS `left_ventricular_outflow_tract_obstruction`',
    '`bystander_coronary_artery_disease` AS `bystander_coronary_artery_disease`',
    '`lvedp` AS `lvedp`',
    '`cm_rimaging` AS `cm_rimaging`',
    '`positive_inotropic_supporttxt` AS `positive_inotropic_supporttxt`',
    '`intraaortic_balloon_pump_support` AS `intraaortic_balloon_pump_support`',
    '`reminder` AS `reminder`',
    '`lved_pvalue` AS `lved_pvalue`',
    '`dataset_version` AS `dataset_version`',
    '`ambulance_job_number` AS `ambulance_job_number`',
    '`daycase_transfer_dt` AS `daycase_transfer_dt`',
    '`referral_dt_tm` AS `referral_dt_tm`',
    '`interventional_hospitaltxt` AS `interventional_hospitaltxt`',
    '`m_hac_team_audit_notes` AS `m_hac_team_audit_notes`',
    '`data_complete` AS `data_complete`',
    '`cm_rimaging_performed` AS `cm_rimaging_performed`',
    '`ticagrelor_use` AS `ticagrelor_use`',
    '`date_last_changed` AS `date_last_changed`',
    '`adc_updt` AS `adc_updt`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`linkage_status` AS `linkage_status`',
    '`person_id` AS `person_id`',
    '`linkage_method` AS `linkage_method`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
    '`admission_date_quality` AS `admission_date_quality`',
    '`admission_date_clean` AS `admission_date_clean`',
    '`source_table` AS `source_table`',
    '`source_record_key` AS `source_record_key`',
    '`row_hash` AS `row_hash`',
    '`pipeline_loaded_at` AS `pipeline_loaded_at`',
    '`is_current_in_source` AS `is_current_in_source`',
    '`source_present_ind` AS `source_present_ind`',
    '`source_absent_detected_ts` AS `source_absent_detected_ts`',
    '`surname` AS `surname`',
    '`forename` AS `forename`',
    '`nhs_number` AS `nhs_number`',
    '`date_of_birth` AS `date_of_birth`',
    '`hospital_identifier` AS `hospital_identifier`',
]


CLINICAL_REGISTRY_ACS_TRANSFER_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`peak_troponin_ng_l` AS `peak_troponin_ng_l`',
    '`unit_method` AS `unit_method`',
]


CLINICAL_REGISTRY_ACS_TRANSFER_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_acs_transfer.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_acs_transfer"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_ACS_TRANSFER_MANDATORY_RULES)
def _gold_qc_clinical_registry_acs_transfer():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_acs_transfer"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_ACS_TRANSFER_SELECT, "__gold_parent_present AS __gold_parent_present")
    return df


@dp.materialized_view(
    name=_n("gold_clinical.registry_acs_transfer"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_registry_acs_transfer():
    """Publish clinical_registry_acs_transfer without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_acs_transfer")).drop("__gold_parent_present")


finalise()


COMMENTS = {'_qc_clinical_registry_acs_transfer': {'columns': {'ac_ei_arb': 'Coded indicator of whether an ACE '
                                                                 'inhibitor or angiotensin receptor blocker '
                                                                 'was given during the acute admission '
                                                                 'episode.',
                                                    'ac_eor_ar_buse': 'Indicates whether the patient was '
                                                                      'taking an ACE inhibitor or '
                                                                      'angiotensin receptor blocker prior to '
                                                                      'the ACS admission.',
                                                    'ace_ior_ar_bdischarge': 'Coded indicator of whether an '
                                                                             'ACE inhibitor or angiotensin '
                                                                             'receptor blocker was '
                                                                             'prescribed at discharge.',
                                                    'adc_updt': 'Source system extract timestamp indicating '
                                                                'when the row was last updated in the '
                                                                'upstream feed.',
                                                    'additional_reperfusion': 'Coded record of any '
                                                                              'additional reperfusion '
                                                                              'treatment given after the '
                                                                              'initial reperfusion attempt.',
                                                    'admission_date_clean': 'Admission date with '
                                                                            'sentinel/future values nulled; '
                                                                            'raw ADMISSIONDATE retained.',
                                                    'admission_date_quality': 'VALID | SENTINEL (year '
                                                                              '<=2000) | FUTURE | MISSING.',
                                                    'admission_method': 'Coded method by which the patient '
                                                                        'was admitted for this ACS episode, '
                                                                        'as recorded in the source registry.',
                                                    'admission_ward': 'Ward or clinical area to which the '
                                                                      'patient was admitted, as recorded in '
                                                                      'the source registry.',
                                                    'admissiondate': 'Raw hospital admission date/time from '
                                                                     'the source record, which may contain '
                                                                     'sentinel or future values flagged by '
                                                                     'ADMISSION_DATE_QUALITY.',
                                                    'admitting_consultant': 'Category or identity of the '
                                                                            'consultant responsible for the '
                                                                            'admission, as recorded in the '
                                                                            'source registry.',
                                                    'aldosterone_antagonist': 'Coded indicator of whether an '
                                                                              'aldosterone antagonist was '
                                                                              'given during the acute '
                                                                              'admission episode.',
                                                    'aldosterone_antagonist_discharge': 'Coded indicator of '
                                                                                        'whether an '
                                                                                        'aldosterone '
                                                                                        'antagonist was '
                                                                                        'prescribed at '
                                                                                        'discharge.',
                                                    'ambulance_job_number': 'Ambulance service job or '
                                                                            'incident reference recorded for '
                                                                            "the patient's conveyance.",
                                                    'ambulance_trust_codetxt': 'Decoded text label of the '
                                                                               'ambulance trust involved in '
                                                                               "the patient's conveyance, "
                                                                               'following the txt suffix '
                                                                               'convention for lookup '
                                                                               'descriptions in this '
                                                                               'extract.',
                                                    'androgen_modifying_therapy': 'Source-coded indicator of '
                                                                                  'androgen-modifying '
                                                                                  'therapy use recorded in '
                                                                                  'the Takotsubo assessment.',
                                                    'angio_table_time': 'Date and time the patient was on '
                                                                        'the angiography table, as recorded '
                                                                        'in the source registry.',
                                                    'arrest_presenting_rhythm': 'Coded presenting cardiac '
                                                                                'rhythm at the time of the '
                                                                                'cardiac arrest.',
                                                    'arrival_of_ambulance': 'Date and time of ambulance '
                                                                            'arrival at the patient, as '
                                                                            'recorded in the source '
                                                                            'registry.',
                                                    'arrive_dgh': 'Date and time the patient arrived at the '
                                                                  'district general hospital, as recorded in '
                                                                  'the source registry.',
                                                    'arrive_here': 'Date and time the patient arrived at the '
                                                                   'reporting hospital, as recorded in the '
                                                                   'source registry.',
                                                    'aspirin_discharge': 'Coded indicator of whether aspirin '
                                                                         'was prescribed at discharge.',
                                                    'assessment_at_interventional_centre': 'Coded outcome of '
                                                                                           'clinical '
                                                                                           'assessment of '
                                                                                           'the patient at '
                                                                                           'the '
                                                                                           'interventional '
                                                                                           '(PCI) centre.',
                                                    'assessment_at_non_interventional_hospital': 'Categorical '
                                                                                                 'record of '
                                                                                                 'the '
                                                                                                 'assessment '
                                                                                                 'or care '
                                                                                                 'received '
                                                                                                 'at the '
                                                                                                 'referring '
                                                                                                 'non-interventional '
                                                                                                 'hospital '
                                                                                                 'before '
                                                                                                 'transfer.',
                                                    'asthma_copd': 'Indicates whether the patient has a '
                                                                   'recorded history of asthma or chronic '
                                                                   'obstructive pulmonary disease.',
                                                    'atrial_tachycardia_or_fibrillation': 'Coded source '
                                                                                          'indicator of '
                                                                                          'whether atrial '
                                                                                          'tachycardia or '
                                                                                          'atrial '
                                                                                          'fibrillation was '
                                                                                          'recorded for the '
                                                                                          'episode.',
                                                    'beta_adrenergic_receptor_agonists': 'Source-coded '
                                                                                         'indicator of '
                                                                                         'beta-adrenergic '
                                                                                         'receptor agonist '
                                                                                         'use recorded in '
                                                                                         'the Takotsubo '
                                                                                         'assessment.',
                                                    'beta_blocker_use': 'Indicates whether the patient was '
                                                                        'taking a beta blocker prior to the '
                                                                        'ACS admission.',
                                                    'beta_blockerdischarge': 'Coded indicator of whether a '
                                                                             'beta blocker was prescribed at '
                                                                             'discharge.',
                                                    'bivalirudin': 'Coded indicator of whether bivalirudin '
                                                                   'was given during the acute admission '
                                                                   'episode.',
                                                    'bleeding_complications': 'Coded bleeding complications '
                                                                              'recorded during the admission '
                                                                              'or after intervention.',
                                                    'bystander_coronary_artery_disease': 'Coded source '
                                                                                         'indicator of '
                                                                                         'whether incidental '
                                                                                         '(bystander) '
                                                                                         'coronary artery '
                                                                                         'disease was '
                                                                                         'present alongside '
                                                                                         'the presenting '
                                                                                         'condition.',
                                                    'cad': 'Source-recorded indicator of known coronary '
                                                           'artery disease for the patient.',
                                                    'calcium_channel_blocker': 'Coded indicator of whether a '
                                                                               'calcium channel blocker was '
                                                                               'given during the acute '
                                                                               'admission episode.',
                                                    'call_for_help': 'Date and time the patient or bystander '
                                                                     'called for medical help, as recorded '
                                                                     'in the source registry.',
                                                    'cardiac_arrest': 'Date and time of cardiac arrest '
                                                                      'recorded for the episode.',
                                                    'cardiac_arrest_location': 'Coded location at which the '
                                                                               'cardiac arrest occurred, '
                                                                               'including a no-arrest '
                                                                               'category, as recorded in the '
                                                                               'source registry.',
                                                    'cardiac_enzymes_raised': 'Indicator of whether cardiac '
                                                                              'enzyme or troponin levels '
                                                                              'were raised during the '
                                                                              'episode.',
                                                    'cardiac_rehabilitation': 'Source-coded record of '
                                                                              'whether cardiac '
                                                                              'rehabilitation was arranged '
                                                                              'or indicated following the '
                                                                              'admission.',
                                                    'cardiogenic_shock': 'Indicator of whether cardiogenic '
                                                                         'shock was present during the acute '
                                                                         'coronary syndrome episode.',
                                                    'cardiological_care': 'Categorical record of whether the '
                                                                          'patient received cardiological '
                                                                          'care during the admission.',
                                                    'cerebrovascular_disease': 'Indicates whether the '
                                                                               'patient has a recorded '
                                                                               'history of cerebrovascular '
                                                                               'disease.',
                                                    'chronic_renal_failure': 'Indicates whether the patient '
                                                                             'has a recorded history of '
                                                                             'chronic renal failure.',
                                                    'cm_rimaging': 'Coded source field describing cardiac '
                                                                   'magnetic resonance imaging assessment '
                                                                   'recorded for the episode.',
                                                    'cm_rimaging_performed': 'Source flag indicating whether '
                                                                             'cardiac magnetic resonance '
                                                                             'imaging was performed.',
                                                    'coronary_angiography': 'Coded status of coronary '
                                                                            'angiography for the episode, '
                                                                            'including whether and where it '
                                                                            'was performed or arranged.',
                                                    'coronary_intervention': 'Coded status of coronary '
                                                                             'intervention for the episode, '
                                                                             'including whether it was '
                                                                             'performed or arranged.',
                                                    'creatinine': 'Recorded serum creatinine laboratory '
                                                                  'result for the acute episode; source '
                                                                  'units are not documented.',
                                                    'critical_transfer_call': 'Date and time of the critical '
                                                                              'transfer call for the '
                                                                              'patient, as recorded in the '
                                                                              'source registry.',
                                                    'data_complete': 'Source flag indicating whether the '
                                                                     'registry record has been marked as '
                                                                     'complete.',
                                                    'dataset_version': 'Version label of the registry '
                                                                       'dataset specification under which '
                                                                       'the record was collected.',
                                                    'date_last_changed': 'Timestamp of the last change made '
                                                                         'to the record in the source '
                                                                         'registry system.',
                                                    'date_of_birth': 'Patient date of birth as recorded in '
                                                                     'the source ACS transfer registry '
                                                                     'record.',
                                                    'date_of_death': 'Recorded date of patient death as held '
                                                                     'in the source registry record.',
                                                    'date_of_discharge': 'Date and time of discharge from '
                                                                         'the admission recorded in this ACS '
                                                                         'transfer record.',
                                                    'date_of_return_to_referring_hospital': 'Date and time '
                                                                                            'the patient was '
                                                                                            'repatriated to '
                                                                                            'the referring '
                                                                                            'district '
                                                                                            'general '
                                                                                            'hospital after '
                                                                                            'intervention.',
                                                    'datetime_of_stressful_precipitant': 'Date and time of '
                                                                                         'the stressful '
                                                                                         'precipitant event '
                                                                                         'recorded in the '
                                                                                         'Takotsubo '
                                                                                         'assessment.',
                                                    'daycase_transfer_dt': 'Date and time of daycase '
                                                                           'transfer recorded in the source '
                                                                           'registry record.',
                                                    'death_in_hospital': 'Source-coded indicator of whether '
                                                                         'the patient died during the '
                                                                         'hospital admission.',
                                                    'delay_before_treatment': 'Coded MINAP reason for any '
                                                                              'delay before reperfusion '
                                                                              'treatment was given.',
                                                    'delay_to_performance_of_angiogram': 'Coded reason for '
                                                                                         'any delay to '
                                                                                         'performance of the '
                                                                                         'coronary '
                                                                                         'angiogram.',
                                                    'diabetes': 'Recorded diabetes status of the patient, '
                                                                'including non-diabetic and treatment-based '
                                                                'categories.',
                                                    'diabetic_therapy_at_discharge': 'Coded description of '
                                                                                     'the diabetes therapy '
                                                                                     'in place at discharge.',
                                                    'dietary_advice_given': 'Source-coded record of whether '
                                                                            'dietary advice was given before '
                                                                            'discharge.',
                                                    'discharge_destination': 'Coded destination category to '
                                                                             'which the patient was '
                                                                             'discharged at the end of the '
                                                                             'acute coronary syndrome '
                                                                             'admission.',
                                                    'discharge_diagnosis': 'Source-coded discharge diagnosis '
                                                                           'label for the acute coronary '
                                                                           'syndrome episode, retained as '
                                                                           'recorded in the source registry.',
                                                    'ec_gdetermining_treatment': 'Coded ECG finding that '
                                                                                 'determined the treatment '
                                                                                 'given for the acute '
                                                                                 'coronary syndrome episode.',
                                                    'ec_gsubcategoriestxt': 'Text label of the ECG '
                                                                            'subcategory classification '
                                                                            'recorded alongside the '
                                                                            'determining ECG finding, blank '
                                                                            'where not recorded.',
                                                    'echocardiography': 'Coded source record of whether '
                                                                        'echocardiography was carried out, '
                                                                        'arranged for later or judged '
                                                                        'unnecessary during the episode.',
                                                    'entry_id': 'Internal source registry record identifier '
                                                                'for the ACS transfer entry, carried through '
                                                                'from the feeder registry table.',
                                                    'ethnic_origin': 'Patient ethnic origin code as recorded '
                                                                     'in the source ACS registry.',
                                                    'exercise_test': 'Coded source record of whether an '
                                                                     'exercise tolerance test was carried '
                                                                     'out, planned or considered unnecessary '
                                                                     'during the episode.',
                                                    'family_history_of_chd': 'Indicates whether the patient '
                                                                             'has a family history of '
                                                                             'coronary heart disease.',
                                                    'family_history_of_takotsubo': 'Source-coded indicator '
                                                                                   'of whether there is a '
                                                                                   'family history of '
                                                                                   'Takotsubo syndrome.',
                                                    'first_responder': 'Date and time the first responder '
                                                                       'attended the patient, as recorded in '
                                                                       'the source registry.',
                                                    'followed_up_by': 'Coded category describing the '
                                                                      'clinician or service responsible for '
                                                                      'follow-up after discharge.',
                                                    'fondaparinux': 'Coded indicator of whether fondaparinux '
                                                                    'was given during the acute admission '
                                                                    'episode.',
                                                    'forename': 'Patient forename as recorded in the source '
                                                                'ACS transfer registry record.',
                                                    'gender': 'Patient gender as recorded in the source ACS '
                                                              'registry record.',
                                                    'grac_escore': 'GRACE risk score recorded for the acute '
                                                                   'coronary syndrome episode, stored as a '
                                                                   'string and frequently unpopulated.',
                                                    'haemoglobin': 'Recorded haemoglobin laboratory result '
                                                                   'for the acute episode; source units are '
                                                                   'not documented.',
                                                    'heart_failure': 'Indicates whether the patient has a '
                                                                     'recorded history of heart failure.',
                                                    'heart_rate': 'Heart rate recorded at initial assessment '
                                                                  'for the episode; source units are not '
                                                                  'documented.',
                                                    'height': 'Patient height recorded for the episode; '
                                                              'source units are not documented.',
                                                    'high_risk_nstemi': 'Indicates whether the patient was '
                                                                        'assessed as having high-risk '
                                                                        'non-ST-elevation myocardial '
                                                                        'infarction.',
                                                    'hormonal_status': 'Source-coded hormonal status '
                                                                       'recorded as part of the Takotsubo '
                                                                       'assessment.',
                                                    'hospital_identifier': 'Hospital identifier recorded '
                                                                           'against the patient in the '
                                                                           'source registry record.',
                                                    'hr_tor_oral_contraceptive': 'Source-coded indicator of '
                                                                                 'hormone replacement '
                                                                                 'therapy or oral '
                                                                                 'contraceptive use, '
                                                                                 'recorded in the Takotsubo '
                                                                                 'assessment.',
                                                    'hypercholesterolaemia': 'Indicates whether the patient '
                                                                             'has a recorded history of '
                                                                             'hypercholesterolaemia.',
                                                    'hypertension': 'Indicates whether the patient has a '
                                                                    'recorded history of hypertension.',
                                                    'i_vnitrate': 'Coded indicator of whether an intravenous '
                                                                  'nitrate was given during the acute '
                                                                  'admission episode.',
                                                    'initial_diagnosis': 'Initial working diagnosis recorded '
                                                                         'at presentation, retained as the '
                                                                         'source label.',
                                                    'initial_reperfusion': 'Categorical record of the '
                                                                           'initial reperfusion treatment '
                                                                           'given for the acute coronary '
                                                                           'syndrome episode.',
                                                    'inpatient_management_of_hyperglycaemia_diabetes': 'Coded '
                                                                                                       'description '
                                                                                                       'of '
                                                                                                       'how '
                                                                                                       'hyperglycaemia '
                                                                                                       'or '
                                                                                                       'diabetes '
                                                                                                       'was '
                                                                                                       'managed '
                                                                                                       'during '
                                                                                                       'the '
                                                                                                       'inpatient '
                                                                                                       'stay.',
                                                    'intended_reperfusion_procedure': 'Coded reperfusion '
                                                                                      'procedure that was '
                                                                                      'intended for the '
                                                                                      'patient at the '
                                                                                      'interventional '
                                                                                      'centre.',
                                                    'interventional_hospitaltxt': 'Decoded name of the '
                                                                                  'interventional hospital '
                                                                                  'involved in the transfer '
                                                                                  'pathway.',
                                                    'intraaortic_balloon_pump_support': 'Coded source '
                                                                                        'indicator of '
                                                                                        'whether '
                                                                                        'intra-aortic '
                                                                                        'balloon pump '
                                                                                        'support was used '
                                                                                        'during the episode.',
                                                    'is_current_in_source': 'Boolean flag indicating whether '
                                                                            'the record was still present as '
                                                                            'current in the source system at '
                                                                            'the last load.',
                                                    'iv2b3a_agent': 'Coded indication of whether an '
                                                                    'intravenous glycoprotein IIb/IIIa agent '
                                                                    'was given during the admission.',
                                                    'iv_beta_blocker': 'Coded indicator of whether an '
                                                                       'intravenous beta blocker was given '
                                                                       'during the acute admission episode.',
                                                    'killip_class': 'Killip classification of heart failure '
                                                                    'severity recorded at presentation.',
                                                    'left_ventricular_apical_thrombus': 'Coded source '
                                                                                        'indicator of '
                                                                                        'whether left '
                                                                                        'ventricular apical '
                                                                                        'thrombus was '
                                                                                        'identified.',
                                                    'left_ventricular_outflow_tract_obstruction': 'Coded '
                                                                                                  'source '
                                                                                                  'indicator '
                                                                                                  'of '
                                                                                                  'whether '
                                                                                                  'left '
                                                                                                  'ventricular '
                                                                                                  'outflow '
                                                                                                  'tract '
                                                                                                  'obstruction '
                                                                                                  'was '
                                                                                                  'identified.',
                                                    'linkage_fallback_conflict_ind': 'Boolean flag '
                                                                                     'indicating that the '
                                                                                     'fallback linkage '
                                                                                     'produced a conflicting '
                                                                                     'person match during '
                                                                                     'processing.',
                                                    'linkage_historical_fallback_ind': 'Boolean flag '
                                                                                       'indicating that the '
                                                                                       'person linkage was '
                                                                                       'resolved using a '
                                                                                       'historical fallback '
                                                                                       'identifier rather '
                                                                                       'than a current '
                                                                                       'match.',
                                                    'linkage_method': 'Code describing which identifier '
                                                                      'combination was used to link this '
                                                                      'record to the master person '
                                                                      'identifier during pipeline '
                                                                      'processing.',
                                                    'linkage_status': 'Derived code describing the outcome '
                                                                      'of linking the registry record to a '
                                                                      'master person record.',
                                                    'local_intervention_date': 'Date and time of coronary '
                                                                               'intervention performed at '
                                                                               'the local hospital, as '
                                                                               'recorded in the source '
                                                                               'registry.',
                                                    'loop_diuretic': 'Coded indicator of whether a loop '
                                                                     'diuretic was given during the acute '
                                                                     'admission episode.',
                                                    'low_molecular_weight_heparin': 'Coded indication of '
                                                                                    'whether low molecular '
                                                                                    'weight heparin was '
                                                                                    'given during the '
                                                                                    'admission.',
                                                    'lved_pvalue': 'Recorded numeric left ventricular '
                                                                   'end-diastolic pressure value as held in '
                                                                   'the source registry, with no unit '
                                                                   'asserted in source metadata.',
                                                    'lvedp': 'Coded source field recording left ventricular '
                                                             'end-diastolic pressure assessment status, with '
                                                             'any measured figure held in LVED_PVALUE.',
                                                    'lvef': 'Categorical banding of left ventricular '
                                                            'ejection fraction assessed during the episode, '
                                                            'including a not-assessed option.',
                                                    'm_hac_team_audit_notes': 'Free-text audit notes entered '
                                                                              'by the heart attack centre '
                                                                              'team about the registry '
                                                                              'record.',
                                                    'm_regional_lv_dysfunction_distribution_other': 'Free-text '
                                                                                                    'description '
                                                                                                    'of a '
                                                                                                    'regional '
                                                                                                    'left '
                                                                                                    'ventricular '
                                                                                                    'dysfunction '
                                                                                                    'distribution '
                                                                                                    'not '
                                                                                                    'covered '
                                                                                                    'by the '
                                                                                                    'coded '
                                                                                                    'categories.',
                                                    'm_stress_precipitant_other': 'Free-text description of '
                                                                                  'a stressful precipitant '
                                                                                  'not covered by the coded '
                                                                                  'Takotsubo precipitant '
                                                                                  'categories.',
                                                    'mrn': 'Local hospital medical record number identifying '
                                                           'the patient for this registry record.',
                                                    'nhs_number': 'Patient NHS number as recorded in the '
                                                                  'source registry, with validity indicated '
                                                                  'by NHS_NUMBER_VALID_IND.',
                                                    'nhs_number_valid_ind': 'Derived boolean flag indicating '
                                                                            'whether the source NHS number '
                                                                            'passed validity checking; the '
                                                                            'NHS number itself is not '
                                                                            'retained in this column.',
                                                    'oral_beta_blocker': 'Coded indicator of whether an oral '
                                                                         'beta blocker was given during the '
                                                                         'acute admission episode.',
                                                    'oral_nitrate': 'Coded indicator of whether an oral '
                                                                    'nitrate was given during the acute '
                                                                    'admission episode.',
                                                    'other_discharge_diagnosistxt': 'Decoded text label of '
                                                                                    'an additional or other '
                                                                                    'discharge diagnosis '
                                                                                    'category, retained as '
                                                                                    'recorded in the source '
                                                                                    'registry.',
                                                    'outcome_of_arrest': 'Coded outcome of the cardiac '
                                                                         'arrest.',
                                                    'pathway': 'Care pathway classification for the acute '
                                                               'coronary syndrome episode, as recorded in '
                                                               'the source registry.',
                                                    'patient_admin_status': 'Administrative status of the '
                                                                            'patient for the admission, such '
                                                                            'as NHS or private funding '
                                                                            'category, as recorded in the '
                                                                            'source registry.',
                                                    'patient_event_key': 'Deterministic SHA-256 key matching '
                                                                         'clinical_registry_entry.patient_event_key '
                                                                         'for the acs_transfer source family '
                                                                         'and native entry_id.',
                                                    'patient_location_at_time_of_stemi': 'Categorical record '
                                                                                         "of the patient's "
                                                                                         'care location when '
                                                                                         'the STEMI '
                                                                                         'occurred, '
                                                                                         'including a '
                                                                                         'not-applicable '
                                                                                         'option for '
                                                                                         'non-STEMI '
                                                                                         'presentations.',
                                                    'patient_ventilated': 'Indicator of whether the patient '
                                                                          'required mechanical ventilation '
                                                                          'during the acute coronary '
                                                                          'syndrome episode.',
                                                    'peak_ck': 'Peak creatine kinase laboratory result '
                                                               'recorded for the episode; source units are '
                                                               'not documented.',
                                                    'peak_troponin': 'Peak troponin laboratory result '
                                                                     'recorded for the episode, interpreted '
                                                                     'with reference to the assay recorded '
                                                                     'in TROPONIN_ASSAY; source units are '
                                                                     'not documented.',
                                                    'peak_troponin_ng_l': 'Peak troponin in ng/L, unit '
                                                                          'decided per assay: High '
                                                                          'sensitivity Troponin I is '
                                                                          'recorded in ng/L and kept; High '
                                                                          'sensitivity Troponin T, Troponin '
                                                                          'T and Troponin I are recorded in '
                                                                          'ug/L, so values below 5 are x1000 '
                                                                          '(values >= 5 were already ng/L). '
                                                                          'NULL when the assay is unknown or '
                                                                          'missing. INSR_W2 W2.11 (L3029).',
                                                    'peripheral_vascular_disease': 'Indicates whether the '
                                                                                   'patient has a recorded '
                                                                                   'history of peripheral '
                                                                                   'vascular disease.',
                                                    'person_id': 'Internal surrogate person identifier from '
                                                                 'the source patient administration system '
                                                                 'used to link the record to the master '
                                                                 'person record.',
                                                    'pipeline_loaded_at': 'Timestamp recording when the '
                                                                          'pipeline loaded this row into the '
                                                                          'bronze table.',
                                                    'place_first12_lead_ec_gperformed': 'Location where the '
                                                                                        'first 12-lead ECG '
                                                                                        'was performed for '
                                                                                        'the patient.',
                                                    'positive_inotropic_supporttxt': 'Decoded source text '
                                                                                     'describing positive '
                                                                                     'inotropic support '
                                                                                     'given, following the '
                                                                                     'txt suffix convention '
                                                                                     'for lookup '
                                                                                     'descriptions in this '
                                                                                     'extract.',
                                                    'potassium_channel_modulator': 'Coded indicator of '
                                                                                   'whether a potassium '
                                                                                   'channel modulator (for '
                                                                                   'example nicorandil) was '
                                                                                   'given during the acute '
                                                                                   'admission episode.',
                                                    'previous_ami': 'Source-coded indicator of whether the '
                                                                    'patient has a history of previous acute '
                                                                    'myocardial infarction.',
                                                    'previous_angina': 'Source-coded indicator of whether '
                                                                       'the patient has a history of '
                                                                       'previous angina.',
                                                    'previous_cabg': 'Indicates whether the patient has '
                                                                     'undergone previous coronary artery '
                                                                     'bypass graft surgery.',
                                                    'previous_history_of_takotsubo': 'Source-coded indicator '
                                                                                     'of whether the patient '
                                                                                     'has a previous history '
                                                                                     'of Takotsubo syndrome.',
                                                    'previous_pci': 'Indicates whether the patient has '
                                                                    'undergone previous percutaneous '
                                                                    'coronary intervention.',
                                                    'procedure_performed': 'Coded procedure actually '
                                                                           'performed at the interventional '
                                                                           'centre for this ACS episode.',
                                                    'psychiatric_disease': 'Source-coded indicator of '
                                                                           'psychiatric disease recorded as '
                                                                           'part of the Takotsubo assessment '
                                                                           'comorbidities.',
                                                    'qr_sduration': 'Recorded QRS duration category from the '
                                                                    'ECG assessment, held as '
                                                                    'source-formatted text.',
                                                    'qtc_interval': 'Corrected QT interval recorded from the '
                                                                    'ECG in the Takotsubo assessment, held '
                                                                    'as source-formatted text.',
                                                    'radionuclide_study': 'Coded source record of whether a '
                                                                          'radionuclide myocardial study was '
                                                                          'carried out, arranged or judged '
                                                                          'unnecessary during the episode.',
                                                    'reason_reperfusion_treatment_not_given': 'Coded MINAP '
                                                                                              'reason why '
                                                                                              'reperfusion '
                                                                                              'treatment was '
                                                                                              'not given for '
                                                                                              'the acute '
                                                                                              'coronary '
                                                                                              'syndrome '
                                                                                              'episode.',
                                                    'referral_dt_tm': 'Date and time the referral was made '
                                                                      'within the acute coronary syndrome '
                                                                      'transfer pathway.',
                                                    'referral_for_investigationintervention': 'Date and time '
                                                                                              'of referral '
                                                                                              'for further '
                                                                                              'investigation '
                                                                                              'or '
                                                                                              'intervention, '
                                                                                              'as recorded '
                                                                                              'in the source '
                                                                                              'registry.',
                                                    'referring_hospitaltxt': 'Text label of the hospital '
                                                                             'that referred or transferred '
                                                                             'the patient, as held in the '
                                                                             'source registry.',
                                                    'regional_lv_dysfunction_distribution': 'Source-coded '
                                                                                            'distribution '
                                                                                            'pattern of '
                                                                                            'regional left '
                                                                                            'ventricular '
                                                                                            'dysfunction '
                                                                                            'recorded in the '
                                                                                            'Takotsubo '
                                                                                            'assessment.',
                                                    'reinfarction': 'Source-coded indicator of whether '
                                                                    'reinfarction occurred during the '
                                                                    'admission.',
                                                    'reminder': 'Free-text reminder or follow-up note '
                                                                'entered by data collectors against the '
                                                                'registry record.',
                                                    'reperfusion_date_time': 'Date and time reperfusion '
                                                                             'treatment was delivered, as '
                                                                             'recorded in the source '
                                                                             'registry.',
                                                    'row_hash': "Hash of the source record's field values "
                                                                'used to detect row-level changes between '
                                                                'pipeline loads.',
                                                    'serum_cholesterol': 'Recorded serum cholesterol '
                                                                         'laboratory result for the acute '
                                                                         'episode; source units are not '
                                                                         'documented.',
                                                    'serum_glucose': 'Recorded serum glucose laboratory '
                                                                     'result for the acute episode; source '
                                                                     'units are not documented.',
                                                    'site_of_infarction': 'Coded anatomical site of the '
                                                                          'myocardial infarction.',
                                                    'smoking_cessation_advice_given': 'Source-coded record '
                                                                                      'of whether smoking '
                                                                                      'cessation advice was '
                                                                                      'given before '
                                                                                      'discharge.',
                                                    'smoking_status': 'Recorded smoking status of the '
                                                                      'patient at the time of the ACS '
                                                                      'episode.',
                                                    'source_absent_detected_ts': 'Timestamp recording when '
                                                                                 'the pipeline first '
                                                                                 'detected that the record '
                                                                                 'was no longer present in '
                                                                                 'the source system.',
                                                    'source_present_ind': 'Canonical boolean mirror of '
                                                                          'IS_CURRENT_IN_SOURCE; false rows '
                                                                          'are retained source tombstones.',
                                                    'source_record_key': 'Source system record identifier '
                                                                         'for this row, taken from the '
                                                                         'feeder registry entry key.',
                                                    'source_table': 'Name of the source system table from '
                                                                    'which this record was ingested.',
                                                    'start_of_insulin_infusion': 'Date and time that an '
                                                                                 'insulin infusion was '
                                                                                 'started during the '
                                                                                 'inpatient stay.',
                                                    'statin_discharge': 'Coded indicator of whether a statin '
                                                                        'was prescribed at discharge.',
                                                    'statin_use': 'Indicates whether the patient was taking '
                                                                  'a statin prior to the ACS admission.',
                                                    'stress_echo': 'Coded source record of whether stress '
                                                                   'echocardiography was carried out, '
                                                                   'arranged or judged unnecessary during '
                                                                   'the episode.',
                                                    'stress_precipitanttxt': 'Source text label for the '
                                                                             'stressful precipitant category '
                                                                             'recorded in the Takotsubo '
                                                                             'assessment.',
                                                    'surname': 'Patient surname as recorded in the source '
                                                               'ACS transfer registry record.',
                                                    'symptom_onset': 'Date and time of onset of the '
                                                                     "patient's cardiac symptoms, as "
                                                                     'recorded in the source registry.',
                                                    'systolic_bp': 'Systolic blood pressure recorded at '
                                                                   'initial assessment for the episode; '
                                                                   'source units are not documented.',
                                                    'thiazide_diuretic': 'Coded indicator of whether a '
                                                                         'thiazide diuretic was given during '
                                                                         'the acute admission episode.',
                                                    'thienopyridine_inhibitor_discharge': 'Coded indicator '
                                                                                          'of whether a '
                                                                                          'thienopyridine '
                                                                                          'platelet '
                                                                                          'inhibitor was '
                                                                                          'prescribed at '
                                                                                          'discharge.',
                                                    'thienopyridine_inhibitor_use': 'Indicates whether the '
                                                                                    'patient was taking a '
                                                                                    'thienopyridine platelet '
                                                                                    'inhibitor prior to the '
                                                                                    'ACS admission.',
                                                    'thienopyridine_platelet_inhibitor': 'Coded indication '
                                                                                         'of whether a '
                                                                                         'thienopyridine '
                                                                                         'platelet inhibitor '
                                                                                         'was given during '
                                                                                         'the admission.',
                                                    'thrombolytic_drug': 'Coded record of the thrombolytic '
                                                                         'drug used, where thrombolysis was '
                                                                         'given for the acute coronary '
                                                                         'event.',
                                                    'thyroid_disease': 'Source-coded indicator of thyroid '
                                                                       'disease recorded as part of the '
                                                                       'Takotsubo assessment comorbidities.',
                                                    'ticagrelor_discharge': 'Coded indicator of whether '
                                                                            'ticagrelor was prescribed at '
                                                                            'discharge.',
                                                    'ticagrelor_use': 'Source field recording ticagrelor use '
                                                                      'for the patient as captured in the '
                                                                      'registry medication section.',
                                                    'time_left_dgh': 'Date and time the patient left the '
                                                                     'district general hospital for '
                                                                     'transfer, as recorded in the source '
                                                                     'registry.',
                                                    'time_of_diagnostic_ecg': 'Date and time of the ECG that '
                                                                              'determined the diagnosis, as '
                                                                              'recorded in the source '
                                                                              'registry.',
                                                    'transfer_cad': 'Source-recorded coronary artery disease '
                                                                    'status captured in the transfer section '
                                                                    'of the record.',
                                                    'troponin_assay': 'Categorical record of the troponin '
                                                                      'assay type used for the recorded '
                                                                      'troponin result.',
                                                    'unfractionated_heparin': 'Coded indicator of whether '
                                                                              'unfractionated heparin was '
                                                                              'given during the acute '
                                                                              'admission episode.',
                                                    'unit_method': 'How peak_troponin_ng_l was derived: '
                                                                   'assay_ng_l, assay_ug_l_x1000, '
                                                                   'assay_ug_l_value_already_ng_l, or '
                                                                   'undetermined (unknown/missing assay; '
                                                                   'peak_troponin_ng_l NULL). INSR_W2 W2.11.',
                                                    'ventricular_tachycardia_or_fibrillation': 'Coded source '
                                                                                               'indicator of '
                                                                                               'whether '
                                                                                               'ventricular '
                                                                                               'tachycardia '
                                                                                               'or '
                                                                                               'ventricular '
                                                                                               'fibrillation '
                                                                                               'was recorded '
                                                                                               'for the '
                                                                                               'episode.',
                                                    'warfarin': 'Coded indicator of whether warfarin was '
                                                                'given during the acute admission episode.',
                                                    'weight': 'Patient weight recorded for the episode; '
                                                              'source units are not documented.',
                                                    'what_procedure_was_performed_at_the_interventional_hospit': 'Source-coded '
                                                                                                                 'description '
                                                                                                                 'of '
                                                                                                                 'the '
                                                                                                                 'procedure '
                                                                                                                 'performed '
                                                                                                                 'at '
                                                                                                                 'the '
                                                                                                                 'interventional '
                                                                                                                 '(receiving) '
                                                                                                                 'hospital, '
                                                                                                                 'with '
                                                                                                                 'the '
                                                                                                                 'name '
                                                                                                                 'truncated '
                                                                                                                 'in '
                                                                                                                 'the '
                                                                                                                 'source '
                                                                                                                 'system.',
                                                    'where_was_aspirin_given': 'Categorical record of the '
                                                                               'setting in which aspirin was '
                                                                               'first administered during '
                                                                               'the acute episode.',
                                                    'where_was_initial_reperfusion_treatment_given': 'Categorical '
                                                                                                     'record '
                                                                                                     'of the '
                                                                                                     'setting '
                                                                                                     'in '
                                                                                                     'which '
                                                                                                     'initial '
                                                                                                     'reperfusion '
                                                                                                     'treatment '
                                                                                                     'was '
                                                                                                     'delivered.',
                                                    'why_was_no_angiogram_performed': 'Coded reason recorded '
                                                                                      'when no coronary '
                                                                                      'angiogram was '
                                                                                      'performed.',
                                                    'why_was_no_intervention_performed': 'Coded reason '
                                                                                         'recorded when no '
                                                                                         'coronary '
                                                                                         'intervention was '
                                                                                         'performed.'},
                                        'quantity_override': True,
                                        'table': 'Internal QC of clinical_registry_acs_transfer; counts rows '
                                                 'whose Gold parent is absent.'},
 'clinical_registry_acs_transfer': {'columns': {'date_last_changed': 'Timestamp of the last change made to '
                                                                     'the record in the source registry '
                                                                     'system. iWeb last-changed time; reset '
                                                                     'for most entries in April 2026 by a '
                                                                     'registry maintenance run, so it does '
                                                                     'not show when an entry was last '
                                                                     'clinically edited.',
                                                'time_of_diagnostic_ecg': 'Date and time of the ECG that '
                                                                          'determined the diagnosis, as '
                                                                          'recorded in the source registry. '
                                                                          'Time of the diagnostic (STEMI '
                                                                          'trigger) ECG; recorded on only a '
                                                                          'few percent of transfers, so '
                                                                          'door-to-balloon intervals cannot '
                                                                          'be computed for most.'},
                                    'quantity_override': False,
                                    'table': 'Typed child of clinical_registry_entry for the acs_transfer '
                                             'family. MINAP-style ACS transfer pathway: symptom, transfer '
                                             'and reperfusion timestamps, GRACE/Killip, drugs, laboratory '
                                             'results and Takotsubo fields. Diagnoses remain source labels. '
                                             'ADMISSION_DATE_QUALITY identifies the known 2000-01-01 '
                                             'placeholder range. Every governed bronze column is retained '
                                             'under its lower-case name. Coverage: iWeb registries start at '
                                             'different years: coronary 2013, non-coronary 2016, surgery '
                                             '2017, cardiac MDT 2018 (CT aortic 2021), mortality review '
                                             '2018, ERACS 2025, ACS transfer 2000; complete-through month '
                                             'per release: 6_mgmt.data_quality.release_finding (finding_code '
                                             'F2_complete_through).'}}
