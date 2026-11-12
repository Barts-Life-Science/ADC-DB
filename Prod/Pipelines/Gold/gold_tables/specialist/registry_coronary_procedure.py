"""Gold specialist.registry_coronary_procedure: 2 dataset(s).

- datasets: _qc_clinical_registry_coronary_procedure, clinical_registry_coronary_procedure
- moved verbatim from: 3_gold_specialist_care:3835, 3_gold_specialist_care:4369, 3_gold_specialist_care:4375, 3_gold_specialist_care:4390

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.registry_coronary_procedure ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_registry_entry applies the parent Gold drop rule.
CLINICAL_REGISTRY_CORONARY_PROCEDURE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entry_id` AS `entry_id`',
    '`pre_date_last_changed` AS `pre_date_last_changed`',
    '`pre_adc_updt` AS `pre_adc_updt`',
    '`pre_gender` AS `pre_gender`',
    '`pre_date_of_death` AS `pre_date_of_death`',
    '`pre_ethnic_origin` AS `pre_ethnic_origin`',
    '`pre_dateandtimeofoperation` AS `pre_dateandtimeofoperation`',
    '`pre_age` AS `pre_age`',
    '`pre_sex` AS `pre_sex`',
    '`pre_patient_ethnic_group` AS `pre_patient_ethnic_group`',
    '`pre_administrative_category` AS `pre_administrative_category`',
    '`pre_clinical_syndromepci` AS `pre_clinical_syndromepci`',
    '`pre_indication_for_intervention` AS `pre_indication_for_intervention`',
    '`pre_procedure_urgency` AS `pre_procedure_urgency`',
    '`pre_cardiogenic_shockpre_procedure` AS `pre_cardiogenic_shockpre_procedure`',
    '`pre_ccs_angina_statuspre_procedure_stable_only` AS `pre_ccs_angina_statuspre_procedure_stable_only`',
    '`pre_nyha_dyspnoea_statuspre_procedure_stable_only` AS `pre_nyha_dyspnoea_statuspre_procedure_stable_only`',
    '`pre_datetime_arrival_at_first_hospitalacs_only` AS `pre_datetime_arrival_at_first_hospitalacs_only`',
    '`pre_admission_routeacs_only` AS `pre_admission_routeacs_only`',
    '`pre_presenting_ecgacs_only` AS `pre_presenting_ecgacs_only`',
    '`pre_recent_lysisacs_only` AS `pre_recent_lysisacs_only`',
    '`pre_cardiac_enzymesmarkers_raised` AS `pre_cardiac_enzymesmarkers_raised`',
    '`pre_previous_mi` AS `pre_previous_mi`',
    '`pre_previous_cabg` AS `pre_previous_cabg`',
    '`pre_previous_pci` AS `pre_previous_pci`',
    '`pre_diabetes` AS `pre_diabetes`',
    '`pre_height` AS `pre_height`',
    '`pre_weight` AS `pre_weight`',
    '`pre_body_surface_area` AS `pre_body_surface_area`',
    '`pre_body_mass_index` AS `pre_body_mass_index`',
    '`pre_lv_ejection_fraction_category` AS `pre_lv_ejection_fraction_category`',
    '`pre_lv_ejection_fraction` AS `pre_lv_ejection_fraction`',
    '`pre_consultant_responsible_for_proceduretxt` AS `pre_consultant_responsible_for_proceduretxt`',
    '`pre_primary_operatortxt` AS `pre_primary_operatortxt`',
    '`pre_primary_operator_status` AS `pre_primary_operator_status`',
    '`pre_second_operatortxt` AS `pre_second_operatortxt`',
    '`pre_second_operator_status` AS `pre_second_operator_status`',
    '`pre_third_operatortxt` AS `pre_third_operatortxt`',
    '`pre_third_operator_status` AS `pre_third_operator_status`',
    '`pre_gp_iibiiia_drugs_used_during_proceduretxt` AS `pre_gp_iibiiia_drugs_used_during_proceduretxt`',
    '`pre_emboli_protection_devices_usedtxt` AS `pre_emboli_protection_devices_usedtxt`',
    '`pre_circulatory_supporttxt` AS `pre_circulatory_supporttxt`',
    '`pre_local_procedure_identifier` AS `pre_local_procedure_identifier`',
    '`pre_cholesterol` AS `pre_cholesterol`',
    '`pre_smoking_history` AS `pre_smoking_history`',
    '`pre_family_history_of_cad` AS `pre_family_history_of_cad`',
    '`pre_medical_historytxt` AS `pre_medical_historytxt`',
    '`pre_ventilated_preop` AS `pre_ventilated_preop`',
    '`pre_q_wave_on_ecg` AS `pre_q_wave_on_ecg`',
    '`pre_ecg_ischaemia` AS `pre_ecg_ischaemia`',
    '`pre_follow_onad_hoc_procedure` AS `pre_follow_onad_hoc_procedure`',
    '`pre_training_procedure` AS `pre_training_procedure`',
    '`pre_research_procedure` AS `pre_research_procedure`',
    '`pre_research_title` AS `pre_research_title`',
    '`pre_arterial_accesstxt` AS `pre_arterial_accesstxt`',
    '`pre_why_no_iibiiia_during_procedure` AS `pre_why_no_iibiiia_during_procedure`',
    '`pre_surgical_cover` AS `pre_surgical_cover`',
    '`pre_date_time_arrival_at_pci_hospital` AS `pre_date_time_arrival_at_pci_hospital`',
    '`pre_date_time_of_call_for_helpstemi` AS `pre_date_time_of_call_for_helpstemi`',
    '`pre_referring_hospitaltxt` AS `pre_referring_hospitaltxt`',
    '`pre_datetime_of_ecg_triggering_ppci_pathway` AS `pre_datetime_of_ecg_triggering_ppci_pathway`',
    '`pre_patient_location_at_time_of_stemi_onset_raw` AS `pre_patient_location_at_time_of_stemi_onset_raw`',
    '`pre_patient_location_at_time_of_stemi_onset_ts` AS `pre_patient_location_at_time_of_stemi_onset_ts`',
    '`pre_serum_creatinine` AS `pre_serum_creatinine`',
    '`pre_datetime_of_symptom_onsetpci_acs_only` AS `pre_datetime_of_symptom_onsetpci_acs_only`',
    '`pre_echo_report` AS `pre_echo_report`',
    '`pre_arterial_blood_gas_on_arrival_in_cath_lab_ph` AS `pre_arterial_blood_gas_on_arrival_in_cath_lab_ph`',
    '`pre_arterial_blood_gas_on_arrival_in_cath_lab_lactate` AS `pre_arterial_blood_gas_on_arrival_in_cath_lab_lactate`',
    '`pre_arterial_blood_gas_on_arrival_in_cath_lab_base_excess` AS `pre_arterial_blood_gas_on_arrival_in_cath_lab_base_excess`',
    '`pre_glasgow_coma_scale_on_arrival_in_cath_lab` AS `pre_glasgow_coma_scale_on_arrival_in_cath_lab`',
    '`pre_other_therapeutic_hypothermia` AS `pre_other_therapeutic_hypothermia`',
    '`pre_therapeutic_hypothermiatxt` AS `pre_therapeutic_hypothermiatxt`',
    '`pre_cath_proceduretxt` AS `pre_cath_proceduretxt`',
    '`pre_out_of_hospital_cardiac_arrest` AS `pre_out_of_hospital_cardiac_arrest`',
    '`pre_presumed_date_time_of_arrest` AS `pre_presumed_date_time_of_arrest`',
    '`pre_coronary_dominance` AS `pre_coronary_dominance`',
    '`pre_m_procedure_findings` AS `pre_m_procedure_findings`',
    '`pre_arterial_managementtxt` AS `pre_arterial_managementtxt`',
    '`pre_drug_therapy_preproceduretxt` AS `pre_drug_therapy_preproceduretxt`',
    '`pre_referring_cardiologisttxt` AS `pre_referring_cardiologisttxt`',
    '`pre_history_of_renal_disease1` AS `pre_history_of_renal_disease1`',
    '`pre_temporary_pacing_wire` AS `pre_temporary_pacing_wire`',
    '`pre_cath_devicestxt` AS `pre_cath_devicestxt`',
    '`pre_cath_lab_number` AS `pre_cath_lab_number`',
    '`pre_date_and_time_on_table` AS `pre_date_and_time_on_table`',
    '`pre_ventilation` AS `pre_ventilation`',
    '`pre_hospital_identifier` AS `pre_hospital_identifier`',
    '`pre_date_time_shortcuts_raw` AS `pre_date_time_shortcuts_raw`',
    '`pre_date_time_shortcuts_ts` AS `pre_date_time_shortcuts_ts`',
    '`pre_m_admission_history` AS `pre_m_admission_history`',
    '`pre_diabetes_type` AS `pre_diabetes_type`',
    '`pre_interventions` AS `pre_interventions`',
    '`pre_convalescent_stem_ior_nstemi` AS `pre_convalescent_stem_ior_nstemi`',
    '`pre_us_sguided` AS `pre_us_sguided`',
    '`pre_screen_of_left_femoral_head` AS `pre_screen_of_left_femoral_head`',
    '`pre_left_femoral_micropuncture` AS `pre_left_femoral_micropuncture`',
    '`pre_left_height_of_puncture` AS `pre_left_height_of_puncture`',
    '`pre_height_of_puncture_diagram` AS `pre_height_of_puncture_diagram`',
    '`pre_right_femoral_micropuncture` AS `pre_right_femoral_micropuncture`',
    '`pre_right_height_of_puncture` AS `pre_right_height_of_puncture`',
    '`pre_left_femoral_us_sguided` AS `pre_left_femoral_us_sguided`',
    '`pre_right_femoral_us_sguided` AS `pre_right_femoral_us_sguided`',
    '`pre_screen_of_right_femoral_head` AS `pre_screen_of_right_femoral_head`',
    '`pre_clinical_frailty_scale` AS `pre_clinical_frailty_scale`',
    '`pre_clinical_frailty_scale_image` AS `pre_clinical_frailty_scale_image`',
    '`post_date_last_changed` AS `post_date_last_changed`',
    '`post_adc_updt` AS `post_adc_updt`',
    '`post_date_of_discharge_raw` AS `post_date_of_discharge_raw`',
    '`post_date_of_discharge_ts` AS `post_date_of_discharge_ts`',
    '`post_gender` AS `post_gender`',
    '`post_date_of_death` AS `post_date_of_death`',
    '`post_ethnic_origin` AS `post_ethnic_origin`',
    '`post_vessels_attemptedtxt` AS `post_vessels_attemptedtxt`',
    '`post_number_of_vessels_attemptednot_epicardial_territories` AS `post_number_of_vessels_attemptednot_epicardial_territories`',
    '`post_number_of_lesions_attempted` AS `post_number_of_lesions_attempted`',
    '`post_number_of_chronic_occlusions_attempted` AS `post_number_of_chronic_occlusions_attempted`',
    '`post_number_restenoses_attempted` AS `post_number_restenoses_attempted`',
    '`post_number_instent_stenoses_attempted` AS `post_number_instent_stenoses_attempted`',
    '`post_number_stents_used` AS `post_number_stents_used`',
    '`post_number_of_drug_eluting_stents_used` AS `post_number_of_drug_eluting_stents_used`',
    '`post_drugs_eluted_by_stentstxt` AS `post_drugs_eluted_by_stentstxt`',
    '`post_left_main_stem_stenosispost_pci` AS `post_left_main_stem_stenosispost_pci`',
    '`post_lad_proximal_stenosispost_pci` AS `post_lad_proximal_stenosispost_pci`',
    '`post_lad_other_stenosispost_pci` AS `post_lad_other_stenosispost_pci`',
    '`post_rca_stenosispost_pci` AS `post_rca_stenosispost_pci`',
    '`post_cx_stenosispost_pci` AS `post_cx_stenosispost_pci`',
    '`post_number_coronary_grafts_patent_postop` AS `post_number_coronary_grafts_patent_postop`',
    '`post_flow_in_ira_postopacs` AS `post_flow_in_ira_postopacs`',
    '`post_m_operation_reportcomment` AS `post_m_operation_reportcomment`',
    '`post_device_failure` AS `post_device_failure`',
    '`post_pci_hospital_outcometxt` AS `post_pci_hospital_outcometxt`',
    '`post_enzymes_postop` AS `post_enzymes_postop`',
    '`post_status_at_discharge` AS `post_status_at_discharge`',
    '`post_largest_ballon_stent_diameter` AS `post_largest_ballon_stent_diameter`',
    '`post_longest_stented_treated_segment` AS `post_longest_stented_treated_segment`',
    '`post_procedural_complicationtxt` AS `post_procedural_complicationtxt`',
    '`post_arterial_complicationstxt` AS `post_arterial_complicationstxt`',
    '`post_time_to_bypass` AS `post_time_to_bypass`',
    '`post_left_main_stem_protected` AS `post_left_main_stem_protected`',
    '`post_patient_status_during_transfer_to_theatre` AS `post_patient_status_during_transfer_to_theatre`',
    '`post_indication_for_stent` AS `post_indication_for_stent`',
    '`post_number_of_lesions_successful` AS `post_number_of_lesions_successful`',
    '`post_bleeding_up_to_discharge` AS `post_bleeding_up_to_discharge`',
    '`post_ventilation` AS `post_ventilation`',
    '`post_external_cooling_pads_blankets_wrapstxt` AS `post_external_cooling_pads_blankets_wrapstxt`',
    '`post_consultant_responsible_for_procedure_gmc_numbertxt` AS `post_consultant_responsible_for_procedure_gmc_numbertxt`',
    '`post_primary_operator_gmc_numbertxt` AS `post_primary_operator_gmc_numbertxt`',
    '`post_second_operator_gmc_numbertxt` AS `post_second_operator_gmc_numbertxt`',
    '`post_third_operator_gmc_numbertxt` AS `post_third_operator_gmc_numbertxt`',
    '`post_left_main_stem_stenosispre_pci` AS `post_left_main_stem_stenosispre_pci`',
    '`post_lad_proximal_stenosis_pre_pci` AS `post_lad_proximal_stenosis_pre_pci`',
    '`post_lad_other_stenosis_pre_pci` AS `post_lad_other_stenosis_pre_pci`',
    '`post_rca_stenosis_pre_pci` AS `post_rca_stenosis_pre_pci`',
    '`post_cx_stenosis_pre_pci` AS `post_cx_stenosis_pre_pci`',
    '`post_management_plan` AS `post_management_plan`',
    '`post_other_management_plan` AS `post_other_management_plan`',
    '`post_cath_proceduretxt` AS `post_cath_proceduretxt`',
    '`post_m_angiography_findings` AS `post_m_angiography_findings`',
    '`post_m_procedure_findings_and_report` AS `post_m_procedure_findings_and_report`',
    '`post_left_main_stem_stenosis_procedure` AS `post_left_main_stem_stenosis_procedure`',
    '`post_lad_proximal_stenosis_procedure` AS `post_lad_proximal_stenosis_procedure`',
    '`post_lad_other_stenosis_procedure` AS `post_lad_other_stenosis_procedure`',
    '`post_cx_stenosis_procedure` AS `post_cx_stenosis_procedure`',
    '`post_flow_in_ira_at_angio` AS `post_flow_in_ira_at_angio`',
    '`post_rca_stenosis_procedure` AS `post_rca_stenosis_procedure`',
    '`post_number_grafts_presentangio` AS `post_number_grafts_presentangio`',
    '`post_number_grafts_patent_pre_pci` AS `post_number_grafts_patent_pre_pci`',
    '`post_procedural_devices_used_detailtxt` AS `post_procedural_devices_used_detailtxt`',
    '`post_athero_thrombus_removal_devices_usedtxt` AS `post_athero_thrombus_removal_devices_usedtxt`',
    '`post_left_main_stem_stenosis_comment` AS `post_left_main_stem_stenosis_comment`',
    '`post_lad_proximal_stenosis_comment` AS `post_lad_proximal_stenosis_comment`',
    '`post_lad_other_stenosis_comment` AS `post_lad_other_stenosis_comment`',
    '`post_rca_stenosis_comment` AS `post_rca_stenosis_comment`',
    '`post_cx_stenosis_comment` AS `post_cx_stenosis_comment`',
    '`post_diagnostic_devices_used_during_proceduretxt` AS `post_diagnostic_devices_used_during_proceduretxt`',
    '`post_procedural_devices_usedtxt` AS `post_procedural_devices_usedtxt`',
    '`post_brachytherapy_devices_usedtxt` AS `post_brachytherapy_devices_usedtxt`',
    '`post_emboli_protection_devices_usedtxt` AS `post_emboli_protection_devices_usedtxt`',
    '`post_datetime_of_first_balloon_inflationpci` AS `post_datetime_of_first_balloon_inflationpci`',
    '`post_gp_iibiiia_drugs_used_during_proceduretxt` AS `post_gp_iibiiia_drugs_used_during_proceduretxt`',
    '`post_why_no_iibiiia_during_procedure` AS `post_why_no_iibiiia_during_procedure`',
    '`post_dataset_complete` AS `post_dataset_complete`',
    '`post_pci_for_stent_thrombosis` AS `post_pci_for_stent_thrombosis`',
    '`post_m_pre_discharge_instructions` AS `post_m_pre_discharge_instructions`',
    '`post_m_discharge_management_plan` AS `post_m_discharge_management_plan`',
    '`post_m_information_for_the_patient` AS `post_m_information_for_the_patient`',
    '`post_mg_pinfo` AS `post_mg_pinfo`',
    '`post_nurse_led_discharge` AS `post_nurse_led_discharge`',
    '`post_m_details_of_all_devices_implanted` AS `post_m_details_of_all_devices_implanted`',
    '`post_diagnostic_interventions_undertakentxt` AS `post_diagnostic_interventions_undertakentxt`',
    '`post_therapeutic_interventions_undertakentxt` AS `post_therapeutic_interventions_undertakentxt`',
    '`post_contrast_volume` AS `post_contrast_volume`',
    '`post_interesting_case` AS `post_interesting_case`',
    '`post_m_case_details` AS `post_m_case_details`',
    '`post_sca_istage_pre_pci` AS `post_sca_istage_pre_pci`',
    '`post_sca_istage_post_cci` AS `post_sca_istage_post_cci`',
    '`post_echo_physiology_pre_pci` AS `post_echo_physiology_pre_pci`',
    '`post_peak_lactate` AS `post_peak_lactate`',
    '`post_troughp_h` AS `post_troughp_h`',
    '`post_trough_sys_bp` AS `post_trough_sys_bp`',
    '`post_inopressor_use_pre_pci` AS `post_inopressor_use_pre_pci`',
    '`post_peak_noradrenaline_dose_during_cath_lab` AS `post_peak_noradrenaline_dose_during_cath_lab`',
    '`post_ic_uadmission_post_pci` AS `post_ic_uadmission_post_pci`',
    '`post_csa_iimage` AS `post_csa_iimage`',
    '`post_l_vgram` AS `post_l_vgram`',
    '`source_join_status` AS `source_join_status`',
    '`nhs_number` AS `nhs_number`',
    '`mrn` AS `mrn`',
    '`identifier_concordance_ind` AS `identifier_concordance_ind`',
    '`date_last_changed` AS `date_last_changed`',
    '`adc_updt` AS `adc_updt`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`linkage_status` AS `linkage_status`',
    '`person_id` AS `person_id`',
    '`linkage_method` AS `linkage_method`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
    '`lesion_child_count` AS `lesion_child_count`',
    '`pre_date_of_death_future_ind` AS `pre_date_of_death_future_ind`',
    '`pre_date_of_death_sentinel_ind` AS `pre_date_of_death_sentinel_ind`',
    '`pre_date_of_death_clean` AS `pre_date_of_death_clean`',
    '`pre_dateandtimeofoperation_future_ind` AS `pre_dateandtimeofoperation_future_ind`',
    '`pre_dateandtimeofoperation_sentinel_ind` AS `pre_dateandtimeofoperation_sentinel_ind`',
    '`pre_dateandtimeofoperation_clean` AS `pre_dateandtimeofoperation_clean`',
    '`pre_datetime_arrival_at_first_hospitalacs_only_future_ind` AS `pre_datetime_arrival_at_first_hospitalacs_only_future_ind`',
    '`pre_datetime_arrival_at_first_hospitalacs_only_sentinel_ind` AS `pre_datetime_arrival_at_first_hospitalacs_only_sentinel_ind`',
    '`pre_datetime_arrival_at_first_hospitalacs_only_clean` AS `pre_datetime_arrival_at_first_hospitalacs_only_clean`',
    '`pre_date_time_arrival_at_pci_hospital_future_ind` AS `pre_date_time_arrival_at_pci_hospital_future_ind`',
    '`pre_date_time_arrival_at_pci_hospital_sentinel_ind` AS `pre_date_time_arrival_at_pci_hospital_sentinel_ind`',
    '`pre_date_time_arrival_at_pci_hospital_clean` AS `pre_date_time_arrival_at_pci_hospital_clean`',
    '`pre_date_time_of_call_for_helpstemi_future_ind` AS `pre_date_time_of_call_for_helpstemi_future_ind`',
    '`pre_date_time_of_call_for_helpstemi_sentinel_ind` AS `pre_date_time_of_call_for_helpstemi_sentinel_ind`',
    '`pre_date_time_of_call_for_helpstemi_clean` AS `pre_date_time_of_call_for_helpstemi_clean`',
    '`pre_datetime_of_ecg_triggering_ppci_pathway_future_ind` AS `pre_datetime_of_ecg_triggering_ppci_pathway_future_ind`',
    '`pre_datetime_of_ecg_triggering_ppci_pathway_sentinel_ind` AS `pre_datetime_of_ecg_triggering_ppci_pathway_sentinel_ind`',
    '`pre_datetime_of_ecg_triggering_ppci_pathway_clean` AS `pre_datetime_of_ecg_triggering_ppci_pathway_clean`',
    '`pre_patient_location_at_time_of_stemi_onset_ts_future_ind` AS `pre_patient_location_at_time_of_stemi_onset_ts_future_ind`',
    '`pre_patient_location_at_time_of_stemi_onset_ts_sentinel_ind` AS `pre_patient_location_at_time_of_stemi_onset_ts_sentinel_ind`',
    '`pre_patient_location_at_time_of_stemi_onset_ts_clean` AS `pre_patient_location_at_time_of_stemi_onset_ts_clean`',
    '`pre_datetime_of_symptom_onsetpci_acs_only_future_ind` AS `pre_datetime_of_symptom_onsetpci_acs_only_future_ind`',
    '`pre_datetime_of_symptom_onsetpci_acs_only_sentinel_ind` AS `pre_datetime_of_symptom_onsetpci_acs_only_sentinel_ind`',
    '`pre_datetime_of_symptom_onsetpci_acs_only_clean` AS `pre_datetime_of_symptom_onsetpci_acs_only_clean`',
    '`pre_presumed_date_time_of_arrest_future_ind` AS `pre_presumed_date_time_of_arrest_future_ind`',
    '`pre_presumed_date_time_of_arrest_sentinel_ind` AS `pre_presumed_date_time_of_arrest_sentinel_ind`',
    '`pre_presumed_date_time_of_arrest_clean` AS `pre_presumed_date_time_of_arrest_clean`',
    '`pre_date_and_time_on_table_future_ind` AS `pre_date_and_time_on_table_future_ind`',
    '`pre_date_and_time_on_table_sentinel_ind` AS `pre_date_and_time_on_table_sentinel_ind`',
    '`pre_date_and_time_on_table_clean` AS `pre_date_and_time_on_table_clean`',
    '`pre_date_time_shortcuts_ts_future_ind` AS `pre_date_time_shortcuts_ts_future_ind`',
    '`pre_date_time_shortcuts_ts_sentinel_ind` AS `pre_date_time_shortcuts_ts_sentinel_ind`',
    '`pre_date_time_shortcuts_ts_clean` AS `pre_date_time_shortcuts_ts_clean`',
    '`post_date_of_discharge_ts_future_ind` AS `post_date_of_discharge_ts_future_ind`',
    '`post_date_of_discharge_ts_sentinel_ind` AS `post_date_of_discharge_ts_sentinel_ind`',
    '`post_date_of_discharge_ts_clean` AS `post_date_of_discharge_ts_clean`',
    '`post_date_of_death_future_ind` AS `post_date_of_death_future_ind`',
    '`post_date_of_death_sentinel_ind` AS `post_date_of_death_sentinel_ind`',
    '`post_date_of_death_clean` AS `post_date_of_death_clean`',
    '`post_datetime_of_first_balloon_inflationpci_future_ind` AS `post_datetime_of_first_balloon_inflationpci_future_ind`',
    '`post_datetime_of_first_balloon_inflationpci_sentinel_ind` AS `post_datetime_of_first_balloon_inflationpci_sentinel_ind`',
    '`post_datetime_of_first_balloon_inflationpci_clean` AS `post_datetime_of_first_balloon_inflationpci_clean`',
    '`pipeline_updt_dt_tm` AS `pipeline_updt_dt_tm`',
    '`row_hash` AS `row_hash`',
    '`source_present_ind` AS `source_present_ind`',
]


CLINICAL_REGISTRY_CORONARY_PROCEDURE_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_coronary_procedure.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_coronary_procedure"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_CORONARY_PROCEDURE_MANDATORY_RULES)
def _gold_qc_clinical_registry_coronary_procedure():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_coronary_procedure"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_CORONARY_PROCEDURE_SELECT, "__gold_parent_present AS __gold_parent_present")
    return df


@dp.materialized_view(
    name=_n("gold_clinical.registry_coronary_procedure"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_registry_coronary_procedure():
    """Publish clinical_registry_coronary_procedure without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_coronary_procedure")).drop("__gold_parent_present")


finalise()


COMMENTS = {'_qc_clinical_registry_coronary_procedure': {'columns': {'adc_updt': 'Greatest landing timestamp across the '
                                                                      'pre-procedure and post-procedure '
                                                                      'source records.',
                                                          'date_last_changed': 'Greatest Pre/Post source '
                                                                               'edit timestamp; not the '
                                                                               'clinical procedure date.',
                                                          'entry_id': 'Reg_CORONARY_Pre/PostProcedure '
                                                                      'EntryId; parent key for lesion '
                                                                      'PARENT_ENTRY_ID.',
                                                          'identifier_concordance_ind': 'True when the '
                                                                                        'pre-procedure and '
                                                                                        'post-procedure NHS '
                                                                                        'number and MRN '
                                                                                        'agree; null when '
                                                                                        'either module lacks '
                                                                                        'a comparison '
                                                                                        'identifier.',
                                                          'lesion_child_count': 'Count of current '
                                                                                'iweb_coronary_lesion rows '
                                                                                'whose PARENT_ENTRY_ID '
                                                                                'equals this EntryId.',
                                                          'linkage_fallback_conflict_ind': 'True when '
                                                                                           'historical alias '
                                                                                           'evidence '
                                                                                           'conflicted and '
                                                                                           'therefore could '
                                                                                           'not safely '
                                                                                           'resolve a '
                                                                                           'person.',
                                                          'linkage_historical_fallback_ind': 'True when '
                                                                                             'person '
                                                                                             'resolution '
                                                                                             'required the '
                                                                                             'consensus-safe '
                                                                                             'historical '
                                                                                             'alias '
                                                                                             'fallback.',
                                                          'linkage_method': 'Identifier route used to '
                                                                            'resolve PERSON_ID, using MRN, '
                                                                            'NHS number, or both.',
                                                          'linkage_status': 'MATCHED_BOTH, MATCHED_MRN, '
                                                                            'MATCHED_NHS, CONFLICT, '
                                                                            'AMBIGUOUS, or UNMATCHED.',
                                                          'mrn': 'Source local medical record number '
                                                                 'published under identifier doctrine; '
                                                                 'ig_risk=4 and ig_severity=2.',
                                                          'nhs_number': 'Source NHS number published under '
                                                                        'identifier doctrine; ig_risk=4 and '
                                                                        'ig_severity=2.',
                                                          'nhs_number_valid_ind': 'True when the published '
                                                                                  'NHS number passes the '
                                                                                  'bronze NHS-number '
                                                                                  'validity check.',
                                                          'patient_event_key': 'Deterministic SHA-256 key '
                                                                               'matching '
                                                                               'clinical_registry_entry.patient_event_key '
                                                                               'for the coronary_procedure '
                                                                               'source family and native '
                                                                               'entry_id.',
                                                          'person_id': 'Millennium PERSON_ID from the live '
                                                                       'iWeb consensus-safe MRN/NHS '
                                                                       'resolver.',
                                                          'pipeline_updt_dt_tm': 'Timestamp when the bronze '
                                                                                 'coronary-procedure '
                                                                                 'pipeline last processed '
                                                                                 'this row.',
                                                          'post_adc_updt': 'Landing timestamp carried by the '
                                                                           'post-procedure iWeb coronary '
                                                                           'source record.',
                                                          'post_arterial_complicationstxt': 'Free-text '
                                                                                            'arterial '
                                                                                            'complications '
                                                                                            'recorded in the '
                                                                                            'post-procedure '
                                                                                            'iWeb coronary '
                                                                                            'record.',
                                                          'post_athero_thrombus_removal_devices_usedtxt': 'Free-text '
                                                                                                          'athero '
                                                                                                          'thrombus '
                                                                                                          'removal '
                                                                                                          'devices '
                                                                                                          'used '
                                                                                                          'recorded '
                                                                                                          'in '
                                                                                                          'the '
                                                                                                          'post-procedure '
                                                                                                          'iWeb '
                                                                                                          'coronary '
                                                                                                          'record.',
                                                          'post_bleeding_up_to_discharge': 'Bleeding up to '
                                                                                           'discharge '
                                                                                           'recorded in the '
                                                                                           'post-procedure '
                                                                                           'iWeb coronary '
                                                                                           'record.',
                                                          'post_brachytherapy_devices_usedtxt': 'Free-text '
                                                                                                'brachytherapy '
                                                                                                'devices '
                                                                                                'used '
                                                                                                'recorded in '
                                                                                                'the '
                                                                                                'post-procedure '
                                                                                                'iWeb '
                                                                                                'coronary '
                                                                                                'record.',
                                                          'post_cath_proceduretxt': 'Free-text cath '
                                                                                    'procedure recorded in '
                                                                                    'the post-procedure iWeb '
                                                                                    'coronary record.',
                                                          'post_consultant_responsible_for_procedure_gmc_numbertxt': 'Free-text '
                                                                                                                     'consultant '
                                                                                                                     'responsible '
                                                                                                                     'for '
                                                                                                                     'procedure '
                                                                                                                     'General '
                                                                                                                     'Medical '
                                                                                                                     'Council '
                                                                                                                     'number '
                                                                                                                     'recorded '
                                                                                                                     'in '
                                                                                                                     'the '
                                                                                                                     'post-procedure '
                                                                                                                     'iWeb '
                                                                                                                     'coronary '
                                                                                                                     'record.',
                                                          'post_contrast_volume': 'Contrast volume recorded '
                                                                                  'in the post-procedure '
                                                                                  'iWeb coronary record.',
                                                          'post_csa_iimage': 'SCAI image or image reference '
                                                                             'retained from the '
                                                                             'post-procedure iWeb module.',
                                                          'post_cx_stenosis_comment': 'Circumflex coronary '
                                                                                      'artery stenosis '
                                                                                      'comment recorded in '
                                                                                      'the post-procedure '
                                                                                      'iWeb coronary record.',
                                                          'post_cx_stenosis_pre_pci': 'Circumflex coronary '
                                                                                      'artery stenosis pre '
                                                                                      'percutaneous coronary '
                                                                                      'intervention (PCI) '
                                                                                      'recorded in the '
                                                                                      'post-procedure iWeb '
                                                                                      'coronary record.',
                                                          'post_cx_stenosis_procedure': 'Circumflex coronary '
                                                                                        'artery stenosis '
                                                                                        'procedure recorded '
                                                                                        'in the '
                                                                                        'post-procedure iWeb '
                                                                                        'coronary record.',
                                                          'post_cx_stenosispost_pci': 'Circumflex coronary '
                                                                                      'artery stenosis post '
                                                                                      'percutaneous coronary '
                                                                                      'intervention (PCI) '
                                                                                      'recorded in the '
                                                                                      'post-procedure iWeb '
                                                                                      'coronary record.',
                                                          'post_dataset_complete': 'Dataset complete '
                                                                                   'recorded in the '
                                                                                   'post-procedure iWeb '
                                                                                   'coronary record.',
                                                          'post_date_last_changed': 'Source edit timestamp '
                                                                                    'from the post-procedure '
                                                                                    'iWeb coronary record; '
                                                                                    'it is not a clinical '
                                                                                    'event time.',
                                                          'post_date_of_death': 'Date of death recorded in '
                                                                                'the post-procedure iWeb '
                                                                                'coronary record.',
                                                          'post_date_of_death_clean': 'Post-procedure date '
                                                                                      'of death after '
                                                                                      'date-quality '
                                                                                      'cleaning; null when '
                                                                                      'missing, future-dated '
                                                                                      'or a configured '
                                                                                      'sentinel.',
                                                          'post_date_of_death_future_ind': 'True when the '
                                                                                           'post-procedure '
                                                                                           'date of death is '
                                                                                           'later than the '
                                                                                           'pipeline '
                                                                                           'evaluation time.',
                                                          'post_date_of_death_sentinel_ind': 'True when the '
                                                                                             'post-procedure '
                                                                                             'date of death '
                                                                                             'matches a '
                                                                                             'configured '
                                                                                             'sentinel date '
                                                                                             'or timestamp.',
                                                          'post_date_of_discharge_raw': 'Verbatim source '
                                                                                        'discharge value; '
                                                                                        'retained even when '
                                                                                        'future-dated.',
                                                          'post_date_of_discharge_ts': 'Parsed discharge '
                                                                                       'timestamp; use its '
                                                                                       'DQ triplet and CLEAN '
                                                                                       'value.',
                                                          'post_date_of_discharge_ts_clean': 'Post-procedure '
                                                                                             'date of '
                                                                                             'discharge ts '
                                                                                             'after '
                                                                                             'date-quality '
                                                                                             'cleaning; null '
                                                                                             'when missing, '
                                                                                             'future-dated '
                                                                                             'or a '
                                                                                             'configured '
                                                                                             'sentinel.',
                                                          'post_date_of_discharge_ts_future_ind': 'True when '
                                                                                                  'the '
                                                                                                  'post-procedure '
                                                                                                  'date of '
                                                                                                  'discharge '
                                                                                                  'ts is '
                                                                                                  'later '
                                                                                                  'than the '
                                                                                                  'pipeline '
                                                                                                  'evaluation '
                                                                                                  'time.',
                                                          'post_date_of_discharge_ts_sentinel_ind': 'True '
                                                                                                    'when '
                                                                                                    'the '
                                                                                                    'post-procedure '
                                                                                                    'date of '
                                                                                                    'discharge '
                                                                                                    'ts '
                                                                                                    'matches '
                                                                                                    'a '
                                                                                                    'configured '
                                                                                                    'sentinel '
                                                                                                    'date or '
                                                                                                    'timestamp.',
                                                          'post_datetime_of_first_balloon_inflationpci': 'Date '
                                                                                                         'and '
                                                                                                         'time '
                                                                                                         'of '
                                                                                                         'first '
                                                                                                         'balloon '
                                                                                                         'inflation '
                                                                                                         'percutaneous '
                                                                                                         'coronary '
                                                                                                         'intervention '
                                                                                                         '(PCI) '
                                                                                                         'recorded '
                                                                                                         'in '
                                                                                                         'the '
                                                                                                         'post-procedure '
                                                                                                         'iWeb '
                                                                                                         'coronary '
                                                                                                         'record.',
                                                          'post_datetime_of_first_balloon_inflationpci_clean': 'Post-procedure '
                                                                                                               'date '
                                                                                                               'and '
                                                                                                               'time '
                                                                                                               'of '
                                                                                                               'first '
                                                                                                               'balloon '
                                                                                                               'inflation '
                                                                                                               'percutaneous '
                                                                                                               'coronary '
                                                                                                               'intervention '
                                                                                                               '(PCI) '
                                                                                                               'after '
                                                                                                               'date-quality '
                                                                                                               'cleaning; '
                                                                                                               'null '
                                                                                                               'when '
                                                                                                               'missing, '
                                                                                                               'future-dated '
                                                                                                               'or '
                                                                                                               'a '
                                                                                                               'configured '
                                                                                                               'sentinel.',
                                                          'post_datetime_of_first_balloon_inflationpci_future_ind': 'True '
                                                                                                                    'when '
                                                                                                                    'the '
                                                                                                                    'post-procedure '
                                                                                                                    'date '
                                                                                                                    'and '
                                                                                                                    'time '
                                                                                                                    'of '
                                                                                                                    'first '
                                                                                                                    'balloon '
                                                                                                                    'inflation '
                                                                                                                    'percutaneous '
                                                                                                                    'coronary '
                                                                                                                    'intervention '
                                                                                                                    '(PCI) '
                                                                                                                    'is '
                                                                                                                    'later '
                                                                                                                    'than '
                                                                                                                    'the '
                                                                                                                    'pipeline '
                                                                                                                    'evaluation '
                                                                                                                    'time.',
                                                          'post_datetime_of_first_balloon_inflationpci_sentinel_ind': 'True '
                                                                                                                      'when '
                                                                                                                      'the '
                                                                                                                      'post-procedure '
                                                                                                                      'date '
                                                                                                                      'and '
                                                                                                                      'time '
                                                                                                                      'of '
                                                                                                                      'first '
                                                                                                                      'balloon '
                                                                                                                      'inflation '
                                                                                                                      'percutaneous '
                                                                                                                      'coronary '
                                                                                                                      'intervention '
                                                                                                                      '(PCI) '
                                                                                                                      'matches '
                                                                                                                      'a '
                                                                                                                      'configured '
                                                                                                                      'sentinel '
                                                                                                                      'date '
                                                                                                                      'or '
                                                                                                                      'timestamp.',
                                                          'post_device_failure': 'Device failure recorded in '
                                                                                 'the post-procedure iWeb '
                                                                                 'coronary record.',
                                                          'post_diagnostic_devices_used_during_proceduretxt': 'Free-text '
                                                                                                              'diagnostic '
                                                                                                              'devices '
                                                                                                              'used '
                                                                                                              'during '
                                                                                                              'procedure '
                                                                                                              'recorded '
                                                                                                              'in '
                                                                                                              'the '
                                                                                                              'post-procedure '
                                                                                                              'iWeb '
                                                                                                              'coronary '
                                                                                                              'record.',
                                                          'post_diagnostic_interventions_undertakentxt': 'Free-text '
                                                                                                         'diagnostic '
                                                                                                         'interventions '
                                                                                                         'undertaken '
                                                                                                         'recorded '
                                                                                                         'in '
                                                                                                         'the '
                                                                                                         'post-procedure '
                                                                                                         'iWeb '
                                                                                                         'coronary '
                                                                                                         'record.',
                                                          'post_drugs_eluted_by_stentstxt': 'Free-text drugs '
                                                                                            'eluted by '
                                                                                            'stents recorded '
                                                                                            'in the '
                                                                                            'post-procedure '
                                                                                            'iWeb coronary '
                                                                                            'record.',
                                                          'post_echo_physiology_pre_pci': 'Echocardiographic '
                                                                                          'or physiological '
                                                                                          'assessment '
                                                                                          'recorded before '
                                                                                          'percutaneous '
                                                                                          'coronary '
                                                                                          'intervention.',
                                                          'post_emboli_protection_devices_usedtxt': 'Free-text '
                                                                                                    'emboli '
                                                                                                    'protection '
                                                                                                    'devices '
                                                                                                    'used '
                                                                                                    'recorded '
                                                                                                    'in the '
                                                                                                    'post-procedure '
                                                                                                    'iWeb '
                                                                                                    'coronary '
                                                                                                    'record.',
                                                          'post_enzymes_postop': 'Enzymes post-operative '
                                                                                 'recorded in the '
                                                                                 'post-procedure iWeb '
                                                                                 'coronary record.',
                                                          'post_ethnic_origin': 'Ethnic origin recorded in '
                                                                                'the post-procedure iWeb '
                                                                                'coronary record.',
                                                          'post_external_cooling_pads_blankets_wrapstxt': 'Free-text '
                                                                                                          'external '
                                                                                                          'cooling '
                                                                                                          'pads '
                                                                                                          'blankets '
                                                                                                          'wraps '
                                                                                                          'recorded '
                                                                                                          'in '
                                                                                                          'the '
                                                                                                          'post-procedure '
                                                                                                          'iWeb '
                                                                                                          'coronary '
                                                                                                          'record.',
                                                          'post_flow_in_ira_at_angio': 'Flow in '
                                                                                       'infarct-related '
                                                                                       'artery at angio '
                                                                                       'recorded in the '
                                                                                       'post-procedure iWeb '
                                                                                       'coronary record.',
                                                          'post_flow_in_ira_postopacs': 'Flow in '
                                                                                        'infarct-related '
                                                                                        'artery post '
                                                                                        'operativeacs '
                                                                                        'recorded in the '
                                                                                        'post-procedure iWeb '
                                                                                        'coronary record.',
                                                          'post_gender': 'Gender recorded in the '
                                                                         'post-procedure iWeb coronary '
                                                                         'record.',
                                                          'post_gp_iibiiia_drugs_used_during_proceduretxt': 'Glycoprotein '
                                                                                                            'IIb/IIIa '
                                                                                                            'drugs '
                                                                                                            'recorded '
                                                                                                            'as '
                                                                                                            'used '
                                                                                                            'during '
                                                                                                            'the '
                                                                                                            'coronary '
                                                                                                            'procedure.',
                                                          'post_ic_uadmission_post_pci': 'Intensive-care '
                                                                                         'admission status '
                                                                                         'after percutaneous '
                                                                                         'coronary '
                                                                                         'intervention; the '
                                                                                         'source field '
                                                                                         'retains its ICU '
                                                                                         'spelling.',
                                                          'post_indication_for_stent': 'Indication for stent '
                                                                                       'recorded in the '
                                                                                       'post-procedure iWeb '
                                                                                       'coronary record.',
                                                          'post_inopressor_use_pre_pci': 'Inopressor use pre '
                                                                                         'percutaneous '
                                                                                         'coronary '
                                                                                         'intervention (PCI) '
                                                                                         'recorded in the '
                                                                                         'post-procedure '
                                                                                         'iWeb coronary '
                                                                                         'record.',
                                                          'post_interesting_case': 'Interesting case '
                                                                                   'recorded in the '
                                                                                   'post-procedure iWeb '
                                                                                   'coronary record.',
                                                          'post_l_vgram': 'Left ventriculogram finding or '
                                                                          'status retained from the '
                                                                          'post-procedure iWeb module.',
                                                          'post_lad_other_stenosis_comment': 'Left anterior '
                                                                                             'descending '
                                                                                             'artery other '
                                                                                             'stenosis '
                                                                                             'comment '
                                                                                             'recorded in '
                                                                                             'the '
                                                                                             'post-procedure '
                                                                                             'iWeb coronary '
                                                                                             'record.',
                                                          'post_lad_other_stenosis_pre_pci': 'Left anterior '
                                                                                             'descending '
                                                                                             'artery other '
                                                                                             'stenosis pre '
                                                                                             'percutaneous '
                                                                                             'coronary '
                                                                                             'intervention '
                                                                                             '(PCI) recorded '
                                                                                             'in the '
                                                                                             'post-procedure '
                                                                                             'iWeb coronary '
                                                                                             'record.',
                                                          'post_lad_other_stenosis_procedure': 'Left '
                                                                                               'anterior '
                                                                                               'descending '
                                                                                               'artery other '
                                                                                               'stenosis '
                                                                                               'procedure '
                                                                                               'recorded in '
                                                                                               'the '
                                                                                               'post-procedure '
                                                                                               'iWeb '
                                                                                               'coronary '
                                                                                               'record.',
                                                          'post_lad_other_stenosispost_pci': 'Left anterior '
                                                                                             'descending '
                                                                                             'artery other '
                                                                                             'stenosis post '
                                                                                             'percutaneous '
                                                                                             'coronary '
                                                                                             'intervention '
                                                                                             '(PCI) recorded '
                                                                                             'in the '
                                                                                             'post-procedure '
                                                                                             'iWeb coronary '
                                                                                             'record.',
                                                          'post_lad_proximal_stenosis_comment': 'Left '
                                                                                                'anterior '
                                                                                                'descending '
                                                                                                'artery '
                                                                                                'proximal '
                                                                                                'stenosis '
                                                                                                'comment '
                                                                                                'recorded in '
                                                                                                'the '
                                                                                                'post-procedure '
                                                                                                'iWeb '
                                                                                                'coronary '
                                                                                                'record.',
                                                          'post_lad_proximal_stenosis_pre_pci': 'Left '
                                                                                                'anterior '
                                                                                                'descending '
                                                                                                'artery '
                                                                                                'proximal '
                                                                                                'stenosis '
                                                                                                'pre '
                                                                                                'percutaneous '
                                                                                                'coronary '
                                                                                                'intervention '
                                                                                                '(PCI) '
                                                                                                'recorded in '
                                                                                                'the '
                                                                                                'post-procedure '
                                                                                                'iWeb '
                                                                                                'coronary '
                                                                                                'record.',
                                                          'post_lad_proximal_stenosis_procedure': 'Left '
                                                                                                  'anterior '
                                                                                                  'descending '
                                                                                                  'artery '
                                                                                                  'proximal '
                                                                                                  'stenosis '
                                                                                                  'procedure '
                                                                                                  'recorded '
                                                                                                  'in the '
                                                                                                  'post-procedure '
                                                                                                  'iWeb '
                                                                                                  'coronary '
                                                                                                  'record.',
                                                          'post_lad_proximal_stenosispost_pci': 'Left '
                                                                                                'anterior '
                                                                                                'descending '
                                                                                                'artery '
                                                                                                'proximal '
                                                                                                'stenosis '
                                                                                                'post '
                                                                                                'percutaneous '
                                                                                                'coronary '
                                                                                                'intervention '
                                                                                                '(PCI) '
                                                                                                'recorded in '
                                                                                                'the '
                                                                                                'post-procedure '
                                                                                                'iWeb '
                                                                                                'coronary '
                                                                                                'record.',
                                                          'post_largest_ballon_stent_diameter': 'Largest '
                                                                                                'balloon '
                                                                                                'stent '
                                                                                                'diameter '
                                                                                                'recorded in '
                                                                                                'the '
                                                                                                'post-procedure '
                                                                                                'iWeb '
                                                                                                'coronary '
                                                                                                'record.',
                                                          'post_left_main_stem_protected': 'Left main stem '
                                                                                           'protected '
                                                                                           'recorded in the '
                                                                                           'post-procedure '
                                                                                           'iWeb coronary '
                                                                                           'record.',
                                                          'post_left_main_stem_stenosis_comment': 'Left main '
                                                                                                  'stem '
                                                                                                  'stenosis '
                                                                                                  'comment '
                                                                                                  'recorded '
                                                                                                  'in the '
                                                                                                  'post-procedure '
                                                                                                  'iWeb '
                                                                                                  'coronary '
                                                                                                  'record.',
                                                          'post_left_main_stem_stenosis_procedure': 'Left '
                                                                                                    'main '
                                                                                                    'stem '
                                                                                                    'stenosis '
                                                                                                    'procedure '
                                                                                                    'recorded '
                                                                                                    'in the '
                                                                                                    'post-procedure '
                                                                                                    'iWeb '
                                                                                                    'coronary '
                                                                                                    'record.',
                                                          'post_left_main_stem_stenosispost_pci': 'Left main '
                                                                                                  'stem '
                                                                                                  'stenosis '
                                                                                                  'post '
                                                                                                  'percutaneous '
                                                                                                  'coronary '
                                                                                                  'intervention '
                                                                                                  '(PCI) '
                                                                                                  'recorded '
                                                                                                  'in the '
                                                                                                  'post-procedure '
                                                                                                  'iWeb '
                                                                                                  'coronary '
                                                                                                  'record.',
                                                          'post_left_main_stem_stenosispre_pci': 'Left main '
                                                                                                 'stem '
                                                                                                 'stenosis '
                                                                                                 'pre '
                                                                                                 'percutaneous '
                                                                                                 'coronary '
                                                                                                 'intervention '
                                                                                                 '(PCI) '
                                                                                                 'recorded '
                                                                                                 'in the '
                                                                                                 'post-procedure '
                                                                                                 'iWeb '
                                                                                                 'coronary '
                                                                                                 'record.',
                                                          'post_longest_stented_treated_segment': 'Longest '
                                                                                                  'stented '
                                                                                                  'treated '
                                                                                                  'segment '
                                                                                                  'recorded '
                                                                                                  'in the '
                                                                                                  'post-procedure '
                                                                                                  'iWeb '
                                                                                                  'coronary '
                                                                                                  'record.',
                                                          'post_m_angiography_findings': 'Free-text '
                                                                                         'angiography '
                                                                                         'findings recorded '
                                                                                         'in the '
                                                                                         'post-procedure '
                                                                                         'iWeb coronary '
                                                                                         'record.',
                                                          'post_m_case_details': 'Free-text case details '
                                                                                 'recorded in the '
                                                                                 'post-procedure iWeb '
                                                                                 'coronary record.',
                                                          'post_m_details_of_all_devices_implanted': 'Free-text '
                                                                                                     'details '
                                                                                                     'of all '
                                                                                                     'devices '
                                                                                                     'implanted '
                                                                                                     'recorded '
                                                                                                     'in the '
                                                                                                     'post-procedure '
                                                                                                     'iWeb '
                                                                                                     'coronary '
                                                                                                     'record.',
                                                          'post_m_discharge_management_plan': 'Free-text '
                                                                                              'discharge '
                                                                                              'management '
                                                                                              'plan recorded '
                                                                                              'in the '
                                                                                              'post-procedure '
                                                                                              'iWeb coronary '
                                                                                              'record.',
                                                          'post_m_information_for_the_patient': 'Free-text '
                                                                                                'information '
                                                                                                'for the '
                                                                                                'patient '
                                                                                                'recorded in '
                                                                                                'the '
                                                                                                'post-procedure '
                                                                                                'iWeb '
                                                                                                'coronary '
                                                                                                'record.',
                                                          'post_m_operation_reportcomment': 'Free-text '
                                                                                            'operation '
                                                                                            'reportcomment '
                                                                                            'recorded in the '
                                                                                            'post-procedure '
                                                                                            'iWeb coronary '
                                                                                            'record.',
                                                          'post_m_pre_discharge_instructions': 'Free-text '
                                                                                               'pre '
                                                                                               'discharge '
                                                                                               'instructions '
                                                                                               'recorded in '
                                                                                               'the '
                                                                                               'post-procedure '
                                                                                               'iWeb '
                                                                                               'coronary '
                                                                                               'record.',
                                                          'post_m_procedure_findings_and_report': 'Free-text '
                                                                                                  'procedure '
                                                                                                  'findings '
                                                                                                  'and '
                                                                                                  'report '
                                                                                                  'recorded '
                                                                                                  'in the '
                                                                                                  'post-procedure '
                                                                                                  'iWeb '
                                                                                                  'coronary '
                                                                                                  'record.',
                                                          'post_management_plan': 'Management plan recorded '
                                                                                  'in the post-procedure '
                                                                                  'iWeb coronary record.',
                                                          'post_mg_pinfo': 'Mg pinfo recorded in the '
                                                                           'post-procedure iWeb coronary '
                                                                           'record.',
                                                          'post_number_coronary_grafts_patent_postop': 'Number '
                                                                                                       'coronary '
                                                                                                       'grafts '
                                                                                                       'patent '
                                                                                                       'post-operative '
                                                                                                       'recorded '
                                                                                                       'in '
                                                                                                       'the '
                                                                                                       'post-procedure '
                                                                                                       'iWeb '
                                                                                                       'coronary '
                                                                                                       'record.',
                                                          'post_number_grafts_patent_pre_pci': 'Number '
                                                                                               'grafts '
                                                                                               'patent pre '
                                                                                               'percutaneous '
                                                                                               'coronary '
                                                                                               'intervention '
                                                                                               '(PCI) '
                                                                                               'recorded in '
                                                                                               'the '
                                                                                               'post-procedure '
                                                                                               'iWeb '
                                                                                               'coronary '
                                                                                               'record.',
                                                          'post_number_grafts_presentangio': 'Number grafts '
                                                                                             'present angio '
                                                                                             'recorded in '
                                                                                             'the '
                                                                                             'post-procedure '
                                                                                             'iWeb coronary '
                                                                                             'record.',
                                                          'post_number_instent_stenoses_attempted': 'Number '
                                                                                                    'of '
                                                                                                    'in-stent '
                                                                                                    'stenoses '
                                                                                                    'attempted '
                                                                                                    'recorded '
                                                                                                    'in the '
                                                                                                    'post-procedure '
                                                                                                    'iWeb '
                                                                                                    'coronary '
                                                                                                    'record.',
                                                          'post_number_of_chronic_occlusions_attempted': 'Number '
                                                                                                         'of '
                                                                                                         'chronic '
                                                                                                         'occlusions '
                                                                                                         'attempted '
                                                                                                         'recorded '
                                                                                                         'in '
                                                                                                         'the '
                                                                                                         'post-procedure '
                                                                                                         'iWeb '
                                                                                                         'coronary '
                                                                                                         'record.',
                                                          'post_number_of_drug_eluting_stents_used': 'Number '
                                                                                                     'of '
                                                                                                     'drug '
                                                                                                     'eluting '
                                                                                                     'stents '
                                                                                                     'used '
                                                                                                     'recorded '
                                                                                                     'in the '
                                                                                                     'post-procedure '
                                                                                                     'iWeb '
                                                                                                     'coronary '
                                                                                                     'record.',
                                                          'post_number_of_lesions_attempted': 'Number of '
                                                                                              'lesions '
                                                                                              'attempted '
                                                                                              'recorded in '
                                                                                              'the '
                                                                                              'post-procedure '
                                                                                              'iWeb coronary '
                                                                                              'record.',
                                                          'post_number_of_lesions_successful': 'Number of '
                                                                                               'lesions '
                                                                                               'successful '
                                                                                               'recorded in '
                                                                                               'the '
                                                                                               'post-procedure '
                                                                                               'iWeb '
                                                                                               'coronary '
                                                                                               'record.',
                                                          'post_number_of_vessels_attemptednot_epicardial_territories': 'Number '
                                                                                                                        'of '
                                                                                                                        'vessels '
                                                                                                                        'attempted '
                                                                                                                        'not '
                                                                                                                        'epicardial '
                                                                                                                        'territories '
                                                                                                                        'recorded '
                                                                                                                        'in '
                                                                                                                        'the '
                                                                                                                        'post-procedure '
                                                                                                                        'iWeb '
                                                                                                                        'coronary '
                                                                                                                        'record.',
                                                          'post_number_restenoses_attempted': 'Number of '
                                                                                              'restenoses '
                                                                                              'attempted '
                                                                                              'recorded in '
                                                                                              'the '
                                                                                              'post-procedure '
                                                                                              'iWeb coronary '
                                                                                              'record.',
                                                          'post_number_stents_used': 'Number stents used '
                                                                                     'recorded in the '
                                                                                     'post-procedure iWeb '
                                                                                     'coronary record.',
                                                          'post_nurse_led_discharge': 'Nurse led discharge '
                                                                                      'recorded in the '
                                                                                      'post-procedure iWeb '
                                                                                      'coronary record.',
                                                          'post_other_management_plan': 'Other management '
                                                                                        'plan recorded in '
                                                                                        'the post-procedure '
                                                                                        'iWeb coronary '
                                                                                        'record.',
                                                          'post_patient_status_during_transfer_to_theatre': 'Patient '
                                                                                                            'status '
                                                                                                            'during '
                                                                                                            'transfer '
                                                                                                            'to '
                                                                                                            'theatre '
                                                                                                            'recorded '
                                                                                                            'in '
                                                                                                            'the '
                                                                                                            'post-procedure '
                                                                                                            'iWeb '
                                                                                                            'coronary '
                                                                                                            'record.',
                                                          'post_pci_for_stent_thrombosis': 'Percutaneous '
                                                                                           'coronary '
                                                                                           'intervention '
                                                                                           '(PCI) for stent '
                                                                                           'thrombosis '
                                                                                           'recorded in the '
                                                                                           'post-procedure '
                                                                                           'iWeb coronary '
                                                                                           'record.',
                                                          'post_pci_hospital_outcometxt': 'Free-text '
                                                                                          'percutaneous '
                                                                                          'coronary '
                                                                                          'intervention '
                                                                                          '(PCI) hospital '
                                                                                          'outcome recorded '
                                                                                          'in the '
                                                                                          'post-procedure '
                                                                                          'iWeb coronary '
                                                                                          'record.',
                                                          'post_peak_lactate': 'Peak lactate recorded in the '
                                                                               'post-procedure iWeb coronary '
                                                                               'record.',
                                                          'post_peak_noradrenaline_dose_during_cath_lab': 'Peak '
                                                                                                          'noradrenaline '
                                                                                                          'dose '
                                                                                                          'during '
                                                                                                          'cath '
                                                                                                          'lab '
                                                                                                          'recorded '
                                                                                                          'in '
                                                                                                          'the '
                                                                                                          'post-procedure '
                                                                                                          'iWeb '
                                                                                                          'coronary '
                                                                                                          'record.',
                                                          'post_primary_operator_gmc_numbertxt': 'Free-text '
                                                                                                 'primary '
                                                                                                 'operator '
                                                                                                 'General '
                                                                                                 'Medical '
                                                                                                 'Council '
                                                                                                 'number '
                                                                                                 'recorded '
                                                                                                 'in the '
                                                                                                 'post-procedure '
                                                                                                 'iWeb '
                                                                                                 'coronary '
                                                                                                 'record.',
                                                          'post_procedural_complicationtxt': 'Free-text '
                                                                                             'procedural '
                                                                                             'complication '
                                                                                             'recorded in '
                                                                                             'the '
                                                                                             'post-procedure '
                                                                                             'iWeb coronary '
                                                                                             'record.',
                                                          'post_procedural_devices_used_detailtxt': 'Free-text '
                                                                                                    'procedural '
                                                                                                    'devices '
                                                                                                    'used '
                                                                                                    'detail '
                                                                                                    'recorded '
                                                                                                    'in the '
                                                                                                    'post-procedure '
                                                                                                    'iWeb '
                                                                                                    'coronary '
                                                                                                    'record.',
                                                          'post_procedural_devices_usedtxt': 'Free-text '
                                                                                             'procedural '
                                                                                             'devices used '
                                                                                             'recorded in '
                                                                                             'the '
                                                                                             'post-procedure '
                                                                                             'iWeb coronary '
                                                                                             'record.',
                                                          'post_rca_stenosis_comment': 'Right coronary '
                                                                                       'artery stenosis '
                                                                                       'comment recorded in '
                                                                                       'the post-procedure '
                                                                                       'iWeb coronary '
                                                                                       'record.',
                                                          'post_rca_stenosis_pre_pci': 'Right coronary '
                                                                                       'artery stenosis pre '
                                                                                       'percutaneous '
                                                                                       'coronary '
                                                                                       'intervention (PCI) '
                                                                                       'recorded in the '
                                                                                       'post-procedure iWeb '
                                                                                       'coronary record.',
                                                          'post_rca_stenosis_procedure': 'Right coronary '
                                                                                         'artery stenosis '
                                                                                         'procedure recorded '
                                                                                         'in the '
                                                                                         'post-procedure '
                                                                                         'iWeb coronary '
                                                                                         'record.',
                                                          'post_rca_stenosispost_pci': 'Right coronary '
                                                                                       'artery stenosis post '
                                                                                       'percutaneous '
                                                                                       'coronary '
                                                                                       'intervention (PCI) '
                                                                                       'recorded in the '
                                                                                       'post-procedure iWeb '
                                                                                       'coronary record.',
                                                          'post_sca_istage_post_cci': 'SCAI '
                                                                                      'cardiogenic-shock '
                                                                                      'stage recorded after '
                                                                                      'coronary '
                                                                                      'intervention; the '
                                                                                      'source field retains '
                                                                                      'its CCI spelling.',
                                                          'post_sca_istage_pre_pci': 'SCAI cardiogenic-shock '
                                                                                     'stage recorded before '
                                                                                     'percutaneous coronary '
                                                                                     'intervention.',
                                                          'post_second_operator_gmc_numbertxt': 'Free-text '
                                                                                                'second '
                                                                                                'operator '
                                                                                                'General '
                                                                                                'Medical '
                                                                                                'Council '
                                                                                                'number '
                                                                                                'recorded in '
                                                                                                'the '
                                                                                                'post-procedure '
                                                                                                'iWeb '
                                                                                                'coronary '
                                                                                                'record.',
                                                          'post_status_at_discharge': 'Status at discharge '
                                                                                      'recorded in the '
                                                                                      'post-procedure iWeb '
                                                                                      'coronary record.',
                                                          'post_therapeutic_interventions_undertakentxt': 'Free-text '
                                                                                                          'therapeutic '
                                                                                                          'interventions '
                                                                                                          'undertaken '
                                                                                                          'recorded '
                                                                                                          'in '
                                                                                                          'the '
                                                                                                          'post-procedure '
                                                                                                          'iWeb '
                                                                                                          'coronary '
                                                                                                          'record.',
                                                          'post_third_operator_gmc_numbertxt': 'Free-text '
                                                                                               'third '
                                                                                               'operator '
                                                                                               'General '
                                                                                               'Medical '
                                                                                               'Council '
                                                                                               'number '
                                                                                               'recorded in '
                                                                                               'the '
                                                                                               'post-procedure '
                                                                                               'iWeb '
                                                                                               'coronary '
                                                                                               'record.',
                                                          'post_time_to_bypass': 'Time to bypass recorded in '
                                                                                 'the post-procedure iWeb '
                                                                                 'coronary record.',
                                                          'post_trough_sys_bp': 'Trough sys bp recorded in '
                                                                                'the post-procedure iWeb '
                                                                                'coronary record.',
                                                          'post_troughp_h': 'Lowest recorded pH associated '
                                                                            'with the coronary procedure.',
                                                          'post_ventilation': 'Ventilation recorded in the '
                                                                              'post-procedure iWeb coronary '
                                                                              'record.',
                                                          'post_vessels_attemptedtxt': 'Free-text vessels '
                                                                                       'attempted recorded '
                                                                                       'in the '
                                                                                       'post-procedure iWeb '
                                                                                       'coronary record.',
                                                          'post_why_no_iibiiia_during_procedure': 'Why no '
                                                                                                  'IIb/IIIa '
                                                                                                  'during '
                                                                                                  'procedure '
                                                                                                  'recorded '
                                                                                                  'in the '
                                                                                                  'post-procedure '
                                                                                                  'iWeb '
                                                                                                  'coronary '
                                                                                                  'record.',
                                                          'pre_adc_updt': 'Landing timestamp carried by the '
                                                                          'pre-procedure iWeb coronary '
                                                                          'source record.',
                                                          'pre_administrative_category': 'Administrative '
                                                                                         'category recorded '
                                                                                         'in the '
                                                                                         'pre-procedure iWeb '
                                                                                         'coronary '
                                                                                         'assessment.',
                                                          'pre_admission_routeacs_only': 'Admission routeacs '
                                                                                         'only recorded in '
                                                                                         'the pre-procedure '
                                                                                         'iWeb coronary '
                                                                                         'assessment.',
                                                          'pre_age': 'Age recorded in the pre-procedure iWeb '
                                                                     'coronary assessment.',
                                                          'pre_arterial_accesstxt': 'Free-text arterial '
                                                                                    'access recorded in the '
                                                                                    'pre-procedure iWeb '
                                                                                    'coronary assessment.',
                                                          'pre_arterial_blood_gas_on_arrival_in_cath_lab_base_excess': 'Arterial '
                                                                                                                       'blood '
                                                                                                                       'gas '
                                                                                                                       'on '
                                                                                                                       'arrival '
                                                                                                                       'in '
                                                                                                                       'cath '
                                                                                                                       'lab '
                                                                                                                       'base '
                                                                                                                       'excess '
                                                                                                                       'recorded '
                                                                                                                       'in '
                                                                                                                       'the '
                                                                                                                       'pre-procedure '
                                                                                                                       'iWeb '
                                                                                                                       'coronary '
                                                                                                                       'assessment.',
                                                          'pre_arterial_blood_gas_on_arrival_in_cath_lab_lactate': 'Arterial '
                                                                                                                   'blood '
                                                                                                                   'gas '
                                                                                                                   'on '
                                                                                                                   'arrival '
                                                                                                                   'in '
                                                                                                                   'cath '
                                                                                                                   'lab '
                                                                                                                   'lactate '
                                                                                                                   'recorded '
                                                                                                                   'in '
                                                                                                                   'the '
                                                                                                                   'pre-procedure '
                                                                                                                   'iWeb '
                                                                                                                   'coronary '
                                                                                                                   'assessment.',
                                                          'pre_arterial_blood_gas_on_arrival_in_cath_lab_ph': 'Arterial '
                                                                                                              'blood '
                                                                                                              'gas '
                                                                                                              'on '
                                                                                                              'arrival '
                                                                                                              'in '
                                                                                                              'cath '
                                                                                                              'lab '
                                                                                                              'pH '
                                                                                                              'recorded '
                                                                                                              'in '
                                                                                                              'the '
                                                                                                              'pre-procedure '
                                                                                                              'iWeb '
                                                                                                              'coronary '
                                                                                                              'assessment.',
                                                          'pre_arterial_managementtxt': 'Free-text arterial '
                                                                                        'management recorded '
                                                                                        'in the '
                                                                                        'pre-procedure iWeb '
                                                                                        'coronary '
                                                                                        'assessment.',
                                                          'pre_body_mass_index': 'Body mass index recorded '
                                                                                 'in the pre-procedure iWeb '
                                                                                 'coronary assessment.',
                                                          'pre_body_surface_area': 'Body surface area '
                                                                                   'recorded in the '
                                                                                   'pre-procedure iWeb '
                                                                                   'coronary assessment.',
                                                          'pre_cardiac_enzymesmarkers_raised': 'Cardiac '
                                                                                               'enzymes '
                                                                                               'markers '
                                                                                               'raised '
                                                                                               'recorded in '
                                                                                               'the '
                                                                                               'pre-procedure '
                                                                                               'iWeb '
                                                                                               'coronary '
                                                                                               'assessment.',
                                                          'pre_cardiogenic_shockpre_procedure': 'Cardiogenic '
                                                                                                'shockpre '
                                                                                                'procedure '
                                                                                                'recorded in '
                                                                                                'the '
                                                                                                'pre-procedure '
                                                                                                'iWeb '
                                                                                                'coronary '
                                                                                                'assessment.',
                                                          'pre_cath_devicestxt': 'Free-text cath devices '
                                                                                 'recorded in the '
                                                                                 'pre-procedure iWeb '
                                                                                 'coronary assessment.',
                                                          'pre_cath_lab_number': 'Cath lab number recorded '
                                                                                 'in the pre-procedure iWeb '
                                                                                 'coronary assessment.',
                                                          'pre_cath_proceduretxt': 'Free-text cath procedure '
                                                                                   'recorded in the '
                                                                                   'pre-procedure iWeb '
                                                                                   'coronary assessment.',
                                                          'pre_ccs_angina_statuspre_procedure_stable_only': 'Canadian '
                                                                                                            'Cardiovascular '
                                                                                                            'Society '
                                                                                                            'angina '
                                                                                                            'status '
                                                                                                            'before '
                                                                                                            'the '
                                                                                                            'procedure '
                                                                                                            'for '
                                                                                                            'stable '
                                                                                                            'presentations.',
                                                          'pre_cholesterol': 'Cholesterol recorded in the '
                                                                             'pre-procedure iWeb coronary '
                                                                             'assessment.',
                                                          'pre_circulatory_supporttxt': 'Free-text '
                                                                                        'circulatory support '
                                                                                        'recorded in the '
                                                                                        'pre-procedure iWeb '
                                                                                        'coronary '
                                                                                        'assessment.',
                                                          'pre_clinical_frailty_scale': 'Clinical frailty '
                                                                                        'scale recorded in '
                                                                                        'the pre-procedure '
                                                                                        'iWeb coronary '
                                                                                        'assessment.',
                                                          'pre_clinical_frailty_scale_image': 'Clinical '
                                                                                              'frailty scale '
                                                                                              'image '
                                                                                              'recorded in '
                                                                                              'the '
                                                                                              'pre-procedure '
                                                                                              'iWeb coronary '
                                                                                              'assessment.',
                                                          'pre_clinical_syndromepci': 'Clinical syndrome '
                                                                                      'percutaneous coronary '
                                                                                      'intervention (PCI) '
                                                                                      'recorded in the '
                                                                                      'pre-procedure iWeb '
                                                                                      'coronary assessment.',
                                                          'pre_consultant_responsible_for_proceduretxt': 'Free-text '
                                                                                                         'consultant '
                                                                                                         'responsible '
                                                                                                         'for '
                                                                                                         'procedure '
                                                                                                         'recorded '
                                                                                                         'in '
                                                                                                         'the '
                                                                                                         'pre-procedure '
                                                                                                         'iWeb '
                                                                                                         'coronary '
                                                                                                         'assessment.',
                                                          'pre_convalescent_stem_ior_nstemi': 'Convalescent '
                                                                                              'stem or '
                                                                                              'non-ST-elevation '
                                                                                              'myocardial '
                                                                                              'infarction '
                                                                                              '(NSTEMI) '
                                                                                              'recorded in '
                                                                                              'the '
                                                                                              'pre-procedure '
                                                                                              'iWeb coronary '
                                                                                              'assessment.',
                                                          'pre_coronary_dominance': 'Coronary dominance '
                                                                                    'recorded in the '
                                                                                    'pre-procedure iWeb '
                                                                                    'coronary assessment.',
                                                          'pre_date_and_time_on_table': 'Date and time on '
                                                                                        'table recorded in '
                                                                                        'the pre-procedure '
                                                                                        'iWeb coronary '
                                                                                        'assessment.',
                                                          'pre_date_and_time_on_table_clean': 'Pre-procedure '
                                                                                              'date and time '
                                                                                              'on table '
                                                                                              'after '
                                                                                              'date-quality '
                                                                                              'cleaning; '
                                                                                              'null when '
                                                                                              'missing, '
                                                                                              'future-dated '
                                                                                              'or a '
                                                                                              'configured '
                                                                                              'sentinel.',
                                                          'pre_date_and_time_on_table_future_ind': 'True '
                                                                                                   'when the '
                                                                                                   'pre-procedure '
                                                                                                   'date and '
                                                                                                   'time on '
                                                                                                   'table is '
                                                                                                   'later '
                                                                                                   'than the '
                                                                                                   'pipeline '
                                                                                                   'evaluation '
                                                                                                   'time.',
                                                          'pre_date_and_time_on_table_sentinel_ind': 'True '
                                                                                                     'when '
                                                                                                     'the '
                                                                                                     'pre-procedure '
                                                                                                     'date '
                                                                                                     'and '
                                                                                                     'time '
                                                                                                     'on '
                                                                                                     'table '
                                                                                                     'matches '
                                                                                                     'a '
                                                                                                     'configured '
                                                                                                     'sentinel '
                                                                                                     'date '
                                                                                                     'or '
                                                                                                     'timestamp.',
                                                          'pre_date_last_changed': 'Source edit timestamp '
                                                                                   'from the pre-procedure '
                                                                                   'iWeb coronary record; it '
                                                                                   'is not a clinical event '
                                                                                   'time.',
                                                          'pre_date_of_death': 'Date of death recorded in '
                                                                               'the pre-procedure iWeb '
                                                                               'coronary assessment.',
                                                          'pre_date_of_death_clean': 'Pre-procedure date of '
                                                                                     'death after '
                                                                                     'date-quality cleaning; '
                                                                                     'null when missing, '
                                                                                     'future-dated or a '
                                                                                     'configured sentinel.',
                                                          'pre_date_of_death_future_ind': 'True when the '
                                                                                          'pre-procedure '
                                                                                          'date of death is '
                                                                                          'later than the '
                                                                                          'pipeline '
                                                                                          'evaluation time.',
                                                          'pre_date_of_death_sentinel_ind': 'True when the '
                                                                                            'pre-procedure '
                                                                                            'date of death '
                                                                                            'matches a '
                                                                                            'configured '
                                                                                            'sentinel date '
                                                                                            'or timestamp.',
                                                          'pre_date_time_arrival_at_pci_hospital': 'Date and '
                                                                                                   'time '
                                                                                                   'arrival '
                                                                                                   'at '
                                                                                                   'percutaneous '
                                                                                                   'coronary '
                                                                                                   'intervention '
                                                                                                   '(PCI) '
                                                                                                   'hospital '
                                                                                                   'recorded '
                                                                                                   'in the '
                                                                                                   'pre-procedure '
                                                                                                   'iWeb '
                                                                                                   'coronary '
                                                                                                   'assessment.',
                                                          'pre_date_time_arrival_at_pci_hospital_clean': 'Pre-procedure '
                                                                                                         'date '
                                                                                                         'and '
                                                                                                         'time '
                                                                                                         'arrival '
                                                                                                         'at '
                                                                                                         'percutaneous '
                                                                                                         'coronary '
                                                                                                         'intervention '
                                                                                                         '(PCI) '
                                                                                                         'hospital '
                                                                                                         'after '
                                                                                                         'date-quality '
                                                                                                         'cleaning; '
                                                                                                         'null '
                                                                                                         'when '
                                                                                                         'missing, '
                                                                                                         'future-dated '
                                                                                                         'or '
                                                                                                         'a '
                                                                                                         'configured '
                                                                                                         'sentinel.',
                                                          'pre_date_time_arrival_at_pci_hospital_future_ind': 'True '
                                                                                                              'when '
                                                                                                              'the '
                                                                                                              'pre-procedure '
                                                                                                              'date '
                                                                                                              'and '
                                                                                                              'time '
                                                                                                              'arrival '
                                                                                                              'at '
                                                                                                              'percutaneous '
                                                                                                              'coronary '
                                                                                                              'intervention '
                                                                                                              '(PCI) '
                                                                                                              'hospital '
                                                                                                              'is '
                                                                                                              'later '
                                                                                                              'than '
                                                                                                              'the '
                                                                                                              'pipeline '
                                                                                                              'evaluation '
                                                                                                              'time.',
                                                          'pre_date_time_arrival_at_pci_hospital_sentinel_ind': 'True '
                                                                                                                'when '
                                                                                                                'the '
                                                                                                                'pre-procedure '
                                                                                                                'date '
                                                                                                                'and '
                                                                                                                'time '
                                                                                                                'arrival '
                                                                                                                'at '
                                                                                                                'percutaneous '
                                                                                                                'coronary '
                                                                                                                'intervention '
                                                                                                                '(PCI) '
                                                                                                                'hospital '
                                                                                                                'matches '
                                                                                                                'a '
                                                                                                                'configured '
                                                                                                                'sentinel '
                                                                                                                'date '
                                                                                                                'or '
                                                                                                                'timestamp.',
                                                          'pre_date_time_of_call_for_helpstemi': 'Date and '
                                                                                                 'time of '
                                                                                                 'call for '
                                                                                                 'help '
                                                                                                 'ST-elevation '
                                                                                                 'myocardial '
                                                                                                 'infarction '
                                                                                                 '(STEMI) '
                                                                                                 'recorded '
                                                                                                 'in the '
                                                                                                 'pre-procedure '
                                                                                                 'iWeb '
                                                                                                 'coronary '
                                                                                                 'assessment.',
                                                          'pre_date_time_of_call_for_helpstemi_clean': 'Pre-procedure '
                                                                                                       'date '
                                                                                                       'and '
                                                                                                       'time '
                                                                                                       'of '
                                                                                                       'call '
                                                                                                       'for '
                                                                                                       'help '
                                                                                                       'ST-elevation '
                                                                                                       'myocardial '
                                                                                                       'infarction '
                                                                                                       '(STEMI) '
                                                                                                       'after '
                                                                                                       'date-quality '
                                                                                                       'cleaning; '
                                                                                                       'null '
                                                                                                       'when '
                                                                                                       'missing, '
                                                                                                       'future-dated '
                                                                                                       'or a '
                                                                                                       'configured '
                                                                                                       'sentinel.',
                                                          'pre_date_time_of_call_for_helpstemi_future_ind': 'True '
                                                                                                            'when '
                                                                                                            'the '
                                                                                                            'pre-procedure '
                                                                                                            'date '
                                                                                                            'and '
                                                                                                            'time '
                                                                                                            'of '
                                                                                                            'call '
                                                                                                            'for '
                                                                                                            'help '
                                                                                                            'ST-elevation '
                                                                                                            'myocardial '
                                                                                                            'infarction '
                                                                                                            '(STEMI) '
                                                                                                            'is '
                                                                                                            'later '
                                                                                                            'than '
                                                                                                            'the '
                                                                                                            'pipeline '
                                                                                                            'evaluation '
                                                                                                            'time.',
                                                          'pre_date_time_of_call_for_helpstemi_sentinel_ind': 'True '
                                                                                                              'when '
                                                                                                              'the '
                                                                                                              'pre-procedure '
                                                                                                              'date '
                                                                                                              'and '
                                                                                                              'time '
                                                                                                              'of '
                                                                                                              'call '
                                                                                                              'for '
                                                                                                              'help '
                                                                                                              'ST-elevation '
                                                                                                              'myocardial '
                                                                                                              'infarction '
                                                                                                              '(STEMI) '
                                                                                                              'matches '
                                                                                                              'a '
                                                                                                              'configured '
                                                                                                              'sentinel '
                                                                                                              'date '
                                                                                                              'or '
                                                                                                              'timestamp.',
                                                          'pre_date_time_shortcuts_raw': 'Verbatim '
                                                                                         'pre-procedure '
                                                                                         'date/time shortcut '
                                                                                         'value before '
                                                                                         'timestamp parsing.',
                                                          'pre_date_time_shortcuts_ts': 'Timestamp parsed '
                                                                                        'from the '
                                                                                        'pre-procedure '
                                                                                        'date/time shortcut '
                                                                                        'source value.',
                                                          'pre_date_time_shortcuts_ts_clean': 'Pre-procedure '
                                                                                              'date and time '
                                                                                              'shortcuts ts '
                                                                                              'after '
                                                                                              'date-quality '
                                                                                              'cleaning; '
                                                                                              'null when '
                                                                                              'missing, '
                                                                                              'future-dated '
                                                                                              'or a '
                                                                                              'configured '
                                                                                              'sentinel.',
                                                          'pre_date_time_shortcuts_ts_future_ind': 'True '
                                                                                                   'when the '
                                                                                                   'pre-procedure '
                                                                                                   'date and '
                                                                                                   'time '
                                                                                                   'shortcuts '
                                                                                                   'ts is '
                                                                                                   'later '
                                                                                                   'than the '
                                                                                                   'pipeline '
                                                                                                   'evaluation '
                                                                                                   'time.',
                                                          'pre_date_time_shortcuts_ts_sentinel_ind': 'True '
                                                                                                     'when '
                                                                                                     'the '
                                                                                                     'pre-procedure '
                                                                                                     'date '
                                                                                                     'and '
                                                                                                     'time '
                                                                                                     'shortcuts '
                                                                                                     'ts '
                                                                                                     'matches '
                                                                                                     'a '
                                                                                                     'configured '
                                                                                                     'sentinel '
                                                                                                     'date '
                                                                                                     'or '
                                                                                                     'timestamp.',
                                                          'pre_dateandtimeofoperation': 'Date and '
                                                                                        'timeofoperation '
                                                                                        'recorded in the '
                                                                                        'pre-procedure iWeb '
                                                                                        'coronary '
                                                                                        'assessment.',
                                                          'pre_dateandtimeofoperation_clean': 'Pre-procedure '
                                                                                              'date and '
                                                                                              'timeofoperation '
                                                                                              'after '
                                                                                              'date-quality '
                                                                                              'cleaning; '
                                                                                              'null when '
                                                                                              'missing, '
                                                                                              'future-dated '
                                                                                              'or a '
                                                                                              'configured '
                                                                                              'sentinel.',
                                                          'pre_dateandtimeofoperation_future_ind': 'True '
                                                                                                   'when the '
                                                                                                   'pre-procedure '
                                                                                                   'date and '
                                                                                                   'timeofoperation '
                                                                                                   'is later '
                                                                                                   'than the '
                                                                                                   'pipeline '
                                                                                                   'evaluation '
                                                                                                   'time.',
                                                          'pre_dateandtimeofoperation_sentinel_ind': 'True '
                                                                                                     'when '
                                                                                                     'the '
                                                                                                     'pre-procedure '
                                                                                                     'date '
                                                                                                     'and '
                                                                                                     'timeofoperation '
                                                                                                     'matches '
                                                                                                     'a '
                                                                                                     'configured '
                                                                                                     'sentinel '
                                                                                                     'date '
                                                                                                     'or '
                                                                                                     'timestamp.',
                                                          'pre_datetime_arrival_at_first_hospitalacs_only': 'Date '
                                                                                                            'and '
                                                                                                            'time '
                                                                                                            'arrival '
                                                                                                            'at '
                                                                                                            'first '
                                                                                                            'hospital '
                                                                                                            'acute '
                                                                                                            'coronary '
                                                                                                            'syndrome '
                                                                                                            '(ACS) '
                                                                                                            'only '
                                                                                                            'recorded '
                                                                                                            'in '
                                                                                                            'the '
                                                                                                            'pre-procedure '
                                                                                                            'iWeb '
                                                                                                            'coronary '
                                                                                                            'assessment.',
                                                          'pre_datetime_arrival_at_first_hospitalacs_only_clean': 'Pre-procedure '
                                                                                                                  'date '
                                                                                                                  'and '
                                                                                                                  'time '
                                                                                                                  'arrival '
                                                                                                                  'at '
                                                                                                                  'first '
                                                                                                                  'hospital '
                                                                                                                  'acute '
                                                                                                                  'coronary '
                                                                                                                  'syndrome '
                                                                                                                  '(ACS) '
                                                                                                                  'only '
                                                                                                                  'after '
                                                                                                                  'date-quality '
                                                                                                                  'cleaning; '
                                                                                                                  'null '
                                                                                                                  'when '
                                                                                                                  'missing, '
                                                                                                                  'future-dated '
                                                                                                                  'or '
                                                                                                                  'a '
                                                                                                                  'configured '
                                                                                                                  'sentinel.',
                                                          'pre_datetime_arrival_at_first_hospitalacs_only_future_ind': 'True '
                                                                                                                       'when '
                                                                                                                       'the '
                                                                                                                       'pre-procedure '
                                                                                                                       'date '
                                                                                                                       'and '
                                                                                                                       'time '
                                                                                                                       'arrival '
                                                                                                                       'at '
                                                                                                                       'first '
                                                                                                                       'hospital '
                                                                                                                       'acute '
                                                                                                                       'coronary '
                                                                                                                       'syndrome '
                                                                                                                       '(ACS) '
                                                                                                                       'only '
                                                                                                                       'is '
                                                                                                                       'later '
                                                                                                                       'than '
                                                                                                                       'the '
                                                                                                                       'pipeline '
                                                                                                                       'evaluation '
                                                                                                                       'time.',
                                                          'pre_datetime_arrival_at_first_hospitalacs_only_sentinel_ind': 'True '
                                                                                                                         'when '
                                                                                                                         'the '
                                                                                                                         'pre-procedure '
                                                                                                                         'date '
                                                                                                                         'and '
                                                                                                                         'time '
                                                                                                                         'arrival '
                                                                                                                         'at '
                                                                                                                         'first '
                                                                                                                         'hospital '
                                                                                                                         'acute '
                                                                                                                         'coronary '
                                                                                                                         'syndrome '
                                                                                                                         '(ACS) '
                                                                                                                         'only '
                                                                                                                         'matches '
                                                                                                                         'a '
                                                                                                                         'configured '
                                                                                                                         'sentinel '
                                                                                                                         'date '
                                                                                                                         'or '
                                                                                                                         'timestamp.',
                                                          'pre_datetime_of_ecg_triggering_ppci_pathway': 'Date '
                                                                                                         'and '
                                                                                                         'time '
                                                                                                         'of '
                                                                                                         'electrocardiogram '
                                                                                                         '(ECG) '
                                                                                                         'triggering '
                                                                                                         'primary '
                                                                                                         'percutaneous '
                                                                                                         'coronary '
                                                                                                         'intervention '
                                                                                                         '(PPCI) '
                                                                                                         'pathway '
                                                                                                         'recorded '
                                                                                                         'in '
                                                                                                         'the '
                                                                                                         'pre-procedure '
                                                                                                         'iWeb '
                                                                                                         'coronary '
                                                                                                         'assessment.',
                                                          'pre_datetime_of_ecg_triggering_ppci_pathway_clean': 'Pre-procedure '
                                                                                                               'date '
                                                                                                               'and '
                                                                                                               'time '
                                                                                                               'of '
                                                                                                               'electrocardiogram '
                                                                                                               '(ECG) '
                                                                                                               'triggering '
                                                                                                               'primary '
                                                                                                               'percutaneous '
                                                                                                               'coronary '
                                                                                                               'intervention '
                                                                                                               '(PPCI) '
                                                                                                               'pathway '
                                                                                                               'after '
                                                                                                               'date-quality '
                                                                                                               'cleaning; '
                                                                                                               'null '
                                                                                                               'when '
                                                                                                               'missing, '
                                                                                                               'future-dated '
                                                                                                               'or '
                                                                                                               'a '
                                                                                                               'configured '
                                                                                                               'sentinel.',
                                                          'pre_datetime_of_ecg_triggering_ppci_pathway_future_ind': 'True '
                                                                                                                    'when '
                                                                                                                    'the '
                                                                                                                    'pre-procedure '
                                                                                                                    'date '
                                                                                                                    'and '
                                                                                                                    'time '
                                                                                                                    'of '
                                                                                                                    'electrocardiogram '
                                                                                                                    '(ECG) '
                                                                                                                    'triggering '
                                                                                                                    'primary '
                                                                                                                    'percutaneous '
                                                                                                                    'coronary '
                                                                                                                    'intervention '
                                                                                                                    '(PPCI) '
                                                                                                                    'pathway '
                                                                                                                    'is '
                                                                                                                    'later '
                                                                                                                    'than '
                                                                                                                    'the '
                                                                                                                    'pipeline '
                                                                                                                    'evaluation '
                                                                                                                    'time.',
                                                          'pre_datetime_of_ecg_triggering_ppci_pathway_sentinel_ind': 'True '
                                                                                                                      'when '
                                                                                                                      'the '
                                                                                                                      'pre-procedure '
                                                                                                                      'date '
                                                                                                                      'and '
                                                                                                                      'time '
                                                                                                                      'of '
                                                                                                                      'electrocardiogram '
                                                                                                                      '(ECG) '
                                                                                                                      'triggering '
                                                                                                                      'primary '
                                                                                                                      'percutaneous '
                                                                                                                      'coronary '
                                                                                                                      'intervention '
                                                                                                                      '(PPCI) '
                                                                                                                      'pathway '
                                                                                                                      'matches '
                                                                                                                      'a '
                                                                                                                      'configured '
                                                                                                                      'sentinel '
                                                                                                                      'date '
                                                                                                                      'or '
                                                                                                                      'timestamp.',
                                                          'pre_datetime_of_symptom_onsetpci_acs_only': 'Date '
                                                                                                       'and '
                                                                                                       'time '
                                                                                                       'of '
                                                                                                       'symptom '
                                                                                                       'onset '
                                                                                                       'percutaneous '
                                                                                                       'coronary '
                                                                                                       'intervention '
                                                                                                       '(PCI) '
                                                                                                       'acute '
                                                                                                       'coronary '
                                                                                                       'syndrome '
                                                                                                       '(ACS) '
                                                                                                       'only '
                                                                                                       'recorded '
                                                                                                       'in '
                                                                                                       'the '
                                                                                                       'pre-procedure '
                                                                                                       'iWeb '
                                                                                                       'coronary '
                                                                                                       'assessment.',
                                                          'pre_datetime_of_symptom_onsetpci_acs_only_clean': 'Pre-procedure '
                                                                                                             'date '
                                                                                                             'and '
                                                                                                             'time '
                                                                                                             'of '
                                                                                                             'symptom '
                                                                                                             'onset '
                                                                                                             'percutaneous '
                                                                                                             'coronary '
                                                                                                             'intervention '
                                                                                                             '(PCI) '
                                                                                                             'acute '
                                                                                                             'coronary '
                                                                                                             'syndrome '
                                                                                                             '(ACS) '
                                                                                                             'only '
                                                                                                             'after '
                                                                                                             'date-quality '
                                                                                                             'cleaning; '
                                                                                                             'null '
                                                                                                             'when '
                                                                                                             'missing, '
                                                                                                             'future-dated '
                                                                                                             'or '
                                                                                                             'a '
                                                                                                             'configured '
                                                                                                             'sentinel.',
                                                          'pre_datetime_of_symptom_onsetpci_acs_only_future_ind': 'True '
                                                                                                                  'when '
                                                                                                                  'the '
                                                                                                                  'pre-procedure '
                                                                                                                  'date '
                                                                                                                  'and '
                                                                                                                  'time '
                                                                                                                  'of '
                                                                                                                  'symptom '
                                                                                                                  'onset '
                                                                                                                  'percutaneous '
                                                                                                                  'coronary '
                                                                                                                  'intervention '
                                                                                                                  '(PCI) '
                                                                                                                  'acute '
                                                                                                                  'coronary '
                                                                                                                  'syndrome '
                                                                                                                  '(ACS) '
                                                                                                                  'only '
                                                                                                                  'is '
                                                                                                                  'later '
                                                                                                                  'than '
                                                                                                                  'the '
                                                                                                                  'pipeline '
                                                                                                                  'evaluation '
                                                                                                                  'time.',
                                                          'pre_datetime_of_symptom_onsetpci_acs_only_sentinel_ind': 'True '
                                                                                                                    'when '
                                                                                                                    'the '
                                                                                                                    'pre-procedure '
                                                                                                                    'date '
                                                                                                                    'and '
                                                                                                                    'time '
                                                                                                                    'of '
                                                                                                                    'symptom '
                                                                                                                    'onset '
                                                                                                                    'percutaneous '
                                                                                                                    'coronary '
                                                                                                                    'intervention '
                                                                                                                    '(PCI) '
                                                                                                                    'acute '
                                                                                                                    'coronary '
                                                                                                                    'syndrome '
                                                                                                                    '(ACS) '
                                                                                                                    'only '
                                                                                                                    'matches '
                                                                                                                    'a '
                                                                                                                    'configured '
                                                                                                                    'sentinel '
                                                                                                                    'date '
                                                                                                                    'or '
                                                                                                                    'timestamp.',
                                                          'pre_diabetes': 'Diabetes recorded in the '
                                                                          'pre-procedure iWeb coronary '
                                                                          'assessment.',
                                                          'pre_diabetes_type': 'Diabetes type recorded in '
                                                                               'the pre-procedure iWeb '
                                                                               'coronary assessment.',
                                                          'pre_drug_therapy_preproceduretxt': 'Free-text '
                                                                                              'drug therapy '
                                                                                              'before the '
                                                                                              'procedure '
                                                                                              'recorded in '
                                                                                              'the '
                                                                                              'pre-procedure '
                                                                                              'iWeb coronary '
                                                                                              'assessment.',
                                                          'pre_ecg_ischaemia': 'Electrocardiogram (ECG) '
                                                                               'ischaemia recorded in the '
                                                                               'pre-procedure iWeb coronary '
                                                                               'assessment.',
                                                          'pre_echo_report': 'Echo report recorded in the '
                                                                             'pre-procedure iWeb coronary '
                                                                             'assessment.',
                                                          'pre_emboli_protection_devices_usedtxt': 'Free-text '
                                                                                                   'emboli '
                                                                                                   'protection '
                                                                                                   'devices '
                                                                                                   'used '
                                                                                                   'recorded '
                                                                                                   'in the '
                                                                                                   'pre-procedure '
                                                                                                   'iWeb '
                                                                                                   'coronary '
                                                                                                   'assessment.',
                                                          'pre_ethnic_origin': 'Ethnic origin recorded in '
                                                                               'the pre-procedure iWeb '
                                                                               'coronary assessment.',
                                                          'pre_family_history_of_cad': 'Family history of '
                                                                                       'coronary artery '
                                                                                       'disease (CAD) '
                                                                                       'recorded in the '
                                                                                       'pre-procedure iWeb '
                                                                                       'coronary assessment.',
                                                          'pre_follow_onad_hoc_procedure': 'Follow onad hoc '
                                                                                           'procedure '
                                                                                           'recorded in the '
                                                                                           'pre-procedure '
                                                                                           'iWeb coronary '
                                                                                           'assessment.',
                                                          'pre_gender': 'Gender recorded in the '
                                                                        'pre-procedure iWeb coronary '
                                                                        'assessment.',
                                                          'pre_glasgow_coma_scale_on_arrival_in_cath_lab': 'Glasgow '
                                                                                                           'coma '
                                                                                                           'scale '
                                                                                                           'on '
                                                                                                           'arrival '
                                                                                                           'in '
                                                                                                           'cath '
                                                                                                           'lab '
                                                                                                           'recorded '
                                                                                                           'in '
                                                                                                           'the '
                                                                                                           'pre-procedure '
                                                                                                           'iWeb '
                                                                                                           'coronary '
                                                                                                           'assessment.',
                                                          'pre_gp_iibiiia_drugs_used_during_proceduretxt': 'Glycoprotein '
                                                                                                           'IIb/IIIa '
                                                                                                           'drugs '
                                                                                                           'recorded '
                                                                                                           'as '
                                                                                                           'used '
                                                                                                           'during '
                                                                                                           'the '
                                                                                                           'coronary '
                                                                                                           'procedure.',
                                                          'pre_height': 'Height recorded in the '
                                                                        'pre-procedure iWeb coronary '
                                                                        'assessment.',
                                                          'pre_height_of_puncture_diagram': 'Height of '
                                                                                            'puncture '
                                                                                            'diagram '
                                                                                            'recorded in the '
                                                                                            'pre-procedure '
                                                                                            'iWeb coronary '
                                                                                            'assessment.',
                                                          'pre_history_of_renal_disease1': 'History of renal '
                                                                                           'disease1 '
                                                                                           'recorded in the '
                                                                                           'pre-procedure '
                                                                                           'iWeb coronary '
                                                                                           'assessment.',
                                                          'pre_hospital_identifier': 'Hospital identifier '
                                                                                     'recorded in the '
                                                                                     'pre-procedure iWeb '
                                                                                     'coronary assessment.',
                                                          'pre_indication_for_intervention': 'Indication for '
                                                                                             'intervention '
                                                                                             'recorded in '
                                                                                             'the '
                                                                                             'pre-procedure '
                                                                                             'iWeb coronary '
                                                                                             'assessment.',
                                                          'pre_interventions': 'Interventions recorded in '
                                                                               'the pre-procedure iWeb '
                                                                               'coronary assessment.',
                                                          'pre_left_femoral_micropuncture': 'Left femoral '
                                                                                            'micropuncture '
                                                                                            'recorded in the '
                                                                                            'pre-procedure '
                                                                                            'iWeb coronary '
                                                                                            'assessment.',
                                                          'pre_left_femoral_us_sguided': 'Indicates whether '
                                                                                         'left femoral '
                                                                                         'access was '
                                                                                         'ultrasound-guided.',
                                                          'pre_left_height_of_puncture': 'Left height of '
                                                                                         'puncture recorded '
                                                                                         'in the '
                                                                                         'pre-procedure iWeb '
                                                                                         'coronary '
                                                                                         'assessment.',
                                                          'pre_local_procedure_identifier': 'Local procedure '
                                                                                            'identifier '
                                                                                            'recorded in the '
                                                                                            'pre-procedure '
                                                                                            'iWeb coronary '
                                                                                            'assessment.',
                                                          'pre_lv_ejection_fraction': 'Left-ventricular '
                                                                                      'ejection fraction '
                                                                                      'recorded in the '
                                                                                      'pre-procedure iWeb '
                                                                                      'coronary assessment.',
                                                          'pre_lv_ejection_fraction_category': 'Left-ventricular '
                                                                                               'ejection '
                                                                                               'fraction '
                                                                                               'category '
                                                                                               'recorded in '
                                                                                               'the '
                                                                                               'pre-procedure '
                                                                                               'iWeb '
                                                                                               'coronary '
                                                                                               'assessment.',
                                                          'pre_m_admission_history': 'Free-text admission '
                                                                                     'history recorded in '
                                                                                     'the pre-procedure iWeb '
                                                                                     'coronary assessment.',
                                                          'pre_m_procedure_findings': 'Free-text procedure '
                                                                                      'findings recorded in '
                                                                                      'the pre-procedure '
                                                                                      'iWeb coronary '
                                                                                      'assessment.',
                                                          'pre_medical_historytxt': 'Free-text medical '
                                                                                    'history recorded in the '
                                                                                    'pre-procedure iWeb '
                                                                                    'coronary assessment.',
                                                          'pre_nyha_dyspnoea_statuspre_procedure_stable_only': 'New '
                                                                                                               'York '
                                                                                                               'Heart '
                                                                                                               'Association '
                                                                                                               'dyspnoea '
                                                                                                               'status '
                                                                                                               'before '
                                                                                                               'the '
                                                                                                               'procedure '
                                                                                                               'for '
                                                                                                               'stable '
                                                                                                               'presentations.',
                                                          'pre_other_therapeutic_hypothermia': 'Other '
                                                                                               'therapeutic '
                                                                                               'hypothermia '
                                                                                               'recorded in '
                                                                                               'the '
                                                                                               'pre-procedure '
                                                                                               'iWeb '
                                                                                               'coronary '
                                                                                               'assessment.',
                                                          'pre_out_of_hospital_cardiac_arrest': 'Out of '
                                                                                                'hospital '
                                                                                                'cardiac '
                                                                                                'arrest '
                                                                                                'recorded in '
                                                                                                'the '
                                                                                                'pre-procedure '
                                                                                                'iWeb '
                                                                                                'coronary '
                                                                                                'assessment.',
                                                          'pre_patient_ethnic_group': 'Patient ethnic group '
                                                                                      'recorded in the '
                                                                                      'pre-procedure iWeb '
                                                                                      'coronary assessment.',
                                                          'pre_patient_location_at_time_of_stemi_onset_raw': 'Verbatim '
                                                                                                             'pre-procedure '
                                                                                                             'patient-location-at-STEMI-onset '
                                                                                                             'value '
                                                                                                             'before '
                                                                                                             'timestamp '
                                                                                                             'parsing.',
                                                          'pre_patient_location_at_time_of_stemi_onset_ts': 'Timestamp '
                                                                                                            'parsed '
                                                                                                            'from '
                                                                                                            'the '
                                                                                                            'pre-procedure '
                                                                                                            'patient-location-at-STEMI-onset '
                                                                                                            'source '
                                                                                                            'value.',
                                                          'pre_patient_location_at_time_of_stemi_onset_ts_clean': 'Pre-procedure '
                                                                                                                  'patient '
                                                                                                                  'location '
                                                                                                                  'at '
                                                                                                                  'time '
                                                                                                                  'of '
                                                                                                                  'ST-elevation '
                                                                                                                  'myocardial '
                                                                                                                  'infarction '
                                                                                                                  '(STEMI) '
                                                                                                                  'onset '
                                                                                                                  'ts '
                                                                                                                  'after '
                                                                                                                  'date-quality '
                                                                                                                  'cleaning; '
                                                                                                                  'null '
                                                                                                                  'when '
                                                                                                                  'missing, '
                                                                                                                  'future-dated '
                                                                                                                  'or '
                                                                                                                  'a '
                                                                                                                  'configured '
                                                                                                                  'sentinel.',
                                                          'pre_patient_location_at_time_of_stemi_onset_ts_future_ind': 'True '
                                                                                                                       'when '
                                                                                                                       'the '
                                                                                                                       'pre-procedure '
                                                                                                                       'patient '
                                                                                                                       'location '
                                                                                                                       'at '
                                                                                                                       'time '
                                                                                                                       'of '
                                                                                                                       'ST-elevation '
                                                                                                                       'myocardial '
                                                                                                                       'infarction '
                                                                                                                       '(STEMI) '
                                                                                                                       'onset '
                                                                                                                       'ts '
                                                                                                                       'is '
                                                                                                                       'later '
                                                                                                                       'than '
                                                                                                                       'the '
                                                                                                                       'pipeline '
                                                                                                                       'evaluation '
                                                                                                                       'time.',
                                                          'pre_patient_location_at_time_of_stemi_onset_ts_sentinel_ind': 'True '
                                                                                                                         'when '
                                                                                                                         'the '
                                                                                                                         'pre-procedure '
                                                                                                                         'patient '
                                                                                                                         'location '
                                                                                                                         'at '
                                                                                                                         'time '
                                                                                                                         'of '
                                                                                                                         'ST-elevation '
                                                                                                                         'myocardial '
                                                                                                                         'infarction '
                                                                                                                         '(STEMI) '
                                                                                                                         'onset '
                                                                                                                         'ts '
                                                                                                                         'matches '
                                                                                                                         'a '
                                                                                                                         'configured '
                                                                                                                         'sentinel '
                                                                                                                         'date '
                                                                                                                         'or '
                                                                                                                         'timestamp.',
                                                          'pre_presenting_ecgacs_only': 'Presenting ecgacs '
                                                                                        'only recorded in '
                                                                                        'the pre-procedure '
                                                                                        'iWeb coronary '
                                                                                        'assessment.',
                                                          'pre_presumed_date_time_of_arrest': 'Presumed date '
                                                                                              'and time of '
                                                                                              'arrest '
                                                                                              'recorded in '
                                                                                              'the '
                                                                                              'pre-procedure '
                                                                                              'iWeb coronary '
                                                                                              'assessment.',
                                                          'pre_presumed_date_time_of_arrest_clean': 'Pre-procedure '
                                                                                                    'presumed '
                                                                                                    'date '
                                                                                                    'and '
                                                                                                    'time of '
                                                                                                    'arrest '
                                                                                                    'after '
                                                                                                    'date-quality '
                                                                                                    'cleaning; '
                                                                                                    'null '
                                                                                                    'when '
                                                                                                    'missing, '
                                                                                                    'future-dated '
                                                                                                    'or a '
                                                                                                    'configured '
                                                                                                    'sentinel.',
                                                          'pre_presumed_date_time_of_arrest_future_ind': 'True '
                                                                                                         'when '
                                                                                                         'the '
                                                                                                         'pre-procedure '
                                                                                                         'presumed '
                                                                                                         'date '
                                                                                                         'and '
                                                                                                         'time '
                                                                                                         'of '
                                                                                                         'arrest '
                                                                                                         'is '
                                                                                                         'later '
                                                                                                         'than '
                                                                                                         'the '
                                                                                                         'pipeline '
                                                                                                         'evaluation '
                                                                                                         'time.',
                                                          'pre_presumed_date_time_of_arrest_sentinel_ind': 'True '
                                                                                                           'when '
                                                                                                           'the '
                                                                                                           'pre-procedure '
                                                                                                           'presumed '
                                                                                                           'date '
                                                                                                           'and '
                                                                                                           'time '
                                                                                                           'of '
                                                                                                           'arrest '
                                                                                                           'matches '
                                                                                                           'a '
                                                                                                           'configured '
                                                                                                           'sentinel '
                                                                                                           'date '
                                                                                                           'or '
                                                                                                           'timestamp.',
                                                          'pre_previous_cabg': 'Previous coronary artery '
                                                                               'bypass grafting (CABG) '
                                                                               'recorded in the '
                                                                               'pre-procedure iWeb coronary '
                                                                               'assessment.',
                                                          'pre_previous_mi': 'Previous mi recorded in the '
                                                                             'pre-procedure iWeb coronary '
                                                                             'assessment.',
                                                          'pre_previous_pci': 'Previous percutaneous '
                                                                              'coronary intervention (PCI) '
                                                                              'recorded in the pre-procedure '
                                                                              'iWeb coronary assessment.',
                                                          'pre_primary_operator_status': 'Primary operator '
                                                                                         'status recorded in '
                                                                                         'the pre-procedure '
                                                                                         'iWeb coronary '
                                                                                         'assessment.',
                                                          'pre_primary_operatortxt': 'Free-text primary '
                                                                                     'operator recorded in '
                                                                                     'the pre-procedure iWeb '
                                                                                     'coronary assessment.',
                                                          'pre_procedure_urgency': 'Procedure urgency '
                                                                                   'recorded in the '
                                                                                   'pre-procedure iWeb '
                                                                                   'coronary assessment.',
                                                          'pre_q_wave_on_ecg': 'Q wave on electrocardiogram '
                                                                               '(ECG) recorded in the '
                                                                               'pre-procedure iWeb coronary '
                                                                               'assessment.',
                                                          'pre_recent_lysisacs_only': 'Recent lysisacs only '
                                                                                      'recorded in the '
                                                                                      'pre-procedure iWeb '
                                                                                      'coronary assessment.',
                                                          'pre_referring_cardiologisttxt': 'Free-text '
                                                                                           'referring '
                                                                                           'cardiologist '
                                                                                           'recorded in the '
                                                                                           'pre-procedure '
                                                                                           'iWeb coronary '
                                                                                           'assessment.',
                                                          'pre_referring_hospitaltxt': 'Free-text referring '
                                                                                       'hospital recorded in '
                                                                                       'the pre-procedure '
                                                                                       'iWeb coronary '
                                                                                       'assessment.',
                                                          'pre_research_procedure': 'Research procedure '
                                                                                    'recorded in the '
                                                                                    'pre-procedure iWeb '
                                                                                    'coronary assessment.',
                                                          'pre_research_title': 'Research title recorded in '
                                                                                'the pre-procedure iWeb '
                                                                                'coronary assessment.',
                                                          'pre_right_femoral_micropuncture': 'Right femoral '
                                                                                             'micropuncture '
                                                                                             'recorded in '
                                                                                             'the '
                                                                                             'pre-procedure '
                                                                                             'iWeb coronary '
                                                                                             'assessment.',
                                                          'pre_right_femoral_us_sguided': 'Indicates whether '
                                                                                          'right femoral '
                                                                                          'access was '
                                                                                          'ultrasound-guided.',
                                                          'pre_right_height_of_puncture': 'Right height of '
                                                                                          'puncture recorded '
                                                                                          'in the '
                                                                                          'pre-procedure '
                                                                                          'iWeb coronary '
                                                                                          'assessment.',
                                                          'pre_screen_of_left_femoral_head': 'Screen of left '
                                                                                             'femoral head '
                                                                                             'recorded in '
                                                                                             'the '
                                                                                             'pre-procedure '
                                                                                             'iWeb coronary '
                                                                                             'assessment.',
                                                          'pre_screen_of_right_femoral_head': 'Screen of '
                                                                                              'right femoral '
                                                                                              'head recorded '
                                                                                              'in the '
                                                                                              'pre-procedure '
                                                                                              'iWeb coronary '
                                                                                              'assessment.',
                                                          'pre_second_operator_status': 'Second operator '
                                                                                        'status recorded in '
                                                                                        'the pre-procedure '
                                                                                        'iWeb coronary '
                                                                                        'assessment.',
                                                          'pre_second_operatortxt': 'Free-text second '
                                                                                    'operator recorded in '
                                                                                    'the pre-procedure iWeb '
                                                                                    'coronary assessment.',
                                                          'pre_serum_creatinine': 'Serum creatinine recorded '
                                                                                  'in the pre-procedure iWeb '
                                                                                  'coronary assessment.',
                                                          'pre_sex': 'Sex recorded in the pre-procedure iWeb '
                                                                     'coronary assessment.',
                                                          'pre_smoking_history': 'Smoking history recorded '
                                                                                 'in the pre-procedure iWeb '
                                                                                 'coronary assessment.',
                                                          'pre_surgical_cover': 'Surgical cover recorded in '
                                                                                'the pre-procedure iWeb '
                                                                                'coronary assessment.',
                                                          'pre_temporary_pacing_wire': 'Temporary pacing '
                                                                                       'wire recorded in the '
                                                                                       'pre-procedure iWeb '
                                                                                       'coronary assessment.',
                                                          'pre_therapeutic_hypothermiatxt': 'Free-text '
                                                                                            'therapeutic '
                                                                                            'hypothermia '
                                                                                            'recorded in the '
                                                                                            'pre-procedure '
                                                                                            'iWeb coronary '
                                                                                            'assessment.',
                                                          'pre_third_operator_status': 'Third operator '
                                                                                       'status recorded in '
                                                                                       'the pre-procedure '
                                                                                       'iWeb coronary '
                                                                                       'assessment.',
                                                          'pre_third_operatortxt': 'Free-text third operator '
                                                                                   'recorded in the '
                                                                                   'pre-procedure iWeb '
                                                                                   'coronary assessment.',
                                                          'pre_training_procedure': 'Training procedure '
                                                                                    'recorded in the '
                                                                                    'pre-procedure iWeb '
                                                                                    'coronary assessment.',
                                                          'pre_us_sguided': 'Indicates whether ultrasound '
                                                                            'guidance was recorded for '
                                                                            'vascular access before the '
                                                                            'procedure.',
                                                          'pre_ventilated_preop': 'Ventilated pre-operative '
                                                                                  'recorded in the '
                                                                                  'pre-procedure iWeb '
                                                                                  'coronary assessment.',
                                                          'pre_ventilation': 'Ventilation recorded in the '
                                                                             'pre-procedure iWeb coronary '
                                                                             'assessment.',
                                                          'pre_weight': 'Weight recorded in the '
                                                                        'pre-procedure iWeb coronary '
                                                                        'assessment.',
                                                          'pre_why_no_iibiiia_during_procedure': 'Why no '
                                                                                                 'IIb/IIIa '
                                                                                                 'during '
                                                                                                 'procedure '
                                                                                                 'recorded '
                                                                                                 'in the '
                                                                                                 'pre-procedure '
                                                                                                 'iWeb '
                                                                                                 'coronary '
                                                                                                 'assessment.',
                                                          'row_hash': 'SHA-256 content fingerprint over the '
                                                                      "row's business columns, excluding "
                                                                      'volatile pipeline timestamps.',
                                                          'source_join_status': 'Indicates whether the '
                                                                                'coronary procedure was '
                                                                                'present in both source '
                                                                                'modules or only the '
                                                                                'pre-procedure or '
                                                                                'post-procedure module.',
                                                          'source_present_ind': 'True while the procedure '
                                                                                'remains present in the '
                                                                                'latest source snapshot; '
                                                                                'false identifies a retained '
                                                                                'source retraction.'},
                                              'quantity_override': True,
                                              'table': 'Internal QC of clinical_registry_coronary_procedure; '
                                                       'counts rows whose Gold parent is absent.'},
 'clinical_registry_coronary_procedure': {'columns': {},
                                          'quantity_override': False,
                                          'table': 'Typed child of clinical_registry_entry for the '
                                                   'coronary_procedure family. One row per unioned '
                                                   'Reg_CORONARY_Pre/Reg_CORONARY_PostProcedure EntryId, '
                                                   'with PRE_ and POST_ source fields kept separate. NHS '
                                                   'number and MRN are deliberately published and '
                                                   'column-tagged ig_risk=4/ig_severity=2 under the '
                                                   '2026-08-13 identifier doctrine. Person linkage copies '
                                                   'the live iWeb current-alias plus consensus-safe '
                                                   'historical fallback. DateOfDischarge raw text is '
                                                   'retained beside parsed future/sentinel/clean fields; '
                                                   'known source maximum 2026-12-10 is a future typo, never '
                                                   'silently accepted. DATE_LAST_CHANGED is an edit '
                                                   'timestamp, not the procedure date. LESION_CHILD_COUNT '
                                                   'counts current iweb_coronary_lesion children; parent '
                                                   'procedures with zero lesions are legitimate and '
                                                   'retained. Every governed bronze column is retained under '
                                                   'its lower-case name. Coverage: iWeb registries start at '
                                                   'different years: coronary 2013, non-coronary 2016, '
                                                   'surgery 2017, cardiac MDT 2018 (CT aortic 2021), '
                                                   'mortality review 2018, ERACS 2025, ACS transfer 2000; '
                                                   'The iWeb pre-procedure embolic-protection and GP '
                                                   'IIb/IIIa fields are empty at source; their post_* twins '
                                                   'carry the data; complete-through month per release: '
                                                   '6_mgmt.data_quality.release_finding (finding_code '
                                                   'F2_complete_through).'}}
