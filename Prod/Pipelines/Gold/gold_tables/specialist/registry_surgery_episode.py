"""Gold specialist.registry_surgery_episode: 2 dataset(s).

- datasets: _qc_clinical_registry_surgery_episode, clinical_registry_surgery_episode
- moved verbatim from: 3_gold_specialist_care:5322, 3_gold_specialist_care:5738, 3_gold_specialist_care:5744, 3_gold_specialist_care:5759

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.registry_surgery_episode ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_registry_entry applies the parent Gold drop rule.
CLINICAL_REGISTRY_SURGERY_EPISODE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entry_id` AS `entry_id`',
    '`mrn` AS `mrn`',
    '`surname` AS `surname`',
    '`forename` AS `forename`',
    '`nhs_number` AS `nhs_number`',
    '`gender` AS `gender`',
    '`date_of_birth` AS `date_of_birth`',
    '`date_of_death` AS `date_of_death`',
    '`ethnic_origin` AS `ethnic_origin`',
    '`date_of_operation` AS `date_of_operation`',
    '`age_at_operation` AS `age_at_operation`',
    '`date_of_admission` AS `date_of_admission`',
    '`administrative_category` AS `administrative_category`',
    '`angina_status_pre_surgery` AS `angina_status_pre_surgery`',
    '`dyspnoea_status_pre_surgery` AS `dyspnoea_status_pre_surgery`',
    '`number_of_previous_mis` AS `number_of_previous_mis`',
    '`interval_between_surgery_and_last_mi` AS `interval_between_surgery_and_last_mi`',
    '`previous_pci` AS `previous_pci`',
    '`previous_cardiac_surgerytxt` AS `previous_cardiac_surgerytxt`',
    '`date_of_last_cardiac_operation` AS `date_of_last_cardiac_operation`',
    '`diabetes_management` AS `diabetes_management`',
    '`cigarette_smoking_history` AS `cigarette_smoking_history`',
    '`history_of_hypertension` AS `history_of_hypertension`',
    '`dialysis` AS `dialysis`',
    '`history_of_pulmonary_disease` AS `history_of_pulmonary_disease`',
    '`history_of_neurological_diseasetxt` AS `history_of_neurological_diseasetxt`',
    '`history_of_neurological_dysfunction` AS `history_of_neurological_dysfunction`',
    '`extracardiac_arteriopathy` AS `extracardiac_arteriopathy`',
    '`pre_operative_heart_rhythm` AS `pre_operative_heart_rhythm`',
    '`left_heart_catheterisation` AS `left_heart_catheterisation`',
    '`date_of_last_catheterisation` AS `date_of_last_catheterisation`',
    '`extent_of_coronary_vessel_disease` AS `extent_of_coronary_vessel_disease`',
    '`ejection_fraction_category` AS `ejection_fraction_category`',
    '`pa_systolic` AS `pa_systolic`',
    '`severity_of_aortic_valve_stenosis_eoa` AS `severity_of_aortic_valve_stenosis_eoa`',
    '`intravenous_nitrates_or_any_heparin` AS `intravenous_nitrates_or_any_heparin`',
    '`intravenous_inotropes_prior_to_anaesthesia` AS `intravenous_inotropes_prior_to_anaesthesia`',
    '`ventilated_pre_operation` AS `ventilated_pre_operation`',
    '`cardiogenic_shock_pre_operation` AS `cardiogenic_shock_pre_operation`',
    '`date_and_time_of_operation` AS `date_and_time_of_operation`',
    '`operative_urgency` AS `operative_urgency`',
    '`number_of_previous_heart_operations` AS `number_of_previous_heart_operations`',
    '`responsible_consultant_anaesthetisttxt` AS `responsible_consultant_anaesthetisttxt`',
    '`first_operatortxt` AS `first_operatortxt`',
    '`first_operator_grade` AS `first_operator_grade`',
    '`first_operator_calman_year_of_trainee` AS `first_operator_calman_year_of_trainee`',
    '`first_assistanttxt` AS `first_assistanttxt`',
    '`first_assistant_grade` AS `first_assistant_grade`',
    '`first_assistant_calman_year_of_trainee` AS `first_assistant_calman_year_of_trainee`',
    '`left_main_stem_disease` AS `left_main_stem_disease`',
    '`left_ventricular_function` AS `left_ventricular_function`',
    '`cardiac_procedures` AS `cardiac_procedures`',
    '`other_actual_cardiac_procedurestxt` AS `other_actual_cardiac_procedurestxt`',
    '`actual_creatinine_at_time_of_surgery` AS `actual_creatinine_at_time_of_surgery`',
    '`severity_of_aortic_valve_stenosis_gradient` AS `severity_of_aortic_valve_stenosis_gradient`',
    '`category_of_aortic_valve_stenosis` AS `category_of_aortic_valve_stenosis`',
    '`cabg` AS `cabg`',
    '`valve` AS `valve`',
    '`major_aortic` AS `major_aortic`',
    '`other_cardiac_procedures` AS `other_cardiac_procedures`',
    '`presentationtxt` AS `presentationtxt`',
    '`other_thoracic_and_vascular_procedurestxt` AS `other_thoracic_and_vascular_procedurestxt`',
    '`date_and_time_of_operation_finished` AS `date_and_time_of_operation_finished`',
    '`reason_pre_operative_intra_aortic_balloon_pump_used` AS `reason_pre_operative_intra_aortic_balloon_pump_used`',
    '`pre_op_support_devices_usedtxt` AS `pre_op_support_devices_usedtxt`',
    '`reason_for_pre_operative_impeller_device_use` AS `reason_for_pre_operative_impeller_device_use`',
    '`reason_for_pre_operative_ventricular_assist_device_use` AS `reason_for_pre_operative_ventricular_assist_device_use`',
    '`reason_for_use_of_other_support_device_pre_operation` AS `reason_for_use_of_other_support_device_pre_operation`',
    '`postcode_of_usual_address` AS `postcode_of_usual_address`',
    '`pulmonary_hypertension` AS `pulmonary_hypertension`',
    '`aetiologytxt` AS `aetiologytxt`',
    '`height_in_cm` AS `height_in_cm`',
    '`poor_mobility_non_cardiac_reason` AS `poor_mobility_non_cardiac_reason`',
    '`creatinine_clearance` AS `creatinine_clearance`',
    '`renal_impairment` AS `renal_impairment`',
    '`hospital_identifier_old` AS `hospital_identifier_old`',
    '`diabetes_type` AS `diabetes_type`',
    '`cardiology_consultanttxt` AS `cardiology_consultanttxt`',
    '`hypercholesterolaemia` AS `hypercholesterolaemia`',
    '`mean_pawpla` AS `mean_pawpla`',
    '`antibiotic_treatment_for_endocarditis` AS `antibiotic_treatment_for_endocarditis`',
    '`second_assistanttxt` AS `second_assistanttxt`',
    '`second_assistant_grade` AS `second_assistant_grade`',
    '`second_assistant_calman_year_of_trainee` AS `second_assistant_calman_year_of_trainee`',
    '`incision` AS `incision`',
    '`arterial_cannulation_version5pt2` AS `arterial_cannulation_version5pt2`',
    '`venous_cannulation` AS `venous_cannulation`',
    '`perfusionisttxt` AS `perfusionisttxt`',
    '`other_cardiology_consultant` AS `other_cardiology_consultant`',
    '`other_cardiology_consultant_gm_cnumber` AS `other_cardiology_consultant_gm_cnumber`',
    '`weight_in_kg` AS `weight_in_kg`',
    '`other_cardiac_procedure_details` AS `other_cardiac_procedure_details`',
    '`m_pre_operation_history` AS `m_pre_operation_history`',
    '`history_of_poor_mobility` AS `history_of_poor_mobility`',
    '`surgical_incisiontxt` AS `surgical_incisiontxt`',
    '`consultant_responsible_for_proceduretxt` AS `consultant_responsible_for_proceduretxt`',
    '`responsible_consultant_surgeontxt` AS `responsible_consultant_surgeontxt`',
    '`primary_operator` AS `primary_operator`',
    '`hospital_identifiertxt` AS `hospital_identifiertxt`',
    '`myectomy_optionstxt` AS `myectomy_optionstxt`',
    '`conversion_to_sternotomy` AS `conversion_to_sternotomy`',
    '`referring_hospitaltxt` AS `referring_hospitaltxt`',
    '`unit_collective_responsibility_case` AS `unit_collective_responsibility_case`',
    '`secondary_assisting_consultant_surgeontxt` AS `secondary_assisting_consultant_surgeontxt`',
    '`covid19_diagnosis` AS `covid19_diagnosis`',
    '`covid19_pt` AS `covid19_pt`',
    '`harley_street_clinic` AS `harley_street_clinic`',
    '`discussed_at_documented_quorate_preop_mdt` AS `discussed_at_documented_quorate_preop_mdt`',
    '`haemoglobin_at_time_of_surgery` AS `haemoglobin_at_time_of_surgery`',
    '`clinical_presentationtxt` AS `clinical_presentationtxt`',
    '`clinical_presentation_malperfusiontxt` AS `clinical_presentation_malperfusiontxt`',
    '`ct_aorta_dt` AS `ct_aorta_dt`',
    '`ct_aorta_diagnosistxt` AS `ct_aorta_diagnosistxt`',
    '`ct_aorta_other_dx` AS `ct_aorta_other_dx`',
    '`ct_aorta_extent_of_dissectiontxt` AS `ct_aorta_extent_of_dissectiontxt`',
    '`ct_aorta_primary_entry_teartxt` AS `ct_aorta_primary_entry_teartxt`',
    '`ct_aorta_secondary_entry_tear_arch` AS `ct_aorta_secondary_entry_tear_arch`',
    '`ct_aorta_root` AS `ct_aorta_root`',
    '`ct_aorta_stj` AS `ct_aorta_stj`',
    '`ct_aorta_mid_aa` AS `ct_aorta_mid_aa`',
    '`ct_aorta_distal_aa` AS `ct_aorta_distal_aa`',
    '`ct_aorta_arch_lcca` AS `ct_aorta_arch_lcca`',
    '`ct_aorta_arch_las` AS `ct_aorta_arch_las`',
    '`ct_aorta_proximal_dta` AS `ct_aorta_proximal_dta`',
    '`ct_aorta_mid_dta` AS `ct_aorta_mid_dta`',
    '`ct_aorta_distal_dt_adiaphragm` AS `ct_aorta_distal_dt_adiaphragm`',
    '`ct_aorta_visceral_level` AS `ct_aorta_visceral_level`',
    '`ct_aorta_infrarenal` AS `ct_aorta_infrarenal`',
    '`fev1` AS `fev1`',
    '`fev1_percent_predicted` AS `fev1_percent_predicted`',
    '`fev1_fv_cratio` AS `fev1_fv_cratio`',
    '`fev1_fv_cpercent` AS `fev1_fv_cpercent`',
    '`diffusion_capacitymlminkpa` AS `diffusion_capacitymlminkpa`',
    '`diffusion_capacity_percent_predicted` AS `diffusion_capacity_percent_predicted`',
    '`cpet_anaerobic_threshold` AS `cpet_anaerobic_threshold`',
    '`maximal_oxygen_uptake` AS `maximal_oxygen_uptake`',
    '`surgical_incisionnon_nicor_optionstxt` AS `surgical_incisionnon_nicor_optionstxt`',
    '`previous_abdominal_descending_aortic_surgtxt` AS `previous_abdominal_descending_aortic_surgtxt`',
    '`previous_endovascular_surgerytxt` AS `previous_endovascular_surgerytxt`',
    '`prior_right_chest_surgery_or_radiation` AS `prior_right_chest_surgery_or_radiation`',
    '`significant_chest_wall_deformity` AS `significant_chest_wall_deformity`',
    '`preoperative_transcatheter_interventionstxt` AS `preoperative_transcatheter_interventionstxt`',
    '`rv_fractional_area_change` AS `rv_fractional_area_change`',
    '`tapse` AS `tapse`',
    '`coronary_ct` AS `coronary_ct`',
    '`abdominal_or_descending` AS `abdominal_or_descending`',
    '`rockwood_frailty_score` AS `rockwood_frailty_score`',
    '`abdominal_or_descending_aortic` AS `abdominal_or_descending_aortic`',
    '`m_other_previous_endovascular_surgery` AS `m_other_previous_endovascular_surgery`',
    '`aortic_valve_anatomy` AS `aortic_valve_anatomy`',
    '`other_aortic_valve_anatomy` AS `other_aortic_valve_anatomy`',
    '`aortic_valve_diseasetxt` AS `aortic_valve_diseasetxt`',
    '`aortic_valve_regurgitation` AS `aortic_valve_regurgitation`',
    '`ero_aaortic` AS `ero_aaortic`',
    '`vena_contracta_width_aortic` AS `vena_contracta_width_aortic`',
    '`ar_jet_direction` AS `ar_jet_direction`',
    '`lv_end_systolic_diameter_aortic` AS `lv_end_systolic_diameter_aortic`',
    '`lv_end_diastolic_diameter_aortic` AS `lv_end_diastolic_diameter_aortic`',
    '`severity_of_aortic_valve_stenosis_ava` AS `severity_of_aortic_valve_stenosis_ava`',
    '`mitral_valve_diseasetxt` AS `mitral_valve_diseasetxt`',
    '`mitral_valve_regurgitation` AS `mitral_valve_regurgitation`',
    '`mechanism_of_mitral_regurgitationtxt` AS `mechanism_of_mitral_regurgitationtxt`',
    '`flail` AS `flail`',
    '`prolapse` AS `prolapse`',
    '`restriction` AS `restriction`',
    '`degree_of_myxomatous_change` AS `degree_of_myxomatous_change`',
    '`ero_amitral` AS `ero_amitral`',
    '`vena_contracta_width_mitral` AS `vena_contracta_width_mitral`',
    '`mr_jet_direction` AS `mr_jet_direction`',
    '`lv_end_systolic_diameter_mitral` AS `lv_end_systolic_diameter_mitral`',
    '`lv_end_diastolic_diameter_mitral` AS `lv_end_diastolic_diameter_mitral`',
    '`l_adiameter` AS `l_adiameter`',
    '`r_vimpairment` AS `r_vimpairment`',
    '`pericardial_effusion` AS `pericardial_effusion`',
    '`mitral_valve_stenosis` AS `mitral_valve_stenosis`',
    '`mitral_annulus_calcification` AS `mitral_annulus_calcification`',
    '`severity_of_mitral_valve_stenosis_eoa` AS `severity_of_mitral_valve_stenosis_eoa`',
    '`severity_of_mitral_valve_stenosis_gradient` AS `severity_of_mitral_valve_stenosis_gradient`',
    '`date_last_changed` AS `date_last_changed`',
    '`pre1_adc_updt` AS `pre1_adc_updt`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`linkage_status` AS `linkage_status`',
    '`person_id` AS `person_id`',
    '`linkage_method` AS `linkage_method`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
    '`pre2_date_last_changed` AS `pre2_date_last_changed`',
    '`pre2_adc_updt` AS `pre2_adc_updt`',
    '`post1_date_last_changed` AS `post1_date_last_changed`',
    '`post1_adc_updt` AS `post1_adc_updt`',
    '`post2_date_last_changed` AS `post2_date_last_changed`',
    '`post2_adc_updt` AS `post2_adc_updt`',
    '`episode_last_changed` AS `episode_last_changed`',
    '`adc_updt` AS `adc_updt`',
    '`source_table` AS `source_table`',
    '`source_record_key` AS `source_record_key`',
    '`pre2_module` AS `pre2_module`',
    '`post1_module` AS `post1_module`',
    '`post2_module` AS `post2_module`',
    '`row_hash` AS `row_hash`',
    '`pipeline_loaded_at` AS `pipeline_loaded_at`',
    '`is_current_in_source` AS `is_current_in_source`',
    '`source_present_ind` AS `source_present_ind`',
    '`source_absent_detected_ts` AS `source_absent_detected_ts`',
]


CLINICAL_REGISTRY_SURGERY_EPISODE_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_surgery_episode.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_surgery_episode"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_SURGERY_EPISODE_MANDATORY_RULES)
def _gold_qc_clinical_registry_surgery_episode():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_surgery_episode"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_SURGERY_EPISODE_SELECT, "__gold_parent_present AS __gold_parent_present")
    return df


@dp.materialized_view(
    name=_n("gold_clinical.registry_surgery_episode"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_registry_surgery_episode():
    """Publish clinical_registry_surgery_episode without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_surgery_episode")).drop("__gold_parent_present")


finalise()


COMMENTS = {'_qc_clinical_registry_surgery_episode': {'columns': {'abdominal_or_descending': 'Coded pre-operative field '
                                                                                  'relating to abdominal or '
                                                                                  'descending aortic disease '
                                                                                  'or prior surgery.',
                                                       'abdominal_or_descending_aortic': 'Coded '
                                                                                         'pre-operative '
                                                                                         'field relating to '
                                                                                         'abdominal or '
                                                                                         'descending aortic '
                                                                                         'pathology or '
                                                                                         'intervention, held '
                                                                                         'alongside the '
                                                                                         'related abdominal '
                                                                                         'or descending '
                                                                                         'field.',
                                                       'actual_creatinine_at_time_of_surgery': 'Serum '
                                                                                               'creatinine '
                                                                                               'result '
                                                                                               'recorded at '
                                                                                               'the time of '
                                                                                               'surgery, as '
                                                                                               'captured in '
                                                                                               'the '
                                                                                               'registry.',
                                                       'adc_updt': 'Greatest landing timestamp across the '
                                                                   'four source modules; excluded from '
                                                                   'ROW_HASH.',
                                                       'administrative_category': 'Administrative funding '
                                                                                  'category of the patient '
                                                                                  'for this episode, such as '
                                                                                  'NHS or private care.',
                                                       'aetiologytxt': 'Coded aetiology option or options '
                                                                       'recorded for the valve or aortic '
                                                                       'pathology, stored as concatenated '
                                                                       'code-label text.',
                                                       'age_at_operation': 'Patient age in completed years '
                                                                           'at the date of the operation.',
                                                       'angina_status_pre_surgery': 'Pre-operative angina '
                                                                                    'severity classification '
                                                                                    'recorded before '
                                                                                    'surgery.',
                                                       'antibiotic_treatment_for_endocarditis': 'Indicator '
                                                                                                'of whether '
                                                                                                'the patient '
                                                                                                'was '
                                                                                                'receiving '
                                                                                                'antibiotic '
                                                                                                'treatment '
                                                                                                'for '
                                                                                                'endocarditis '
                                                                                                'before '
                                                                                                'surgery.',
                                                       'aortic_valve_anatomy': 'Coded description of aortic '
                                                                               'valve anatomy recorded at '
                                                                               'pre-operative assessment.',
                                                       'aortic_valve_diseasetxt': 'Text label of aortic '
                                                                                  'valve disease selected '
                                                                                  'from the pre-operative '
                                                                                  'valve assessment option '
                                                                                  'list.',
                                                       'aortic_valve_regurgitation': 'Coded severity grade '
                                                                                     'of aortic valve '
                                                                                     'regurgitation recorded '
                                                                                     'pre-operatively.',
                                                       'ar_jet_direction': 'Coded direction of the aortic '
                                                                           'regurgitant jet observed on '
                                                                           'pre-operative echocardiography.',
                                                       'arterial_cannulation_version5pt2': 'Arterial '
                                                                                           'cannulation site '
                                                                                           'or method '
                                                                                           'recorded using '
                                                                                           'the version 5.2 '
                                                                                           'registry option '
                                                                                           'list.',
                                                       'cabg': 'Indicator of whether the episode included '
                                                               'coronary artery bypass grafting.',
                                                       'cardiac_procedures': 'Coded cardiac procedure '
                                                                             'category recorded in the '
                                                                             'pre-operative module for this '
                                                                             'episode.',
                                                       'cardiogenic_shock_pre_operation': 'Indicator of '
                                                                                          'whether the '
                                                                                          'patient was in '
                                                                                          'cardiogenic shock '
                                                                                          'before the '
                                                                                          'operation.',
                                                       'cardiology_consultanttxt': 'Recorded name of the '
                                                                                   'cardiology consultant '
                                                                                   'associated with the '
                                                                                   'episode.',
                                                       'category_of_aortic_valve_stenosis': 'Coded category '
                                                                                            'of aortic valve '
                                                                                            'stenosis '
                                                                                            'recorded at '
                                                                                            'pre-operative '
                                                                                            'assessment in '
                                                                                            'the CS2010G '
                                                                                            'pre1 module.',
                                                       'cigarette_smoking_history': 'Recorded cigarette '
                                                                                    'smoking status history '
                                                                                    'of the patient before '
                                                                                    'surgery.',
                                                       'clinical_presentation_malperfusiontxt': 'Text label '
                                                                                                'of the '
                                                                                                'recorded '
                                                                                                'malperfusion '
                                                                                                'component '
                                                                                                'of the '
                                                                                                'clinical '
                                                                                                'presentation '
                                                                                                'in the iWeb '
                                                                                                'CS2010G '
                                                                                                'pre-operative '
                                                                                                'module.',
                                                       'clinical_presentationtxt': 'Text label of the '
                                                                                   'recorded clinical '
                                                                                   'presentation category '
                                                                                   'for the episode, as '
                                                                                   'captured in the iWeb '
                                                                                   'CS2010G pre-operative '
                                                                                   'module.',
                                                       'consultant_responsible_for_proceduretxt': 'Text '
                                                                                                  'label '
                                                                                                  'naming '
                                                                                                  'the '
                                                                                                  'consultant '
                                                                                                  'responsible '
                                                                                                  'for the '
                                                                                                  'procedure, '
                                                                                                  'as '
                                                                                                  'selected '
                                                                                                  'from the '
                                                                                                  'iWeb '
                                                                                                  'staff '
                                                                                                  'picklist.',
                                                       'conversion_to_sternotomy': 'Coded indicator of '
                                                                                   'whether the procedure '
                                                                                   'was converted to a '
                                                                                   'sternotomy.',
                                                       'coronary_ct': 'Coded indicator of whether '
                                                                      'pre-operative coronary CT imaging was '
                                                                      'performed.',
                                                       'covid19_diagnosis': 'Coded record of COVID-19 '
                                                                            'diagnosis status relating to '
                                                                            'the surgical episode.',
                                                       'covid19_pt': 'Coded indicator of whether the patient '
                                                                     'was a COVID-19 patient at the time of '
                                                                     'the episode.',
                                                       'cpet_anaerobic_threshold': 'Anaerobic threshold '
                                                                                   'measured at '
                                                                                   'pre-operative '
                                                                                   'cardiopulmonary exercise '
                                                                                   'testing, recorded in the '
                                                                                   'CS2010G pre-operative '
                                                                                   'module.',
                                                       'creatinine_clearance': 'Pre-operative creatinine '
                                                                               'clearance value recorded in '
                                                                               'the registry.',
                                                       'ct_aorta_arch_las': 'Aortic arch measurement at the '
                                                                            'left subclavian artery level '
                                                                            'recorded from the pre-operative '
                                                                            'CT aorta study.',
                                                       'ct_aorta_arch_lcca': 'Aortic arch measurement at the '
                                                                             'left common carotid artery '
                                                                             'recorded from the '
                                                                             'pre-operative CT aorta study.',
                                                       'ct_aorta_diagnosistxt': 'Text label of the diagnosis '
                                                                                'recorded from the '
                                                                                'pre-operative CT aorta '
                                                                                'study in the iWeb CS2010G '
                                                                                'pre-operative module.',
                                                       'ct_aorta_distal_aa': 'Distal ascending aorta '
                                                                             'measurement recorded from the '
                                                                             'pre-operative CT aorta study.',
                                                       'ct_aorta_distal_dt_adiaphragm': 'Distal descending '
                                                                                        'thoracic aorta '
                                                                                        'measurement at '
                                                                                        'diaphragm level '
                                                                                        'recorded from the '
                                                                                        'pre-operative CT '
                                                                                        'aorta study.',
                                                       'ct_aorta_dt': 'Date and time of the pre-operative CT '
                                                                      'aorta study recorded in the iWeb '
                                                                      'CS2010G pre-operative module.',
                                                       'ct_aorta_extent_of_dissectiontxt': 'Text label '
                                                                                           'describing the '
                                                                                           'extent of aortic '
                                                                                           'dissection seen '
                                                                                           'on the '
                                                                                           'pre-operative CT '
                                                                                           'aorta study.',
                                                       'ct_aorta_infrarenal': 'Infrarenal aortic measurement '
                                                                              'recorded from the '
                                                                              'pre-operative CT aorta study.',
                                                       'ct_aorta_mid_aa': 'Mid ascending aorta measurement '
                                                                          'recorded from the pre-operative '
                                                                          'CT aorta study.',
                                                       'ct_aorta_mid_dta': 'Mid descending thoracic aorta '
                                                                           'measurement recorded from the '
                                                                           'pre-operative CT aorta study.',
                                                       'ct_aorta_other_dx': 'Free-text description of any '
                                                                            'other diagnosis identified on '
                                                                            'the pre-operative CT aorta '
                                                                            'study.',
                                                       'ct_aorta_primary_entry_teartxt': 'Text label '
                                                                                         'describing the '
                                                                                         'location of the '
                                                                                         'primary entry tear '
                                                                                         'identified on the '
                                                                                         'pre-operative CT '
                                                                                         'aorta study.',
                                                       'ct_aorta_proximal_dta': 'Proximal descending '
                                                                                'thoracic aorta measurement '
                                                                                'recorded from the '
                                                                                'pre-operative CT aorta '
                                                                                'study.',
                                                       'ct_aorta_root': 'Aortic root measurement recorded '
                                                                        'from the pre-operative CT aorta '
                                                                        'study.',
                                                       'ct_aorta_secondary_entry_tear_arch': 'Recorded '
                                                                                             'indication of '
                                                                                             'a secondary '
                                                                                             'entry tear at '
                                                                                             'the aortic '
                                                                                             'arch on the '
                                                                                             'pre-operative '
                                                                                             'CT aorta '
                                                                                             'study.',
                                                       'ct_aorta_stj': 'Sinotubular junction measurement '
                                                                       'recorded from the pre-operative CT '
                                                                       'aorta study.',
                                                       'ct_aorta_visceral_level': 'Aortic measurement at the '
                                                                                  'visceral segment level '
                                                                                  'recorded from the '
                                                                                  'pre-operative CT aorta '
                                                                                  'study.',
                                                       'date_and_time_of_operation': 'Date and time at which '
                                                                                     'the surgical procedure '
                                                                                     'started for this '
                                                                                     'episode.',
                                                       'date_and_time_of_operation_finished': 'Date and time '
                                                                                              'at which the '
                                                                                              'surgical '
                                                                                              'operation '
                                                                                              'finished.',
                                                       'date_last_changed': 'Timestamp of the most recent '
                                                                            'change to the pre1 module '
                                                                            'record in the source iWeb '
                                                                            'registry.',
                                                       'date_of_admission': 'Date the patient was admitted '
                                                                            'for the episode of care leading '
                                                                            'to this operation.',
                                                       'date_of_birth': 'Patient date of birth as recorded '
                                                                        'in the iWeb CS2010G pre-operative '
                                                                        'registry module.',
                                                       'date_of_death': 'Recorded date of patient death, '
                                                                        'null where no death has been '
                                                                        'recorded on the registry.',
                                                       'date_of_last_cardiac_operation': 'Date of the '
                                                                                         "patient's most "
                                                                                         'recent previous '
                                                                                         'cardiac operation, '
                                                                                         'null where there '
                                                                                         'was no prior '
                                                                                         'surgery.',
                                                       'date_of_last_catheterisation': 'Date of the '
                                                                                       "patient's most "
                                                                                       'recent cardiac '
                                                                                       'catheterisation '
                                                                                       'prior to the '
                                                                                       'surgical episode, as '
                                                                                       'recorded in the '
                                                                                       'CS2010G '
                                                                                       'pre-operative '
                                                                                       'module.',
                                                       'date_of_operation': 'Date of the cardiac surgical '
                                                                            'operation for this episode.',
                                                       'degree_of_myxomatous_change': 'Recorded degree of '
                                                                                      'myxomatous change of '
                                                                                      'the mitral valve on '
                                                                                      'pre-operative '
                                                                                      'assessment.',
                                                       'diabetes_management': 'Method of diabetes management '
                                                                              'recorded before surgery, '
                                                                              'including a not-diabetic '
                                                                              'option.',
                                                       'diabetes_type': 'Coded type of diabetes recorded '
                                                                        'pre-operatively.',
                                                       'dialysis': 'Pre-operative dialysis status, including '
                                                                   'type and timing of dialysis relative to '
                                                                   'cardiac surgery.',
                                                       'diffusion_capacity_percent_predicted': 'Pre-operative '
                                                                                               'lung '
                                                                                               'diffusion '
                                                                                               'capacity '
                                                                                               'expressed as '
                                                                                               'a percentage '
                                                                                               'of the '
                                                                                               'predicted '
                                                                                               'value, from '
                                                                                               'the CS2010G '
                                                                                               'pre-operative '
                                                                                               'module.',
                                                       'diffusion_capacitymlminkpa': 'Pre-operative lung '
                                                                                     'diffusion capacity '
                                                                                     'measured in '
                                                                                     'ml/min/kPa.',
                                                       'discussed_at_documented_quorate_preop_mdt': 'Coded '
                                                                                                    'indicator '
                                                                                                    'of '
                                                                                                    'whether '
                                                                                                    'the '
                                                                                                    'case '
                                                                                                    'was '
                                                                                                    'discussed '
                                                                                                    'at a '
                                                                                                    'documented '
                                                                                                    'quorate '
                                                                                                    'pre-operative '
                                                                                                    'multidisciplinary '
                                                                                                    'team '
                                                                                                    'meeting.',
                                                       'dyspnoea_status_pre_surgery': 'Pre-operative '
                                                                                      'dyspnoea severity '
                                                                                      'classification '
                                                                                      'recorded before '
                                                                                      'surgery.',
                                                       'ejection_fraction_category': 'Coded category of '
                                                                                     'pre-operative left '
                                                                                     'ventricular ejection '
                                                                                     'fraction used in '
                                                                                     'cardiac surgery risk '
                                                                                     'assessment.',
                                                       'entry_id': 'Stable iWeb episode key and parent of '
                                                                   'iweb_cardiac_surgery_procedure.',
                                                       'episode_last_changed': 'Greatest DateLastChanged '
                                                                               'timestamp across the four '
                                                                               'source modules.',
                                                       'ero_aaortic': 'Effective regurgitant orifice area '
                                                                      'for aortic regurgitation measured on '
                                                                      'pre-operative echocardiography.',
                                                       'ero_amitral': 'Effective regurgitant orifice area of '
                                                                      'the mitral valve measured on '
                                                                      'pre-operative echocardiography.',
                                                       'ethnic_origin': 'Coded ethnic origin of the patient '
                                                                        'as recorded in the pre-operative '
                                                                        'registry module.',
                                                       'extent_of_coronary_vessel_disease': 'Coded '
                                                                                            'pre-operative '
                                                                                            'assessment of '
                                                                                            'the number of '
                                                                                            'coronary '
                                                                                            'vessels with '
                                                                                            'significant '
                                                                                            'stenosis.',
                                                       'extracardiac_arteriopathy': 'Indicates whether '
                                                                                    'extracardiac '
                                                                                    'arteriopathy was '
                                                                                    'recorded as a '
                                                                                    'pre-operative risk '
                                                                                    'factor.',
                                                       'fev1': 'Pre-operative forced expiratory volume in '
                                                               'one second recorded from lung function '
                                                               'testing.',
                                                       'fev1_fv_cpercent': 'Pre-operative FEV1/FVC expressed '
                                                                           'as a percentage from lung '
                                                                           'function testing.',
                                                       'fev1_fv_cratio': 'Pre-operative ratio of FEV1 to '
                                                                         'forced vital capacity from lung '
                                                                         'function testing.',
                                                       'fev1_percent_predicted': 'Pre-operative FEV1 '
                                                                                 'expressed as a percentage '
                                                                                 'of the predicted value.',
                                                       'first_assistant_calman_year_of_trainee': 'Calman '
                                                                                                 'training '
                                                                                                 'year of '
                                                                                                 'the first '
                                                                                                 'assistant '
                                                                                                 'when that '
                                                                                                 'assistant '
                                                                                                 'is a '
                                                                                                 'trainee.',
                                                       'first_assistant_grade': 'Coded staff grade of the '
                                                                                'first assistant in the '
                                                                                'operating team.',
                                                       'first_assistanttxt': 'Free-text name of the first '
                                                                             'assistant in the operating '
                                                                             'team.',
                                                       'first_operator_calman_year_of_trainee': 'Calman '
                                                                                                'training '
                                                                                                'year of the '
                                                                                                'first '
                                                                                                'operator '
                                                                                                'when that '
                                                                                                'operator is '
                                                                                                'a trainee.',
                                                       'first_operator_grade': 'Coded staff grade of the '
                                                                               'first operator performing '
                                                                               'the procedure, as recorded '
                                                                               'in the CS2010G pre-operative '
                                                                               'module.',
                                                       'first_operatortxt': 'Free-text name of the first '
                                                                            'operator performing the '
                                                                            'procedure.',
                                                       'flail': 'Indicator of mitral leaflet flail '
                                                                'identified on pre-operative valve '
                                                                'assessment.',
                                                       'forename': 'Patient forename as recorded in the iWeb '
                                                                   'CS2010G pre-operative registry module.',
                                                       'gender': 'Patient gender recorded on the '
                                                                 'pre-operative CS2010G module for this '
                                                                 'surgical episode.',
                                                       'haemoglobin_at_time_of_surgery': 'Pre-operative '
                                                                                         'haemoglobin '
                                                                                         'concentration '
                                                                                         'recorded at the '
                                                                                         'time of the '
                                                                                         'surgical episode, '
                                                                                         'as entered in the '
                                                                                         'iWeb CS2010G '
                                                                                         'pre-operative '
                                                                                         'module.',
                                                       'harley_street_clinic': 'Coded flag indicating '
                                                                               'whether the episode was '
                                                                               'associated with the Harley '
                                                                               'Street Clinic.',
                                                       'height_in_cm': 'Patient height in centimetres '
                                                                       'recorded pre-operatively.',
                                                       'history_of_hypertension': 'Indicates whether the '
                                                                                  'patient has a recorded '
                                                                                  'history of hypertension '
                                                                                  'prior to admission.',
                                                       'history_of_neurological_diseasetxt': 'Coded '
                                                                                             'description of '
                                                                                             'any history of '
                                                                                             'neurological '
                                                                                             'disease '
                                                                                             'recorded '
                                                                                             'before '
                                                                                             'surgery.',
                                                       'history_of_neurological_dysfunction': 'Indicates '
                                                                                              'whether '
                                                                                              'pre-operative '
                                                                                              'neurological '
                                                                                              'dysfunction '
                                                                                              'severely '
                                                                                              'affecting '
                                                                                              'ambulation or '
                                                                                              'day-to-day '
                                                                                              'functioning '
                                                                                              'was recorded.',
                                                       'history_of_poor_mobility': 'Coded indicator of '
                                                                                   'pre-operative poor '
                                                                                   'mobility used in cardiac '
                                                                                   'surgical risk '
                                                                                   'assessment.',
                                                       'history_of_pulmonary_disease': 'Recorded history of '
                                                                                       'chronic pulmonary '
                                                                                       'disease before '
                                                                                       'surgery.',
                                                       'hospital_identifier_old': 'Legacy hospital or unit '
                                                                                  'identifier code retained '
                                                                                  'from the iWeb CS2010G '
                                                                                  'pre-operative module.',
                                                       'hospital_identifiertxt': 'Text label of the hospital '
                                                                                 'or trust identifier '
                                                                                 'recorded for the episode '
                                                                                 'in the iWeb CS2010G '
                                                                                 'pre-operative module.',
                                                       'hypercholesterolaemia': 'Indicator of a recorded '
                                                                                'history of '
                                                                                'hypercholesterolaemia '
                                                                                'before surgery.',
                                                       'incision': 'Coded surgical incision type recorded '
                                                                   'for the episode in the CS2010G '
                                                                   'pre-operative module.',
                                                       'interval_between_surgery_and_last_mi': 'Categorised '
                                                                                               'time '
                                                                                               'interval '
                                                                                               'between the '
                                                                                               'most recent '
                                                                                               'myocardial '
                                                                                               'infarction '
                                                                                               'and the '
                                                                                               'operation.',
                                                       'intravenous_inotropes_prior_to_anaesthesia': 'Indicator '
                                                                                                     'of '
                                                                                                     'whether '
                                                                                                     'intravenous '
                                                                                                     'inotropes '
                                                                                                     'were '
                                                                                                     'administered '
                                                                                                     'prior '
                                                                                                     'to '
                                                                                                     'induction '
                                                                                                     'of '
                                                                                                     'anaesthesia.',
                                                       'intravenous_nitrates_or_any_heparin': 'Indicator of '
                                                                                              'whether the '
                                                                                              'patient '
                                                                                              'received '
                                                                                              'intravenous '
                                                                                              'nitrates or '
                                                                                              'any heparin '
                                                                                              'before '
                                                                                              'operation.',
                                                       'is_current_in_source': 'Boolean flag indicating '
                                                                               'whether the episode is still '
                                                                               'present as a current record '
                                                                               'in the iWeb source.',
                                                       'l_adiameter': 'Left atrial diameter measured on '
                                                                      'pre-operative echocardiography.',
                                                       'left_heart_catheterisation': 'Indicates whether left '
                                                                                     'heart catheterisation '
                                                                                     'was performed before '
                                                                                     'surgery.',
                                                       'left_main_stem_disease': 'Coded assessment of left '
                                                                                 'main stem coronary artery '
                                                                                 'disease severity before '
                                                                                 'surgery.',
                                                       'left_ventricular_function': 'Recorded pre-operative '
                                                                                    'left ventricular '
                                                                                    'function value from '
                                                                                    'cardiac imaging or '
                                                                                    'catheterisation.',
                                                       'linkage_fallback_conflict_ind': 'Boolean flag '
                                                                                        'indicating that the '
                                                                                        'fallback linkage '
                                                                                        'step returned '
                                                                                        'conflicting '
                                                                                        'candidate person '
                                                                                        'matches.',
                                                       'linkage_historical_fallback_ind': 'Boolean flag '
                                                                                          'indicating that '
                                                                                          'the PERSON_ID '
                                                                                          'linkage was '
                                                                                          'resolved using a '
                                                                                          'historical alias '
                                                                                          'fallback rather '
                                                                                          'than a current '
                                                                                          'identifier match.',
                                                       'linkage_method': 'Derived indicator of which '
                                                                         'identifier combination was used to '
                                                                         'resolve the patient linkage.',
                                                       'linkage_status': 'Derived status describing the '
                                                                         'outcome of linking the registry '
                                                                         'episode to the master patient '
                                                                         'index.',
                                                       'lv_end_diastolic_diameter_aortic': 'Left ventricular '
                                                                                           'end-diastolic '
                                                                                           'diameter '
                                                                                           'recorded within '
                                                                                           'the '
                                                                                           'pre-operative '
                                                                                           'aortic valve '
                                                                                           'assessment.',
                                                       'lv_end_diastolic_diameter_mitral': 'Left ventricular '
                                                                                           'end-diastolic '
                                                                                           'diameter '
                                                                                           'recorded as part '
                                                                                           'of the '
                                                                                           'pre-operative '
                                                                                           'mitral valve '
                                                                                           'assessment.',
                                                       'lv_end_systolic_diameter_aortic': 'Left ventricular '
                                                                                          'end-systolic '
                                                                                          'diameter recorded '
                                                                                          'within the '
                                                                                          'pre-operative '
                                                                                          'aortic valve '
                                                                                          'assessment.',
                                                       'lv_end_systolic_diameter_mitral': 'Left ventricular '
                                                                                          'end-systolic '
                                                                                          'diameter recorded '
                                                                                          'as part of the '
                                                                                          'pre-operative '
                                                                                          'mitral valve '
                                                                                          'assessment.',
                                                       'm_other_previous_endovascular_surgery': 'Free-text '
                                                                                                'detail '
                                                                                                'describing '
                                                                                                'other '
                                                                                                'previous '
                                                                                                'endovascular '
                                                                                                'surgery not '
                                                                                                'covered by '
                                                                                                'the coded '
                                                                                                'options.',
                                                       'm_pre_operation_history': 'Free-text pre-operative '
                                                                                  'clinical history '
                                                                                  'narrative for the '
                                                                                  'episode.',
                                                       'major_aortic': 'Indicator of whether the episode '
                                                                       'included a major aortic procedure.',
                                                       'maximal_oxygen_uptake': 'Maximal oxygen uptake '
                                                                                'recorded at pre-operative '
                                                                                'cardiopulmonary exercise '
                                                                                'testing in the CS2010G '
                                                                                'pre-operative module.',
                                                       'mean_pawpla': 'Mean pulmonary artery wedge or left '
                                                                      'atrial pressure recorded at '
                                                                      'pre-operative cardiac investigation.',
                                                       'mechanism_of_mitral_regurgitationtxt': 'Coded '
                                                                                               'mechanism of '
                                                                                               'mitral '
                                                                                               'regurgitation '
                                                                                               'identified '
                                                                                               'pre-operatively, '
                                                                                               'held as the '
                                                                                               "registry's "
                                                                                               'decoded text '
                                                                                               'label.',
                                                       'mitral_annulus_calcification': 'Recorded presence or '
                                                                                       'degree of mitral '
                                                                                       'annular '
                                                                                       'calcification on '
                                                                                       'pre-operative '
                                                                                       'assessment.',
                                                       'mitral_valve_diseasetxt': 'Coded description of '
                                                                                  'pre-operative mitral '
                                                                                  'valve disease, held as '
                                                                                  "the registry's decoded "
                                                                                  'text label.',
                                                       'mitral_valve_regurgitation': 'Pre-operative grade of '
                                                                                     'mitral valve '
                                                                                     'regurgitation recorded '
                                                                                     'as a coded category.',
                                                       'mitral_valve_stenosis': 'Pre-operative grade or '
                                                                                'presence of mitral valve '
                                                                                'stenosis recorded as a '
                                                                                'coded category.',
                                                       'mr_jet_direction': 'Direction of the mitral '
                                                                           'regurgitant jet recorded on '
                                                                           'pre-operative echocardiography.',
                                                       'mrn': 'Native source hospital number retained for '
                                                              'trace-back.',
                                                       'myectomy_optionstxt': 'Text label of the myectomy '
                                                                              'option recorded for the '
                                                                              'procedure.',
                                                       'nhs_number': 'Native source NHS number retained for '
                                                                     'governed trace-back.',
                                                       'nhs_number_valid_ind': 'Derived flag indicating '
                                                                               'whether the source NHS '
                                                                               'number passed validation, '
                                                                               'retained after the NHS '
                                                                               'number itself was dropped.',
                                                       'number_of_previous_heart_operations': 'Count of '
                                                                                              'previous '
                                                                                              'cardiac '
                                                                                              'operations '
                                                                                              'undergone by '
                                                                                              'the patient '
                                                                                              'before this '
                                                                                              'episode.',
                                                       'number_of_previous_mis': 'Categorised count of '
                                                                                 'previous myocardial '
                                                                                 'infarctions recorded '
                                                                                 'before surgery.',
                                                       'operative_urgency': 'Coded urgency category of the '
                                                                            'cardiac surgical operation as '
                                                                            'recorded pre-operatively in the '
                                                                            'CS2010G registry.',
                                                       'other_actual_cardiac_procedurestxt': 'Text field '
                                                                                             'listing '
                                                                                             'additional '
                                                                                             'cardiac '
                                                                                             'procedures '
                                                                                             'performed, '
                                                                                             'generally '
                                                                                             'holding coded '
                                                                                             'picklist '
                                                                                             'descriptions.',
                                                       'other_aortic_valve_anatomy': 'Text detail of aortic '
                                                                                     'valve anatomy when not '
                                                                                     'covered by the coded '
                                                                                     'anatomy options.',
                                                       'other_cardiac_procedure_details': 'Free-text detail '
                                                                                          'describing other '
                                                                                          'cardiac '
                                                                                          'procedures '
                                                                                          'performed that '
                                                                                          'are not covered '
                                                                                          'by the coded '
                                                                                          'procedure fields.',
                                                       'other_cardiac_procedures': 'Indicator of whether '
                                                                                   'other cardiac procedures '
                                                                                   'were performed in '
                                                                                   'addition to the main '
                                                                                   'procedure categories.',
                                                       'other_cardiology_consultant': 'Additional cardiology '
                                                                                      'consultant recorded '
                                                                                      'for the patient where '
                                                                                      'different from the '
                                                                                      'main cardiology '
                                                                                      'consultant.',
                                                       'other_cardiology_consultant_gm_cnumber': 'GMC '
                                                                                                 'registration '
                                                                                                 'number of '
                                                                                                 'the other '
                                                                                                 'cardiology '
                                                                                                 'consultant '
                                                                                                 'recorded '
                                                                                                 'for the '
                                                                                                 'episode.',
                                                       'other_thoracic_and_vascular_procedurestxt': 'Coded '
                                                                                                    'selection '
                                                                                                    'of '
                                                                                                    'other '
                                                                                                    'thoracic '
                                                                                                    'and '
                                                                                                    'vascular '
                                                                                                    'procedures '
                                                                                                    'associated '
                                                                                                    'with '
                                                                                                    'the '
                                                                                                    'episode, '
                                                                                                    'stored '
                                                                                                    'as '
                                                                                                    'concatenated '
                                                                                                    'code-label '
                                                                                                    'text.',
                                                       'pa_systolic': 'Pre-operative pulmonary artery '
                                                                      'systolic pressure as recorded in the '
                                                                      'registry.',
                                                       'patient_event_key': 'Deterministic SHA-256 key '
                                                                            'matching '
                                                                            'clinical_registry_entry.patient_event_key '
                                                                            'for the surgery_episode source '
                                                                            'family and native entry_id.',
                                                       'perfusionisttxt': 'Text label identifying the '
                                                                          'perfusionist selected for the '
                                                                          'case from the iWeb staff '
                                                                          'picklist.',
                                                       'pericardial_effusion': 'Indicator of pericardial '
                                                                               'effusion identified on '
                                                                               'pre-operative assessment.',
                                                       'person_id': 'Resolved independently through NHS '
                                                                    'number and MRN; NULL on conflict or '
                                                                    'ambiguity.',
                                                       'pipeline_loaded_at': 'Timestamp recording when the '
                                                                             'pipeline loaded this row into '
                                                                             'the table.',
                                                       'poor_mobility_non_cardiac_reason': 'Indicator of '
                                                                                           'whether poor '
                                                                                           'mobility is '
                                                                                           'attributable to '
                                                                                           'a non-cardiac '
                                                                                           'reason.',
                                                       'post1_adc_updt': 'Technical timestamp of the last '
                                                                         'extract or load update of the '
                                                                         'post1 module row.',
                                                       'post1_date_last_changed': 'Timestamp of the last '
                                                                                  'change to the post1 '
                                                                                  'module record in the '
                                                                                  'source iWeb registry.',
                                                       'post1_module': 'Typed lossless payload of '
                                                                       'reg_cs2010g_post1 fields except '
                                                                       'promoted module timestamps.',
                                                       'post2_adc_updt': 'Ingestion timestamp indicating '
                                                                         'when the post2 module row was last '
                                                                         'updated in the data platform.',
                                                       'post2_date_last_changed': 'Timestamp of the last '
                                                                                  'recorded change to the '
                                                                                  'post2 module record in '
                                                                                  'the iWeb source system.',
                                                       'post2_module': 'Typed lossless payload of '
                                                                       'reg_cs2010g_post2 fields except '
                                                                       'promoted module timestamps.',
                                                       'postcode_of_usual_address': 'Postcode of the '
                                                                                    "patient's usual address "
                                                                                    'as recorded in the iWeb '
                                                                                    'CS2010G pre-operative '
                                                                                    'registry module.',
                                                       'pre1_adc_updt': 'Technical ingestion/update '
                                                                        'timestamp for the pre1 source row '
                                                                        'within the data platform.',
                                                       'pre2_adc_updt': 'Technical timestamp of the last '
                                                                        'ingestion or update of the CS2010G '
                                                                        'pre2 row in the data platform.',
                                                       'pre2_date_last_changed': 'Timestamp of the last edit '
                                                                                 'to the CS2010G pre2 record '
                                                                                 'in the iWeb source system.',
                                                       'pre2_module': 'Typed lossless payload of '
                                                                      'reg_cs2010g_pre2 fields except '
                                                                      'promoted module timestamps.',
                                                       'pre_op_support_devices_usedtxt': 'Coded selection of '
                                                                                         'mechanical support '
                                                                                         'devices used '
                                                                                         'before the '
                                                                                         'operation, stored '
                                                                                         'as concatenated '
                                                                                         'code-label text.',
                                                       'pre_operative_heart_rhythm': 'Coded cardiac rhythm '
                                                                                     'recorded at '
                                                                                     'pre-operative '
                                                                                     'assessment in the '
                                                                                     'CS2010G pre-operative '
                                                                                     'module.',
                                                       'preoperative_transcatheter_interventionstxt': 'Text '
                                                                                                      'label '
                                                                                                      'of '
                                                                                                      'pre-operative '
                                                                                                      'transcatheter '
                                                                                                      'interventions '
                                                                                                      'selected '
                                                                                                      'from '
                                                                                                      'the '
                                                                                                      'registry '
                                                                                                      'option '
                                                                                                      'list.',
                                                       'presentationtxt': 'Coded presentation option or '
                                                                          'options recorded for the valve or '
                                                                          'aortic pathology, stored as '
                                                                          'concatenated code-label text.',
                                                       'previous_abdominal_descending_aortic_surgtxt': 'Text '
                                                                                                       'label '
                                                                                                       'recording '
                                                                                                       'previous '
                                                                                                       'abdominal '
                                                                                                       'or '
                                                                                                       'descending '
                                                                                                       'aortic '
                                                                                                       'surgery '
                                                                                                       'selected '
                                                                                                       'from '
                                                                                                       'the '
                                                                                                       'pre-operative '
                                                                                                       'option '
                                                                                                       'list.',
                                                       'previous_cardiac_surgerytxt': 'Coded description of '
                                                                                      'any previous cardiac '
                                                                                      'surgery undertaken '
                                                                                      'before this episode.',
                                                       'previous_endovascular_surgerytxt': 'Text label '
                                                                                           'recording '
                                                                                           'previous '
                                                                                           'endovascular '
                                                                                           'surgery selected '
                                                                                           'from the '
                                                                                           'pre-operative '
                                                                                           'option list.',
                                                       'previous_pci': 'Indicates whether the patient had '
                                                                       'previous percutaneous coronary '
                                                                       'intervention and its timing '
                                                                       'category.',
                                                       'primary_operator': 'Primary operator recorded for '
                                                                           'the procedure in the CS2010G '
                                                                           'pre-operative module.',
                                                       'prior_right_chest_surgery_or_radiation': 'Coded '
                                                                                                 'indicator '
                                                                                                 'of prior '
                                                                                                 'right '
                                                                                                 'chest '
                                                                                                 'surgery or '
                                                                                                 'radiotherapy '
                                                                                                 'recorded '
                                                                                                 'pre-operatively.',
                                                       'prolapse': 'Indicator of mitral leaflet prolapse '
                                                                   'identified on pre-operative valve '
                                                                   'assessment.',
                                                       'pulmonary_hypertension': 'Coded pre-operative '
                                                                                 'pulmonary hypertension '
                                                                                 'status.',
                                                       'r_vimpairment': 'Recorded degree of right '
                                                                        'ventricular impairment on '
                                                                        'pre-operative assessment.',
                                                       'reason_for_pre_operative_impeller_device_use': 'Coded '
                                                                                                       'reason '
                                                                                                       'for '
                                                                                                       'use '
                                                                                                       'of '
                                                                                                       'an '
                                                                                                       'impeller '
                                                                                                       'device '
                                                                                                       'before '
                                                                                                       'the '
                                                                                                       'operation.',
                                                       'reason_for_pre_operative_ventricular_assist_device_use': 'Coded '
                                                                                                                 'reason '
                                                                                                                 'for '
                                                                                                                 'use '
                                                                                                                 'of '
                                                                                                                 'a '
                                                                                                                 'ventricular '
                                                                                                                 'assist '
                                                                                                                 'device '
                                                                                                                 'before '
                                                                                                                 'the '
                                                                                                                 'operation.',
                                                       'reason_for_use_of_other_support_device_pre_operation': 'Coded '
                                                                                                               'reason '
                                                                                                               'for '
                                                                                                               'use '
                                                                                                               'of '
                                                                                                               'another '
                                                                                                               'type '
                                                                                                               'of '
                                                                                                               'circulatory '
                                                                                                               'support '
                                                                                                               'device '
                                                                                                               'before '
                                                                                                               'the '
                                                                                                               'operation.',
                                                       'reason_pre_operative_intra_aortic_balloon_pump_used': 'Coded '
                                                                                                              'reason '
                                                                                                              'for '
                                                                                                              'use '
                                                                                                              'of '
                                                                                                              'an '
                                                                                                              'intra-aortic '
                                                                                                              'balloon '
                                                                                                              'pump '
                                                                                                              'before '
                                                                                                              'the '
                                                                                                              'operation.',
                                                       'referring_hospitaltxt': 'Text label of the hospital '
                                                                                'that referred the patient '
                                                                                'for the cardiac surgical '
                                                                                'episode.',
                                                       'renal_impairment': 'Coded category of pre-operative '
                                                                           'renal impairment.',
                                                       'responsible_consultant_anaesthetisttxt': 'Text label '
                                                                                                 'naming the '
                                                                                                 'consultant '
                                                                                                 'anaesthetist '
                                                                                                 'responsible '
                                                                                                 'for the '
                                                                                                 'case, '
                                                                                                 'selected '
                                                                                                 'from the '
                                                                                                 'iWeb staff '
                                                                                                 'picklist.',
                                                       'responsible_consultant_surgeontxt': 'Text label '
                                                                                            'naming the '
                                                                                            'consultant '
                                                                                            'surgeon '
                                                                                            'responsible for '
                                                                                            "the patient's "
                                                                                            'care for this '
                                                                                            'episode.',
                                                       'restriction': 'Indicator of mitral leaflet '
                                                                      'restriction identified on '
                                                                      'pre-operative valve assessment.',
                                                       'rockwood_frailty_score': 'Rockwood clinical frailty '
                                                                                 'scale category recorded at '
                                                                                 'pre-operative assessment.',
                                                       'row_hash': "Deterministic hash of the merged row's "
                                                                   'source column values used for change '
                                                                   'detection.',
                                                       'rv_fractional_area_change': 'Right ventricular '
                                                                                    'fractional area change '
                                                                                    'measured on '
                                                                                    'pre-operative '
                                                                                    'echocardiography.',
                                                       'second_assistant_calman_year_of_trainee': 'Calman '
                                                                                                  'training '
                                                                                                  'year '
                                                                                                  'recorded '
                                                                                                  'for the '
                                                                                                  'second '
                                                                                                  'assistant '
                                                                                                  'when that '
                                                                                                  'assistant '
                                                                                                  'is a '
                                                                                                  'trainee.',
                                                       'second_assistant_grade': 'Coded staff grade of the '
                                                                                 'second assistant at the '
                                                                                 'operation, as recorded in '
                                                                                 'the iWeb CS2010G '
                                                                                 'pre-operative module.',
                                                       'second_assistanttxt': 'Recorded name of the second '
                                                                              'assistant present at the '
                                                                              'operation.',
                                                       'secondary_assisting_consultant_surgeontxt': 'Text '
                                                                                                    'label '
                                                                                                    'naming '
                                                                                                    'the '
                                                                                                    'secondary '
                                                                                                    'assisting '
                                                                                                    'consultant '
                                                                                                    'surgeon '
                                                                                                    'for the '
                                                                                                    'procedure.',
                                                       'severity_of_aortic_valve_stenosis_ava': 'Pre-operative '
                                                                                                'aortic '
                                                                                                'valve area '
                                                                                                'measured on '
                                                                                                'echocardiography, '
                                                                                                'recorded as '
                                                                                                'a marker of '
                                                                                                'aortic '
                                                                                                'stenosis '
                                                                                                'severity.',
                                                       'severity_of_aortic_valve_stenosis_eoa': 'Aortic '
                                                                                                'valve '
                                                                                                'effective '
                                                                                                'orifice '
                                                                                                'area '
                                                                                                'recorded as '
                                                                                                'a measure '
                                                                                                'of aortic '
                                                                                                'stenosis '
                                                                                                'severity '
                                                                                                'before '
                                                                                                'surgery.',
                                                       'severity_of_aortic_valve_stenosis_gradient': 'Aortic '
                                                                                                     'valve '
                                                                                                     'pressure '
                                                                                                     'gradient '
                                                                                                     'recorded '
                                                                                                     'as a '
                                                                                                     'measure '
                                                                                                     'of '
                                                                                                     'aortic '
                                                                                                     'stenosis '
                                                                                                     'severity '
                                                                                                     'before '
                                                                                                     'surgery.',
                                                       'severity_of_mitral_valve_stenosis_eoa': 'Effective '
                                                                                                'orifice '
                                                                                                'area of the '
                                                                                                'mitral '
                                                                                                'valve '
                                                                                                'recorded as '
                                                                                                'a measure '
                                                                                                'of mitral '
                                                                                                'stenosis '
                                                                                                'severity.',
                                                       'severity_of_mitral_valve_stenosis_gradient': 'Transmitral '
                                                                                                     'pressure '
                                                                                                     'gradient '
                                                                                                     'recorded '
                                                                                                     'as a '
                                                                                                     'measure '
                                                                                                     'of '
                                                                                                     'mitral '
                                                                                                     'stenosis '
                                                                                                     'severity.',
                                                       'significant_chest_wall_deformity': 'Coded indicator '
                                                                                           'of significant '
                                                                                           'chest wall '
                                                                                           'deformity '
                                                                                           'identified '
                                                                                           'pre-operatively.',
                                                       'source_absent_detected_ts': 'Technical timestamp '
                                                                                    'recording when the '
                                                                                    'episode was first '
                                                                                    'detected as no longer '
                                                                                    'present in the iWeb '
                                                                                    'source.',
                                                       'source_present_ind': 'Canonical boolean mirror of '
                                                                             'IS_CURRENT_IN_SOURCE; false '
                                                                             'rows are retained source '
                                                                             'tombstones.',
                                                       'source_record_key': 'Source system EntryId for the '
                                                                            'episode carried through as a '
                                                                            'string provenance key.',
                                                       'source_table': 'Label listing the iWeb CS2010G '
                                                                       'source modules contributing to the '
                                                                       'merged episode row.',
                                                       'surgical_incisionnon_nicor_optionstxt': 'Text label '
                                                                                                'of the '
                                                                                                'surgical '
                                                                                                'incision '
                                                                                                'selected '
                                                                                                'from local '
                                                                                                'non-NICOR '
                                                                                                'option list '
                                                                                                'in the '
                                                                                                'pre-operative '
                                                                                                'module.',
                                                       'surgical_incisiontxt': 'Text label of the surgical '
                                                                               'incision option selected for '
                                                                               'the procedure.',
                                                       'surname': 'Patient surname as recorded in the iWeb '
                                                                  'CS2010G pre-operative registry module.',
                                                       'tapse': 'Tricuspid annular plane systolic excursion '
                                                                'measured on pre-operative echocardiography.',
                                                       'unit_collective_responsibility_case': 'Coded '
                                                                                              'indicator of '
                                                                                              'whether the '
                                                                                              'case was '
                                                                                              'designated as '
                                                                                              'a unit '
                                                                                              'collective '
                                                                                              'responsibility '
                                                                                              'case.',
                                                       'valve': 'Indicator of whether the episode included a '
                                                                'valve procedure.',
                                                       'vena_contracta_width_aortic': 'Vena contracta width '
                                                                                      'of the aortic '
                                                                                      'regurgitant jet '
                                                                                      'measured on '
                                                                                      'pre-operative '
                                                                                      'echocardiography.',
                                                       'vena_contracta_width_mitral': 'Vena contracta width '
                                                                                      'of the mitral '
                                                                                      'regurgitant jet '
                                                                                      'measured on '
                                                                                      'pre-operative '
                                                                                      'echocardiography.',
                                                       'venous_cannulation': 'Coded venous cannulation '
                                                                             'technique or cannula type used '
                                                                             'for cardiopulmonary bypass.',
                                                       'ventilated_pre_operation': 'Indicator of whether the '
                                                                                   'patient was mechanically '
                                                                                   'ventilated immediately '
                                                                                   'before the operation.',
                                                       'weight_in_kg': 'Patient weight in kilograms recorded '
                                                                       'pre-operatively for the episode.'},
                                           'quantity_override': True,
                                           'table': 'Internal QC of clinical_registry_surgery_episode; '
                                                    'counts rows whose Gold parent is absent.'},
 'clinical_registry_surgery_episode': {'columns': {},
                                       'quantity_override': False,
                                       'table': 'Typed child of clinical_registry_entry for the '
                                                'surgery_episode family. iWeb CS2010G adult cardiac surgery '
                                                'registry. Complete PRE1 fields remain top-level; PRE2, '
                                                'POST1 and POST2 source fields are retained losslessly in '
                                                'typed module structs. This preserves identifiers and '
                                                "clinical content while remaining within Unity Catalog's "
                                                '1,000-tag-per-table quota. Every governed bronze column is '
                                                'retained under its lower-case name. Coverage: iWeb '
                                                'registries start at different years: coronary 2013, '
                                                'non-coronary 2016, surgery 2017, cardiac MDT 2018 (CT '
                                                'aortic 2021), mortality review 2018, ERACS 2025, ACS '
                                                'transfer 2000; complete-through month per release: '
                                                '6_mgmt.data_quality.release_finding (finding_code '
                                                'F2_complete_through).'}}
