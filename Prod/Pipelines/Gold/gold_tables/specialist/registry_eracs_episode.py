"""Gold specialist.registry_eracs_episode: 2 dataset(s).

- datasets: _qc_clinical_registry_eracs_episode, clinical_registry_eracs_episode
- moved verbatim from: 3_gold_specialist_care:4420, 3_gold_specialist_care:4579, 3_gold_specialist_care:4769, 3_gold_specialist_care:4775, 3_gold_specialist_care:4791

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.registry_eracs_episode ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_registry_entry applies the parent Gold drop rule.
CLINICAL_REGISTRY_ERACS_EPISODE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entry_id` AS `entry_id`',
    '`mrn` AS `mrn`',
    '`gender` AS `gender`',
    '`date_of_death` AS `date_of_death`',
    '`ethnic_origin` AS `ethnic_origin`',
    '`date_of_entry` AS `date_of_entry`',
    '`erac_spatient` AS `erac_spatient`',
    '`consultant_cardiothoracictxt` AS `consultant_cardiothoracictxt`',
    '`primary_operatortxt` AS `primary_operatortxt`',
    '`cardiopulmonary_bypass_time` AS `cardiopulmonary_bypass_time`',
    '`cross_clamp_time` AS `cross_clamp_time`',
    '`complicationstxt` AS `complicationstxt`',
    '`m_other_complications` AS `m_other_complications`',
    '`blood_products_administeredtxt` AS `blood_products_administeredtxt`',
    '`medical_historytxt` AS `medical_historytxt`',
    '`m_other_medical_history` AS `m_other_medical_history`',
    '`echo_post_cb_preport_on_pacs` AS `echo_post_cb_preport_on_pacs`',
    '`l_vfunction_pre` AS `l_vfunction_pre`',
    '`r_vfunction_pre` AS `r_vfunction_pre`',
    '`chest_drainstxt` AS `chest_drainstxt`',
    '`pacing_wires` AS `pacing_wires`',
    '`m_underlying_rhythm_details` AS `m_underlying_rhythm_details`',
    '`underlying_rhythmtxt` AS `underlying_rhythmtxt`',
    '`setup_pacing_mode` AS `setup_pacing_mode`',
    '`setup_pacing_rate` AS `setup_pacing_rate`',
    '`vcap_threshold` AS `vcap_threshold`',
    '`acap_threshold` AS `acap_threshold`',
    '`dripstxt` AS `dripstxt`',
    '`pca` AS `pca`',
    '`vein_harvest_sitetxt` AS `vein_harvest_sitetxt`',
    '`antibioticstxt` AS `antibioticstxt`',
    '`timing_of2nd` AS `timing_of2nd`',
    '`timing_of1st` AS `timing_of1st`',
    '`admission_dt_tm` AS `admission_dt_tm`',
    '`patient_medically_ready_dt_tm` AS `patient_medically_ready_dt_tm`',
    '`actual_discharge_dt_tm` AS `actual_discharge_dt_tm`',
    '`discharge_to` AS `discharge_to`',
    '`first_time_sitting_dt_tm` AS `first_time_sitting_dt_tm`',
    '`first_mobilisation_dt_tm` AS `first_mobilisation_dt_tm`',
    '`drains_out_on_icu` AS `drains_out_on_icu`',
    '`drains_removed_dt` AS `drains_removed_dt`',
    '`total_volume_drained` AS `total_volume_drained`',
    '`cv_couton_icu` AS `cv_couton_icu`',
    '`two_couton_icu` AS `two_couton_icu`',
    '`temperature_at_arrival_on_icu` AS `temperature_at_arrival_on_icu`',
    '`p_wouton_icu` AS `p_wouton_icu`',
    '`blood_products_ic_utxt` AS `blood_products_ic_utxt`',
    '`patient_paced6hrs` AS `patient_paced6hrs`',
    '`pacing_mode` AS `pacing_mode`',
    '`arrhythmia_on_ic_utxt` AS `arrhythmia_on_ic_utxt`',
    '`pon_vscore` AS `pon_vscore`',
    '`pain_score` AS `pain_score`',
    '`pc_ausage12_hrs` AS `pc_ausage12_hrs`',
    '`total_amount_pca` AS `total_amount_pca`',
    '`m_other_medical_reason_for_delay` AS `m_other_medical_reason_for_delay`',
    '`m_other_non_medical_reason_for_delay` AS `m_other_non_medical_reason_for_delay`',
    '`non_medical_reasonstxt` AS `non_medical_reasonstxt`',
    '`filtertxt` AS `filtertxt`',
    '`m_other_arrhythmia_icu` AS `m_other_arrhythmia_icu`',
    '`l_vfunction_post_cpb` AS `l_vfunction_post_cpb`',
    '`r_vfunction_post_cpb` AS `r_vfunction_post_cpb`',
    '`eracs_medication_pathway` AS `eracs_medication_pathway`',
    '`m_other_drip_details` AS `m_other_drip_details`',
    '`other_regional_block` AS `other_regional_block`',
    '`discharged_hdu_dt_tm` AS `discharged_hdu_dt_tm`',
    '`procedure_performedtxt` AS `procedure_performedtxt`',
    '`aortic_valve` AS `aortic_valve`',
    '`mitral_valve` AS `mitral_valve`',
    '`tricuspid_valve` AS `tricuspid_valve`',
    '`aorta` AS `aorta`',
    '`achd` AS `achd`',
    '`m_other_procedure_performed` AS `m_other_procedure_performed`',
    '`other_aortic_procedure` AS `other_aortic_procedure`',
    '`other_ach_dprocedure` AS `other_ach_dprocedure`',
    '`heart_rate` AS `heart_rate`',
    '`crossclamp_time120_min` AS `crossclamp_time120_min`',
    '`bypasstime180_min` AS `bypasstime180_min`',
    '`patient_on_first_floor` AS `patient_on_first_floor`',
    '`allergiestxt` AS `allergiestxt`',
    '`m_other_allergies` AS `m_other_allergies`',
    '`medical_reasons_for_delaytxt` AS `medical_reasons_for_delaytxt`',
    '`antiemetictxt` AS `antiemetictxt`',
    '`ulcer_prophylaxistxt` AS `ulcer_prophylaxistxt`',
    '`i_vopiate_loading_at_end` AS `i_vopiate_loading_at_end`',
    '`anaesthetic_registrar_fellow` AS `anaesthetic_registrar_fellow`',
    '`complete_revascularisation` AS `complete_revascularisation`',
    '`manadatory` AS `manadatory`',
    '`medication_prescribed_on_powerchart` AS `medication_prescribed_on_powerchart`',
    '`post_op_plantxt` AS `post_op_plantxt`',
    '`m_other_post_op_plan_details` AS `m_other_post_op_plan_details`',
    '`p_min_backup` AS `p_min_backup`',
    '`backup_rate` AS `backup_rate`',
    '`cabg` AS `cabg`',
    '`baseline_act` AS `baseline_act`',
    '`post_protamine_act` AS `post_protamine_act`',
    '`platelet_mapping_performed` AS `platelet_mapping_performed`',
    '`teg_post_protamine_performed` AS `teg_post_protamine_performed`',
    '`surgical_site_checked` AS `surgical_site_checked`',
    '`m_volume_local_anaesthetic` AS `m_volume_local_anaesthetic`',
    '`adpma35mm` AS `adpma35mm`',
    '`aama35mm` AS `aama35mm`',
    '`rbc_number_of_units` AS `rbc_number_of_units`',
    '`ffp_number_of_units` AS `ffp_number_of_units`',
    '`cryo_number_of_units_transfused` AS `cryo_number_of_units_transfused`',
    '`octoplex_units_transfused` AS `octoplex_units_transfused`',
    '`cell_salvage_transfused` AS `cell_salvage_transfused`',
    '`fibrinogen_transfused` AS `fibrinogen_transfused`',
    '`areas_of_concern` AS `areas_of_concern`',
    '`m_teg_result_summary` AS `m_teg_result_summary`',
    '`timing_of_ondansetron` AS `timing_of_ondansetron`',
    '`paracetamol_administered` AS `paracetamol_administered`',
    '`timing_of_paracetamol_administered` AS `timing_of_paracetamol_administered`',
    '`suggamedex_administered` AS `suggamedex_administered`',
    '`post_op_analgesiatxt` AS `post_op_analgesiatxt`',
    '`platelet_number_of_units` AS `platelet_number_of_units`',
    '`grade_of_intubation` AS `grade_of_intubation`',
    '`antiplateletstxt` AS `antiplateletstxt`',
    '`other_antiplatelet` AS `other_antiplatelet`',
    '`spinal_drain_inserted` AS `spinal_drain_inserted`',
    '`spinal_drainage_above` AS `spinal_drainage_above`',
    '`issues_with_spinal_drain` AS `issues_with_spinal_drain`',
    '`m_details_of_spinal_drain_issues` AS `m_details_of_spinal_drain_issues`',
    '`m_thoracic_proceduredetails` AS `m_thoracic_proceduredetails`',
    '`cardiac_or_thoracic` AS `cardiac_or_thoracic`',
    '`thoracic_proceduretxt` AS `thoracic_proceduretxt`',
    '`bronchoscopy` AS `bronchoscopy`',
    '`lvrs` AS `lvrs`',
    '`lobes_operated_ontxt` AS `lobes_operated_ontxt`',
    '`lymph_node_resection` AS `lymph_node_resection`',
    '`consultant_thoracictxt` AS `consultant_thoracictxt`',
    '`intraoperative_pain_controltxt` AS `intraoperative_pain_controltxt`',
    '`postoperative_pain_controltxt` AS `postoperative_pain_controltxt`',
    '`tof` AS `tof`',
    '`instruction` AS `instruction`',
    '`post_op_planthoracic_patientstxt` AS `post_op_planthoracic_patientstxt`',
    '`consultant_responsible_for_procedure` AS `consultant_responsible_for_procedure`',
    '`belmont_volume` AS `belmont_volume`',
    '`date_last_changed` AS `date_last_changed`',
    '`adc_updt` AS `adc_updt`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`linkage_status` AS `linkage_status`',
    '`person_id` AS `person_id`',
    '`linkage_method` AS `linkage_method`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
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
]


CLINICAL_REGISTRY_ERACS_EPISODE_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`timing_of1st_time` AS `timing_of1st_time`',
    '`timing_of1st_parse_status` AS `timing_of1st_parse_status`',
    '`timing_of2nd_time` AS `timing_of2nd_time`',
    '`timing_of2nd_parse_status` AS `timing_of2nd_parse_status`',
    '`timing_of_ondansetron_time` AS `timing_of_ondansetron_time`',
    '`timing_of_ondansetron_parse_status` AS `timing_of_ondansetron_parse_status`',
    '`timing_of_paracetamol_administered_time` AS `timing_of_paracetamol_administered_time`',
    '`timing_of_paracetamol_administered_parse_status` AS `timing_of_paracetamol_administered_parse_status`',
    '`baseline_act_num` AS `baseline_act_num`',
    '`baseline_act_parse_status` AS `baseline_act_parse_status`',
    '`post_protamine_act_num` AS `post_protamine_act_num`',
    '`post_protamine_act_parse_status` AS `post_protamine_act_parse_status`',
]


CLINICAL_REGISTRY_ERACS_EPISODE_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_eracs_episode.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_eracs_episode"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_ERACS_EPISODE_MANDATORY_RULES)
def _gold_qc_clinical_registry_eracs_episode():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_eracs_episode"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_ERACS_EPISODE_SELECT, "__gold_parent_present AS __gold_parent_present")
    df = insr_w4_apply_value_rules('clinical_registry_eracs_episode', df)  # INSR_W4
    return df


@dp.materialized_view(
    name=_n("gold_clinical.registry_eracs_episode"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_registry_eracs_episode():
    """Publish clinical_registry_eracs_episode without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_eracs_episode")).drop("__gold_parent_present")


RULES = {
    'clinical_registry_eracs_episode': GoldRules(
        table='clinical_registry_eracs_episode',
        select=CLINICAL_REGISTRY_ERACS_EPISODE_SELECT,
        mandatory=CLINICAL_REGISTRY_ERACS_EPISODE_MANDATORY_RULES,
        value_rules={'gold.clinical.registry_eracs_episode.cryo_number_of_units_transfused.insr_f9_unit_count_cap': {'action': 'invalidate',
                                                                                                         'column': 'cryo_number_of_units_transfused',
                                                                                                         'companions': [],
                                                                                                         'match': '`cryo_number_of_units_transfused` '
                                                                                                                  '> '
                                                                                                                  '50',
                                                                                                         'table': 'clinical_registry_eracs_episode'},
         'gold.clinical.registry_eracs_episode.ffp_number_of_units.insr_f9_unit_count_cap': {'action': 'invalidate',
                                                                                             'column': 'ffp_number_of_units',
                                                                                             'companions': [],
                                                                                             'match': '`ffp_number_of_units` '
                                                                                                      '> '
                                                                                                      '50',
                                                                                             'table': 'clinical_registry_eracs_episode'},
         'gold.clinical.registry_eracs_episode.platelet_number_of_units.insr_f9_unit_count_cap': {'action': 'invalidate',
                                                                                                  'column': 'platelet_number_of_units',
                                                                                                  'companions': [],
                                                                                                  'match': '`platelet_number_of_units` '
                                                                                                           '> '
                                                                                                           '50',
                                                                                                  'table': 'clinical_registry_eracs_episode'},
         'gold.clinical.registry_eracs_episode.rbc_number_of_units.insr_f9_unit_count_cap': {'action': 'invalidate',
                                                                                             'column': 'rbc_number_of_units',
                                                                                             'companions': [],
                                                                                             'match': '`rbc_number_of_units` '
                                                                                                      '> '
                                                                                                      '50',
                                                                                             'table': 'clinical_registry_eracs_episode'}}),
}
_rules = QcBinding(RULES)
insr_w4_apply_value_rules = _rules.insr_w4_apply_value_rules


finalise()


COMMENTS = {'_qc_clinical_registry_eracs_episode': {'columns': {'aama35mm': 'Indicator of whether the platelet mapping '
                                                                 'arachidonic acid maximum amplitude reached '
                                                                 'the 35 mm threshold, recorded as a yes/no '
                                                                 'response on the ERACS form.',
                                                     'acap_threshold': 'Atrial capture threshold recorded at '
                                                                       'the epicardial pacing wire check, '
                                                                       'held as a free-text value.',
                                                     'achd': 'Adult congenital heart disease procedure or '
                                                             'intervention type recorded for the episode.',
                                                     'actual_discharge_dt_tm': 'Date and time the patient '
                                                                               'was actually discharged.',
                                                     'adc_updt': 'Audit timestamp recording when the record '
                                                                 'was last updated during ingestion from the '
                                                                 'source registry.',
                                                     'admission_dt_tm': "Date and time of the patient's "
                                                                        'admission for the ERACS pathway '
                                                                        'episode.',
                                                     'adpma35mm': 'Yes/No indicator of whether the ADP '
                                                                  'maximum amplitude (MA) on platelet '
                                                                  'mapping was above 35 mm.',
                                                     'allergiestxt': 'Coded picklist label(s) describing the '
                                                                     "patient's recorded allergies captured "
                                                                     'on the ERACS form.',
                                                     'anaesthetic_registrar_fellow': 'Name of the '
                                                                                     'anaesthetic registrar '
                                                                                     'or fellow involved in '
                                                                                     "the patient's "
                                                                                     'procedure, entered as '
                                                                                     'free text.',
                                                     'antibioticstxt': 'Coded prophylactic antibiotic agents '
                                                                       'administered, stored as concatenated '
                                                                       'code and description pairs.',
                                                     'antiemetictxt': 'Coded picklist label(s) recording the '
                                                                      'antiemetic agent administered during '
                                                                      'the ERACS pathway.',
                                                     'antiplateletstxt': 'Antiplatelet agent(s) recorded for '
                                                                         'the episode, selected from the '
                                                                         'ERACS coded picklist.',
                                                     'aorta': 'Aortic procedure or intervention type '
                                                              'recorded for the episode.',
                                                     'aortic_valve': 'Aortic valve procedure or intervention '
                                                                     'type recorded for the episode.',
                                                     'areas_of_concern': 'Free-text note describing clinical '
                                                                         'areas of concern identified during '
                                                                         'the ERACS pathway episode.',
                                                     'arrhythmia_on_ic_utxt': 'Coded arrhythmias observed on '
                                                                              'ICU, stored as concatenated '
                                                                              'code and description pairs.',
                                                     'backup_rate': 'Numeric backup pacing rate set for the '
                                                                    'patient post-operatively.',
                                                     'baseline_act': 'Baseline activated clotting time (ACT) '
                                                                     'result recorded before heparinisation, '
                                                                     'stored as text.',
                                                     'baseline_act_num': 'Leading number parsed from '
                                                                         'BASELINE_ACT (ERACS_TYPED_V1).',
                                                     'baseline_act_parse_status': 'PARSED, BLANK or UNPARSED '
                                                                                  'for BASELINE_ACT.',
                                                     'belmont_volume': 'Volume recorded as delivered through '
                                                                       'the Belmont rapid infuser during the '
                                                                       'episode.',
                                                     'blood_products_administeredtxt': 'Coded list of blood '
                                                                                       'and salvage products '
                                                                                       'administered during '
                                                                                       'the episode.',
                                                     'blood_products_ic_utxt': 'Coded blood products '
                                                                               'administered on ICU, stored '
                                                                               'as concatenated code and '
                                                                               'description pairs.',
                                                     'bronchoscopy': 'Indicator of whether bronchoscopy was '
                                                                     'performed as part of the thoracic '
                                                                     'surgical episode.',
                                                     'bypasstime180_min': 'Yes/No indicator recording '
                                                                          'whether the cardiopulmonary '
                                                                          'bypass time exceeded 180 minutes '
                                                                          'for this ERACS episode.',
                                                     'cabg': 'Recorded detail of coronary artery bypass '
                                                             'grafting performed, including the number of '
                                                             'grafts.',
                                                     'cardiac_or_thoracic': 'Categorisation of the episode '
                                                                            'as a cardiac or thoracic '
                                                                            'surgical pathway.',
                                                     'cardiopulmonary_bypass_time': 'Recorded '
                                                                                    'cardiopulmonary bypass '
                                                                                    'duration in minutes for '
                                                                                    'the operation.',
                                                     'cell_salvage_transfused': 'Volume of cell-salvaged '
                                                                                'blood returned to the '
                                                                                'patient during the ERACS '
                                                                                'episode.',
                                                     'chest_drainstxt': 'Coded list of chest drain sites in '
                                                                        'place following the procedure.',
                                                     'complete_revascularisation': 'Yes/No indicator of '
                                                                                   'whether complete '
                                                                                   'surgical '
                                                                                   'revascularisation was '
                                                                                   'achieved for the '
                                                                                   'coronary procedure.',
                                                     'complicationstxt': 'Coded list of peri-operative '
                                                                         'complications recorded for the '
                                                                         'episode, with any additional '
                                                                         'detail held in the other '
                                                                         'complications field.',
                                                     'consultant_cardiothoracictxt': 'Name of the '
                                                                                     'responsible '
                                                                                     'cardiothoracic '
                                                                                     'consultant recorded '
                                                                                     'for the episode.',
                                                     'consultant_responsible_for_procedure': 'Free-text '
                                                                                             'entry '
                                                                                             'recording the '
                                                                                             'consultant '
                                                                                             'responsible '
                                                                                             'for the '
                                                                                             'procedure.',
                                                     'consultant_thoracictxt': 'Decoded name of the '
                                                                               'responsible thoracic surgery '
                                                                               'consultant recorded for the '
                                                                               'episode.',
                                                     'cross_clamp_time': 'Recorded aortic cross-clamp '
                                                                         'duration in minutes for the '
                                                                         'operation.',
                                                     'crossclamp_time120_min': 'Yes/No indicator recording '
                                                                               'whether the aortic '
                                                                               'cross-clamp time exceeded '
                                                                               '120 minutes for this ERACS '
                                                                               'episode.',
                                                     'cryo_number_of_units_transfused': 'Number of '
                                                                                        'cryoprecipitate '
                                                                                        'units transfused '
                                                                                        'for the ERACS '
                                                                                        'episode.',
                                                     'cv_couton_icu': 'Indicator of whether the central '
                                                                      'venous catheter was removed while the '
                                                                      'patient was on ICU.',
                                                     'date_last_changed': 'Date and time the registry record '
                                                                          'was last changed in the source '
                                                                          'ERACS system.',
                                                     'date_of_birth': "Patient's date of birth as recorded "
                                                                      'on the source ERACS registry record.',
                                                     'date_of_death': 'Recorded date of death of the '
                                                                      'patient, populated only where a death '
                                                                      'has been registered against the '
                                                                      'record.',
                                                     'date_of_entry': 'Date and time the ERACS registry '
                                                                      'entry was recorded for the patient.',
                                                     'discharge_to': 'Recorded destination the patient was '
                                                                     'discharged to at the end of the '
                                                                     'episode.',
                                                     'discharged_hdu_dt_tm': 'Date and time the patient was '
                                                                             'discharged from the high '
                                                                             'dependency unit.',
                                                     'drains_out_on_icu': 'Indicator of whether the chest '
                                                                          'drains were removed while the '
                                                                          'patient was on ICU.',
                                                     'drains_removed_dt': 'Date and time the chest drains '
                                                                          'were removed.',
                                                     'dripstxt': 'Coded list of intravenous infusions '
                                                                 'running for the episode, stored as '
                                                                 'concatenated code and description pairs.',
                                                     'echo_post_cb_preport_on_pacs': 'Indicator of whether '
                                                                                     'the '
                                                                                     'post-cardiopulmonary '
                                                                                     'bypass echocardiogram '
                                                                                     'report was available '
                                                                                     'on PACS.',
                                                     'entry_id': 'Internal surrogate identifier for the '
                                                                 'ERACS registry entry, carried through from '
                                                                 'the source registry record.',
                                                     'erac_spatient': 'Indicator of whether the patient was '
                                                                      'managed on the Enhanced Recovery '
                                                                      'After Cardiac Surgery pathway.',
                                                     'eracs_medication_pathway': 'Indicator of whether the '
                                                                                 'ERACS medication pathway '
                                                                                 'was followed for the '
                                                                                 'episode.',
                                                     'ethnic_origin': 'Coded ethnic category recorded for '
                                                                      'the patient in the ERACS registry.',
                                                     'ffp_number_of_units': 'Number of fresh frozen plasma '
                                                                            'units transfused for the ERACS '
                                                                            'episode.',
                                                     'fibrinogen_transfused': 'Amount of fibrinogen '
                                                                              'concentrate transfused for '
                                                                              'the ERACS episode.',
                                                     'filtertxt': 'Decoded description of the filter option '
                                                                  'selected on the ERACS peri-operative '
                                                                  'form.',
                                                     'first_mobilisation_dt_tm': 'Date and time of the '
                                                                                 "patient's first "
                                                                                 'mobilisation after '
                                                                                 'surgery.',
                                                     'first_time_sitting_dt_tm': 'Date and time the patient '
                                                                                 'first sat out as part of '
                                                                                 'the enhanced recovery '
                                                                                 'mobilisation pathway.',
                                                     'forename': "Patient's forename as recorded on the "
                                                                 'source ERACS registry record.',
                                                     'gender': 'Recorded gender of the patient associated '
                                                               'with the ERACS episode.',
                                                     'grade_of_intubation': 'Recorded laryngoscopy grade at '
                                                                            "intubation for the episode's "
                                                                            'airway management.',
                                                     'heart_rate': 'Recorded heart rate for the patient at '
                                                                   'the relevant peri-operative assessment '
                                                                   'point.',
                                                     'i_vopiate_loading_at_end': 'Yes/No indicator of '
                                                                                 'whether intravenous opiate '
                                                                                 'loading was given at the '
                                                                                 'end of the operation.',
                                                     'instruction': 'Form instruction or guidance text field '
                                                                    'on the ERACS data-entry form.',
                                                     'intraoperative_pain_controltxt': 'Decoded description '
                                                                                       'of the '
                                                                                       'intraoperative pain '
                                                                                       'control method or '
                                                                                       'methods used for the '
                                                                                       'episode.',
                                                     'is_current_in_source': 'Boolean flag indicating '
                                                                             'whether the record is still '
                                                                             'present as current in the '
                                                                             'source system.',
                                                     'issues_with_spinal_drain': 'Indicator of whether any '
                                                                                 'issues occurred with the '
                                                                                 'spinal drain during the '
                                                                                 'episode.',
                                                     'l_vfunction_post_cpb': 'Assessed left ventricular '
                                                                             'function following '
                                                                             'cardiopulmonary bypass, '
                                                                             'recorded as a graded category.',
                                                     'l_vfunction_pre': 'Categorical assessment of left '
                                                                        'ventricular function recorded '
                                                                        'before the procedure.',
                                                     'linkage_fallback_conflict_ind': 'Boolean flag '
                                                                                      'indicating whether a '
                                                                                      'conflict was detected '
                                                                                      'when a fallback '
                                                                                      'identifier was used '
                                                                                      'for person linkage.',
                                                     'linkage_historical_fallback_ind': 'Boolean flag '
                                                                                        'indicating whether '
                                                                                        'the person linkage '
                                                                                        'relied on a '
                                                                                        'historical '
                                                                                        'identifier '
                                                                                        'fallback.',
                                                     'linkage_method': 'Code indicating which identifiers '
                                                                       'were used to link the registry '
                                                                       'record to the person record.',
                                                     'linkage_status': 'Coded status describing the outcome '
                                                                       'of linking the registry record to a '
                                                                       'master person record.',
                                                     'lobes_operated_ontxt': 'Decoded description of the '
                                                                             'lung lobe or lobes operated on '
                                                                             'during the thoracic procedure.',
                                                     'lvrs': 'Indicator of whether lung volume reduction '
                                                             'surgery (LVRS) was performed for the thoracic '
                                                             'episode.',
                                                     'lymph_node_resection': 'Indicator of whether lymph '
                                                                             'node resection was carried out '
                                                                             'during the thoracic procedure.',
                                                     'm_details_of_spinal_drain_issues': 'Free-text '
                                                                                         'description of '
                                                                                         'issues encountered '
                                                                                         'with the spinal '
                                                                                         'drain during the '
                                                                                         'episode.',
                                                     'm_other_allergies': 'Free-text description of an '
                                                                          'allergy selected as Other on the '
                                                                          'ERACS allergies question.',
                                                     'm_other_arrhythmia_icu': 'Free-text description of an '
                                                                               'other arrhythmia observed on '
                                                                               'the intensive care unit when '
                                                                               'not covered by the coded '
                                                                               'arrhythmia list.',
                                                     'm_other_complications': 'Free-text description of '
                                                                              'complications not covered by '
                                                                              'the coded complications list.',
                                                     'm_other_drip_details': 'Free-text details of other '
                                                                             'infusions (drips) administered '
                                                                             'that are not covered by the '
                                                                             'coded drips list.',
                                                     'm_other_medical_history': 'Free-text narrative of '
                                                                                'additional medical history '
                                                                                'not captured by the coded '
                                                                                'medical history list.',
                                                     'm_other_medical_reason_for_delay': 'Free-text '
                                                                                         'description of an '
                                                                                         'other medical '
                                                                                         'reason for delayed '
                                                                                         'discharge or '
                                                                                         'pathway '
                                                                                         'progression, '
                                                                                         'captured when the '
                                                                                         'coded medical '
                                                                                         'reason list does '
                                                                                         'not apply.',
                                                     'm_other_non_medical_reason_for_delay': 'Free-text '
                                                                                             'description of '
                                                                                             'an other '
                                                                                             'non-medical '
                                                                                             'reason for '
                                                                                             'delayed '
                                                                                             'discharge or '
                                                                                             'pathway '
                                                                                             'progression, '
                                                                                             'captured when '
                                                                                             'the coded '
                                                                                             'non-medical '
                                                                                             'reason list '
                                                                                             'does not '
                                                                                             'apply.',
                                                     'm_other_post_op_plan_details': 'Free-text narrative '
                                                                                     'detailing additional '
                                                                                     'post-operative plan '
                                                                                     'instructions where '
                                                                                     "'Other' was selected "
                                                                                     'on the post-operative '
                                                                                     'plan question.',
                                                     'm_other_procedure_performed': 'Free-text description '
                                                                                    'of the procedure '
                                                                                    'performed when the '
                                                                                    'coded procedure list '
                                                                                    'records an other '
                                                                                    'procedure.',
                                                     'm_teg_result_summary': 'Free-text summary of '
                                                                             'thromboelastography (TEG) '
                                                                             'results recorded for the '
                                                                             'episode.',
                                                     'm_thoracic_proceduredetails': 'Free-text detail of the '
                                                                                    'thoracic procedure '
                                                                                    'performed where not '
                                                                                    'fully captured by the '
                                                                                    'coded procedure '
                                                                                    'picklist.',
                                                     'm_underlying_rhythm_details': 'Free-text detail '
                                                                                    'describing the '
                                                                                    "patient's underlying "
                                                                                    'cardiac rhythm where '
                                                                                    'the coded value '
                                                                                    'requires clarification.',
                                                     'm_volume_local_anaesthetic': 'Free-text entry '
                                                                                   'recording the volume of '
                                                                                   'local anaesthetic '
                                                                                   'administered.',
                                                     'manadatory': 'Form control field (name misspelled from '
                                                                   "'mandatory') used by the ERACS "
                                                                   'data-entry form, with no populated '
                                                                   'values observed.',
                                                     'medical_historytxt': 'Coded list of relevant '
                                                                           'pre-existing medical conditions '
                                                                           'recorded for the patient, with '
                                                                           'additional detail held in the '
                                                                           'other medical history field.',
                                                     'medical_reasons_for_delaytxt': 'Coded picklist '
                                                                                     'label(s) recording '
                                                                                     'medical reasons for '
                                                                                     "delay in the patient's "
                                                                                     'discharge or pathway '
                                                                                     'progression.',
                                                     'medication_prescribed_on_powerchart': 'Yes/No '
                                                                                            'indicator of '
                                                                                            'whether the '
                                                                                            'ERACS pathway '
                                                                                            'medication was '
                                                                                            'prescribed in '
                                                                                            'the PowerChart '
                                                                                            'electronic '
                                                                                            'record.',
                                                     'mitral_valve': 'Mitral valve procedure or intervention '
                                                                     'type recorded for the episode.',
                                                     'mrn': 'Local hospital medical record number '
                                                            'identifying the patient for this ERACS registry '
                                                            'entry.',
                                                     'nhs_number': 'NHS number recorded for the patient on '
                                                                   'the source ERACS registry record, used '
                                                                   'for national person identification and '
                                                                   'linkage.',
                                                     'nhs_number_valid_ind': 'Boolean flag indicating '
                                                                             'whether the NHS number held on '
                                                                             'the source registry record '
                                                                             'passed validation.',
                                                     'non_medical_reasonstxt': 'Decoded description of the '
                                                                               'non-medical reason(s) '
                                                                               'recorded for delay in '
                                                                               'discharge or pathway '
                                                                               'progression.',
                                                     'octoplex_units_transfused': 'Amount of Octaplex '
                                                                                  'prothrombin complex '
                                                                                  'concentrate transfused '
                                                                                  'for the ERACS episode.',
                                                     'other_ach_dprocedure': 'Record of an other adult '
                                                                             'congenital heart disease '
                                                                             'procedure performed that is '
                                                                             'not covered by the standard '
                                                                             'options.',
                                                     'other_antiplatelet': 'Free-text detail of an '
                                                                           'antiplatelet agent not covered '
                                                                           'by the coded antiplatelet '
                                                                           'picklist.',
                                                     'other_aortic_procedure': 'Record of an other aortic '
                                                                               'procedure performed that is '
                                                                               'not covered by the standard '
                                                                               'aortic procedure options.',
                                                     'other_regional_block': 'Record of an other regional '
                                                                             'anaesthetic block used, where '
                                                                             'not covered by the standard '
                                                                             'block options.',
                                                     'p_min_backup': 'Yes/No indicator of whether backup '
                                                                     '(P-min) pacing was set for the patient '
                                                                     'post-operatively.',
                                                     'p_wouton_icu': 'Indicator of whether the temporary '
                                                                     'pacing wires were removed while the '
                                                                     'patient was on ICU.',
                                                     'pacing_mode': 'Pacing mode in use on ICU for the '
                                                                    'episode.',
                                                     'pacing_wires': 'Categorical record of which temporary '
                                                                     'epicardial pacing wires were placed.',
                                                     'pain_score': 'Recorded post-operative pain score for '
                                                                   'the episode, held as a coded text value.',
                                                     'paracetamol_administered': 'Indicator of whether '
                                                                                 'paracetamol was '
                                                                                 'administered as part of '
                                                                                 'the ERACS analgesia '
                                                                                 'pathway.',
                                                     'patient_event_key': 'Deterministic SHA-256 key '
                                                                          'matching '
                                                                          'clinical_registry_entry.patient_event_key '
                                                                          'for the eracs_episode source '
                                                                          'family and native entry_id.',
                                                     'patient_medically_ready_dt_tm': 'Date and time the '
                                                                                      'patient was recorded '
                                                                                      'as medically ready '
                                                                                      'for discharge.',
                                                     'patient_on_first_floor': 'Indicator recording whether '
                                                                               'the patient was located on '
                                                                               'the first floor (step-down '
                                                                               'ward area) as part of the '
                                                                               'ERACS pathway.',
                                                     'patient_paced6hrs': 'Indicator of whether the patient '
                                                                          'was paced during the first six '
                                                                          'hours on ICU.',
                                                     'pc_ausage12_hrs': 'Patient-controlled analgesia (PCA) '
                                                                        'usage recorded over the first 12 '
                                                                        'hours post-operatively.',
                                                     'pca': 'Indicator of whether patient-controlled '
                                                            'analgesia was used for the episode.',
                                                     'person_id': 'Internal surrogate identifier of the '
                                                                  'linked person record in the source '
                                                                  'patient administration system.',
                                                     'pipeline_loaded_at': 'Timestamp when the ingestion '
                                                                           'pipeline loaded the record into '
                                                                           'this table.',
                                                     'platelet_mapping_performed': 'Yes/No indicator of '
                                                                                   'whether platelet mapping '
                                                                                   'testing was performed '
                                                                                   'for this episode.',
                                                     'platelet_number_of_units': 'Number of platelet units '
                                                                                 'transfused for the ERACS '
                                                                                 'episode.',
                                                     'pon_vscore': 'Recorded post-operative nausea and '
                                                                   'vomiting (PONV) score for the patient '
                                                                   'following cardiac or thoracic surgery, '
                                                                   'held as a coded text value.',
                                                     'post_op_analgesiatxt': 'Post-operative analgesia '
                                                                             'option(s) recorded for the '
                                                                             'episode from the ERACS coded '
                                                                             'picklist.',
                                                     'post_op_planthoracic_patientstxt': 'Decoded '
                                                                                         'description of the '
                                                                                         'agreed '
                                                                                         'post-operative '
                                                                                         'management plan '
                                                                                         'for thoracic '
                                                                                         'surgery patients.',
                                                     'post_op_plantxt': 'Coded picklist label(s) describing '
                                                                        'the agreed post-operative '
                                                                        'management plan for the patient.',
                                                     'post_protamine_act': 'Activated clotting time (ACT) '
                                                                           'result recorded after protamine '
                                                                           'administration, stored as text.',
                                                     'post_protamine_act_num': 'Leading number parsed from '
                                                                               'POST_PROTAMINE_ACT '
                                                                               '(ERACS_TYPED_V1).',
                                                     'post_protamine_act_parse_status': 'PARSED, BLANK or '
                                                                                        'UNPARSED for '
                                                                                        'POST_PROTAMINE_ACT.',
                                                     'postoperative_pain_controltxt': 'Decoded description '
                                                                                      'of the post-operative '
                                                                                      'pain control method '
                                                                                      'or methods used for '
                                                                                      'the episode.',
                                                     'primary_operatortxt': 'Name of the clinician recorded '
                                                                            'as the primary operator for the '
                                                                            'procedure.',
                                                     'procedure_performedtxt': 'Decoded description of the '
                                                                               'cardiac surgical procedure '
                                                                               'performed for the episode.',
                                                     'r_vfunction_post_cpb': 'Assessed right ventricular '
                                                                             'function following '
                                                                             'cardiopulmonary bypass, '
                                                                             'recorded as a graded category.',
                                                     'r_vfunction_pre': 'Categorical assessment of right '
                                                                        'ventricular function recorded '
                                                                        'before the procedure.',
                                                     'rbc_number_of_units': 'Number of red blood cell units '
                                                                            'transfused for the ERACS '
                                                                            'episode.',
                                                     'row_hash': 'Hash value computed across the source '
                                                                 "record's field values to support change "
                                                                 'detection during ingestion.',
                                                     'setup_pacing_mode': 'Pacing mode configured when '
                                                                          'temporary pacing was set up for '
                                                                          'the patient.',
                                                     'setup_pacing_rate': 'Pacing rate configured when '
                                                                          'temporary pacing was set up for '
                                                                          'the patient.',
                                                     'source_absent_detected_ts': 'Timestamp recording when '
                                                                                  'the ingestion process '
                                                                                  'first detected that the '
                                                                                  'record was no longer '
                                                                                  'present in the source '
                                                                                  'registry.',
                                                     'source_present_ind': 'Canonical boolean mirror of '
                                                                           'IS_CURRENT_IN_SOURCE; false rows '
                                                                           'are retained source tombstones.',
                                                     'source_record_key': 'Key of the originating source '
                                                                          'registry record, derived from the '
                                                                          'source entry identifier.',
                                                     'source_table': 'Name of the source system table from '
                                                                     'which the record was ingested.',
                                                     'spinal_drain_inserted': 'Indicator of whether a spinal '
                                                                              '(cerebrospinal fluid) drain '
                                                                              'was inserted during the '
                                                                              'episode.',
                                                     'spinal_drainage_above': 'Numeric spinal drainage '
                                                                              'threshold value recorded in '
                                                                              'the ERACS spinal drain '
                                                                              'section.',
                                                     'suggamedex_administered': 'Indicator of whether '
                                                                                'sugammadex was administered '
                                                                                'for neuromuscular blockade '
                                                                                'reversal during the '
                                                                                'episode.',
                                                     'surgical_site_checked': 'Yes/No indicator of whether '
                                                                              'the surgical site was checked '
                                                                              'as part of the ERACS '
                                                                              'peri-operative compliance '
                                                                              'checks.',
                                                     'surname': "Patient's surname as recorded on the source "
                                                                'ERACS registry record.',
                                                     'teg_post_protamine_performed': 'Yes/No indicator of '
                                                                                     'whether a '
                                                                                     'thromboelastography '
                                                                                     '(TEG) test was '
                                                                                     'performed after '
                                                                                     'protamine '
                                                                                     'administration.',
                                                     'temperature_at_arrival_on_icu': 'Patient temperature '
                                                                                      'recorded on arrival '
                                                                                      'on ICU, held as free '
                                                                                      'text.',
                                                     'thoracic_proceduretxt': 'Thoracic procedure recorded '
                                                                              'for the episode, selected '
                                                                              'from the ERACS coded '
                                                                              'picklist.',
                                                     'timing_of1st': 'Recorded timing of the first '
                                                                     'prophylactic antibiotic dose, held as '
                                                                     'free text.',
                                                     'timing_of1st_parse_status': 'PARSED, BLANK or UNPARSED '
                                                                                  'for TIMING_OF1ST.',
                                                     'timing_of1st_time': "'HH:MM' parsed from TIMING_OF1ST "
                                                                          '(ERACS_TYPED_V1).',
                                                     'timing_of2nd': 'Recorded timing of the second '
                                                                     'prophylactic antibiotic dose, held as '
                                                                     'free text.',
                                                     'timing_of2nd_parse_status': 'PARSED, BLANK or UNPARSED '
                                                                                  'for TIMING_OF2ND.',
                                                     'timing_of2nd_time': "'HH:MM' parsed from TIMING_OF2ND "
                                                                          '(ERACS_TYPED_V1).',
                                                     'timing_of_ondansetron': 'Recorded time at which '
                                                                              'ondansetron was administered '
                                                                              'during the episode.',
                                                     'timing_of_ondansetron_parse_status': 'PARSED, BLANK or '
                                                                                           'UNPARSED for '
                                                                                           'TIMING_OF_ONDANSETRON.',
                                                     'timing_of_ondansetron_time': "'HH:MM' parsed from "
                                                                                   'TIMING_OF_ONDANSETRON '
                                                                                   '(ERACS_TYPED_V1).',
                                                     'timing_of_paracetamol_administered': 'Recorded time at '
                                                                                           'which '
                                                                                           'paracetamol was '
                                                                                           'administered '
                                                                                           'during the '
                                                                                           'episode.',
                                                     'timing_of_paracetamol_administered_parse_status': 'PARSED, '
                                                                                                        'BLANK '
                                                                                                        'or '
                                                                                                        'UNPARSED '
                                                                                                        'for '
                                                                                                        'TIMING_OF_PARACETAMOL_ADMINISTERED.',
                                                     'timing_of_paracetamol_administered_time': "'HH:MM' "
                                                                                                'parsed from '
                                                                                                'TIMING_OF_PARACETAMOL_ADMINISTERED '
                                                                                                '(ERACS_TYPED_V1).',
                                                     'tof': 'Recorded train-of-four (TOF) count from '
                                                            'neuromuscular blockade monitoring for the '
                                                            'episode.',
                                                     'total_amount_pca': 'Total patient-controlled analgesia '
                                                                         'amount recorded for the episode, '
                                                                         'stored as text without a '
                                                                         'documented unit.',
                                                     'total_volume_drained': 'Total volume drained via the '
                                                                             'chest drains as recorded by '
                                                                             'the clinical team, held as '
                                                                             'free text.',
                                                     'tricuspid_valve': 'Tricuspid valve procedure or '
                                                                        'intervention type recorded for the '
                                                                        'episode.',
                                                     'two_couton_icu': 'Indicator of whether a trial without '
                                                                       'urinary catheter took place while '
                                                                       'the patient was on ICU.',
                                                     'ulcer_prophylaxistxt': 'Coded picklist label(s) '
                                                                             'recording the ulcer '
                                                                             'prophylaxis medication given '
                                                                             'during the ERACS pathway.',
                                                     'underlying_rhythmtxt': 'Coded underlying cardiac '
                                                                             'rhythm recorded for the '
                                                                             'patient.',
                                                     'vcap_threshold': 'Ventricular capture threshold '
                                                                       'recorded at the epicardial pacing '
                                                                       'wire check, held as a free-text '
                                                                       'value.',
                                                     'vein_harvest_sitetxt': 'Coded conduit vein harvest '
                                                                             'site or sites recorded for the '
                                                                             'operation, stored as '
                                                                             'concatenated code and '
                                                                             'description pairs.'},
                                         'quantity_override': True,
                                         'table': 'Internal QC of clinical_registry_eracs_episode; counts '
                                                  'rows whose Gold parent is absent.'},
 'clinical_registry_eracs_episode': {'columns': {},
                                     'quantity_override': False,
                                     'table': 'Typed child of clinical_registry_entry for the eracs_episode '
                                              'family. Enhanced Recovery After Cardiac Surgery pathway '
                                              'registry, live since 2025: pathway timestamps, pacing, '
                                              'TEG/platelet mapping, transfusion, mobilisation and discharge '
                                              'milestones. Every governed bronze column is retained under '
                                              'its lower-case name. Coverage: iWeb registries start at '
                                              'different years: coronary 2013, non-coronary 2016, surgery '
                                              '2017, cardiac MDT 2018 (CT aortic 2021), mortality review '
                                              '2018, ERACS 2025, ACS transfer 2000; ERACS episodes carry no '
                                              'encounter id and an admission time on only 5 of 1,744 '
                                              'episodes at source, so they are not matched to a hospital '
                                              'stay and admission and discharge times are not derived from '
                                              'one; complete-through month per release: '
                                              '6_mgmt.data_quality.release_finding (finding_code '
                                              'F2_complete_through).'}}
