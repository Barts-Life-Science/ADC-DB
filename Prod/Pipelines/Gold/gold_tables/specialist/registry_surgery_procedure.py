"""Gold specialist.registry_surgery_procedure: 2 dataset(s).

- datasets: _qc_clinical_registry_surgery_procedure, clinical_registry_surgery_procedure
- moved verbatim from: 3_gold_specialist_care:5963, 3_gold_specialist_care:6169, 3_gold_specialist_care:6175, 3_gold_specialist_care:6181, 3_gold_specialist_care:6197

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.registry_surgery_procedure ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_registry_entry applies the parent Gold drop rule.
CLINICAL_REGISTRY_SURGERY_PROCEDURE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entry_id` AS `entry_id`',
    '`mrn` AS `mrn`',
    '`gender` AS `gender`',
    '`date_of_death` AS `date_of_death`',
    '`ethnic_origin` AS `ethnic_origin`',
    '`parent_entry_id` AS `parent_entry_id`',
    '`procedure_site` AS `procedure_site`',
    '`graft_conduit` AS `graft_conduit`',
    '`graft_anastomosis` AS `graft_anastomosis`',
    '`graft_site` AS `graft_site`',
    '`valve_implant_name` AS `valve_implant_name`',
    '`valve_haemodynamic_pathology` AS `valve_haemodynamic_pathology`',
    '`native_valve_pathology` AS `native_valve_pathology`',
    '`native_valve_other_pathology` AS `native_valve_other_pathology`',
    '`reason_for_repeat_valve_replacementtxt` AS `reason_for_repeat_valve_replacementtxt`',
    '`other_reason_for_repeat_valve_replacement` AS `other_reason_for_repeat_valve_replacement`',
    '`valve_procedure` AS `valve_procedure`',
    '`valve_implant_type` AS `valve_implant_type`',
    '`implant_prosthesis_nametxt` AS `implant_prosthesis_nametxt`',
    '`valve_or_ring_model_number` AS `valve_or_ring_model_number`',
    '`valve_or_ring_serial_number` AS `valve_or_ring_serial_number`',
    '`valve_or_ring_size` AS `valve_or_ring_size`',
    '`valve_explant_type` AS `valve_explant_type`',
    '`aortic_pathologytxt` AS `aortic_pathologytxt`',
    '`aortic_procedure` AS `aortic_procedure`',
    '`aortic_arch_type` AS `aortic_arch_type`',
    '`distal_anastamosis_constructed_bytxt` AS `distal_anastamosis_constructed_bytxt`',
    '`proximal_anastamosis_constructed_bytxt` AS `proximal_anastamosis_constructed_bytxt`',
    '`aortotomytxt` AS `aortotomytxt`',
    '`excision_of_valvetxt` AS `excision_of_valvetxt`',
    '`decalcification_of_annulustxt` AS `decalcification_of_annulustxt`',
    '`implantation_of_valvetxt` AS `implantation_of_valvetxt`',
    '`closure_of_aortotomytxt` AS `closure_of_aortotomytxt`',
    '`de_airing_of_hearttxt` AS `de_airing_of_hearttxt`',
    '`bi_caval_cannulationtxt` AS `bi_caval_cannulationtxt`',
    '`access_to_mitral_valvetxt` AS `access_to_mitral_valvetxt`',
    '`assessment_and_repairtxt` AS `assessment_and_repairtxt`',
    '`excision_of_valve_and_annular_debridementtxt` AS `excision_of_valve_and_annular_debridementtxt`',
    '`repair_of_valvetxt` AS `repair_of_valvetxt`',
    '`ringtxt` AS `ringtxt`',
    '`implantation_of_mitral_valvetxt` AS `implantation_of_mitral_valvetxt`',
    '`atrial_closuretxt` AS `atrial_closuretxt`',
    '`de_airing_of_heart_mitraltxt` AS `de_airing_of_heart_mitraltxt`',
    '`limatxt` AS `limatxt`',
    '`rimatxt` AS `rimatxt`',
    '`lsvtxt` AS `lsvtxt`',
    '`ssvtxt` AS `ssvtxt`',
    '`radialtxt` AS `radialtxt`',
    '`other_veintxt` AS `other_veintxt`',
    '`other_arterytxt` AS `other_arterytxt`',
    '`conduit_quality` AS `conduit_quality`',
    '`grafted_using` AS `grafted_using`',
    '`closed_using` AS `closed_using`',
    '`valve_implanted_using` AS `valve_implanted_using`',
    '`size_of_coronary_artery` AS `size_of_coronary_artery`',
    '`coronary_artery_condition` AS `coronary_artery_condition`',
    '`primary_repair_techniquetxt` AS `primary_repair_techniquetxt`',
    '`neochord` AS `neochord`',
    '`minimally_invasive_mitral_valve_surgery` AS `minimally_invasive_mitral_valve_surgery`',
    '`m_aortic_graft_detail` AS `m_aortic_graft_detail`',
    '`aortic_graft_serial_number` AS `aortic_graft_serial_number`',
    '`reimplantation_of_intercostals` AS `reimplantation_of_intercostals`',
    '`implantation_technique` AS `implantation_technique`',
    '`m_level_of_implantation` AS `m_level_of_implantation`',
    '`aortic_occlusion_bypass` AS `aortic_occlusion_bypass`',
    '`s_vcharacteristicstxt` AS `s_vcharacteristicstxt`',
    '`harvesting_characteristicstxt` AS `harvesting_characteristicstxt`',
    '`holes` AS `holes`',
    '`a_gscore` AS `a_gscore`',
    '`msv_gcomments` AS `msv_gcomments`',
    '`harvesting_technique` AS `harvesting_technique`',
    '`amount_of_heparin_before_evh` AS `amount_of_heparin_before_evh`',
    '`import_child_id` AS `import_child_id`',
    '`native_aortic_valve_morphology` AS `native_aortic_valve_morphology`',
    '`aortic_procedure2txt` AS `aortic_procedure2txt`',
    '`aortic_implant_locationtxt` AS `aortic_implant_locationtxt`',
    '`date_last_changed` AS `date_last_changed`',
    '`wall_quality` AS `wall_quality`',
    '`accessibility` AS `accessibility`',
    '`distal_run_off` AS `distal_run_off`',
    '`tvc_sinterpretation` AS `tvc_sinterpretation`',
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


CLINICAL_REGISTRY_SURGERY_PROCEDURE_INSR_W4_ADVISORY_RULES = {  # INSR_W4
    # INSR_W4 W4.06 string_discipline (review L4946 (documented in the procedure description)): Drug-picklist bleed-through into the prosthesis name (L4946): dose and form words are not devices.
    'gold.clinical.registry_surgery_procedure.implant_prosthesis_nametxt.insr_f19_prosthesis_drug_name':
        "NOT COALESCE((lower(`implant_prosthesis_nametxt`) RLIKE '(^|[^a-z])(mg|mcg|microgram|tablets?|capsules?|injection|infusion|solution|ampoules?|vials?)([^a-z]|$)'), FALSE)",
}


CLINICAL_REGISTRY_SURGERY_PROCEDURE_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_surgery_procedure.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_surgery_procedure"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_SURGERY_PROCEDURE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_REGISTRY_SURGERY_PROCEDURE_INSR_W4_ADVISORY_RULES)  # INSR_W4
def _gold_qc_clinical_registry_surgery_procedure():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_surgery_procedure"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_SURGERY_PROCEDURE_SELECT, "__gold_parent_present AS __gold_parent_present")
    return df


@dp.materialized_view(
    name=_n("gold_clinical.registry_surgery_procedure"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_registry_surgery_procedure():
    """Publish clinical_registry_surgery_procedure without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_surgery_procedure")).drop("__gold_parent_present")


finalise()


COMMENTS = {'_qc_clinical_registry_surgery_procedure': {'columns': {'a_gscore': 'Numeric assessment score recorded for '
                                                                     'the harvested conduit on the iWeb '
                                                                     'CS2010G subprocedure form.',
                                                         'access_to_mitral_valvetxt': 'Free-text record of '
                                                                                      'the operator who '
                                                                                      'performed the access '
                                                                                      'to the mitral valve '
                                                                                      'step of this CS2010G '
                                                                                      'mitral valve '
                                                                                      'procedure.',
                                                         'accessibility': 'Recorded accessibility of the '
                                                                          'target vessel for this graft '
                                                                          'component, as captured in iWeb.',
                                                         'adc_updt': 'Timestamp of the last update applied '
                                                                     'to this record by the upstream data '
                                                                     'collection process.',
                                                         'amount_of_heparin_before_evh': 'Amount of heparin '
                                                                                         'given before '
                                                                                         'endoscopic vein '
                                                                                         'harvesting, '
                                                                                         'recorded as a '
                                                                                         'number without a '
                                                                                         'stated unit in the '
                                                                                         'source.',
                                                         'aortic_arch_type': 'Coded classification of the '
                                                                             'aortic arch type recorded for '
                                                                             'the aortic component of this '
                                                                             'CS2010G surgery.',
                                                         'aortic_graft_serial_number': 'Manufacturer serial '
                                                                                       'number of the aortic '
                                                                                       'graft implanted for '
                                                                                       'this component.',
                                                         'aortic_implant_locationtxt': 'Text value recording '
                                                                                       'the anatomical '
                                                                                       'location of the '
                                                                                       'aortic implant for '
                                                                                       'this surgery '
                                                                                       'component, as '
                                                                                       'captured in iWeb.',
                                                         'aortic_occlusion_bypass': 'Records the aortic '
                                                                                    'occlusion or bypass '
                                                                                    'approach used during '
                                                                                    'this aortic component '
                                                                                    'of the surgery.',
                                                         'aortic_pathologytxt': 'Text description of the '
                                                                                'aortic pathology recorded '
                                                                                'for the aortic component of '
                                                                                'this CS2010G surgery in '
                                                                                'iWeb.',
                                                         'aortic_procedure': 'Type of aortic procedure '
                                                                             'performed for this component, '
                                                                             'as recorded in the iWeb '
                                                                             'CS2010G subprocedure form.',
                                                         'aortic_procedure2txt': 'Text description of an '
                                                                                 'additional aortic '
                                                                                 'procedure recorded for '
                                                                                 'this component alongside '
                                                                                 'AORTIC_PROCEDURE.',
                                                         'aortotomytxt': 'Free-text record of the operator '
                                                                         'who performed the aortotomy step '
                                                                         'of this CS2010G aortic valve '
                                                                         'procedure.',
                                                         'assessment_and_repairtxt': 'Free-text record of '
                                                                                     'the operator who '
                                                                                     'performed the mitral '
                                                                                     'valve assessment and '
                                                                                     'repair step of this '
                                                                                     'CS2010G procedure.',
                                                         'atrial_closuretxt': 'Free-text record of the '
                                                                              'operator who performed the '
                                                                              'atrial closure step of this '
                                                                              'CS2010G mitral valve '
                                                                              'procedure.',
                                                         'bi_caval_cannulationtxt': 'Free-text record of the '
                                                                                    'operator who performed '
                                                                                    'the bi-caval '
                                                                                    'cannulation step of '
                                                                                    'this CS2010G mitral '
                                                                                    'valve procedure.',
                                                         'closed_using': 'Suture or material used for '
                                                                         'closure at this stage of the '
                                                                         'CS2010G surgery component, as '
                                                                         'recorded in iWeb.',
                                                         'closure_of_aortotomytxt': 'Free-text record of the '
                                                                                    'operator who performed '
                                                                                    'the aortotomy closure '
                                                                                    'step of this CS2010G '
                                                                                    'aortic valve procedure.',
                                                         'conduit_quality': 'Recorded quality assessment of '
                                                                            'the graft conduit for this '
                                                                            'CS2010G subprocedure component, '
                                                                            'as captured in iWeb.',
                                                         'coronary_artery_condition': 'Recorded condition of '
                                                                                      'the target coronary '
                                                                                      'artery for this graft '
                                                                                      'component in the iWeb '
                                                                                      'CS2010G form.',
                                                         'date_last_changed': 'Timestamp of the last change '
                                                                              'made to this subprocedure '
                                                                              'record in the iWeb source '
                                                                              'system.',
                                                         'date_of_birth': 'Patient date of birth as recorded '
                                                                          'on the iWeb CS2010G subprocedure '
                                                                          'record.',
                                                         'date_of_death': 'Recorded date of death for the '
                                                                          'patient associated with this '
                                                                          'surgery record, null where no '
                                                                          'death is recorded.',
                                                         'de_airing_of_heart_mitraltxt': 'Free-text record '
                                                                                         'of the operator '
                                                                                         'who performed the '
                                                                                         'de-airing of the '
                                                                                         'heart step of this '
                                                                                         'CS2010G mitral '
                                                                                         'valve procedure.',
                                                         'de_airing_of_hearttxt': 'Free-text record of the '
                                                                                  'operator who performed '
                                                                                  'the de-airing of the '
                                                                                  'heart step of this '
                                                                                  'CS2010G aortic valve '
                                                                                  'procedure.',
                                                         'decalcification_of_annulustxt': 'Free-text record '
                                                                                          'of the operator '
                                                                                          'who performed the '
                                                                                          'annulus '
                                                                                          'decalcification '
                                                                                          'step of this '
                                                                                          'CS2010G aortic '
                                                                                          'valve procedure.',
                                                         'distal_anastamosis_constructed_bytxt': 'Free-text '
                                                                                                 'record of '
                                                                                                 'the '
                                                                                                 'operator '
                                                                                                 'who '
                                                                                                 'constructed '
                                                                                                 'the distal '
                                                                                                 'anastomosis '
                                                                                                 'during '
                                                                                                 'this '
                                                                                                 'CS2010G '
                                                                                                 'surgical '
                                                                                                 'component.',
                                                         'distal_run_off': 'Recorded distal run-off '
                                                                           'assessment of the target '
                                                                           'coronary artery for this graft '
                                                                           'component.',
                                                         'entry_id': 'Stable iWeb child-record key.',
                                                         'ethnic_origin': 'Coded ethnic origin of the '
                                                                          'patient as recorded in iWeb.',
                                                         'excision_of_valve_and_annular_debridementtxt': 'Free-text '
                                                                                                         'record '
                                                                                                         'of '
                                                                                                         'the '
                                                                                                         'operator '
                                                                                                         'who '
                                                                                                         'performed '
                                                                                                         'the '
                                                                                                         'valve '
                                                                                                         'excision '
                                                                                                         'and '
                                                                                                         'annular '
                                                                                                         'debridement '
                                                                                                         'step '
                                                                                                         'of '
                                                                                                         'this '
                                                                                                         'CS2010G '
                                                                                                         'mitral '
                                                                                                         'valve '
                                                                                                         'procedure.',
                                                         'excision_of_valvetxt': 'Free-text record of the '
                                                                                 'operator who performed the '
                                                                                 'valve excision step of '
                                                                                 'this CS2010G aortic valve '
                                                                                 'procedure.',
                                                         'forename': 'Patient forename as recorded on the '
                                                                     'iWeb CS2010G subprocedure record.',
                                                         'gender': 'Patient gender recorded on the iWeb '
                                                                   'cardiac surgery record, held as a '
                                                                   'descriptive text value.',
                                                         'graft_anastomosis': 'Anastomosis technique used '
                                                                              'for this graft, from the iWeb '
                                                                              'picklist.',
                                                         'graft_conduit': 'Type of conduit used for this '
                                                                          'coronary graft, from the iWeb '
                                                                          'picklist.',
                                                         'graft_site': 'Target coronary anatomical site to '
                                                                       'which this graft was anastomosed, '
                                                                       'from the iWeb picklist.',
                                                         'grafted_using': 'Suture or material used to '
                                                                          'construct the graft anastomosis '
                                                                          'for this component, as recorded '
                                                                          'in iWeb.',
                                                         'harvesting_characteristicstxt': 'Text description '
                                                                                          'of conduit '
                                                                                          'harvesting '
                                                                                          'characteristics '
                                                                                          'recorded for this '
                                                                                          'graft component.',
                                                         'harvesting_technique': 'Technique used to harvest '
                                                                                 'the conduit for this graft '
                                                                                 'component, as recorded in '
                                                                                 'iWeb.',
                                                         'holes': 'Records holes identified in the harvested '
                                                                  'conduit for this graft component, as '
                                                                  'entered in iWeb.',
                                                         'implant_prosthesis_nametxt': 'Text label of the '
                                                                                       'implanted prosthesis '
                                                                                       'selected in iWeb; '
                                                                                       'known source defect '
                                                                                       'means some values '
                                                                                       'are drug-picklist '
                                                                                       'bleed-through rather '
                                                                                       'than prosthesis '
                                                                                       'names.',
                                                         'implantation_of_mitral_valvetxt': 'Free-text '
                                                                                            'record of the '
                                                                                            'operator who '
                                                                                            'performed the '
                                                                                            'mitral valve '
                                                                                            'implantation '
                                                                                            'step of this '
                                                                                            'CS2010G '
                                                                                            'procedure.',
                                                         'implantation_of_valvetxt': 'Free-text record of '
                                                                                     'the operator who '
                                                                                     'performed the valve '
                                                                                     'implantation step of '
                                                                                     'this CS2010G aortic '
                                                                                     'valve procedure.',
                                                         'implantation_technique': 'Technique used to '
                                                                                   'implant the prosthesis '
                                                                                   'or graft for this '
                                                                                   'component, as recorded '
                                                                                   'in iWeb.',
                                                         'import_child_id': 'Legacy child-record identifier '
                                                                            'carried over from data imported '
                                                                            'into iWeb for this component '
                                                                            'row.',
                                                         'is_current_in_source': 'Boolean flag indicating '
                                                                                 'whether the record is '
                                                                                 'still present as a current '
                                                                                 'row in the upstream source '
                                                                                 'system.',
                                                         'limatxt': 'Free-text record of the operator who '
                                                                    'harvested or used the left internal '
                                                                    'mammary artery conduit in this CS2010G '
                                                                    'graft component.',
                                                         'linkage_fallback_conflict_ind': 'Boolean indicator '
                                                                                          'flagging that the '
                                                                                          'fallback linkage '
                                                                                          'rule produced '
                                                                                          'conflicting '
                                                                                          'person matches '
                                                                                          'for this record.',
                                                         'linkage_historical_fallback_ind': 'Boolean '
                                                                                            'indicator '
                                                                                            'showing whether '
                                                                                            'the person '
                                                                                            'linkage was '
                                                                                            'resolved using '
                                                                                            'a historical '
                                                                                            'identifier '
                                                                                            'fallback rule.',
                                                         'linkage_method': 'Code describing which identifier '
                                                                           'combination was used to link '
                                                                           'this record to the master person '
                                                                           'index.',
                                                         'linkage_status': 'Coded outcome of the patient '
                                                                           'linkage process between this '
                                                                           'registry record and the master '
                                                                           'person index.',
                                                         'lsvtxt': 'Free-text record of the operator who '
                                                                   'harvested or used the long saphenous '
                                                                   'vein conduit in this CS2010G graft '
                                                                   'component.',
                                                         'm_aortic_graft_detail': 'Descriptive detail of the '
                                                                                  'aortic graft used for '
                                                                                  'this implant component, '
                                                                                  'as recorded in iWeb.',
                                                         'm_level_of_implantation': 'Anatomical level at '
                                                                                    'which the aortic graft '
                                                                                    'or prosthesis was '
                                                                                    'implanted for this '
                                                                                    'component.',
                                                         'minimally_invasive_mitral_valve_surgery': 'Indicates '
                                                                                                    'whether '
                                                                                                    'a '
                                                                                                    'minimally '
                                                                                                    'invasive '
                                                                                                    'approach '
                                                                                                    'was '
                                                                                                    'used '
                                                                                                    'for the '
                                                                                                    'mitral '
                                                                                                    'valve '
                                                                                                    'component '
                                                                                                    'of this '
                                                                                                    'surgery.',
                                                         'mrn': 'Local hospital medical record number '
                                                                'identifying the patient for this cardiac '
                                                                'surgery component record.',
                                                         'msv_gcomments': 'Free-text comments recorded about '
                                                                          'the saphenous vein graft '
                                                                          'harvesting for this component.',
                                                         'native_aortic_valve_morphology': 'Recorded '
                                                                                           'morphology of '
                                                                                           "the patient's "
                                                                                           'native aortic '
                                                                                           'valve for this '
                                                                                           'valve component.',
                                                         'native_valve_other_pathology': 'Free-text '
                                                                                         'description of '
                                                                                         'other native valve '
                                                                                         'pathology where '
                                                                                         'the standard '
                                                                                         'picklist category '
                                                                                         'does not apply.',
                                                         'native_valve_pathology': 'Coded pathology of the '
                                                                                   "patient's native valve "
                                                                                   'recorded for this valve '
                                                                                   'component in iWeb.',
                                                         'neochord': 'Indicates whether neochordae were used '
                                                                     'or describes the neochord detail for '
                                                                     'this mitral valve repair component.',
                                                         'nhs_number': 'Patient NHS number as recorded on '
                                                                       'the iWeb CS2010G subprocedure '
                                                                       'record.',
                                                         'nhs_number_valid_ind': 'Boolean indicator showing '
                                                                                 'whether the NHS number '
                                                                                 'held on the source record '
                                                                                 'passed validation, derived '
                                                                                 'from the source NHS '
                                                                                 'number.',
                                                         'other_arterytxt': 'Free-text record of the '
                                                                            'operator who harvested or used '
                                                                            'another arterial conduit in '
                                                                            'this CS2010G graft component.',
                                                         'other_reason_for_repeat_valve_replacement': 'Free-text '
                                                                                                      'description '
                                                                                                      'of '
                                                                                                      'another '
                                                                                                      'reason '
                                                                                                      'for '
                                                                                                      'repeat '
                                                                                                      'valve '
                                                                                                      'replacement '
                                                                                                      'where '
                                                                                                      'the '
                                                                                                      'standard '
                                                                                                      'picklist '
                                                                                                      'category '
                                                                                                      'does '
                                                                                                      'not '
                                                                                                      'apply.',
                                                         'other_veintxt': 'Free-text record of the operator '
                                                                          'who harvested or used another '
                                                                          'venous conduit in this CS2010G '
                                                                          'graft component.',
                                                         'parent_entry_id': 'Foreign key to '
                                                                            'iweb_cardiac_surgery_episode.ENTRY_ID.',
                                                         'patient_event_key': 'Deterministic SHA-256 key '
                                                                              'matching '
                                                                              'clinical_registry_entry.patient_event_key '
                                                                              'for the surgery_procedure '
                                                                              'source family and native '
                                                                              'entry_id.',
                                                         'person_id': 'Internal surrogate person identifier '
                                                                      'from the Millennium person index, '
                                                                      'used to link this record to the '
                                                                      'patient master.',
                                                         'pipeline_loaded_at': 'Timestamp recording when '
                                                                               'this row was loaded into the '
                                                                               'bronze table by the '
                                                                               'ingestion pipeline.',
                                                         'primary_repair_techniquetxt': 'Text description of '
                                                                                        'the primary valve '
                                                                                        'repair technique '
                                                                                        'used for this '
                                                                                        'component, as '
                                                                                        'recorded in iWeb.',
                                                         'procedure_site': 'Anatomical site of the heart or '
                                                                           'coronary target at which this '
                                                                           'surgery component was performed, '
                                                                           'as recorded in the iWeb CS2010G '
                                                                           'subprocedure form.',
                                                         'proximal_anastamosis_constructed_bytxt': 'Free-text '
                                                                                                   'record '
                                                                                                   'of the '
                                                                                                   'operator '
                                                                                                   'who '
                                                                                                   'constructed '
                                                                                                   'the '
                                                                                                   'proximal '
                                                                                                   'anastomosis '
                                                                                                   'during '
                                                                                                   'this '
                                                                                                   'CS2010G '
                                                                                                   'surgical '
                                                                                                   'component.',
                                                         'radialtxt': 'Free-text record of the operator who '
                                                                      'harvested or used the radial artery '
                                                                      'conduit in this CS2010G graft '
                                                                      'component.',
                                                         'reason_for_repeat_valve_replacementtxt': 'Text '
                                                                                                   'value '
                                                                                                   'recording '
                                                                                                   'the '
                                                                                                   'reason a '
                                                                                                   'repeat '
                                                                                                   'valve '
                                                                                                   'replacement '
                                                                                                   'was '
                                                                                                   'required '
                                                                                                   'for this '
                                                                                                   'valve '
                                                                                                   'component, '
                                                                                                   'as '
                                                                                                   'captured '
                                                                                                   'in iWeb.',
                                                         'reimplantation_of_intercostals': 'Records whether '
                                                                                           'intercostal '
                                                                                           'arteries were '
                                                                                           'reimplanted '
                                                                                           'during this '
                                                                                           'aortic implant '
                                                                                           'component.',
                                                         'repair_of_valvetxt': 'Free-text record of the '
                                                                               'operator who performed the '
                                                                               'mitral valve repair step of '
                                                                               'this CS2010G procedure.',
                                                         'rimatxt': 'Free-text record of the operator who '
                                                                    'harvested or used the right internal '
                                                                    'mammary artery conduit in this CS2010G '
                                                                    'graft component.',
                                                         'ringtxt': 'Free-text record of the operator who '
                                                                    'performed the annuloplasty ring step of '
                                                                    'this CS2010G mitral valve procedure.',
                                                         'row_hash': 'Hash of the source column values for '
                                                                     'this row, used for change detection '
                                                                     'during ingestion.',
                                                         's_vcharacteristicstxt': 'Text description of the '
                                                                                  'saphenous vein conduit '
                                                                                  'characteristics recorded '
                                                                                  'for this graft component.',
                                                         'size_of_coronary_artery': 'Recorded size of the '
                                                                                    'target coronary artery '
                                                                                    'for this graft '
                                                                                    'component, held as text '
                                                                                    'including the unit as '
                                                                                    'entered in iWeb.',
                                                         'source_absent_detected_ts': 'Timestamp recording '
                                                                                      'when this record was '
                                                                                      'first detected as no '
                                                                                      'longer present in the '
                                                                                      'upstream iWeb source '
                                                                                      'system, null while '
                                                                                      'the record remains '
                                                                                      'present.',
                                                         'source_present_ind': 'Canonical boolean mirror of '
                                                                               'IS_CURRENT_IN_SOURCE; false '
                                                                               'rows are retained source '
                                                                               'tombstones.',
                                                         'source_record_key': 'Key identifying this row in '
                                                                              'the upstream source table, '
                                                                              'derived from the source '
                                                                              'record identifier.',
                                                         'source_table': 'Name of the upstream source table '
                                                                         'from which this row was ingested.',
                                                         'ssvtxt': 'Free-text record of the operator who '
                                                                   'harvested or used the short saphenous '
                                                                   'vein conduit in this CS2010G graft '
                                                                   'component.',
                                                         'surname': 'Patient surname as recorded on the iWeb '
                                                                    'CS2010G subprocedure record.',
                                                         'tvc_sinterpretation': 'Recorded interpretation of '
                                                                                'the transit time flow or '
                                                                                'vessel check assessment for '
                                                                                'this graft component, as '
                                                                                'entered in iWeb.',
                                                         'valve_explant_type': 'Type of valve or prior '
                                                                               'repair removed at this '
                                                                               'procedure, from the iWeb '
                                                                               'picklist.',
                                                         'valve_haemodynamic_pathology': 'Haemodynamic '
                                                                                         'pathology of the '
                                                                                         'valve, such as the '
                                                                                         'recorded '
                                                                                         'functional lesion '
                                                                                         'category, from the '
                                                                                         'iWeb picklist.',
                                                         'valve_implant_name': 'Valve position or implant '
                                                                               'designation recorded for '
                                                                               'this valve component in '
                                                                               'iWeb.',
                                                         'valve_implant_type': 'Category of valve implant '
                                                                               'used for this component, as '
                                                                               'recorded in the iWeb '
                                                                               'picklist.',
                                                         'valve_implanted_using': 'Suture technique or '
                                                                                  'material used to implant '
                                                                                  'the valve prosthesis or '
                                                                                  'ring for this component, '
                                                                                  'as recorded in iWeb.',
                                                         'valve_or_ring_model_number': 'Manufacturer model '
                                                                                       'number of the '
                                                                                       'implanted valve or '
                                                                                       'annuloplasty ring.',
                                                         'valve_or_ring_serial_number': 'Manufacturer serial '
                                                                                        'number of the '
                                                                                        'implanted valve or '
                                                                                        'annuloplasty ring, '
                                                                                        'with some entries '
                                                                                        'containing '
                                                                                        'additional '
                                                                                        'free-text device '
                                                                                        'description.',
                                                         'valve_or_ring_size': 'Recorded size of the '
                                                                               'implanted valve or '
                                                                               'annuloplasty ring as an '
                                                                               'integer, as captured in '
                                                                               'iWeb.',
                                                         'valve_procedure': 'Type of valve procedure '
                                                                            'performed for this component, '
                                                                            'such as the recorded operative '
                                                                            'category from the iWeb '
                                                                            'picklist.',
                                                         'wall_quality': 'Recorded quality of the target '
                                                                         'vessel wall for this graft '
                                                                         'component, as captured in iWeb.'},
                                             'quantity_override': True,
                                             'table': 'Internal QC of clinical_registry_surgery_procedure; '
                                                      'counts rows whose Gold parent is absent.'},
 'clinical_registry_surgery_procedure': {'columns': {},
                                         'quantity_override': False,
                                         'table': 'Typed child of clinical_registry_entry for the '
                                                  'surgery_procedure family. One row per graft, valve or '
                                                  'aortic-implant component of a CS2010G surgery. '
                                                  'PARENT_ENTRY_ID joins iweb_cardiac_surgery_episode and is '
                                                  'hard-gated to zero orphans. Known source defect: '
                                                  'IMPLANT_PROSTHESIS_NAMETXT contains some drug-picklist '
                                                  'bleed-through. Every governed bronze column is retained '
                                                  'under its lower-case name. Coverage: iWeb registries '
                                                  'start at different years: coronary 2013, non-coronary '
                                                  '2016, surgery 2017, cardiac MDT 2018 (CT aortic 2021), '
                                                  'mortality review 2018, ERACS 2025, ACS transfer 2000; '
                                                  'complete-through month per release: '
                                                  '6_mgmt.data_quality.release_finding (finding_code '
                                                  'F2_complete_through).'}}
