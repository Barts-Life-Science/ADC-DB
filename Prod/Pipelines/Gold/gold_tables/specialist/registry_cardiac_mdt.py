"""Gold specialist.registry_cardiac_mdt: 2 dataset(s).

- datasets: _qc_clinical_registry_cardiac_mdt, clinical_registry_cardiac_mdt
- moved verbatim from: 3_gold_specialist_care:3322, 3_gold_specialist_care:3586, 3_gold_specialist_care:3592, 3_gold_specialist_care:3607

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.registry_cardiac_mdt ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_registry_entry applies the parent Gold drop rule.
CLINICAL_REGISTRY_CARDIAC_MDT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entry_id` AS `entry_id`',
    '`mrn` AS `mrn`',
    '`gender` AS `gender`',
    '`date_of_death` AS `date_of_death`',
    '`ethnic_origin` AS `ethnic_origin`',
    '`md_tdate` AS `md_tdate`',
    '`consultant_responsible_for_proceduretxt` AS `consultant_responsible_for_proceduretxt`',
    '`presenting_cliniciantxt` AS `presenting_cliniciantxt`',
    '`other_presenting_clinician` AS `other_presenting_clinician`',
    '`referring_hospital` AS `referring_hospital`',
    '`m_presenting_history` AS `m_presenting_history`',
    '`cardiac_risk_factorstxt` AS `cardiac_risk_factorstxt`',
    '`significant_non_coronary_cardiac_historytxt` AS `significant_non_coronary_cardiac_historytxt`',
    '`mguc_hdetail` AS `mguc_hdetail`',
    '`m_cardiomyopathy_detail` AS `m_cardiomyopathy_detail`',
    '`other_referring_hospitaltxt` AS `other_referring_hospitaltxt`',
    '`patient_preference` AS `patient_preference`',
    '`euroscore` AS `euroscore`',
    '`anginal_medicationtxt` AS `anginal_medicationtxt`',
    '`troponin_ng_l` AS `troponin_ng_l`',
    '`e_gfr_ml_min` AS `e_gfr_ml_min`',
    '`significant_valve_diseasetxt` AS `significant_valve_diseasetxt`',
    '`m_aortic_valve_disease` AS `m_aortic_valve_disease`',
    '`m_mitral_valve_disease` AS `m_mitral_valve_disease`',
    '`m_tricuspid_valve_disease` AS `m_tricuspid_valve_disease`',
    '`m_pulmonary_valve_disease` AS `m_pulmonary_valve_disease`',
    '`viability_assessmenttxt` AS `viability_assessmenttxt`',
    '`mcm_rdetail` AS `mcm_rdetail`',
    '`m_stress_echo_detail` AS `m_stress_echo_detail`',
    '`mmps_detail` AS `mmps_detail`',
    '`syntax_score1` AS `syntax_score1`',
    '`clinical_presentation` AS `clinical_presentation`',
    '`m_clinical_presentation_other` AS `m_clinical_presentation_other`',
    '`smoking_status` AS `smoking_status`',
    '`lvef` AS `lvef`',
    '`cardiology_attendeestxt` AS `cardiology_attendeestxt`',
    '`m_discussion` AS `m_discussion`',
    '`treatment_planned` AS `treatment_planned`',
    '`planned_timing` AS `planned_timing`',
    '`responsible_consultant_speciality` AS `responsible_consultant_speciality`',
    '`responsible_consultant_coronarytxt` AS `responsible_consultant_coronarytxt`',
    '`responsible_consultant_cardiothoracictxt` AS `responsible_consultant_cardiothoracictxt`',
    '`pci` AS `pci`',
    '`cabg` AS `cabg`',
    '`medical_therapy_only` AS `medical_therapy_only`',
    '`m_other_treatment` AS `m_other_treatment`',
    '`other_consultant_and_speciality` AS `other_consultant_and_speciality`',
    '`cardiothoracic_attendeestxt` AS `cardiothoracic_attendeestxt`',
    '`imaging_attendee` AS `imaging_attendee`',
    '`md_tchair` AS `md_tchair`',
    '`current_location` AS `current_location`',
    '`haemoglobin` AS `haemoglobin`',
    '`other_coronary_consultant` AS `other_coronary_consultant`',
    '`mff_rdetail` AS `mff_rdetail`',
    '`primary_operator` AS `primary_operator`',
    '`creatinine_umol_l` AS `creatinine_umol_l`',
    '`treatment_deviationtxt` AS `treatment_deviationtxt`',
    '`m_details_of_deviation` AS `m_details_of_deviation`',
    '`treatment_pathway` AS `treatment_pathway`',
    '`pathway_date` AS `pathway_date`',
    '`cause_of_deviationtxt` AS `cause_of_deviationtxt`',
    '`consultant_informed_dt` AS `consultant_informed_dt`',
    '`audit_entered_bytxt` AS `audit_entered_bytxt`',
    '`audit_dt` AS `audit_dt`',
    '`md_tguide` AS `md_tguide`',
    '`complex_mdt` AS `complex_mdt`',
    '`other_therapy_optionstxt` AS `other_therapy_optionstxt`',
    '`cardiology_consultant_of_the_weektxt` AS `cardiology_consultant_of_the_weektxt`',
    '`cardiology_consultant_otw_present` AS `cardiology_consultant_otw_present`',
    '`cardiothoracic_consultant_of_the_weektxt` AS `cardiothoracic_consultant_of_the_weektxt`',
    '`cardiothoracic_consultant_otw_present` AS `cardiothoracic_consultant_otw_present`',
    '`patient_removed_without_discussion` AS `patient_removed_without_discussion`',
    '`removed_without_discussion_other` AS `removed_without_discussion_other`',
    '`removed_dt` AS `removed_dt`',
    '`e_pattendeestxt` AS `e_pattendeestxt`',
    '`ep_consultant_of_the_weektxt` AS `ep_consultant_of_the_weektxt`',
    '`ep_consultant_of_the_week_present` AS `ep_consultant_of_the_week_present`',
    '`date_last_changed` AS `date_last_changed`',
    '`adc_updt` AS `adc_updt`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`linkage_status` AS `linkage_status`',
    '`person_id` AS `person_id`',
    '`linkage_method` AS `linkage_method`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
    '`registry_type` AS `registry_type`',
    '`source_table` AS `source_table`',
    '`source_record_key` AS `source_record_key`',
    '`relevant_medicationtxt` AS `relevant_medicationtxt`',
    '`hybrid_surgery` AS `hybrid_surgery`',
    '`open_surgery` AS `open_surgery`',
    '`other_speciality` AS `other_speciality`',
    '`other_consultants_name` AS `other_consultants_name`',
    '`endovascular` AS `endovascular`',
    '`surveillance` AS `surveillance`',
    '`discharge` AS `discharge`',
    '`aetiologytxt` AS `aetiologytxt`',
    '`pathway_dt` AS `pathway_dt`',
    '`presenting_surgeontxt` AS `presenting_surgeontxt`',
    '`consultant_responsible_for_procedure` AS `consultant_responsible_for_procedure`',
    '`m_other_relevant_medication` AS `m_other_relevant_medication`',
    '`other_patient_preference` AS `other_patient_preference`',
    '`palliative_care_only` AS `palliative_care_only`',
    '`outcome_completed` AS `outcome_completed`',
    '`m_other_attendees` AS `m_other_attendees`',
    '`m_other_aetiology` AS `m_other_aetiology`',
    '`imaging_attendee2nd` AS `imaging_attendee2nd`',
    '`surveillance_pathway_patient` AS `surveillance_pathway_patient`',
    '`use_guidelines_threshold` AS `use_guidelines_threshold`',
    '`new_operative_threshold` AS `new_operative_threshold`',
    '`new_surveillance_interval` AS `new_surveillance_interval`',
    '`operative_threshold_met` AS `operative_threshold_met`',
    '`rationale_for_contradictingtxt` AS `rationale_for_contradictingtxt`',
    '`m_other_high_risk` AS `m_other_high_risk`',
    '`m_other_comorbidities` AS `m_other_comorbidities`',
    '`m_other_reason` AS `m_other_reason`',
    '`row_hash` AS `row_hash`',
    '`pipeline_loaded_at` AS `pipeline_loaded_at`',
    '`is_current_in_source` AS `is_current_in_source`',
    '`source_present_ind` AS `source_present_ind`',
    '`source_absent_detected_ts` AS `source_absent_detected_ts`',
    '`surname` AS `surname`',
    '`forename` AS `forename`',
    '`nhs_number` AS `nhs_number`',
    '`date_of_birth` AS `date_of_birth`',
    '`referring_consultant_email` AS `referring_consultant_email`',
    '`cardiothoracic_consultant_emailtxt` AS `cardiothoracic_consultant_emailtxt`',
    '`cardiologist_consultant_emailtxt` AS `cardiologist_consultant_emailtxt`',
]


CLINICAL_REGISTRY_CARDIAC_MDT_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_cardiac_mdt.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_cardiac_mdt"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_CARDIAC_MDT_MANDATORY_RULES)
def _gold_qc_clinical_registry_cardiac_mdt():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_cardiac_mdt"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_CARDIAC_MDT_SELECT, "__gold_parent_present AS __gold_parent_present")
    return df


@dp.materialized_view(
    name=_n("gold_clinical.registry_cardiac_mdt"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_registry_cardiac_mdt():
    """Publish clinical_registry_cardiac_mdt without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_cardiac_mdt")).drop("__gold_parent_present")


finalise()


COMMENTS = {'_qc_clinical_registry_cardiac_mdt': {'columns': {'adc_updt': 'Timestamp of the last update applied to the '
                                                               'record by the upstream data ingestion '
                                                               'process.',
                                                   'aetiologytxt': 'Recorded aetiology of the condition '
                                                                   'discussed at the CT/aortic MDT, held as '
                                                                   'decoded selection text.',
                                                   'anginal_medicationtxt': 'Coded list of anti-anginal '
                                                                            'medications the patient is '
                                                                            'taking, expressed as resolved '
                                                                            'picklist labels.',
                                                   'audit_dt': 'Date and time the audit entry was recorded '
                                                               'for this MDT record.',
                                                   'audit_entered_bytxt': 'Name of the member of staff who '
                                                                          'entered the audit record for this '
                                                                          'MDT entry.',
                                                   'cabg': 'MDT recommendation status for coronary artery '
                                                           'bypass grafting as a treatment option.',
                                                   'cardiac_risk_factorstxt': 'Coded list of cardiac risk '
                                                                              'factors recorded for the '
                                                                              'patient at MDT, expressed as '
                                                                              'resolved picklist labels.',
                                                   'cardiologist_consultant_emailtxt': 'Email contact '
                                                                                       'recorded for the '
                                                                                       'cardiology '
                                                                                       'consultant '
                                                                                       'associated with the '
                                                                                       'MDT record, held as '
                                                                                       'decoded picklist '
                                                                                       'text in the '
                                                                                       'CT/aortic workflow.',
                                                   'cardiology_attendeestxt': 'Coded text list of the '
                                                                              'cardiology clinicians '
                                                                              'recorded as attending the MDT '
                                                                              'meeting.',
                                                   'cardiology_consultant_of_the_weektxt': 'Name of the '
                                                                                           'cardiology '
                                                                                           'consultant of '
                                                                                           'the week '
                                                                                           'associated with '
                                                                                           'the MDT meeting.',
                                                   'cardiology_consultant_otw_present': 'Indicator of '
                                                                                        'whether the '
                                                                                        'cardiology '
                                                                                        'consultant of the '
                                                                                        'week was present at '
                                                                                        'the MDT meeting.',
                                                   'cardiothoracic_attendeestxt': 'Decoded list of the '
                                                                                  'cardiothoracic surgical '
                                                                                  'clinicians recorded as '
                                                                                  'attending the MDT '
                                                                                  'meeting.',
                                                   'cardiothoracic_consultant_emailtxt': 'Email contact '
                                                                                         'recorded for the '
                                                                                         'cardiothoracic '
                                                                                         'consultant '
                                                                                         'associated with '
                                                                                         'the MDT record, '
                                                                                         'held as decoded '
                                                                                         'picklist text in '
                                                                                         'the CT/aortic '
                                                                                         'workflow.',
                                                   'cardiothoracic_consultant_of_the_weektxt': 'Name of the '
                                                                                               'cardiothoracic '
                                                                                               'consultant '
                                                                                               'of the week '
                                                                                               'associated '
                                                                                               'with the MDT '
                                                                                               'meeting.',
                                                   'cardiothoracic_consultant_otw_present': 'Indicator of '
                                                                                            'whether the '
                                                                                            'cardiothoracic '
                                                                                            'consultant of '
                                                                                            'the week was '
                                                                                            'present at the '
                                                                                            'MDT meeting.',
                                                   'cause_of_deviationtxt': 'Lookup label giving the '
                                                                            'recorded cause of deviation '
                                                                            'from the MDT-recommended '
                                                                            'treatment.',
                                                   'clinical_presentation': 'Coded category describing the '
                                                                            "patient's clinical presentation "
                                                                            'at referral to the MDT.',
                                                   'complex_mdt': 'Categorical indicator of the type of MDT '
                                                                  'meeting in which the case was discussed.',
                                                   'consultant_informed_dt': 'Date and time the responsible '
                                                                             'consultant was informed in '
                                                                             'relation to the MDT record.',
                                                   'consultant_responsible_for_procedure': 'Consultant '
                                                                                           'identified as '
                                                                                           'responsible for '
                                                                                           'the planned '
                                                                                           'procedure '
                                                                                           'following the '
                                                                                           'CT/aortic MDT '
                                                                                           'decision.',
                                                   'consultant_responsible_for_proceduretxt': 'Name of the '
                                                                                              'consultant '
                                                                                              'recorded as '
                                                                                              'responsible '
                                                                                              'for the '
                                                                                              'planned '
                                                                                              'procedure, '
                                                                                              'populated for '
                                                                                              'the coronary '
                                                                                              'revascularisation '
                                                                                              'workflow.',
                                                   'creatinine_umol_l': 'Serum creatinine result in '
                                                                        'micromoles per litre recorded for '
                                                                        'the MDT assessment.',
                                                   'current_location': "Coded description of the patient's "
                                                                       'care setting or location at the time '
                                                                       'of the MDT discussion.',
                                                   'date_last_changed': 'Date and time the record was last '
                                                                        'changed in the source iWeb MDT '
                                                                        'registry.',
                                                   'date_of_birth': 'Date of birth of the patient the MDT '
                                                                    'record relates to, carried through from '
                                                                    'the source iWeb MDT registries.',
                                                   'date_of_death': 'Recorded date of death of the patient, '
                                                                    'where known, as held in the source iWeb '
                                                                    'MDT registry.',
                                                   'discharge': 'Indicator that discharge from the CT/aortic '
                                                                'MDT pathway was the outcome decided.',
                                                   'e_gfr_ml_min': 'Recorded estimated glomerular filtration '
                                                                   'rate for the patient in millilitres per '
                                                                   'minute.',
                                                   'e_pattendeestxt': 'Recorded electrophysiology attendees '
                                                                      'at the MDT meeting.',
                                                   'endovascular': 'Indicator that endovascular treatment '
                                                                   'was the option decided at the CT/aortic '
                                                                   'MDT.',
                                                   'entry_id': 'Source EntryId, unique only within '
                                                               'REGISTRY_TYPE.',
                                                   'ep_consultant_of_the_week_present': 'Indicator of '
                                                                                        'whether the '
                                                                                        'electrophysiology '
                                                                                        'consultant of the '
                                                                                        'week was present at '
                                                                                        'the MDT meeting.',
                                                   'ep_consultant_of_the_weektxt': 'Name of the '
                                                                                   'electrophysiology '
                                                                                   'consultant of the week '
                                                                                   'associated with the MDT '
                                                                                   'meeting, held as decoded '
                                                                                   'text.',
                                                   'ethnic_origin': 'Coded ethnic origin of the patient as '
                                                                    'recorded in the source iWeb MDT '
                                                                    'registry.',
                                                   'euroscore': 'EuroSCORE operative risk score recorded for '
                                                                'the patient at MDT.',
                                                   'forename': 'Forename of the patient the MDT record '
                                                               'relates to, carried through from the source '
                                                               'iWeb MDT registries.',
                                                   'gender': 'Recorded gender of the patient as held in the '
                                                             'source iWeb MDT registry.',
                                                   'haemoglobin': 'Haemoglobin result recorded for the MDT '
                                                                  'assessment; units are not stated in the '
                                                                  'source metadata.',
                                                   'hybrid_surgery': 'Indicator that hybrid surgery was the '
                                                                     'treatment decided at the CT/aortic '
                                                                     'MDT.',
                                                   'imaging_attendee': 'Imaging clinician recorded as '
                                                                       'attending the MDT meeting, entered '
                                                                       'as free text.',
                                                   'imaging_attendee2nd': 'Second imaging clinician recorded '
                                                                          'as attending the MDT meeting, '
                                                                          'populated only for '
                                                                          'REGISTRY_TYPE=CT_AORTIC.',
                                                   'is_current_in_source': 'Boolean flag indicating whether '
                                                                           'the record is still present as '
                                                                           'the current version in the '
                                                                           'source system.',
                                                   'linkage_fallback_conflict_ind': 'Indicator that the '
                                                                                    'fallback linkage '
                                                                                    'produced a conflicting '
                                                                                    'patient match during '
                                                                                    'processing.',
                                                   'linkage_historical_fallback_ind': 'Indicator that '
                                                                                      'patient linkage was '
                                                                                      'achieved using a '
                                                                                      'historical identifier '
                                                                                      'fallback rather than '
                                                                                      'a current identifier '
                                                                                      'match.',
                                                   'linkage_method': 'Code describing which identifier '
                                                                     'combination was used to link the MDT '
                                                                     'record to the master patient record.',
                                                   'linkage_status': 'Coded outcome of linking the MDT '
                                                                     'record to a master patient record, '
                                                                     'indicating which identifiers matched.',
                                                   'lvef': 'Left ventricular ejection fraction value '
                                                           'recorded for the patient at the MDT assessment.',
                                                   'm_aortic_valve_disease': 'Free-text detail describing '
                                                                             "the patient's aortic valve "
                                                                             'disease where recorded.',
                                                   'm_cardiomyopathy_detail': 'Free-text detail describing '
                                                                              "the patient's cardiomyopathy "
                                                                              'where recorded in the cardiac '
                                                                              'history.',
                                                   'm_clinical_presentation_other': 'Free-text description '
                                                                                    'of the clinical '
                                                                                    'presentation when the '
                                                                                    'coded presentation '
                                                                                    'category is recorded as '
                                                                                    'other.',
                                                   'm_details_of_deviation': 'Free-text description of how '
                                                                             'the delivered treatment '
                                                                             'deviated from the MDT '
                                                                             'recommendation.',
                                                   'm_discussion': 'Free-text narrative of the MDT '
                                                                   'discussion and clinical reasoning for '
                                                                   'the patient.',
                                                   'm_mitral_valve_disease': 'Free-text detail describing '
                                                                             "the patient's mitral valve "
                                                                             'disease where recorded.',
                                                   'm_other_aetiology': 'Free-text description of aetiology '
                                                                        'where not covered by the coded '
                                                                        'AETIOLOGYTXT options, populated '
                                                                        'only for REGISTRY_TYPE=CT_AORTIC.',
                                                   'm_other_attendees': 'Free-text record of additional MDT '
                                                                        'attendees not captured in the '
                                                                        'specialty attendee fields, '
                                                                        'populated only for '
                                                                        'REGISTRY_TYPE=CT_AORTIC.',
                                                   'm_other_comorbidities': 'Free-text description of other '
                                                                            'comorbidities not captured in '
                                                                            'the coded history fields, '
                                                                            'populated only for '
                                                                            'REGISTRY_TYPE=CT_AORTIC.',
                                                   'm_other_high_risk': 'Free-text description of other '
                                                                        'high-risk features considered at '
                                                                        'the MDT, populated only for '
                                                                        'REGISTRY_TYPE=CT_AORTIC.',
                                                   'm_other_reason': 'Free-text description of another '
                                                                     'reason recorded for the MDT decision '
                                                                     'or pathway deviation, populated only '
                                                                     'for REGISTRY_TYPE=CT_AORTIC.',
                                                   'm_other_relevant_medication': 'Free-text description of '
                                                                                  'other relevant medication '
                                                                                  'recorded at the CT/aortic '
                                                                                  'MDT, populated only for '
                                                                                  'REGISTRY_TYPE=CT_AORTIC.',
                                                   'm_other_treatment': 'Free-text description of treatment '
                                                                        'agreed at the MDT that is not '
                                                                        'covered by the coded treatment '
                                                                        'options.',
                                                   'm_presenting_history': 'Free-text narrative of the '
                                                                           "patient's presenting history and "
                                                                           'relevant background recorded for '
                                                                           'the MDT discussion.',
                                                   'm_pulmonary_valve_disease': 'Recorded assessment or '
                                                                                'description of pulmonary '
                                                                                'valve disease for the '
                                                                                'patient at the cardiac MDT.',
                                                   'm_stress_echo_detail': 'Free-text detail of stress '
                                                                           'echocardiography findings '
                                                                           'recorded for the MDT assessment.',
                                                   'm_tricuspid_valve_disease': 'Free-text detail describing '
                                                                                "the patient's tricuspid "
                                                                                'valve disease where '
                                                                                'recorded.',
                                                   'mcm_rdetail': 'Free-text detail of the cardiac magnetic '
                                                                  'resonance findings considered at the MDT.',
                                                   'md_tchair': 'Clinician recorded as chairing the MDT '
                                                                'meeting.',
                                                   'md_tdate': 'Date of the multidisciplinary team meeting '
                                                               'at which the case was discussed.',
                                                   'md_tguide': 'Indicator recorded in the coronary MDT '
                                                                'record referencing the MDT guidance applied '
                                                                'to the discussion.',
                                                   'medical_therapy_only': 'MDT recommendation status for '
                                                                           'medical therapy alone as the '
                                                                           'treatment option.',
                                                   'mff_rdetail': 'Free-text detail of the fractional flow '
                                                                  'reserve assessment recorded in the '
                                                                  'coronary MDT record.',
                                                   'mguc_hdetail': 'Free-text detail supporting the grown-up '
                                                                   'congenital heart disease item recorded '
                                                                   'within the cardiac history.',
                                                   'mmps_detail': 'Free-text detail of the myocardial '
                                                                  'perfusion scan findings recorded for the '
                                                                  'MDT assessment.',
                                                   'mrn': 'Local hospital medical record number of the '
                                                          'patient the MDT record relates to, carried '
                                                          'through from the source iWeb MDT registries.',
                                                   'new_operative_threshold': 'Revised operative threshold '
                                                                              'agreed at the MDT for this '
                                                                              'patient, populated only for '
                                                                              'REGISTRY_TYPE=CT_AORTIC; no '
                                                                              'unit is stated in source '
                                                                              'metadata.',
                                                   'new_surveillance_interval': 'Revised surveillance '
                                                                                'imaging interval agreed at '
                                                                                'the MDT, populated only for '
                                                                                'REGISTRY_TYPE=CT_AORTIC; no '
                                                                                'unit is stated in source '
                                                                                'metadata.',
                                                   'nhs_number': 'NHS number of the patient the MDT record '
                                                                 'relates to, carried through from the '
                                                                 'source iWeb MDT registries.',
                                                   'nhs_number_valid_ind': 'Derived boolean indicating '
                                                                           'whether the NHS number held on '
                                                                           'the source MDT record passed '
                                                                           'validation, without exposing the '
                                                                           'number itself.',
                                                   'open_surgery': 'Indicator that open surgery was the '
                                                                   'treatment decided at the CT/aortic MDT.',
                                                   'operative_threshold_met': 'Indicates whether the '
                                                                              'applicable operative '
                                                                              'threshold was met at the time '
                                                                              'of MDT review, populated only '
                                                                              'for REGISTRY_TYPE=CT_AORTIC.',
                                                   'other_consultant_and_speciality': 'Free-text record of '
                                                                                      'an additional '
                                                                                      'consultant and their '
                                                                                      'speciality involved '
                                                                                      'in the MDT decision.',
                                                   'other_consultants_name': 'Name of another consultant '
                                                                             'involved in the CT/aortic MDT '
                                                                             'discussion, entered as free '
                                                                             'text.',
                                                   'other_coronary_consultant': 'Free-text name of an '
                                                                                'additional coronary '
                                                                                'consultant recorded against '
                                                                                'the coronary '
                                                                                'revascularisation MDT '
                                                                                'record.',
                                                   'other_patient_preference': 'Free-text detail of the '
                                                                               "patient's stated treatment "
                                                                               'preference where not covered '
                                                                               'by the coded '
                                                                               'PATIENT_PREFERENCE options, '
                                                                               'populated only for '
                                                                               'REGISTRY_TYPE=CT_AORTIC.',
                                                   'other_presenting_clinician': 'Free-text name of the '
                                                                                 'presenting clinician where '
                                                                                 'the individual is not '
                                                                                 'available in the standard '
                                                                                 'clinician picklist.',
                                                   'other_referring_hospitaltxt': 'Free-text or '
                                                                                  'picklist-derived name of '
                                                                                  'the referring hospital '
                                                                                  'where the organisation is '
                                                                                  'not available in the '
                                                                                  'standard referral list.',
                                                   'other_speciality': 'Free-text specialty of another '
                                                                       'clinician involved in or attending '
                                                                       'the CT/aortic MDT discussion.',
                                                   'other_therapy_optionstxt': 'Recorded label describing '
                                                                               'other therapy options '
                                                                               'considered by the MDT.',
                                                   'outcome_completed': 'Indicates whether the MDT outcome '
                                                                        'record has been completed, '
                                                                        'populated only for '
                                                                        'REGISTRY_TYPE=CT_AORTIC.',
                                                   'palliative_care_only': 'Indicates whether the MDT '
                                                                           'treatment decision was '
                                                                           'palliative care only, populated '
                                                                           'only for '
                                                                           'REGISTRY_TYPE=CT_AORTIC.',
                                                   'pathway_date': 'Date recorded against the treatment '
                                                                   'pathway for the MDT record.',
                                                   'pathway_dt': 'Date and time recorded for the CT/aortic '
                                                                 'MDT treatment pathway milestone.',
                                                   'patient_event_key': 'Deterministic SHA-256 key matching '
                                                                        'clinical_registry_entry.patient_event_key '
                                                                        'for the cardiac_mdt source family '
                                                                        'and native entry_id.',
                                                   'patient_preference': 'Recorded treatment preference '
                                                                         'expressed by the patient to inform '
                                                                         'the MDT decision.',
                                                   'patient_removed_without_discussion': 'Indicator or '
                                                                                         'reason recorded '
                                                                                         'when the patient '
                                                                                         'was removed from '
                                                                                         'the MDT list '
                                                                                         'without being '
                                                                                         'discussed.',
                                                   'pci': 'MDT recommendation status for percutaneous '
                                                          'coronary intervention as a treatment option.',
                                                   'person_id': 'Internal Millennium person identifier '
                                                                'linked to this MDT record via the person '
                                                                'alias table.',
                                                   'pipeline_loaded_at': 'Timestamp when the record was '
                                                                         'loaded into this table by the '
                                                                         'ingestion pipeline.',
                                                   'planned_timing': 'Coded urgency or timing category for '
                                                                     'the treatment planned at the MDT.',
                                                   'presenting_cliniciantxt': 'Name of the clinician who '
                                                                              'presented the case at the MDT '
                                                                              'meeting.',
                                                   'presenting_surgeontxt': 'Named surgeon who presented the '
                                                                            'case at the CT/aortic MDT, held '
                                                                            'as decoded text.',
                                                   'primary_operator': 'Clinician recorded as the primary '
                                                                       'operator for the planned procedure, '
                                                                       'held as free text and sometimes '
                                                                       'containing placeholder entries.',
                                                   'rationale_for_contradictingtxt': 'Free-text rationale '
                                                                                     'for a decision that '
                                                                                     'contradicts the '
                                                                                     'guideline threshold or '
                                                                                     'recommendation, '
                                                                                     'populated only for '
                                                                                     'REGISTRY_TYPE=CT_AORTIC.',
                                                   'referring_consultant_email': 'Email address recorded for '
                                                                                 'the consultant who '
                                                                                 'referred the patient for '
                                                                                 'MDT discussion, as held in '
                                                                                 'the source iWeb MDT '
                                                                                 'registries.',
                                                   'referring_hospital': 'Hospital or organisation that '
                                                                         'referred the patient for MDT '
                                                                         'discussion, selected from the '
                                                                         'source picklist.',
                                                   'registry_type': 'Source workflow discriminator and part '
                                                                    'of the composite key.',
                                                   'relevant_medicationtxt': 'Relevant medication recorded '
                                                                             'at the MDT for the CT/aortic '
                                                                             'surveillance workflow, held as '
                                                                             'decoded text.',
                                                   'removed_dt': 'Date and time the patient was removed from '
                                                                 'the MDT list.',
                                                   'removed_without_discussion_other': 'Free-text detail of '
                                                                                       'the other reason the '
                                                                                       'patient was removed '
                                                                                       'from the MDT list '
                                                                                       'without discussion.',
                                                   'responsible_consultant_cardiothoracictxt': 'Name of the '
                                                                                               'cardiothoracic '
                                                                                               'surgical '
                                                                                               'consultant '
                                                                                               'recorded as '
                                                                                               'responsible '
                                                                                               'for the '
                                                                                               "patient's "
                                                                                               'planned '
                                                                                               'treatment '
                                                                                               'following '
                                                                                               'the MDT '
                                                                                               'decision, '
                                                                                               'held as '
                                                                                               'decoded '
                                                                                               'picklist '
                                                                                               'text.',
                                                   'responsible_consultant_coronarytxt': 'Name of the '
                                                                                         'coronary '
                                                                                         '(cardiology) '
                                                                                         'consultant '
                                                                                         'recorded as '
                                                                                         'responsible for '
                                                                                         "the patient's "
                                                                                         'planned treatment '
                                                                                         'following the MDT '
                                                                                         'decision, held as '
                                                                                         'decoded picklist '
                                                                                         'text.',
                                                   'responsible_consultant_speciality': 'Coded speciality of '
                                                                                        'the consultant '
                                                                                        'designated as '
                                                                                        'responsible for the '
                                                                                        "patient's planned "
                                                                                        'treatment.',
                                                   'row_hash': "Hash of the source record's business columns "
                                                               'used to detect changes between pipeline '
                                                               'loads.',
                                                   'significant_non_coronary_cardiac_historytxt': 'Coded '
                                                                                                  'list of '
                                                                                                  'significant '
                                                                                                  'non-coronary '
                                                                                                  'cardiac '
                                                                                                  'history '
                                                                                                  'items '
                                                                                                  'recorded '
                                                                                                  'at MDT, '
                                                                                                  'expressed '
                                                                                                  'as '
                                                                                                  'resolved '
                                                                                                  'picklist '
                                                                                                  'labels.',
                                                   'significant_valve_diseasetxt': 'Coded list of valves '
                                                                                   'affected by significant '
                                                                                   'valve disease, expressed '
                                                                                   'as resolved picklist '
                                                                                   'labels.',
                                                   'smoking_status': 'Coded smoking status recorded as part '
                                                                     'of the cardiac risk factor assessment '
                                                                     'for the MDT.',
                                                   'source_absent_detected_ts': 'Timestamp recorded by the '
                                                                                'ingestion pipeline when the '
                                                                                'record was first detected '
                                                                                'as no longer present in the '
                                                                                'source iWeb registry.',
                                                   'source_present_ind': 'Canonical boolean mirror of '
                                                                         'IS_CURRENT_IN_SOURCE; false rows '
                                                                         'are retained source tombstones.',
                                                   'source_record_key': 'Identifier of the originating '
                                                                        'source registry record, derived '
                                                                        'from the source EntryId and used '
                                                                        'with REGISTRY_TYPE to identify the '
                                                                        'record uniquely.',
                                                   'source_table': 'Name of the source iWeb registry table '
                                                                   'from which the harmonised record was '
                                                                   'derived.',
                                                   'surname': 'Surname of the patient the MDT record relates '
                                                              'to, carried through from the source iWeb MDT '
                                                              'registries.',
                                                   'surveillance': 'Indicator that continued surveillance '
                                                                   'was the outcome decided at the CT/aortic '
                                                                   'MDT.',
                                                   'surveillance_pathway_patient': 'Indicates whether the '
                                                                                   'patient is managed on '
                                                                                   'the aortic surveillance '
                                                                                   'pathway, populated only '
                                                                                   'for '
                                                                                   'REGISTRY_TYPE=CT_AORTIC.',
                                                   'syntax_score1': "SYNTAX score recorded for the patient's "
                                                                    'coronary anatomy at the MDT.',
                                                   'treatment_deviationtxt': 'Lookup label indicating '
                                                                             'whether the treatment '
                                                                             'delivered deviated from the '
                                                                             'MDT recommendation.',
                                                   'treatment_pathway': 'Coded description of the treatment '
                                                                        'pathway followed relative to the '
                                                                        'MDT recommendation.',
                                                   'treatment_planned': 'Coded category describing the '
                                                                        'treatment setting or pathway '
                                                                        'planned at the MDT.',
                                                   'troponin_ng_l': 'Recorded troponin result for the '
                                                                    'patient in nanograms per litre.',
                                                   'use_guidelines_threshold': 'Indicates whether the '
                                                                               'standard guideline operative '
                                                                               'threshold was applied rather '
                                                                               'than a locally agreed '
                                                                               'threshold, populated only '
                                                                               'for REGISTRY_TYPE=CT_AORTIC.',
                                                   'viability_assessmenttxt': 'Coded text describing the '
                                                                              'myocardial viability '
                                                                              'assessment method recorded at '
                                                                              'the MDT.'},
                                       'quantity_override': True,
                                       'table': 'Internal QC of clinical_registry_cardiac_mdt; counts rows '
                                                'whose Gold parent is absent.'},
 'clinical_registry_cardiac_mdt': {'columns': {'date_last_changed': 'Date and time the record was last '
                                                                    'changed in the source iWeb MDT '
                                                                    'registry. iWeb last-changed time; reset '
                                                                    'for most entries in April 2026 by a '
                                                                    'registry maintenance run, so it does '
                                                                    'not show when an entry was last '
                                                                    'clinically edited.'},
                                   'quantity_override': False,
                                   'table': 'Typed child of clinical_registry_entry for the cardiac_mdt '
                                            'family. Harmonised iWeb cardiac MDT record. '
                                            'REGISTRY_TYPE=CORONARY_REVASC identifies the coronary '
                                            'revascularisation workflow; REGISTRY_TYPE=CT_AORTIC identifies '
                                            'CT/aortic surveillance. Workflow-specific columns are null for '
                                            'the other registry. Every governed bronze column is retained '
                                            'under its lower-case name. Coverage: iWeb registries start at '
                                            'different years: coronary 2013, non-coronary 2016, surgery '
                                            '2017, cardiac MDT 2018 (CT aortic 2021), mortality review 2018, '
                                            'ERACS 2025, ACS transfer 2000; complete-through month per '
                                            'release: 6_mgmt.data_quality.release_finding (finding_code '
                                            'F2_complete_through).'}}
