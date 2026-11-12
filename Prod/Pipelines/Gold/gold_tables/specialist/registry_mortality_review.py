"""Gold specialist.registry_mortality_review: 2 dataset(s).

- datasets: _qc_clinical_registry_mortality_review, clinical_registry_mortality_review
- moved verbatim from: 3_gold_specialist_care:4816, 3_gold_specialist_care:5062, 3_gold_specialist_care:5068, 3_gold_specialist_care:5074, 3_gold_specialist_care:5090

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.registry_mortality_review ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_registry_entry applies the parent Gold drop rule.
CLINICAL_REGISTRY_MORTALITY_REVIEW_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entry_id` AS `entry_id`',
    '`mrn` AS `mrn`',
    '`gender` AS `gender`',
    '`date_of_death_demog` AS `date_of_death_demog`',
    '`ethnic_origin` AS `ethnic_origin`',
    '`dateof_death` AS `dateof_death`',
    '`date_and_time_of_death` AS `date_and_time_of_death`',
    '`consultant_in_charge_at_time_of_deathtxt` AS `consultant_in_charge_at_time_of_deathtxt`',
    '`date_and_time_of_admission` AS `date_and_time_of_admission`',
    '`date_and_time_of_last_medical_review` AS `date_and_time_of_last_medical_review`',
    '`critical_care_unit_episode_during_admission` AS `critical_care_unit_episode_during_admission`',
    '`causeofdeath_1a` AS `causeofdeath_1a`',
    '`causeofdeath_1b` AS `causeofdeath_1b`',
    '`causeofdeath_1c` AS `causeofdeath_1c`',
    '`causeofdeath_2` AS `causeofdeath_2`',
    '`significant_co_existing_factorstxt` AS `significant_co_existing_factorstxt`',
    '`was_the_patient_covered_by_dols_or_mc_a_order` AS `was_the_patient_covered_by_dols_or_mc_a_order`',
    '`coronial_case` AS `coronial_case`',
    '`outcome_of_coronial_referral` AS `outcome_of_coronial_referral`',
    '`serious_incident_or_complaint_raised_regarding_any_aspect` AS `serious_incident_or_complaint_raised_regarding_any_aspect`',
    '`discharge_summary_completed` AS `discharge_summary_completed`',
    '`timeto1st_review_hrs` AS `timeto1st_review_hrs`',
    '`patient_admitted_to_appropriate_ward` AS `patient_admitted_to_appropriate_ward`',
    '`any_period_when_pt_not_reviewed_by_a_consultant_for72_hours` AS `any_period_when_pt_not_reviewed_by_a_consultant_for72_hours`',
    '`did_the_pt_fall_whilst_on_the_wards` AS `did_the_pt_fall_whilst_on_the_wards`',
    '`did_the_pt_develop_a_pressure_ulcer_since_admission` AS `did_the_pt_develop_a_pressure_ulcer_since_admission`',
    '`was_there_a_clear_treatment_escalation_plan_documented` AS `was_there_a_clear_treatment_escalation_plan_documented`',
    '`was_there_evidence_of_a_resuscitation_decision_documented` AS `was_there_evidence_of_a_resuscitation_decision_documented`',
    '`was_the_pt_cared_for_using_the_compassionate_care_plan` AS `was_the_pt_cared_for_using_the_compassionate_care_plan`',
    '`ncepod_classification` AS `ncepod_classification`',
    '`m_why_is_ncepod_bcde` AS `m_why_is_ncepod_bcde`',
    '`m_anything_else_of_note` AS `m_anything_else_of_note`',
    '`date_of_completion_of_form` AS `date_of_completion_of_form`',
    '`date_of_mortality_review` AS `date_of_mortality_review`',
    '`ward_where_patient_died` AS `ward_where_patient_died`',
    '`consultant_responsible_for_procedure` AS `consultant_responsible_for_procedure`',
    '`primary_operator` AS `primary_operator`',
    '`specialitytxt` AS `specialitytxt`',
    '`m_note` AS `m_note`',
    '`m_clinical_historyevents` AS `m_clinical_historyevents`',
    '`entered_into_database` AS `entered_into_database`',
    '`presented_bytxt` AS `presented_bytxt`',
    '`peereview` AS `peereview`',
    '`cardiology_attendeestxt` AS `cardiology_attendeestxt`',
    '`surgical_attendeestxt` AS `surgical_attendeestxt`',
    '`chairman` AS `chairman`',
    '`medical_examiner` AS `medical_examiner`',
    '`cremated` AS `cremated`',
    '`me_discussed` AS `me_discussed`',
    '`survey_off` AS `survey_off`',
    '`me_classification` AS `me_classification`',
    '`me_review_dt` AS `me_review_dt`',
    '`death_ccp` AS `death_ccp`',
    '`ccp_initiated` AS `ccp_initiated`',
    '`m_me_notes` AS `m_me_notes`',
    '`dsummarycomp` AS `dsummarycomp`',
    '`further_action_to_be_taken` AS `further_action_to_be_taken`',
    '`m_action_required` AS `m_action_required`',
    '`lfd_concern` AS `lfd_concern`',
    '`lfd_resus` AS `lfd_resus`',
    '`lfd_recog` AS `lfd_recog`',
    '`lfd_commun` AS `lfd_commun`',
    '`lf_documentation` AS `lf_documentation`',
    '`lfd_procedural` AS `lfd_procedural`',
    '`lfd_other` AS `lfd_other`',
    '`mlfd_details` AS `mlfd_details`',
    '`mlfd_action` AS `mlfd_action`',
    '`cr_sinstruction` AS `cr_sinstruction`',
    '`at_the_end_of_the_admission` AS `at_the_end_of_the_admission`',
    '`during_admission` AS `during_admission`',
    '`was_this_an_elective_death` AS `was_this_an_elective_death`',
    '`problem_in_assessment` AS `problem_in_assessment`',
    '`problem_with_medication` AS `problem_with_medication`',
    '`problem_related_to_treatment_and_management_plan` AS `problem_related_to_treatment_and_management_plan`',
    '`problem_with_infection_management` AS `problem_with_infection_management`',
    '`problem_related_to_operative_invasive_procedure` AS `problem_related_to_operative_invasive_procedure`',
    '`problem_in_clinical_monitoring` AS `problem_in_clinical_monitoring`',
    '`problem_in_resuscitation` AS `problem_in_resuscitation`',
    '`problem_of_any_other_type` AS `problem_of_any_other_type`',
    '`m_sharing_education` AS `m_sharing_education`',
    '`m_quality_improvement_project` AS `m_quality_improvement_project`',
    '`m_other_specific_action` AS `m_other_specific_action`',
    '`were_there_any_problems_with_the_care` AS `were_there_any_problems_with_the_care`',
    '`did_the_problem_lead_to_harm1` AS `did_the_problem_lead_to_harm1`',
    '`did_the_problem_lead_to_harm2` AS `did_the_problem_lead_to_harm2`',
    '`did_the_problem_lead_to_harm3` AS `did_the_problem_lead_to_harm3`',
    '`did_the_problem_lead_to_harm4` AS `did_the_problem_lead_to_harm4`',
    '`did_the_problem_lead_to_harm5` AS `did_the_problem_lead_to_harm5`',
    '`did_the_problem_lead_to_harm6` AS `did_the_problem_lead_to_harm6`',
    '`did_the_problem_lead_to_harm7` AS `did_the_problem_lead_to_harm7`',
    '`did_the_problem_lead_to_harm8` AS `did_the_problem_lead_to_harm8`',
    '`m_other_problem_details` AS `m_other_problem_details`',
    '`presented_by_other` AS `presented_by_other`',
    '`form_completed_bytxt` AS `form_completed_bytxt`',
    '`form_completed_by_other` AS `form_completed_by_other`',
    '`date_last_changed` AS `date_last_changed`',
    '`adc_updt` AS `adc_updt`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`linkage_status` AS `linkage_status`',
    '`person_id` AS `person_id`',
    '`linkage_method` AS `linkage_method`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
    '`date_of_death_quality` AS `date_of_death_quality`',
    '`date_of_death_clean` AS `date_of_death_clean`',
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
    '`nameofkin` AS `nameofkin`',
    '`phone_of_kin` AS `phone_of_kin`',
    '`m_commentskin` AS `m_commentskin`',
]


CLINICAL_REGISTRY_MORTALITY_REVIEW_INSR_W4_ADVISORY_RULES = {  # INSR_W4
    # INSR_W4 W4.05 date_ordering (estate 2026-10-29): A mortality review cannot precede the death it reviews (L3185).
    'gold.clinical.registry_mortality_review.table.insr_f6_review_before_death':
        'NOT COALESCE((CAST(`date_of_mortality_review` AS DATE) < coalesce(`date_of_death_clean`, CAST(`dateof_death` AS DATE))), FALSE)',
}


CLINICAL_REGISTRY_MORTALITY_REVIEW_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_mortality_review.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_mortality_review"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_MORTALITY_REVIEW_MANDATORY_RULES)
@dp.expect_all(CLINICAL_REGISTRY_MORTALITY_REVIEW_INSR_W4_ADVISORY_RULES)  # INSR_W4
def _gold_qc_clinical_registry_mortality_review():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_mortality_review"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_MORTALITY_REVIEW_SELECT, "__gold_parent_present AS __gold_parent_present")
    return df


@dp.materialized_view(
    name=_n("gold_clinical.registry_mortality_review"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_registry_mortality_review():
    """Publish clinical_registry_mortality_review without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_mortality_review")).drop("__gold_parent_present")


finalise()


COMMENTS = {'_qc_clinical_registry_mortality_review': {'columns': {'adc_updt': 'Timestamp of the last extract or update '
                                                                    'of the record by the ingestion process.',
                                                        'any_period_when_pt_not_reviewed_by_a_consultant_for72_hours': 'Indicator '
                                                                                                                       'of '
                                                                                                                       'whether '
                                                                                                                       'there '
                                                                                                                       'was '
                                                                                                                       'any '
                                                                                                                       'period '
                                                                                                                       'during '
                                                                                                                       'which '
                                                                                                                       'the '
                                                                                                                       'patient '
                                                                                                                       'was '
                                                                                                                       'not '
                                                                                                                       'reviewed '
                                                                                                                       'by '
                                                                                                                       'a '
                                                                                                                       'consultant '
                                                                                                                       'for '
                                                                                                                       '72 '
                                                                                                                       'hours.',
                                                        'at_the_end_of_the_admission': 'Review response '
                                                                                       'describing the care '
                                                                                       'position at the end '
                                                                                       'of the admission.',
                                                        'cardiology_attendeestxt': 'Cardiology staff '
                                                                                   'recorded as attending '
                                                                                   'the mortality or M&M '
                                                                                   'meeting, held as name '
                                                                                   'text.',
                                                        'causeofdeath_1a': 'Source-text cause of death '
                                                                           'recorded in part 1a of the death '
                                                                           'certificate chain, not asserted '
                                                                           'to be coded.',
                                                        'causeofdeath_1b': 'Source-text cause of death '
                                                                           'recorded in part 1b of the death '
                                                                           'certificate chain, not asserted '
                                                                           'to be coded.',
                                                        'causeofdeath_1c': 'Source-text cause of death '
                                                                           'recorded in part 1c of the death '
                                                                           'certificate chain, not asserted '
                                                                           'to be coded.',
                                                        'causeofdeath_2': 'Source-text contributory '
                                                                          'conditions recorded in part 2 of '
                                                                          'the death certificate, not '
                                                                          'asserted to be coded.',
                                                        'ccp_initiated': 'Indicator of whether a '
                                                                         'compassionate care plan was '
                                                                         'initiated for the patient.',
                                                        'chairman': 'Chair of the mortality or M&M review '
                                                                    'meeting, recorded as a short staff '
                                                                    'reference or initials.',
                                                        'consultant_in_charge_at_time_of_deathtxt': 'Free-text '
                                                                                                    'name of '
                                                                                                    'the '
                                                                                                    'consultant '
                                                                                                    'responsible '
                                                                                                    'for the '
                                                                                                    'patient '
                                                                                                    'at the '
                                                                                                    'time of '
                                                                                                    'death.',
                                                        'consultant_responsible_for_procedure': 'Consultant '
                                                                                                'with '
                                                                                                'overall '
                                                                                                'responsibility '
                                                                                                'for the '
                                                                                                'procedure, '
                                                                                                'held as a '
                                                                                                'source-system '
                                                                                                'staff '
                                                                                                'reference.',
                                                        'coronial_case': 'Indicator of whether the death was '
                                                                         'referred to or treated as a '
                                                                         'coronial case.',
                                                        'cr_sinstruction': 'Field recording the '
                                                                           'coroner-related instruction '
                                                                           'captured during the mortality '
                                                                           'review.',
                                                        'cremated': 'Indicator of whether the death '
                                                                    'proceeded to cremation, relevant to '
                                                                    'certification and medical examiner '
                                                                    'processes.',
                                                        'critical_care_unit_episode_during_admission': 'Indicator '
                                                                                                       'of '
                                                                                                       'whether '
                                                                                                       'the '
                                                                                                       'patient '
                                                                                                       'had '
                                                                                                       'a '
                                                                                                       'critical '
                                                                                                       'care '
                                                                                                       'unit '
                                                                                                       'episode '
                                                                                                       'during '
                                                                                                       'the '
                                                                                                       'admission.',
                                                        'date_and_time_of_admission': 'Date and time the '
                                                                                      'patient was admitted '
                                                                                      'for the admission '
                                                                                      'during which the '
                                                                                      'death occurred.',
                                                        'date_and_time_of_death': 'Date and time of death '
                                                                                  'recorded on the mortality '
                                                                                  'review form.',
                                                        'date_and_time_of_last_medical_review': 'Date and '
                                                                                                'time of the '
                                                                                                'last '
                                                                                                'documented '
                                                                                                'medical '
                                                                                                'review of '
                                                                                                'the patient '
                                                                                                'before '
                                                                                                'death.',
                                                        'date_last_changed': 'Timestamp recording when the '
                                                                             'mortality review record was '
                                                                             'last modified in the source '
                                                                             'registry.',
                                                        'date_of_birth': 'Date of birth of the patient as '
                                                                         'recorded in the source mortality '
                                                                         'review registry.',
                                                        'date_of_completion_of_form': 'Date on which the '
                                                                                      'mortality review form '
                                                                                      'was completed.',
                                                        'date_of_death_clean': 'Date of death with future '
                                                                               'values nulled.',
                                                        'date_of_death_demog': 'Date of death taken from the '
                                                                               'patient demographic record, '
                                                                               'as distinct from the review '
                                                                               'form death date.',
                                                        'date_of_death_quality': 'VALID | FUTURE | MISSING; '
                                                                                 'raw DATEOF_DEATH is '
                                                                                 'retained.',
                                                        'date_of_mortality_review': 'Date on which the '
                                                                                    'mortality or M&M review '
                                                                                    'of the case took place.',
                                                        'dateof_death': 'Date of death as recorded on the '
                                                                        'mortality review form, retained in '
                                                                        'raw form alongside the cleaned '
                                                                        'derivation.',
                                                        'death_ccp': 'Indicator of whether the patient died '
                                                                     'while on the compassionate care plan '
                                                                     'pathway.',
                                                        'did_the_problem_lead_to_harm1': 'Structured '
                                                                                         'assessment of '
                                                                                         'whether the first '
                                                                                         'identified care '
                                                                                         'problem led to '
                                                                                         'patient harm.',
                                                        'did_the_problem_lead_to_harm2': 'Structured '
                                                                                         'assessment of '
                                                                                         'whether the second '
                                                                                         'identified care '
                                                                                         'problem led to '
                                                                                         'patient harm.',
                                                        'did_the_problem_lead_to_harm3': 'Structured '
                                                                                         'assessment of '
                                                                                         'whether the third '
                                                                                         'identified care '
                                                                                         'problem led to '
                                                                                         'patient harm.',
                                                        'did_the_problem_lead_to_harm4': 'Structured '
                                                                                         'assessment of '
                                                                                         'whether the fourth '
                                                                                         'identified care '
                                                                                         'problem led to '
                                                                                         'patient harm.',
                                                        'did_the_problem_lead_to_harm5': 'Structured '
                                                                                         'assessment of '
                                                                                         'whether the fifth '
                                                                                         'identified care '
                                                                                         'problem led to '
                                                                                         'patient harm.',
                                                        'did_the_problem_lead_to_harm6': 'Structured '
                                                                                         'assessment of '
                                                                                         'whether the sixth '
                                                                                         'identified care '
                                                                                         'problem led to '
                                                                                         'patient harm.',
                                                        'did_the_problem_lead_to_harm7': 'Structured '
                                                                                         'assessment of '
                                                                                         'whether the '
                                                                                         'seventh identified '
                                                                                         'care problem led '
                                                                                         'to patient harm.',
                                                        'did_the_problem_lead_to_harm8': 'Structured '
                                                                                         'assessment of '
                                                                                         'whether the eighth '
                                                                                         'identified care '
                                                                                         'problem led to '
                                                                                         'patient harm.',
                                                        'did_the_pt_develop_a_pressure_ulcer_since_admission': 'Reviewer-recorded '
                                                                                                               'categorical '
                                                                                                               'response '
                                                                                                               'indicating '
                                                                                                               'whether '
                                                                                                               'the '
                                                                                                               'patient '
                                                                                                               'developed '
                                                                                                               'a '
                                                                                                               'pressure '
                                                                                                               'ulcer '
                                                                                                               'after '
                                                                                                               'admission.',
                                                        'did_the_pt_fall_whilst_on_the_wards': 'Indicator of '
                                                                                               'whether the '
                                                                                               'patient '
                                                                                               'sustained a '
                                                                                               'fall while '
                                                                                               'on the wards '
                                                                                               'during the '
                                                                                               'admission.',
                                                        'discharge_summary_completed': 'Indicator of whether '
                                                                                       'a discharge summary '
                                                                                       'was completed for '
                                                                                       'the admission.',
                                                        'dsummarycomp': 'Date and time the discharge summary '
                                                                        'was completed.',
                                                        'during_admission': 'Review response describing the '
                                                                            'care position during the '
                                                                            'admission.',
                                                        'entered_into_database': 'Source-system indicator '
                                                                                 'recording whether the case '
                                                                                 'has been entered into the '
                                                                                 'review database.',
                                                        'entry_id': 'Internal surrogate identifier for the '
                                                                    'mortality review record, carried from '
                                                                    'the source registry entry key.',
                                                        'ethnic_origin': 'Coded ethnic origin of the patient '
                                                                         'as recorded in the source registry '
                                                                         'demographics.',
                                                        'forename': 'Given name of the deceased patient as '
                                                                    'recorded in the source mortality review '
                                                                    'registry.',
                                                        'form_completed_by_other': 'Free-text entry naming '
                                                                                   'the person who completed '
                                                                                   'the form when not '
                                                                                   'available in the '
                                                                                   'standard selection list.',
                                                        'form_completed_bytxt': 'Text value of the staff '
                                                                                'member recorded as having '
                                                                                'completed the mortality '
                                                                                'review form.',
                                                        'further_action_to_be_taken': 'Indicator of whether '
                                                                                      'further action is '
                                                                                      'required following '
                                                                                      'the mortality review.',
                                                        'gender': 'Recorded gender of the patient as held in '
                                                                  'the source registry demographics.',
                                                        'is_current_in_source': 'Boolean flag indicating '
                                                                                'whether the record was '
                                                                                'still present in the source '
                                                                                'system at the latest '
                                                                                'pipeline load.',
                                                        'lf_documentation': 'Learning-from-deaths flag '
                                                                            'indicating whether a concern '
                                                                            'relating to documentation was '
                                                                            'identified.',
                                                        'lfd_commun': 'Learning-from-deaths flag indicating '
                                                                      'whether a concern relating to '
                                                                      'communication was identified.',
                                                        'lfd_concern': 'Learning-from-deaths flag indicating '
                                                                       'whether a general concern about care '
                                                                       'was identified.',
                                                        'lfd_other': 'Learning-from-deaths flag indicating '
                                                                     'whether a concern of any other type '
                                                                     'was identified.',
                                                        'lfd_procedural': 'Learning-from-deaths flag '
                                                                          'indicating whether a concern '
                                                                          'relating to a procedure was '
                                                                          'identified.',
                                                        'lfd_recog': 'Learning-from-deaths flag indicating '
                                                                     'whether a concern relating to '
                                                                     'recognition of deterioration or '
                                                                     'illness was identified.',
                                                        'lfd_resus': 'Learning-from-deaths flag indicating '
                                                                     'whether a concern relating to '
                                                                     'resuscitation was identified.',
                                                        'linkage_fallback_conflict_ind': 'Boolean flag '
                                                                                         'indicating that '
                                                                                         'the fallback '
                                                                                         'linkage attempt '
                                                                                         'produced '
                                                                                         'conflicting person '
                                                                                         'matches.',
                                                        'linkage_historical_fallback_ind': 'Boolean flag '
                                                                                           'indicating that '
                                                                                           'patient linkage '
                                                                                           'was resolved '
                                                                                           'using a '
                                                                                           'historical '
                                                                                           '(non-current) '
                                                                                           'alias fallback '
                                                                                           'rather than a '
                                                                                           'current '
                                                                                           'identifier '
                                                                                           'match.',
                                                        'linkage_method': 'Code describing the identifier '
                                                                          'combination used to link the '
                                                                          'mortality review record to the '
                                                                          'master person record.',
                                                        'linkage_status': 'Coded status describing the '
                                                                          'outcome of linking the review '
                                                                          'record to a master patient '
                                                                          'identity.',
                                                        'm_action_required': 'Free-text description of the '
                                                                             'actions required following the '
                                                                             'mortality review.',
                                                        'm_anything_else_of_note': 'Free-text narrative '
                                                                                   'capturing any additional '
                                                                                   'points of note from the '
                                                                                   'mortality or M&M review.',
                                                        'm_clinical_historyevents': 'Free-text clinical '
                                                                                    'history and sequence of '
                                                                                    'events leading to the '
                                                                                    'death, as summarised '
                                                                                    'for review.',
                                                        'm_commentskin': 'Free-text comments relating to the '
                                                                         "patient's next of kin, such as "
                                                                         'contact or communication notes '
                                                                         'recorded at review.',
                                                        'm_me_notes': 'Free-text notes recorded by or about '
                                                                      'the medical examiner review.',
                                                        'm_note': 'Free-text note recorded against the '
                                                                  'mortality review record.',
                                                        'm_other_problem_details': 'Free-text narrative '
                                                                                   'describing the other '
                                                                                   'care problem identified '
                                                                                   'during the mortality '
                                                                                   'review.',
                                                        'm_other_specific_action': 'Free-text note '
                                                                                   'describing any other '
                                                                                   'specific action agreed '
                                                                                   'following the mortality '
                                                                                   'review.',
                                                        'm_quality_improvement_project': 'Free-text note '
                                                                                         'describing any '
                                                                                         'quality '
                                                                                         'improvement '
                                                                                         'project arising '
                                                                                         'from the mortality '
                                                                                         'review.',
                                                        'm_sharing_education': 'Free-text note describing '
                                                                               'sharing or educational '
                                                                               'actions agreed as a result '
                                                                               'of the mortality review.',
                                                        'm_why_is_ncepod_bcde': 'Free-text reviewer '
                                                                                'explanation of why the case '
                                                                                'was graded in the lower '
                                                                                'NCEPOD categories.',
                                                        'me_classification': "Medical examiner's "
                                                                             'classification outcome '
                                                                             'recorded for the death at '
                                                                             'scrutiny.',
                                                        'me_discussed': 'Indicator of whether the death was '
                                                                        'discussed with the medical '
                                                                        'examiner.',
                                                        'me_review_dt': 'Date and time the medical examiner '
                                                                        'review of the death was carried '
                                                                        'out.',
                                                        'medical_examiner': 'Medical examiner associated '
                                                                            'with scrutiny of the death, '
                                                                            'recorded as a staff reference.',
                                                        'mlfd_action': 'Free-text description of actions '
                                                                       'arising from the '
                                                                       'learning-from-deaths concerns.',
                                                        'mlfd_details': 'Free-text details of the '
                                                                        'learning-from-deaths concerns '
                                                                        'identified at review.',
                                                        'mrn': 'Local hospital medical record number '
                                                               'identifying the deceased patient in the '
                                                               'source registry.',
                                                        'nameofkin': "Free-text name of the patient's next "
                                                                     'of kin as recorded on the mortality '
                                                                     'review form.',
                                                        'ncepod_classification': 'NCEPOD care-quality '
                                                                                 'grading assigned to the '
                                                                                 'death at mortality review, '
                                                                                 'held as a coded category '
                                                                                 'label.',
                                                        'nhs_number': 'NHS number of the patient as held in '
                                                                      'the source mortality review registry, '
                                                                      'retained in its source text '
                                                                      'formatting.',
                                                        'nhs_number_valid_ind': 'Derived boolean indicator '
                                                                                'showing whether the source '
                                                                                'NHS number passed '
                                                                                'validation during '
                                                                                'processing.',
                                                        'outcome_of_coronial_referral': 'Recorded outcome of '
                                                                                        'the referral of the '
                                                                                        'death to the '
                                                                                        'coroner.',
                                                        'patient_admitted_to_appropriate_ward': 'Reviewer '
                                                                                                'assessment '
                                                                                                'of whether '
                                                                                                'the patient '
                                                                                                'was '
                                                                                                'admitted to '
                                                                                                'an '
                                                                                                'appropriate '
                                                                                                'ward.',
                                                        'patient_event_key': 'Deterministic SHA-256 key '
                                                                             'matching '
                                                                             'clinical_registry_entry.patient_event_key '
                                                                             'for the mortality_review '
                                                                             'source family and native '
                                                                             'entry_id.',
                                                        'peereview': 'Indicator of whether the case '
                                                                     'underwent peer review as part of the '
                                                                     'mortality review process.',
                                                        'person_id': 'Internal system-assigned identifier of '
                                                                     'the linked patient record in the '
                                                                     'master person table.',
                                                        'phone_of_kin': 'Contact telephone number for the '
                                                                        "patient's next of kin recorded on "
                                                                        'the mortality review form.',
                                                        'pipeline_loaded_at': 'Timestamp recording when the '
                                                                              'record was loaded into this '
                                                                              'table by the ingestion '
                                                                              'pipeline.',
                                                        'presented_by_other': 'Free-text entry naming the '
                                                                              'presenter of the case at the '
                                                                              'review meeting when not '
                                                                              'available in the standard '
                                                                              'selection list.',
                                                        'presented_bytxt': 'Clinician who presented the case '
                                                                           'at the mortality or M&M meeting, '
                                                                           'recorded as name text.',
                                                        'primary_operator': 'Primary operator performing the '
                                                                            'procedure, held as a '
                                                                            'source-system staff reference.',
                                                        'problem_in_assessment': 'Indicator of whether a '
                                                                                 'problem in patient '
                                                                                 'assessment was identified '
                                                                                 'during the mortality '
                                                                                 'review.',
                                                        'problem_in_clinical_monitoring': 'Structured review '
                                                                                          'response '
                                                                                          'indicating '
                                                                                          'whether a problem '
                                                                                          'in clinical '
                                                                                          'monitoring of the '
                                                                                          'patient was '
                                                                                          'identified during '
                                                                                          'the mortality '
                                                                                          'review.',
                                                        'problem_in_resuscitation': 'Structured review '
                                                                                    'response indicating '
                                                                                    'whether a problem in '
                                                                                    'resuscitation was '
                                                                                    'identified during the '
                                                                                    'mortality review.',
                                                        'problem_of_any_other_type': 'Structured review '
                                                                                     'response indicating '
                                                                                     'whether a care problem '
                                                                                     'of any other type was '
                                                                                     'identified during the '
                                                                                     'mortality review.',
                                                        'problem_related_to_operative_invasive_procedure': 'Structured '
                                                                                                           'review '
                                                                                                           'response '
                                                                                                           'indicating '
                                                                                                           'whether '
                                                                                                           'a '
                                                                                                           'problem '
                                                                                                           'related '
                                                                                                           'to '
                                                                                                           'an '
                                                                                                           'operative '
                                                                                                           'or '
                                                                                                           'invasive '
                                                                                                           'procedure '
                                                                                                           'was '
                                                                                                           'identified '
                                                                                                           'during '
                                                                                                           'the '
                                                                                                           'mortality '
                                                                                                           'review.',
                                                        'problem_related_to_treatment_and_management_plan': 'Indicator '
                                                                                                            'of '
                                                                                                            'whether '
                                                                                                            'a '
                                                                                                            'problem '
                                                                                                            'with '
                                                                                                            'the '
                                                                                                            'treatment '
                                                                                                            'and '
                                                                                                            'management '
                                                                                                            'plan '
                                                                                                            'was '
                                                                                                            'identified '
                                                                                                            'during '
                                                                                                            'the '
                                                                                                            'mortality '
                                                                                                            'review.',
                                                        'problem_with_infection_management': 'Indicator of '
                                                                                             'whether a '
                                                                                             'problem with '
                                                                                             'infection '
                                                                                             'management was '
                                                                                             'identified '
                                                                                             'during the '
                                                                                             'mortality '
                                                                                             'review.',
                                                        'problem_with_medication': 'Indicator of whether a '
                                                                                   'medication-related '
                                                                                   'problem was identified '
                                                                                   'during the mortality '
                                                                                   'review.',
                                                        'row_hash': "Hash of the source record's field "
                                                                    'values used to detect changes between '
                                                                    'pipeline loads.',
                                                        'serious_incident_or_complaint_raised_regarding_any_aspect': 'Indicator '
                                                                                                                     'of '
                                                                                                                     'whether '
                                                                                                                     'a '
                                                                                                                     'serious '
                                                                                                                     'incident '
                                                                                                                     'or '
                                                                                                                     'complaint '
                                                                                                                     'was '
                                                                                                                     'raised '
                                                                                                                     'regarding '
                                                                                                                     'any '
                                                                                                                     'aspect '
                                                                                                                     'of '
                                                                                                                     'the '
                                                                                                                     "patient's "
                                                                                                                     'care.',
                                                        'significant_co_existing_factorstxt': 'Text field '
                                                                                              'listing '
                                                                                              'significant '
                                                                                              'co-existing '
                                                                                              'factors or '
                                                                                              'comorbidities '
                                                                                              'recorded for '
                                                                                              'the death '
                                                                                              'review.',
                                                        'source_absent_detected_ts': 'Timestamp recording '
                                                                                     'when the pipeline '
                                                                                     'first detected that '
                                                                                     'the record was no '
                                                                                     'longer present in the '
                                                                                     'source system.',
                                                        'source_present_ind': 'Canonical boolean mirror of '
                                                                              'IS_CURRENT_IN_SOURCE; false '
                                                                              'rows are retained source '
                                                                              'tombstones.',
                                                        'source_record_key': 'Primary key value of the '
                                                                             'originating source record, '
                                                                             'held as text for provenance '
                                                                             'and deduplication.',
                                                        'source_table': 'Name of the upstream source table '
                                                                        'from which the record was ingested.',
                                                        'specialitytxt': 'Specialty or specialties '
                                                                         'associated with the case, recorded '
                                                                         'as coded label text from the '
                                                                         'source picklist.',
                                                        'surgical_attendeestxt': 'Surgical staff recorded as '
                                                                                 'attending the mortality or '
                                                                                 'M&M meeting, held as name '
                                                                                 'text.',
                                                        'surname': 'Family name of the deceased patient as '
                                                                   'recorded in the source mortality review '
                                                                   'registry.',
                                                        'survey_off': 'Source-system flag indicating whether '
                                                                      'the review survey or questionnaire '
                                                                      'section is switched off for this '
                                                                      'record.',
                                                        'timeto1st_review_hrs': 'Time from admission to the '
                                                                                'first medical review, '
                                                                                'recorded in whole hours.',
                                                        'ward_where_patient_died': 'Ward or clinical area '
                                                                                   'where the patient died, '
                                                                                   'recorded as a local '
                                                                                   'location label.',
                                                        'was_the_patient_covered_by_dols_or_mc_a_order': 'Indicator '
                                                                                                         'of '
                                                                                                         'whether '
                                                                                                         'the '
                                                                                                         'patient '
                                                                                                         'was '
                                                                                                         'subject '
                                                                                                         'to '
                                                                                                         'a '
                                                                                                         'Deprivation '
                                                                                                         'of '
                                                                                                         'Liberty '
                                                                                                         'Safeguards '
                                                                                                         'or '
                                                                                                         'Mental '
                                                                                                         'Capacity '
                                                                                                         'Act '
                                                                                                         'order.',
                                                        'was_the_pt_cared_for_using_the_compassionate_care_plan': 'Reviewer-recorded '
                                                                                                                  'categorical '
                                                                                                                  'response '
                                                                                                                  'indicating '
                                                                                                                  'whether '
                                                                                                                  'the '
                                                                                                                  'patient '
                                                                                                                  'was '
                                                                                                                  'cared '
                                                                                                                  'for '
                                                                                                                  'under '
                                                                                                                  'the '
                                                                                                                  'compassionate '
                                                                                                                  'care '
                                                                                                                  'plan.',
                                                        'was_there_a_clear_treatment_escalation_plan_documented': 'Reviewer-recorded '
                                                                                                                  'categorical '
                                                                                                                  'response '
                                                                                                                  'indicating '
                                                                                                                  'whether '
                                                                                                                  'a '
                                                                                                                  'clear '
                                                                                                                  'treatment '
                                                                                                                  'escalation '
                                                                                                                  'plan '
                                                                                                                  'was '
                                                                                                                  'documented.',
                                                        'was_there_evidence_of_a_resuscitation_decision_documented': 'Reviewer-recorded '
                                                                                                                     'categorical '
                                                                                                                     'response '
                                                                                                                     'indicating '
                                                                                                                     'whether '
                                                                                                                     'a '
                                                                                                                     'documented '
                                                                                                                     'resuscitation '
                                                                                                                     'decision '
                                                                                                                     'was '
                                                                                                                     'evident '
                                                                                                                     'in '
                                                                                                                     'the '
                                                                                                                     'record.',
                                                        'was_this_an_elective_death': 'Indicator of whether '
                                                                                      'the death followed an '
                                                                                      'elective admission or '
                                                                                      'procedure.',
                                                        'were_there_any_problems_with_the_care': 'Structured '
                                                                                                 'reviewer '
                                                                                                 'response '
                                                                                                 'stating '
                                                                                                 'whether '
                                                                                                 'any '
                                                                                                 'problems '
                                                                                                 'with the '
                                                                                                 "patient's "
                                                                                                 'care were '
                                                                                                 'identified, '
                                                                                                 'including '
                                                                                                 'branching '
                                                                                                 'guidance '
                                                                                                 'options.'},
                                            'quantity_override': True,
                                            'table': 'Internal QC of clinical_registry_mortality_review; '
                                                     'counts rows whose Gold parent is absent.'},
 'clinical_registry_mortality_review': {'columns': {},
                                        'quantity_override': False,
                                        'table': 'Typed child of clinical_registry_entry for the '
                                                 'mortality_review family. Cardiac mortality/M&M review '
                                                 'registry: source-text cause-of-death chain (not asserted '
                                                 'to be ICD-coded), NCEPOD classification, '
                                                 'coronial/medical-examiner fields and structured '
                                                 'care-problem flags. Patient names, contact details and '
                                                 'next-of-kin content are retained and governed with IG '
                                                 'tags. Every governed bronze column is retained under its '
                                                 'lower-case name. Coverage: iWeb registries start at '
                                                 'different years: coronary 2013, non-coronary 2016, surgery '
                                                 '2017, cardiac MDT 2018 (CT aortic 2021), mortality review '
                                                 '2018, ERACS 2025, ACS transfer 2000; complete-through '
                                                 'month per release: 6_mgmt.data_quality.release_finding '
                                                 '(finding_code F2_complete_through).'}}
