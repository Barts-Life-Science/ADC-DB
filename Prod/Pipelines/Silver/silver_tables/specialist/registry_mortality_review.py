"""Silver specialist.registry_mortality_review: 1 dataset(s).

- datasets: clinical_registry_mortality_review
- moved verbatim from: 3_silver_specialist_care:1574, 3_silver_specialist_care:1948

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id
from silver_tables._common.journey import IWEB_REGISTRY_SLOTS


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical / registry mortality review

# ==== journey_clinical.registry_mortality_review ====
REGISTRY_MORTALITY_REVIEW_EXPECTED_SCHEMA = [
    ("ENTRY_ID", "int"),
    ("MRN", "string"),
    ("GENDER", "string"),
    ("DATE_OF_DEATH_DEMOG", "date"),
    ("ETHNIC_ORIGIN", "string"),
    ("DATEOF_DEATH", "timestamp"),
    ("DATE_AND_TIME_OF_DEATH", "timestamp"),
    ("CONSULTANT_IN_CHARGE_AT_TIME_OF_DEATHTXT", "string"),
    ("DATE_AND_TIME_OF_ADMISSION", "timestamp"),
    ("DATE_AND_TIME_OF_LAST_MEDICAL_REVIEW", "timestamp"),
    ("CRITICAL_CARE_UNIT_EPISODE_DURING_ADMISSION", "string"),
    ("CAUSEOFDEATH_1A", "string"),
    ("CAUSEOFDEATH_1B", "string"),
    ("CAUSEOFDEATH_1C", "string"),
    ("CAUSEOFDEATH_2", "string"),
    ("SIGNIFICANT_CO_EXISTING_FACTORSTXT", "string"),
    ("WAS_THE_PATIENT_COVERED_BY_DOLS_OR_MC_A_ORDER", "string"),
    ("CORONIAL_CASE", "string"),
    ("OUTCOME_OF_CORONIAL_REFERRAL", "string"),
    ("SERIOUS_INCIDENT_OR_COMPLAINT_RAISED_REGARDING_ANY_ASPECT", "string"),
    ("DISCHARGE_SUMMARY_COMPLETED", "string"),
    ("TIMETO1ST_REVIEW_HRS", "int"),
    ("PATIENT_ADMITTED_TO_APPROPRIATE_WARD", "string"),
    ("ANY_PERIOD_WHEN_PT_NOT_REVIEWED_BY_A_CONSULTANT_FOR72_HOURS", "string"),
    ("DID_THE_PT_FALL_WHILST_ON_THE_WARDS", "string"),
    ("DID_THE_PT_DEVELOP_A_PRESSURE_ULCER_SINCE_ADMISSION", "string"),
    ("WAS_THERE_A_CLEAR_TREATMENT_ESCALATION_PLAN_DOCUMENTED", "string"),
    ("WAS_THERE_EVIDENCE_OF_A_RESUSCITATION_DECISION_DOCUMENTED", "string"),
    ("WAS_THE_PT_CARED_FOR_USING_THE_COMPASSIONATE_CARE_PLAN", "string"),
    ("NCEPOD_CLASSIFICATION", "string"),
    ("M_WHY_IS_NCEPOD_BCDE", "string"),
    ("M_ANYTHING_ELSE_OF_NOTE", "string"),
    ("DATE_OF_COMPLETION_OF_FORM", "timestamp"),
    ("DATE_OF_MORTALITY_REVIEW", "timestamp"),
    ("WARD_WHERE_PATIENT_DIED", "string"),
    ("CONSULTANT_RESPONSIBLE_FOR_PROCEDURE", "string"),
    ("PRIMARY_OPERATOR", "string"),
    ("SPECIALITYTXT", "string"),
    ("M_NOTE", "string"),
    ("M_CLINICAL_HISTORYEVENTS", "string"),
    ("ENTERED_INTO_DATABASE", "string"),
    ("PRESENTED_BYTXT", "string"),
    ("PEEREVIEW", "string"),
    ("CARDIOLOGY_ATTENDEESTXT", "string"),
    ("SURGICAL_ATTENDEESTXT", "string"),
    ("CHAIRMAN", "string"),
    ("MEDICAL_EXAMINER", "string"),
    ("CREMATED", "string"),
    ("ME_DISCUSSED", "string"),
    ("SURVEY_OFF", "string"),
    ("ME_CLASSIFICATION", "string"),
    ("ME_REVIEW_DT", "timestamp"),
    ("DEATH_CCP", "string"),
    ("CCP_INITIATED", "string"),
    ("M_ME_NOTES", "string"),
    ("DSUMMARYCOMP", "timestamp"),
    ("FURTHER_ACTION_TO_BE_TAKEN", "string"),
    ("M_ACTION_REQUIRED", "string"),
    ("LFD_CONCERN", "string"),
    ("LFD_RESUS", "string"),
    ("LFD_RECOG", "string"),
    ("LFD_COMMUN", "string"),
    ("LF_DOCUMENTATION", "string"),
    ("LFD_PROCEDURAL", "string"),
    ("LFD_OTHER", "string"),
    ("MLFD_DETAILS", "string"),
    ("MLFD_ACTION", "string"),
    ("CR_SINSTRUCTION", "string"),
    ("AT_THE_END_OF_THE_ADMISSION", "string"),
    ("DURING_ADMISSION", "string"),
    ("WAS_THIS_AN_ELECTIVE_DEATH", "string"),
    ("PROBLEM_IN_ASSESSMENT", "string"),
    ("PROBLEM_WITH_MEDICATION", "string"),
    ("PROBLEM_RELATED_TO_TREATMENT_AND_MANAGEMENT_PLAN", "string"),
    ("PROBLEM_WITH_INFECTION_MANAGEMENT", "string"),
    ("PROBLEM_RELATED_TO_OPERATIVE_INVASIVE_PROCEDURE", "string"),
    ("PROBLEM_IN_CLINICAL_MONITORING", "string"),
    ("PROBLEM_IN_RESUSCITATION", "string"),
    ("PROBLEM_OF_ANY_OTHER_TYPE", "string"),
    ("M_SHARING_EDUCATION", "string"),
    ("M_QUALITY_IMPROVEMENT_PROJECT", "string"),
    ("M_OTHER_SPECIFIC_ACTION", "string"),
    ("WERE_THERE_ANY_PROBLEMS_WITH_THE_CARE", "string"),
    ("DID_THE_PROBLEM_LEAD_TO_HARM1", "string"),
    ("DID_THE_PROBLEM_LEAD_TO_HARM2", "string"),
    ("DID_THE_PROBLEM_LEAD_TO_HARM3", "string"),
    ("DID_THE_PROBLEM_LEAD_TO_HARM4", "string"),
    ("DID_THE_PROBLEM_LEAD_TO_HARM5", "string"),
    ("DID_THE_PROBLEM_LEAD_TO_HARM6", "string"),
    ("DID_THE_PROBLEM_LEAD_TO_HARM7", "string"),
    ("DID_THE_PROBLEM_LEAD_TO_HARM8", "string"),
    ("M_OTHER_PROBLEM_DETAILS", "string"),
    ("PRESENTED_BY_OTHER", "string"),
    ("FORM_COMPLETED_BYTXT", "string"),
    ("FORM_COMPLETED_BY_OTHER", "string"),
    ("DATE_LAST_CHANGED", "timestamp"),
    ("ADC_UPDT", "timestamp"),
    ("NHS_NUMBER_VALID_IND", "boolean"),
    ("LINKAGE_STATUS", "string"),
    ("PERSON_ID", "bigint"),
    ("LINKAGE_METHOD", "string"),
    ("LINKAGE_HISTORICAL_FALLBACK_IND", "boolean"),
    ("LINKAGE_FALLBACK_CONFLICT_IND", "boolean"),
    ("DATE_OF_DEATH_QUALITY", "string"),
    ("DATE_OF_DEATH_CLEAN", "date"),
    ("SOURCE_TABLE", "string"),
    ("SOURCE_RECORD_KEY", "string"),
    ("ROW_HASH", "string"),
    ("PIPELINE_LOADED_AT", "timestamp"),
    ("IS_CURRENT_IN_SOURCE", "boolean"),
    ("SOURCE_PRESENT_IND", "boolean"),
    ("SOURCE_ABSENT_DETECTED_TS", "timestamp"),
    ("SURNAME", "string"),
    ("FORENAME", "string"),
    ("NHS_NUMBER", "string"),
    ("DATE_OF_BIRTH", "date"),
    ("NAMEOFKIN", "string"),
    ("PHONE_OF_KIN", "string"),
    ("M_COMMENTSKIN", "string"),
]


@materialized_view(
    name=_n("journey_clinical.registry_mortality_review"),
    refresh_policy="auto",
)
def registry_mortality_review():
    # Build the declared dataset: Typed child of clinical_registry_entry for the
    # mortality_review family. Cardiac mortality/M&M review registry: source-text cause-of-death
    # chain (not asserted to be ICD-coded), NCEPOD classification, coronial/medical-examiner
    # fields and structured care-problem flags. Patient names, contact details and next-of-kin
    # content are retained and governed with IG tags. Every governed bronze column is retained
    # under its lower-case name.
    s = read_source(IWEB_REGISTRY_SLOTS["mortality_review"])
    actual_schema = [(field.name, field.dataType.simpleString()) for field in s.schema.fields]
    if actual_schema != REGISTRY_MORTALITY_REVIEW_EXPECTED_SCHEMA:
        raise RuntimeError("mortality_review: bronze schema drift" + f"; expected={REGISTRY_MORTALITY_REVIEW_EXPECTED_SCHEMA!r} actual={actual_schema!r}")
    return s.select(
        stable_id("registry_entry:iweb", F.lit("mortality_review"), F.lit("~"), s["ENTRY_ID"]).alias("patient_event_key"),
        s["ENTRY_ID"].alias("entry_id"),
        s["MRN"].alias("mrn"),
        s["GENDER"].alias("gender"),
        s["DATE_OF_DEATH_DEMOG"].alias("date_of_death_demog"),
        s["ETHNIC_ORIGIN"].alias("ethnic_origin"),
        s["DATEOF_DEATH"].alias("dateof_death"),
        s["DATE_AND_TIME_OF_DEATH"].alias("date_and_time_of_death"),
        s["CONSULTANT_IN_CHARGE_AT_TIME_OF_DEATHTXT"].alias("consultant_in_charge_at_time_of_deathtxt"),
        s["DATE_AND_TIME_OF_ADMISSION"].alias("date_and_time_of_admission"),
        s["DATE_AND_TIME_OF_LAST_MEDICAL_REVIEW"].alias("date_and_time_of_last_medical_review"),
        s["CRITICAL_CARE_UNIT_EPISODE_DURING_ADMISSION"].alias("critical_care_unit_episode_during_admission"),
        s["CAUSEOFDEATH_1A"].alias("causeofdeath_1a"),
        s["CAUSEOFDEATH_1B"].alias("causeofdeath_1b"),
        s["CAUSEOFDEATH_1C"].alias("causeofdeath_1c"),
        s["CAUSEOFDEATH_2"].alias("causeofdeath_2"),
        s["SIGNIFICANT_CO_EXISTING_FACTORSTXT"].alias("significant_co_existing_factorstxt"),
        s["WAS_THE_PATIENT_COVERED_BY_DOLS_OR_MC_A_ORDER"].alias("was_the_patient_covered_by_dols_or_mc_a_order"),
        s["CORONIAL_CASE"].alias("coronial_case"),
        s["OUTCOME_OF_CORONIAL_REFERRAL"].alias("outcome_of_coronial_referral"),
        s["SERIOUS_INCIDENT_OR_COMPLAINT_RAISED_REGARDING_ANY_ASPECT"].alias("serious_incident_or_complaint_raised_regarding_any_aspect"),
        s["DISCHARGE_SUMMARY_COMPLETED"].alias("discharge_summary_completed"),
        s["TIMETO1ST_REVIEW_HRS"].alias("timeto1st_review_hrs"),
        s["PATIENT_ADMITTED_TO_APPROPRIATE_WARD"].alias("patient_admitted_to_appropriate_ward"),
        s["ANY_PERIOD_WHEN_PT_NOT_REVIEWED_BY_A_CONSULTANT_FOR72_HOURS"].alias("any_period_when_pt_not_reviewed_by_a_consultant_for72_hours"),
        s["DID_THE_PT_FALL_WHILST_ON_THE_WARDS"].alias("did_the_pt_fall_whilst_on_the_wards"),
        s["DID_THE_PT_DEVELOP_A_PRESSURE_ULCER_SINCE_ADMISSION"].alias("did_the_pt_develop_a_pressure_ulcer_since_admission"),
        s["WAS_THERE_A_CLEAR_TREATMENT_ESCALATION_PLAN_DOCUMENTED"].alias("was_there_a_clear_treatment_escalation_plan_documented"),
        s["WAS_THERE_EVIDENCE_OF_A_RESUSCITATION_DECISION_DOCUMENTED"].alias("was_there_evidence_of_a_resuscitation_decision_documented"),
        s["WAS_THE_PT_CARED_FOR_USING_THE_COMPASSIONATE_CARE_PLAN"].alias("was_the_pt_cared_for_using_the_compassionate_care_plan"),
        s["NCEPOD_CLASSIFICATION"].alias("ncepod_classification"),
        s["M_WHY_IS_NCEPOD_BCDE"].alias("m_why_is_ncepod_bcde"),
        s["M_ANYTHING_ELSE_OF_NOTE"].alias("m_anything_else_of_note"),
        s["DATE_OF_COMPLETION_OF_FORM"].alias("date_of_completion_of_form"),
        s["DATE_OF_MORTALITY_REVIEW"].alias("date_of_mortality_review"),
        s["WARD_WHERE_PATIENT_DIED"].alias("ward_where_patient_died"),
        s["CONSULTANT_RESPONSIBLE_FOR_PROCEDURE"].alias("consultant_responsible_for_procedure"),
        s["PRIMARY_OPERATOR"].alias("primary_operator"),
        s["SPECIALITYTXT"].alias("specialitytxt"),
        s["M_NOTE"].alias("m_note"),
        s["M_CLINICAL_HISTORYEVENTS"].alias("m_clinical_historyevents"),
        s["ENTERED_INTO_DATABASE"].alias("entered_into_database"),
        s["PRESENTED_BYTXT"].alias("presented_bytxt"),
        s["PEEREVIEW"].alias("peereview"),
        s["CARDIOLOGY_ATTENDEESTXT"].alias("cardiology_attendeestxt"),
        s["SURGICAL_ATTENDEESTXT"].alias("surgical_attendeestxt"),
        s["CHAIRMAN"].alias("chairman"),
        s["MEDICAL_EXAMINER"].alias("medical_examiner"),
        s["CREMATED"].alias("cremated"),
        s["ME_DISCUSSED"].alias("me_discussed"),
        s["SURVEY_OFF"].alias("survey_off"),
        s["ME_CLASSIFICATION"].alias("me_classification"),
        s["ME_REVIEW_DT"].alias("me_review_dt"),
        s["DEATH_CCP"].alias("death_ccp"),
        s["CCP_INITIATED"].alias("ccp_initiated"),
        s["M_ME_NOTES"].alias("m_me_notes"),
        s["DSUMMARYCOMP"].alias("dsummarycomp"),
        s["FURTHER_ACTION_TO_BE_TAKEN"].alias("further_action_to_be_taken"),
        s["M_ACTION_REQUIRED"].alias("m_action_required"),
        s["LFD_CONCERN"].alias("lfd_concern"),
        s["LFD_RESUS"].alias("lfd_resus"),
        s["LFD_RECOG"].alias("lfd_recog"),
        s["LFD_COMMUN"].alias("lfd_commun"),
        s["LF_DOCUMENTATION"].alias("lf_documentation"),
        s["LFD_PROCEDURAL"].alias("lfd_procedural"),
        s["LFD_OTHER"].alias("lfd_other"),
        s["MLFD_DETAILS"].alias("mlfd_details"),
        s["MLFD_ACTION"].alias("mlfd_action"),
        s["CR_SINSTRUCTION"].alias("cr_sinstruction"),
        s["AT_THE_END_OF_THE_ADMISSION"].alias("at_the_end_of_the_admission"),
        s["DURING_ADMISSION"].alias("during_admission"),
        s["WAS_THIS_AN_ELECTIVE_DEATH"].alias("was_this_an_elective_death"),
        s["PROBLEM_IN_ASSESSMENT"].alias("problem_in_assessment"),
        s["PROBLEM_WITH_MEDICATION"].alias("problem_with_medication"),
        s["PROBLEM_RELATED_TO_TREATMENT_AND_MANAGEMENT_PLAN"].alias("problem_related_to_treatment_and_management_plan"),
        s["PROBLEM_WITH_INFECTION_MANAGEMENT"].alias("problem_with_infection_management"),
        s["PROBLEM_RELATED_TO_OPERATIVE_INVASIVE_PROCEDURE"].alias("problem_related_to_operative_invasive_procedure"),
        s["PROBLEM_IN_CLINICAL_MONITORING"].alias("problem_in_clinical_monitoring"),
        s["PROBLEM_IN_RESUSCITATION"].alias("problem_in_resuscitation"),
        s["PROBLEM_OF_ANY_OTHER_TYPE"].alias("problem_of_any_other_type"),
        s["M_SHARING_EDUCATION"].alias("m_sharing_education"),
        s["M_QUALITY_IMPROVEMENT_PROJECT"].alias("m_quality_improvement_project"),
        s["M_OTHER_SPECIFIC_ACTION"].alias("m_other_specific_action"),
        s["WERE_THERE_ANY_PROBLEMS_WITH_THE_CARE"].alias("were_there_any_problems_with_the_care"),
        s["DID_THE_PROBLEM_LEAD_TO_HARM1"].alias("did_the_problem_lead_to_harm1"),
        s["DID_THE_PROBLEM_LEAD_TO_HARM2"].alias("did_the_problem_lead_to_harm2"),
        s["DID_THE_PROBLEM_LEAD_TO_HARM3"].alias("did_the_problem_lead_to_harm3"),
        s["DID_THE_PROBLEM_LEAD_TO_HARM4"].alias("did_the_problem_lead_to_harm4"),
        s["DID_THE_PROBLEM_LEAD_TO_HARM5"].alias("did_the_problem_lead_to_harm5"),
        s["DID_THE_PROBLEM_LEAD_TO_HARM6"].alias("did_the_problem_lead_to_harm6"),
        s["DID_THE_PROBLEM_LEAD_TO_HARM7"].alias("did_the_problem_lead_to_harm7"),
        s["DID_THE_PROBLEM_LEAD_TO_HARM8"].alias("did_the_problem_lead_to_harm8"),
        s["M_OTHER_PROBLEM_DETAILS"].alias("m_other_problem_details"),
        s["PRESENTED_BY_OTHER"].alias("presented_by_other"),
        s["FORM_COMPLETED_BYTXT"].alias("form_completed_bytxt"),
        s["FORM_COMPLETED_BY_OTHER"].alias("form_completed_by_other"),
        s["DATE_LAST_CHANGED"].alias("date_last_changed"),
        s["ADC_UPDT"].alias("adc_updt"),
        s["NHS_NUMBER_VALID_IND"].alias("nhs_number_valid_ind"),
        s["LINKAGE_STATUS"].alias("linkage_status"),
        s["PERSON_ID"].alias("person_id"),
        s["LINKAGE_METHOD"].alias("linkage_method"),
        s["LINKAGE_HISTORICAL_FALLBACK_IND"].alias("linkage_historical_fallback_ind"),
        s["LINKAGE_FALLBACK_CONFLICT_IND"].alias("linkage_fallback_conflict_ind"),
        s["DATE_OF_DEATH_QUALITY"].alias("date_of_death_quality"),
        s["DATE_OF_DEATH_CLEAN"].alias("date_of_death_clean"),
        s["SOURCE_TABLE"].alias("source_table"),
        s["SOURCE_RECORD_KEY"].alias("source_record_key"),
        s["ROW_HASH"].alias("row_hash"),
        s["PIPELINE_LOADED_AT"].alias("pipeline_loaded_at"),
        s["IS_CURRENT_IN_SOURCE"].alias("is_current_in_source"),
        s["SOURCE_PRESENT_IND"].alias("source_present_ind"),
        s["SOURCE_ABSENT_DETECTED_TS"].alias("source_absent_detected_ts"),
        s["SURNAME"].alias("surname"),
        s["FORENAME"].alias("forename"),
        s["NHS_NUMBER"].alias("nhs_number"),
        s["DATE_OF_BIRTH"].alias("date_of_birth"),
        s["NAMEOFKIN"].alias("nameofkin"),
        s["PHONE_OF_KIN"].alias("phone_of_kin"),
        s["M_COMMENTSKIN"].alias("m_commentskin"),
    )


VALUE_POLICIES = {
    'clinical_registry_mortality_review': ValuePolicy(table='clinical_registry_mortality_review', enabled=True),
}


# W2.06 (L3185): mortality review completion, from the dates the form already publishes.
VALUE_POLICIES["clinical_registry_mortality_review"].register("review_status",
         ["date_of_completion_of_form", "date_of_mortality_review"],
         lambda F: F.when(F.col("date_of_completion_of_form").isNotNull(), F.lit("completed"))
                    .when(F.col("date_of_mortality_review").isNotNull(), F.lit("review_dated_not_completed"))
                    .otherwise(F.lit("not_completed")),
         "Mortality review state: completed (form completion date set), review_dated_not_completed (review date "
         "only) or not_completed. INSR_W2 (L3185).")


WRAP_POLICY = {'clinical_registry_mortality_review'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_registry_mortality_review': {'columns': {'adc_updt': 'Timestamp of the last extract or update of '
                                                                'the record by the ingestion process.',
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
                                                                                   'position at the end of '
                                                                                   'the admission.',
                                                    'cardiology_attendeestxt': 'Cardiology staff recorded as '
                                                                               'attending the mortality or '
                                                                               'M&M meeting, held as name '
                                                                               'text.',
                                                    'causeofdeath_1a': 'Source-text cause of death recorded '
                                                                       'in part 1a of the death certificate '
                                                                       'chain, not asserted to be coded.',
                                                    'causeofdeath_1b': 'Source-text cause of death recorded '
                                                                       'in part 1b of the death certificate '
                                                                       'chain, not asserted to be coded.',
                                                    'causeofdeath_1c': 'Source-text cause of death recorded '
                                                                       'in part 1c of the death certificate '
                                                                       'chain, not asserted to be coded.',
                                                    'causeofdeath_2': 'Source-text contributory conditions '
                                                                      'recorded in part 2 of the death '
                                                                      'certificate, not asserted to be '
                                                                      'coded.',
                                                    'ccp_initiated': 'Indicator of whether a compassionate '
                                                                     'care plan was initiated for the '
                                                                     'patient.',
                                                    'chairman': 'Chair of the mortality or M&M review '
                                                                'meeting, recorded as a short staff '
                                                                'reference or initials.',
                                                    'consultant_in_charge_at_time_of_deathtxt': 'Free-text '
                                                                                                'name of the '
                                                                                                'consultant '
                                                                                                'responsible '
                                                                                                'for the '
                                                                                                'patient at '
                                                                                                'the time of '
                                                                                                'death.',
                                                    'consultant_responsible_for_procedure': 'Consultant with '
                                                                                            'overall '
                                                                                            'responsibility '
                                                                                            'for the '
                                                                                            'procedure, held '
                                                                                            'as a '
                                                                                            'source-system '
                                                                                            'staff '
                                                                                            'reference.',
                                                    'coronial_case': 'Indicator of whether the death was '
                                                                     'referred to or treated as a coronial '
                                                                     'case.',
                                                    'cr_sinstruction': 'Field recording the coroner-related '
                                                                       'instruction captured during the '
                                                                       'mortality review.',
                                                    'cremated': 'Indicator of whether the death proceeded to '
                                                                'cremation, relevant to certification and '
                                                                'medical examiner processes.',
                                                    'critical_care_unit_episode_during_admission': 'Indicator '
                                                                                                   'of '
                                                                                                   'whether '
                                                                                                   'the '
                                                                                                   'patient '
                                                                                                   'had a '
                                                                                                   'critical '
                                                                                                   'care '
                                                                                                   'unit '
                                                                                                   'episode '
                                                                                                   'during '
                                                                                                   'the '
                                                                                                   'admission.',
                                                    'date_and_time_of_admission': 'Date and time the patient '
                                                                                  'was admitted for the '
                                                                                  'admission during which '
                                                                                  'the death occurred.',
                                                    'date_and_time_of_death': 'Date and time of death '
                                                                              'recorded on the mortality '
                                                                              'review form.',
                                                    'date_and_time_of_last_medical_review': 'Date and time '
                                                                                            'of the last '
                                                                                            'documented '
                                                                                            'medical review '
                                                                                            'of the patient '
                                                                                            'before death.',
                                                    'date_last_changed': 'Timestamp recording when the '
                                                                         'mortality review record was last '
                                                                         'modified in the source registry.',
                                                    'date_of_birth': 'Date of birth of the patient as '
                                                                     'recorded in the source mortality '
                                                                     'review registry.',
                                                    'date_of_completion_of_form': 'Date on which the '
                                                                                  'mortality review form was '
                                                                                  'completed.',
                                                    'date_of_death_clean': 'Date of death with future values '
                                                                           'nulled.',
                                                    'date_of_death_demog': 'Date of death taken from the '
                                                                           'patient demographic record, as '
                                                                           'distinct from the review form '
                                                                           'death date.',
                                                    'date_of_death_quality': 'VALID | FUTURE | MISSING; raw '
                                                                             'DATEOF_DEATH is retained.',
                                                    'date_of_mortality_review': 'Date on which the mortality '
                                                                                'or M&M review of the case '
                                                                                'took place.',
                                                    'dateof_death': 'Date of death as recorded on the '
                                                                    'mortality review form, retained in raw '
                                                                    'form alongside the cleaned derivation.',
                                                    'death_ccp': 'Indicator of whether the patient died '
                                                                 'while on the compassionate care plan '
                                                                 'pathway.',
                                                    'did_the_problem_lead_to_harm1': 'Structured assessment '
                                                                                     'of whether the first '
                                                                                     'identified care '
                                                                                     'problem led to patient '
                                                                                     'harm.',
                                                    'did_the_problem_lead_to_harm2': 'Structured assessment '
                                                                                     'of whether the second '
                                                                                     'identified care '
                                                                                     'problem led to patient '
                                                                                     'harm.',
                                                    'did_the_problem_lead_to_harm3': 'Structured assessment '
                                                                                     'of whether the third '
                                                                                     'identified care '
                                                                                     'problem led to patient '
                                                                                     'harm.',
                                                    'did_the_problem_lead_to_harm4': 'Structured assessment '
                                                                                     'of whether the fourth '
                                                                                     'identified care '
                                                                                     'problem led to patient '
                                                                                     'harm.',
                                                    'did_the_problem_lead_to_harm5': 'Structured assessment '
                                                                                     'of whether the fifth '
                                                                                     'identified care '
                                                                                     'problem led to patient '
                                                                                     'harm.',
                                                    'did_the_problem_lead_to_harm6': 'Structured assessment '
                                                                                     'of whether the sixth '
                                                                                     'identified care '
                                                                                     'problem led to patient '
                                                                                     'harm.',
                                                    'did_the_problem_lead_to_harm7': 'Structured assessment '
                                                                                     'of whether the seventh '
                                                                                     'identified care '
                                                                                     'problem led to patient '
                                                                                     'harm.',
                                                    'did_the_problem_lead_to_harm8': 'Structured assessment '
                                                                                     'of whether the eighth '
                                                                                     'identified care '
                                                                                     'problem led to patient '
                                                                                     'harm.',
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
                                                                                           'sustained a fall '
                                                                                           'while on the '
                                                                                           'wards during the '
                                                                                           'admission.',
                                                    'discharge_summary_completed': 'Indicator of whether a '
                                                                                   'discharge summary was '
                                                                                   'completed for the '
                                                                                   'admission.',
                                                    'dsummarycomp': 'Date and time the discharge summary was '
                                                                    'completed.',
                                                    'during_admission': 'Review response describing the care '
                                                                        'position during the admission.',
                                                    'entered_into_database': 'Source-system indicator '
                                                                             'recording whether the case has '
                                                                             'been entered into the review '
                                                                             'database.',
                                                    'entry_id': 'Internal surrogate identifier for the '
                                                                'mortality review record, carried from the '
                                                                'source registry entry key.',
                                                    'ethnic_origin': 'Coded ethnic origin of the patient as '
                                                                     'recorded in the source registry '
                                                                     'demographics.',
                                                    'forename': 'Given name of the deceased patient as '
                                                                'recorded in the source mortality review '
                                                                'registry.',
                                                    'form_completed_by_other': 'Free-text entry naming the '
                                                                               'person who completed the '
                                                                               'form when not available in '
                                                                               'the standard selection list.',
                                                    'form_completed_bytxt': 'Text value of the staff member '
                                                                            'recorded as having completed '
                                                                            'the mortality review form.',
                                                    'further_action_to_be_taken': 'Indicator of whether '
                                                                                  'further action is '
                                                                                  'required following the '
                                                                                  'mortality review.',
                                                    'gender': 'Recorded gender of the patient as held in the '
                                                              'source registry demographics.',
                                                    'is_current_in_source': 'Boolean flag indicating whether '
                                                                            'the record was still present in '
                                                                            'the source system at the latest '
                                                                            'pipeline load.',
                                                    'lf_documentation': 'Learning-from-deaths flag '
                                                                        'indicating whether a concern '
                                                                        'relating to documentation was '
                                                                        'identified.',
                                                    'lfd_commun': 'Learning-from-deaths flag indicating '
                                                                  'whether a concern relating to '
                                                                  'communication was identified.',
                                                    'lfd_concern': 'Learning-from-deaths flag indicating '
                                                                   'whether a general concern about care was '
                                                                   'identified.',
                                                    'lfd_other': 'Learning-from-deaths flag indicating '
                                                                 'whether a concern of any other type was '
                                                                 'identified.',
                                                    'lfd_procedural': 'Learning-from-deaths flag indicating '
                                                                      'whether a concern relating to a '
                                                                      'procedure was identified.',
                                                    'lfd_recog': 'Learning-from-deaths flag indicating '
                                                                 'whether a concern relating to recognition '
                                                                 'of deterioration or illness was '
                                                                 'identified.',
                                                    'lfd_resus': 'Learning-from-deaths flag indicating '
                                                                 'whether a concern relating to '
                                                                 'resuscitation was identified.',
                                                    'linkage_fallback_conflict_ind': 'Boolean flag '
                                                                                     'indicating that the '
                                                                                     'fallback linkage '
                                                                                     'attempt produced '
                                                                                     'conflicting person '
                                                                                     'matches.',
                                                    'linkage_historical_fallback_ind': 'Boolean flag '
                                                                                       'indicating that '
                                                                                       'patient linkage was '
                                                                                       'resolved using a '
                                                                                       'historical '
                                                                                       '(non-current) alias '
                                                                                       'fallback rather than '
                                                                                       'a current identifier '
                                                                                       'match.',
                                                    'linkage_method': 'Code describing the identifier '
                                                                      'combination used to link the '
                                                                      'mortality review record to the master '
                                                                      'person record.',
                                                    'linkage_status': 'Coded status describing the outcome '
                                                                      'of linking the review record to a '
                                                                      'master patient identity.',
                                                    'm_action_required': 'Free-text description of the '
                                                                         'actions required following the '
                                                                         'mortality review.',
                                                    'm_anything_else_of_note': 'Free-text narrative '
                                                                               'capturing any additional '
                                                                               'points of note from the '
                                                                               'mortality or M&M review.',
                                                    'm_clinical_historyevents': 'Free-text clinical history '
                                                                                'and sequence of events '
                                                                                'leading to the death, as '
                                                                                'summarised for review.',
                                                    'm_commentskin': 'Free-text comments relating to the '
                                                                     "patient's next of kin, such as contact "
                                                                     'or communication notes recorded at '
                                                                     'review.',
                                                    'm_me_notes': 'Free-text notes recorded by or about the '
                                                                  'medical examiner review.',
                                                    'm_note': 'Free-text note recorded against the mortality '
                                                              'review record.',
                                                    'm_other_problem_details': 'Free-text narrative '
                                                                               'describing the other care '
                                                                               'problem identified during '
                                                                               'the mortality review.',
                                                    'm_other_specific_action': 'Free-text note describing '
                                                                               'any other specific action '
                                                                               'agreed following the '
                                                                               'mortality review.',
                                                    'm_quality_improvement_project': 'Free-text note '
                                                                                     'describing any quality '
                                                                                     'improvement project '
                                                                                     'arising from the '
                                                                                     'mortality review.',
                                                    'm_sharing_education': 'Free-text note describing '
                                                                           'sharing or educational actions '
                                                                           'agreed as a result of the '
                                                                           'mortality review.',
                                                    'm_why_is_ncepod_bcde': 'Free-text reviewer explanation '
                                                                            'of why the case was graded in '
                                                                            'the lower NCEPOD categories.',
                                                    'me_classification': "Medical examiner's classification "
                                                                         'outcome recorded for the death at '
                                                                         'scrutiny.',
                                                    'me_discussed': 'Indicator of whether the death was '
                                                                    'discussed with the medical examiner.',
                                                    'me_review_dt': 'Date and time the medical examiner '
                                                                    'review of the death was carried out.',
                                                    'medical_examiner': 'Medical examiner associated with '
                                                                        'scrutiny of the death, recorded as '
                                                                        'a staff reference.',
                                                    'mlfd_action': 'Free-text description of actions arising '
                                                                   'from the learning-from-deaths concerns.',
                                                    'mlfd_details': 'Free-text details of the '
                                                                    'learning-from-deaths concerns '
                                                                    'identified at review.',
                                                    'mrn': 'Local hospital medical record number identifying '
                                                           'the deceased patient in the source registry.',
                                                    'nameofkin': "Free-text name of the patient's next of "
                                                                 'kin as recorded on the mortality review '
                                                                 'form.',
                                                    'ncepod_classification': 'NCEPOD care-quality grading '
                                                                             'assigned to the death at '
                                                                             'mortality review, held as a '
                                                                             'coded category label.',
                                                    'nhs_number': 'NHS number of the patient as held in the '
                                                                  'source mortality review registry, '
                                                                  'retained in its source text formatting.',
                                                    'nhs_number_valid_ind': 'Derived boolean indicator '
                                                                            'showing whether the source NHS '
                                                                            'number passed validation during '
                                                                            'processing.',
                                                    'outcome_of_coronial_referral': 'Recorded outcome of the '
                                                                                    'referral of the death '
                                                                                    'to the coroner.',
                                                    'patient_admitted_to_appropriate_ward': 'Reviewer '
                                                                                            'assessment of '
                                                                                            'whether the '
                                                                                            'patient was '
                                                                                            'admitted to an '
                                                                                            'appropriate '
                                                                                            'ward.',
                                                    'patient_event_key': 'Deterministic SHA-256 key matching '
                                                                         'clinical_registry_entry.patient_event_key '
                                                                         'for the mortality_review source '
                                                                         'family and native entry_id.',
                                                    'peereview': 'Indicator of whether the case underwent '
                                                                 'peer review as part of the mortality '
                                                                 'review process.',
                                                    'person_id': 'Internal system-assigned identifier of the '
                                                                 'linked patient record in the master person '
                                                                 'table.',
                                                    'phone_of_kin': 'Contact telephone number for the '
                                                                    "patient's next of kin recorded on the "
                                                                    'mortality review form.',
                                                    'pipeline_loaded_at': 'Timestamp recording when the '
                                                                          'record was loaded into this table '
                                                                          'by the ingestion pipeline.',
                                                    'presented_by_other': 'Free-text entry naming the '
                                                                          'presenter of the case at the '
                                                                          'review meeting when not available '
                                                                          'in the standard selection list.',
                                                    'presented_bytxt': 'Clinician who presented the case at '
                                                                       'the mortality or M&M meeting, '
                                                                       'recorded as name text.',
                                                    'primary_operator': 'Primary operator performing the '
                                                                        'procedure, held as a source-system '
                                                                        'staff reference.',
                                                    'problem_in_assessment': 'Indicator of whether a problem '
                                                                             'in patient assessment was '
                                                                             'identified during the '
                                                                             'mortality review.',
                                                    'problem_in_clinical_monitoring': 'Structured review '
                                                                                      'response indicating '
                                                                                      'whether a problem in '
                                                                                      'clinical monitoring '
                                                                                      'of the patient was '
                                                                                      'identified during the '
                                                                                      'mortality review.',
                                                    'problem_in_resuscitation': 'Structured review response '
                                                                                'indicating whether a '
                                                                                'problem in resuscitation '
                                                                                'was identified during the '
                                                                                'mortality review.',
                                                    'problem_of_any_other_type': 'Structured review response '
                                                                                 'indicating whether a care '
                                                                                 'problem of any other type '
                                                                                 'was identified during the '
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
                                                                                         'whether a problem '
                                                                                         'with infection '
                                                                                         'management was '
                                                                                         'identified during '
                                                                                         'the mortality '
                                                                                         'review.',
                                                    'problem_with_medication': 'Indicator of whether a '
                                                                               'medication-related problem '
                                                                               'was identified during the '
                                                                               'mortality review.',
                                                    'review_status': 'Mortality review state: completed '
                                                                     '(form completion date set), '
                                                                     'review_dated_not_completed (review '
                                                                     'date only) or not_completed. INSR_W2 '
                                                                     '(L3185).',
                                                    'row_hash': "Hash of the source record's field values "
                                                                'used to detect changes between pipeline '
                                                                'loads.',
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
                                                                                          'recorded for the '
                                                                                          'death review.',
                                                    'source_absent_detected_ts': 'Timestamp recording when '
                                                                                 'the pipeline first '
                                                                                 'detected that the record '
                                                                                 'was no longer present in '
                                                                                 'the source system.',
                                                    'source_present_ind': 'Canonical boolean mirror of '
                                                                          'IS_CURRENT_IN_SOURCE; false rows '
                                                                          'are retained source tombstones.',
                                                    'source_record_key': 'Primary key value of the '
                                                                         'originating source record, held as '
                                                                         'text for provenance and '
                                                                         'deduplication.',
                                                    'source_table': 'Name of the upstream source table from '
                                                                    'which the record was ingested.',
                                                    'specialitytxt': 'Specialty or specialties associated '
                                                                     'with the case, recorded as coded label '
                                                                     'text from the source picklist.',
                                                    'surgical_attendeestxt': 'Surgical staff recorded as '
                                                                             'attending the mortality or M&M '
                                                                             'meeting, held as name text.',
                                                    'surname': 'Family name of the deceased patient as '
                                                               'recorded in the source mortality review '
                                                               'registry.',
                                                    'survey_off': 'Source-system flag indicating whether the '
                                                                  'review survey or questionnaire section is '
                                                                  'switched off for this record.',
                                                    'timeto1st_review_hrs': 'Time from admission to the '
                                                                            'first medical review, recorded '
                                                                            'in whole hours.',
                                                    'ward_where_patient_died': 'Ward or clinical area where '
                                                                               'the patient died, recorded '
                                                                               'as a local location label.',
                                                    'was_the_patient_covered_by_dols_or_mc_a_order': 'Indicator '
                                                                                                     'of '
                                                                                                     'whether '
                                                                                                     'the '
                                                                                                     'patient '
                                                                                                     'was '
                                                                                                     'subject '
                                                                                                     'to a '
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
                                                    'was_this_an_elective_death': 'Indicator of whether the '
                                                                                  'death followed an '
                                                                                  'elective admission or '
                                                                                  'procedure.',
                                                    'were_there_any_problems_with_the_care': 'Structured '
                                                                                             'reviewer '
                                                                                             'response '
                                                                                             'stating '
                                                                                             'whether any '
                                                                                             'problems with '
                                                                                             "the patient's "
                                                                                             'care were '
                                                                                             'identified, '
                                                                                             'including '
                                                                                             'branching '
                                                                                             'guidance '
                                                                                             'options.'},
                                        'table': 'Typed child of clinical_registry_entry for the '
                                                 'mortality_review family. Cardiac mortality/M&M review '
                                                 'registry: source-text cause-of-death chain (not asserted '
                                                 'to be ICD-coded), NCEPOD classification, '
                                                 'coronial/medical-examiner fields and structured '
                                                 'care-problem flags. Patient names, contact details and '
                                                 'next-of-kin content are retained and governed with IG '
                                                 'tags. Every governed bronze column is retained under its '
                                                 'lower-case name.'}}
