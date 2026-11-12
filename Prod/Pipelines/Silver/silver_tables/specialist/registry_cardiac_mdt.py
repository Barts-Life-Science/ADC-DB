"""Silver specialist.registry_cardiac_mdt: 1 dataset(s).

- datasets: clinical_registry_cardiac_mdt
- moved verbatim from: 3_silver_specialist_care:1016, 3_silver_specialist_care:1417

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


# Clinical / registry cardiac mdt

# ==== journey_clinical.registry_cardiac_mdt ====
REGISTRY_CARDIAC_MDT_EXPECTED_SCHEMA = [
    ("ENTRY_ID", "int"),
    ("MRN", "string"),
    ("GENDER", "string"),
    ("DATE_OF_DEATH", "date"),
    ("ETHNIC_ORIGIN", "string"),
    ("MD_TDATE", "timestamp"),
    ("CONSULTANT_RESPONSIBLE_FOR_PROCEDURETXT", "string"),
    ("PRESENTING_CLINICIANTXT", "string"),
    ("OTHER_PRESENTING_CLINICIAN", "string"),
    ("REFERRING_HOSPITAL", "string"),
    ("M_PRESENTING_HISTORY", "string"),
    ("CARDIAC_RISK_FACTORSTXT", "string"),
    ("SIGNIFICANT_NON_CORONARY_CARDIAC_HISTORYTXT", "string"),
    ("MGUC_HDETAIL", "string"),
    ("M_CARDIOMYOPATHY_DETAIL", "string"),
    ("OTHER_REFERRING_HOSPITALTXT", "string"),
    ("PATIENT_PREFERENCE", "string"),
    ("EUROSCORE", "double"),
    ("ANGINAL_MEDICATIONTXT", "string"),
    ("TROPONIN_NG_L", "double"),
    ("E_GFR_ML_MIN", "double"),
    ("SIGNIFICANT_VALVE_DISEASETXT", "string"),
    ("M_AORTIC_VALVE_DISEASE", "string"),
    ("M_MITRAL_VALVE_DISEASE", "string"),
    ("M_TRICUSPID_VALVE_DISEASE", "string"),
    ("M_PULMONARY_VALVE_DISEASE", "string"),
    ("VIABILITY_ASSESSMENTTXT", "string"),
    ("MCM_RDETAIL", "string"),
    ("M_STRESS_ECHO_DETAIL", "string"),
    ("MMPS_DETAIL", "string"),
    ("SYNTAX_SCORE1", "int"),
    ("CLINICAL_PRESENTATION", "string"),
    ("M_CLINICAL_PRESENTATION_OTHER", "string"),
    ("SMOKING_STATUS", "string"),
    ("LVEF", "int"),
    ("CARDIOLOGY_ATTENDEESTXT", "string"),
    ("M_DISCUSSION", "string"),
    ("TREATMENT_PLANNED", "string"),
    ("PLANNED_TIMING", "string"),
    ("RESPONSIBLE_CONSULTANT_SPECIALITY", "string"),
    ("RESPONSIBLE_CONSULTANT_CORONARYTXT", "string"),
    ("RESPONSIBLE_CONSULTANT_CARDIOTHORACICTXT", "string"),
    ("PCI", "string"),
    ("CABG", "string"),
    ("MEDICAL_THERAPY_ONLY", "string"),
    ("M_OTHER_TREATMENT", "string"),
    ("OTHER_CONSULTANT_AND_SPECIALITY", "string"),
    ("CARDIOTHORACIC_ATTENDEESTXT", "string"),
    ("IMAGING_ATTENDEE", "string"),
    ("MD_TCHAIR", "string"),
    ("CURRENT_LOCATION", "string"),
    ("HAEMOGLOBIN", "double"),
    ("OTHER_CORONARY_CONSULTANT", "string"),
    ("MFF_RDETAIL", "string"),
    ("PRIMARY_OPERATOR", "string"),
    ("CREATININE_UMOL_L", "int"),
    ("TREATMENT_DEVIATIONTXT", "string"),
    ("M_DETAILS_OF_DEVIATION", "string"),
    ("TREATMENT_PATHWAY", "string"),
    ("PATHWAY_DATE", "timestamp"),
    ("CAUSE_OF_DEVIATIONTXT", "string"),
    ("CONSULTANT_INFORMED_DT", "timestamp"),
    ("AUDIT_ENTERED_BYTXT", "string"),
    ("AUDIT_DT", "timestamp"),
    ("MD_TGUIDE", "string"),
    ("COMPLEX_MDT", "string"),
    ("OTHER_THERAPY_OPTIONSTXT", "string"),
    ("CARDIOLOGY_CONSULTANT_OF_THE_WEEKTXT", "string"),
    ("CARDIOLOGY_CONSULTANT_OTW_PRESENT", "string"),
    ("CARDIOTHORACIC_CONSULTANT_OF_THE_WEEKTXT", "string"),
    ("CARDIOTHORACIC_CONSULTANT_OTW_PRESENT", "string"),
    ("PATIENT_REMOVED_WITHOUT_DISCUSSION", "string"),
    ("REMOVED_WITHOUT_DISCUSSION_OTHER", "string"),
    ("REMOVED_DT", "timestamp"),
    ("E_PATTENDEESTXT", "string"),
    ("EP_CONSULTANT_OF_THE_WEEKTXT", "string"),
    ("EP_CONSULTANT_OF_THE_WEEK_PRESENT", "string"),
    ("DATE_LAST_CHANGED", "timestamp"),
    ("ADC_UPDT", "timestamp"),
    ("NHS_NUMBER_VALID_IND", "boolean"),
    ("LINKAGE_STATUS", "string"),
    ("PERSON_ID", "bigint"),
    ("LINKAGE_METHOD", "string"),
    ("LINKAGE_HISTORICAL_FALLBACK_IND", "boolean"),
    ("LINKAGE_FALLBACK_CONFLICT_IND", "boolean"),
    ("REGISTRY_TYPE", "string"),
    ("SOURCE_TABLE", "string"),
    ("SOURCE_RECORD_KEY", "string"),
    ("RELEVANT_MEDICATIONTXT", "string"),
    ("HYBRID_SURGERY", "string"),
    ("OPEN_SURGERY", "string"),
    ("OTHER_SPECIALITY", "string"),
    ("OTHER_CONSULTANTS_NAME", "string"),
    ("ENDOVASCULAR", "string"),
    ("SURVEILLANCE", "string"),
    ("DISCHARGE", "string"),
    ("AETIOLOGYTXT", "string"),
    ("PATHWAY_DT", "timestamp"),
    ("PRESENTING_SURGEONTXT", "string"),
    ("CONSULTANT_RESPONSIBLE_FOR_PROCEDURE", "string"),
    ("M_OTHER_RELEVANT_MEDICATION", "string"),
    ("OTHER_PATIENT_PREFERENCE", "string"),
    ("PALLIATIVE_CARE_ONLY", "string"),
    ("OUTCOME_COMPLETED", "string"),
    ("M_OTHER_ATTENDEES", "string"),
    ("M_OTHER_AETIOLOGY", "string"),
    ("IMAGING_ATTENDEE2ND", "string"),
    ("SURVEILLANCE_PATHWAY_PATIENT", "string"),
    ("USE_GUIDELINES_THRESHOLD", "string"),
    ("NEW_OPERATIVE_THRESHOLD", "double"),
    ("NEW_SURVEILLANCE_INTERVAL", "int"),
    ("OPERATIVE_THRESHOLD_MET", "string"),
    ("RATIONALE_FOR_CONTRADICTINGTXT", "string"),
    ("M_OTHER_HIGH_RISK", "string"),
    ("M_OTHER_COMORBIDITIES", "string"),
    ("M_OTHER_REASON", "string"),
    ("ROW_HASH", "string"),
    ("PIPELINE_LOADED_AT", "timestamp"),
    ("IS_CURRENT_IN_SOURCE", "boolean"),
    ("SOURCE_PRESENT_IND", "boolean"),
    ("SOURCE_ABSENT_DETECTED_TS", "timestamp"),
    ("SURNAME", "string"),
    ("FORENAME", "string"),
    ("NHS_NUMBER", "string"),
    ("DATE_OF_BIRTH", "date"),
    ("REFERRING_CONSULTANT_EMAIL", "string"),
    ("CARDIOTHORACIC_CONSULTANT_EMAILTXT", "string"),
    ("CARDIOLOGIST_CONSULTANT_EMAILTXT", "string"),
]


@materialized_view(
    name=_n("journey_clinical.registry_cardiac_mdt"),
    refresh_policy="auto",
)
def registry_cardiac_mdt():
    # Build the declared dataset: Typed child of clinical_registry_entry for the cardiac_mdt
    # family. Harmonised iWeb cardiac MDT record. REGISTRY_TYPE=CORONARY_REVASC identifies the
    # coronary revascularisation workflow; REGISTRY_TYPE=CT_AORTIC identifies CT/aortic
    # surveillance. Workflow-specific columns are null for the other registry. Every governed
    # bronze column is retained under its lower-case name.
    s = read_source(IWEB_REGISTRY_SLOTS["cardiac_mdt"])
    actual_schema = [(field.name, field.dataType.simpleString()) for field in s.schema.fields]
    if actual_schema != REGISTRY_CARDIAC_MDT_EXPECTED_SCHEMA:
        raise RuntimeError("cardiac_mdt: bronze schema drift" + f"; expected={REGISTRY_CARDIAC_MDT_EXPECTED_SCHEMA!r} actual={actual_schema!r}")
    return s.select(
        stable_id("registry_entry:iweb", F.lit("cardiac_mdt"), F.coalesce(s["REGISTRY_TYPE"], F.lit("~")), s["ENTRY_ID"]).alias("patient_event_key"),
        s["ENTRY_ID"].alias("entry_id"),
        s["MRN"].alias("mrn"),
        s["GENDER"].alias("gender"),
        s["DATE_OF_DEATH"].alias("date_of_death"),
        s["ETHNIC_ORIGIN"].alias("ethnic_origin"),
        s["MD_TDATE"].alias("md_tdate"),
        s["CONSULTANT_RESPONSIBLE_FOR_PROCEDURETXT"].alias("consultant_responsible_for_proceduretxt"),
        s["PRESENTING_CLINICIANTXT"].alias("presenting_cliniciantxt"),
        s["OTHER_PRESENTING_CLINICIAN"].alias("other_presenting_clinician"),
        s["REFERRING_HOSPITAL"].alias("referring_hospital"),
        s["M_PRESENTING_HISTORY"].alias("m_presenting_history"),
        s["CARDIAC_RISK_FACTORSTXT"].alias("cardiac_risk_factorstxt"),
        s["SIGNIFICANT_NON_CORONARY_CARDIAC_HISTORYTXT"].alias("significant_non_coronary_cardiac_historytxt"),
        s["MGUC_HDETAIL"].alias("mguc_hdetail"),
        s["M_CARDIOMYOPATHY_DETAIL"].alias("m_cardiomyopathy_detail"),
        s["OTHER_REFERRING_HOSPITALTXT"].alias("other_referring_hospitaltxt"),
        s["PATIENT_PREFERENCE"].alias("patient_preference"),
        s["EUROSCORE"].alias("euroscore"),
        s["ANGINAL_MEDICATIONTXT"].alias("anginal_medicationtxt"),
        s["TROPONIN_NG_L"].alias("troponin_ng_l"),
        s["E_GFR_ML_MIN"].alias("e_gfr_ml_min"),
        s["SIGNIFICANT_VALVE_DISEASETXT"].alias("significant_valve_diseasetxt"),
        s["M_AORTIC_VALVE_DISEASE"].alias("m_aortic_valve_disease"),
        s["M_MITRAL_VALVE_DISEASE"].alias("m_mitral_valve_disease"),
        s["M_TRICUSPID_VALVE_DISEASE"].alias("m_tricuspid_valve_disease"),
        s["M_PULMONARY_VALVE_DISEASE"].alias("m_pulmonary_valve_disease"),
        s["VIABILITY_ASSESSMENTTXT"].alias("viability_assessmenttxt"),
        s["MCM_RDETAIL"].alias("mcm_rdetail"),
        s["M_STRESS_ECHO_DETAIL"].alias("m_stress_echo_detail"),
        s["MMPS_DETAIL"].alias("mmps_detail"),
        s["SYNTAX_SCORE1"].alias("syntax_score1"),
        s["CLINICAL_PRESENTATION"].alias("clinical_presentation"),
        s["M_CLINICAL_PRESENTATION_OTHER"].alias("m_clinical_presentation_other"),
        s["SMOKING_STATUS"].alias("smoking_status"),
        s["LVEF"].alias("lvef"),
        s["CARDIOLOGY_ATTENDEESTXT"].alias("cardiology_attendeestxt"),
        s["M_DISCUSSION"].alias("m_discussion"),
        s["TREATMENT_PLANNED"].alias("treatment_planned"),
        s["PLANNED_TIMING"].alias("planned_timing"),
        s["RESPONSIBLE_CONSULTANT_SPECIALITY"].alias("responsible_consultant_speciality"),
        s["RESPONSIBLE_CONSULTANT_CORONARYTXT"].alias("responsible_consultant_coronarytxt"),
        s["RESPONSIBLE_CONSULTANT_CARDIOTHORACICTXT"].alias("responsible_consultant_cardiothoracictxt"),
        s["PCI"].alias("pci"),
        s["CABG"].alias("cabg"),
        s["MEDICAL_THERAPY_ONLY"].alias("medical_therapy_only"),
        s["M_OTHER_TREATMENT"].alias("m_other_treatment"),
        s["OTHER_CONSULTANT_AND_SPECIALITY"].alias("other_consultant_and_speciality"),
        s["CARDIOTHORACIC_ATTENDEESTXT"].alias("cardiothoracic_attendeestxt"),
        s["IMAGING_ATTENDEE"].alias("imaging_attendee"),
        s["MD_TCHAIR"].alias("md_tchair"),
        s["CURRENT_LOCATION"].alias("current_location"),
        s["HAEMOGLOBIN"].alias("haemoglobin"),
        s["OTHER_CORONARY_CONSULTANT"].alias("other_coronary_consultant"),
        s["MFF_RDETAIL"].alias("mff_rdetail"),
        s["PRIMARY_OPERATOR"].alias("primary_operator"),
        s["CREATININE_UMOL_L"].alias("creatinine_umol_l"),
        s["TREATMENT_DEVIATIONTXT"].alias("treatment_deviationtxt"),
        s["M_DETAILS_OF_DEVIATION"].alias("m_details_of_deviation"),
        s["TREATMENT_PATHWAY"].alias("treatment_pathway"),
        s["PATHWAY_DATE"].alias("pathway_date"),
        s["CAUSE_OF_DEVIATIONTXT"].alias("cause_of_deviationtxt"),
        s["CONSULTANT_INFORMED_DT"].alias("consultant_informed_dt"),
        s["AUDIT_ENTERED_BYTXT"].alias("audit_entered_bytxt"),
        s["AUDIT_DT"].alias("audit_dt"),
        s["MD_TGUIDE"].alias("md_tguide"),
        s["COMPLEX_MDT"].alias("complex_mdt"),
        s["OTHER_THERAPY_OPTIONSTXT"].alias("other_therapy_optionstxt"),
        s["CARDIOLOGY_CONSULTANT_OF_THE_WEEKTXT"].alias("cardiology_consultant_of_the_weektxt"),
        s["CARDIOLOGY_CONSULTANT_OTW_PRESENT"].alias("cardiology_consultant_otw_present"),
        s["CARDIOTHORACIC_CONSULTANT_OF_THE_WEEKTXT"].alias("cardiothoracic_consultant_of_the_weektxt"),
        s["CARDIOTHORACIC_CONSULTANT_OTW_PRESENT"].alias("cardiothoracic_consultant_otw_present"),
        s["PATIENT_REMOVED_WITHOUT_DISCUSSION"].alias("patient_removed_without_discussion"),
        s["REMOVED_WITHOUT_DISCUSSION_OTHER"].alias("removed_without_discussion_other"),
        s["REMOVED_DT"].alias("removed_dt"),
        s["E_PATTENDEESTXT"].alias("e_pattendeestxt"),
        s["EP_CONSULTANT_OF_THE_WEEKTXT"].alias("ep_consultant_of_the_weektxt"),
        s["EP_CONSULTANT_OF_THE_WEEK_PRESENT"].alias("ep_consultant_of_the_week_present"),
        s["DATE_LAST_CHANGED"].alias("date_last_changed"),
        s["ADC_UPDT"].alias("adc_updt"),
        s["NHS_NUMBER_VALID_IND"].alias("nhs_number_valid_ind"),
        s["LINKAGE_STATUS"].alias("linkage_status"),
        s["PERSON_ID"].alias("person_id"),
        s["LINKAGE_METHOD"].alias("linkage_method"),
        s["LINKAGE_HISTORICAL_FALLBACK_IND"].alias("linkage_historical_fallback_ind"),
        s["LINKAGE_FALLBACK_CONFLICT_IND"].alias("linkage_fallback_conflict_ind"),
        s["REGISTRY_TYPE"].alias("registry_type"),
        s["SOURCE_TABLE"].alias("source_table"),
        s["SOURCE_RECORD_KEY"].alias("source_record_key"),
        s["RELEVANT_MEDICATIONTXT"].alias("relevant_medicationtxt"),
        s["HYBRID_SURGERY"].alias("hybrid_surgery"),
        s["OPEN_SURGERY"].alias("open_surgery"),
        s["OTHER_SPECIALITY"].alias("other_speciality"),
        s["OTHER_CONSULTANTS_NAME"].alias("other_consultants_name"),
        s["ENDOVASCULAR"].alias("endovascular"),
        s["SURVEILLANCE"].alias("surveillance"),
        s["DISCHARGE"].alias("discharge"),
        s["AETIOLOGYTXT"].alias("aetiologytxt"),
        s["PATHWAY_DT"].alias("pathway_dt"),
        s["PRESENTING_SURGEONTXT"].alias("presenting_surgeontxt"),
        s["CONSULTANT_RESPONSIBLE_FOR_PROCEDURE"].alias("consultant_responsible_for_procedure"),
        s["M_OTHER_RELEVANT_MEDICATION"].alias("m_other_relevant_medication"),
        s["OTHER_PATIENT_PREFERENCE"].alias("other_patient_preference"),
        s["PALLIATIVE_CARE_ONLY"].alias("palliative_care_only"),
        s["OUTCOME_COMPLETED"].alias("outcome_completed"),
        s["M_OTHER_ATTENDEES"].alias("m_other_attendees"),
        s["M_OTHER_AETIOLOGY"].alias("m_other_aetiology"),
        s["IMAGING_ATTENDEE2ND"].alias("imaging_attendee2nd"),
        s["SURVEILLANCE_PATHWAY_PATIENT"].alias("surveillance_pathway_patient"),
        s["USE_GUIDELINES_THRESHOLD"].alias("use_guidelines_threshold"),
        s["NEW_OPERATIVE_THRESHOLD"].alias("new_operative_threshold"),
        s["NEW_SURVEILLANCE_INTERVAL"].alias("new_surveillance_interval"),
        s["OPERATIVE_THRESHOLD_MET"].alias("operative_threshold_met"),
        s["RATIONALE_FOR_CONTRADICTINGTXT"].alias("rationale_for_contradictingtxt"),
        s["M_OTHER_HIGH_RISK"].alias("m_other_high_risk"),
        s["M_OTHER_COMORBIDITIES"].alias("m_other_comorbidities"),
        s["M_OTHER_REASON"].alias("m_other_reason"),
        s["ROW_HASH"].alias("row_hash"),
        s["PIPELINE_LOADED_AT"].alias("pipeline_loaded_at"),
        s["IS_CURRENT_IN_SOURCE"].alias("is_current_in_source"),
        s["SOURCE_PRESENT_IND"].alias("source_present_ind"),
        s["SOURCE_ABSENT_DETECTED_TS"].alias("source_absent_detected_ts"),
        s["SURNAME"].alias("surname"),
        s["FORENAME"].alias("forename"),
        s["NHS_NUMBER"].alias("nhs_number"),
        s["DATE_OF_BIRTH"].alias("date_of_birth"),
        s["REFERRING_CONSULTANT_EMAIL"].alias("referring_consultant_email"),
        s["CARDIOTHORACIC_CONSULTANT_EMAILTXT"].alias("cardiothoracic_consultant_emailtxt"),
        s["CARDIOLOGIST_CONSULTANT_EMAILTXT"].alias("cardiologist_consultant_emailtxt"),
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_registry_cardiac_mdt': {'columns': {'adc_updt': 'Timestamp of the last update applied to the '
                                                           'record by the upstream data ingestion process.',
                                               'aetiologytxt': 'Recorded aetiology of the condition '
                                                               'discussed at the CT/aortic MDT, held as '
                                                               'decoded selection text.',
                                               'anginal_medicationtxt': 'Coded list of anti-anginal '
                                                                        'medications the patient is taking, '
                                                                        'expressed as resolved picklist '
                                                                        'labels.',
                                               'audit_dt': 'Date and time the audit entry was recorded for '
                                                           'this MDT record.',
                                               'audit_entered_bytxt': 'Name of the member of staff who '
                                                                      'entered the audit record for this MDT '
                                                                      'entry.',
                                               'cabg': 'MDT recommendation status for coronary artery bypass '
                                                       'grafting as a treatment option.',
                                               'cardiac_risk_factorstxt': 'Coded list of cardiac risk '
                                                                          'factors recorded for the patient '
                                                                          'at MDT, expressed as resolved '
                                                                          'picklist labels.',
                                               'cardiologist_consultant_emailtxt': 'Email contact recorded '
                                                                                   'for the cardiology '
                                                                                   'consultant associated '
                                                                                   'with the MDT record, '
                                                                                   'held as decoded picklist '
                                                                                   'text in the CT/aortic '
                                                                                   'workflow.',
                                               'cardiology_attendeestxt': 'Coded text list of the cardiology '
                                                                          'clinicians recorded as attending '
                                                                          'the MDT meeting.',
                                               'cardiology_consultant_of_the_weektxt': 'Name of the '
                                                                                       'cardiology '
                                                                                       'consultant of the '
                                                                                       'week associated with '
                                                                                       'the MDT meeting.',
                                               'cardiology_consultant_otw_present': 'Indicator of whether '
                                                                                    'the cardiology '
                                                                                    'consultant of the week '
                                                                                    'was present at the MDT '
                                                                                    'meeting.',
                                               'cardiothoracic_attendeestxt': 'Decoded list of the '
                                                                              'cardiothoracic surgical '
                                                                              'clinicians recorded as '
                                                                              'attending the MDT meeting.',
                                               'cardiothoracic_consultant_emailtxt': 'Email contact recorded '
                                                                                     'for the cardiothoracic '
                                                                                     'consultant associated '
                                                                                     'with the MDT record, '
                                                                                     'held as decoded '
                                                                                     'picklist text in the '
                                                                                     'CT/aortic workflow.',
                                               'cardiothoracic_consultant_of_the_weektxt': 'Name of the '
                                                                                           'cardiothoracic '
                                                                                           'consultant of '
                                                                                           'the week '
                                                                                           'associated with '
                                                                                           'the MDT meeting.',
                                               'cardiothoracic_consultant_otw_present': 'Indicator of '
                                                                                        'whether the '
                                                                                        'cardiothoracic '
                                                                                        'consultant of the '
                                                                                        'week was present at '
                                                                                        'the MDT meeting.',
                                               'cause_of_deviationtxt': 'Lookup label giving the recorded '
                                                                        'cause of deviation from the '
                                                                        'MDT-recommended treatment.',
                                               'clinical_presentation': 'Coded category describing the '
                                                                        "patient's clinical presentation at "
                                                                        'referral to the MDT.',
                                               'complex_mdt': 'Categorical indicator of the type of MDT '
                                                              'meeting in which the case was discussed.',
                                               'consultant_informed_dt': 'Date and time the responsible '
                                                                         'consultant was informed in '
                                                                         'relation to the MDT record.',
                                               'consultant_responsible_for_procedure': 'Consultant '
                                                                                       'identified as '
                                                                                       'responsible for the '
                                                                                       'planned procedure '
                                                                                       'following the '
                                                                                       'CT/aortic MDT '
                                                                                       'decision.',
                                               'consultant_responsible_for_proceduretxt': 'Name of the '
                                                                                          'consultant '
                                                                                          'recorded as '
                                                                                          'responsible for '
                                                                                          'the planned '
                                                                                          'procedure, '
                                                                                          'populated for the '
                                                                                          'coronary '
                                                                                          'revascularisation '
                                                                                          'workflow.',
                                               'creatinine_umol_l': 'Serum creatinine result in micromoles '
                                                                    'per litre recorded for the MDT '
                                                                    'assessment.',
                                               'current_location': "Coded description of the patient's care "
                                                                   'setting or location at the time of the '
                                                                   'MDT discussion.',
                                               'date_last_changed': 'Date and time the record was last '
                                                                    'changed in the source iWeb MDT '
                                                                    'registry.',
                                               'date_of_birth': 'Date of birth of the patient the MDT record '
                                                                'relates to, carried through from the source '
                                                                'iWeb MDT registries.',
                                               'date_of_death': 'Recorded date of death of the patient, '
                                                                'where known, as held in the source iWeb MDT '
                                                                'registry.',
                                               'discharge': 'Indicator that discharge from the CT/aortic MDT '
                                                            'pathway was the outcome decided.',
                                               'e_gfr_ml_min': 'Recorded estimated glomerular filtration '
                                                               'rate for the patient in millilitres per '
                                                               'minute.',
                                               'e_pattendeestxt': 'Recorded electrophysiology attendees at '
                                                                  'the MDT meeting.',
                                               'endovascular': 'Indicator that endovascular treatment was '
                                                               'the option decided at the CT/aortic MDT.',
                                               'entry_id': 'Source EntryId, unique only within '
                                                           'REGISTRY_TYPE.',
                                               'ep_consultant_of_the_week_present': 'Indicator of whether '
                                                                                    'the electrophysiology '
                                                                                    'consultant of the week '
                                                                                    'was present at the MDT '
                                                                                    'meeting.',
                                               'ep_consultant_of_the_weektxt': 'Name of the '
                                                                               'electrophysiology consultant '
                                                                               'of the week associated with '
                                                                               'the MDT meeting, held as '
                                                                               'decoded text.',
                                               'ethnic_origin': 'Coded ethnic origin of the patient as '
                                                                'recorded in the source iWeb MDT registry.',
                                               'euroscore': 'EuroSCORE operative risk score recorded for the '
                                                            'patient at MDT.',
                                               'forename': 'Forename of the patient the MDT record relates '
                                                           'to, carried through from the source iWeb MDT '
                                                           'registries.',
                                               'gender': 'Recorded gender of the patient as held in the '
                                                         'source iWeb MDT registry.',
                                               'haemoglobin': 'Haemoglobin result recorded for the MDT '
                                                              'assessment; units are not stated in the '
                                                              'source metadata.',
                                               'hybrid_surgery': 'Indicator that hybrid surgery was the '
                                                                 'treatment decided at the CT/aortic MDT.',
                                               'imaging_attendee': 'Imaging clinician recorded as attending '
                                                                   'the MDT meeting, entered as free text.',
                                               'imaging_attendee2nd': 'Second imaging clinician recorded as '
                                                                      'attending the MDT meeting, populated '
                                                                      'only for REGISTRY_TYPE=CT_AORTIC.',
                                               'is_current_in_source': 'Boolean flag indicating whether the '
                                                                       'record is still present as the '
                                                                       'current version in the source '
                                                                       'system.',
                                               'linkage_fallback_conflict_ind': 'Indicator that the fallback '
                                                                                'linkage produced a '
                                                                                'conflicting patient match '
                                                                                'during processing.',
                                               'linkage_historical_fallback_ind': 'Indicator that patient '
                                                                                  'linkage was achieved '
                                                                                  'using a historical '
                                                                                  'identifier fallback '
                                                                                  'rather than a current '
                                                                                  'identifier match.',
                                               'linkage_method': 'Code describing which identifier '
                                                                 'combination was used to link the MDT '
                                                                 'record to the master patient record.',
                                               'linkage_status': 'Coded outcome of linking the MDT record to '
                                                                 'a master patient record, indicating which '
                                                                 'identifiers matched.',
                                               'lvef': 'Left ventricular ejection fraction value recorded '
                                                       'for the patient at the MDT assessment.',
                                               'm_aortic_valve_disease': 'Free-text detail describing the '
                                                                         "patient's aortic valve disease "
                                                                         'where recorded.',
                                               'm_cardiomyopathy_detail': 'Free-text detail describing the '
                                                                          "patient's cardiomyopathy where "
                                                                          'recorded in the cardiac history.',
                                               'm_clinical_presentation_other': 'Free-text description of '
                                                                                'the clinical presentation '
                                                                                'when the coded presentation '
                                                                                'category is recorded as '
                                                                                'other.',
                                               'm_details_of_deviation': 'Free-text description of how the '
                                                                         'delivered treatment deviated from '
                                                                         'the MDT recommendation.',
                                               'm_discussion': 'Free-text narrative of the MDT discussion '
                                                               'and clinical reasoning for the patient.',
                                               'm_mitral_valve_disease': 'Free-text detail describing the '
                                                                         "patient's mitral valve disease "
                                                                         'where recorded.',
                                               'm_other_aetiology': 'Free-text description of aetiology '
                                                                    'where not covered by the coded '
                                                                    'AETIOLOGYTXT options, populated only '
                                                                    'for REGISTRY_TYPE=CT_AORTIC.',
                                               'm_other_attendees': 'Free-text record of additional MDT '
                                                                    'attendees not captured in the specialty '
                                                                    'attendee fields, populated only for '
                                                                    'REGISTRY_TYPE=CT_AORTIC.',
                                               'm_other_comorbidities': 'Free-text description of other '
                                                                        'comorbidities not captured in the '
                                                                        'coded history fields, populated '
                                                                        'only for REGISTRY_TYPE=CT_AORTIC.',
                                               'm_other_high_risk': 'Free-text description of other '
                                                                    'high-risk features considered at the '
                                                                    'MDT, populated only for '
                                                                    'REGISTRY_TYPE=CT_AORTIC.',
                                               'm_other_reason': 'Free-text description of another reason '
                                                                 'recorded for the MDT decision or pathway '
                                                                 'deviation, populated only for '
                                                                 'REGISTRY_TYPE=CT_AORTIC.',
                                               'm_other_relevant_medication': 'Free-text description of '
                                                                              'other relevant medication '
                                                                              'recorded at the CT/aortic '
                                                                              'MDT, populated only for '
                                                                              'REGISTRY_TYPE=CT_AORTIC.',
                                               'm_other_treatment': 'Free-text description of treatment '
                                                                    'agreed at the MDT that is not covered '
                                                                    'by the coded treatment options.',
                                               'm_presenting_history': "Free-text narrative of the patient's "
                                                                       'presenting history and relevant '
                                                                       'background recorded for the MDT '
                                                                       'discussion.',
                                               'm_pulmonary_valve_disease': 'Recorded assessment or '
                                                                            'description of pulmonary valve '
                                                                            'disease for the patient at the '
                                                                            'cardiac MDT.',
                                               'm_stress_echo_detail': 'Free-text detail of stress '
                                                                       'echocardiography findings recorded '
                                                                       'for the MDT assessment.',
                                               'm_tricuspid_valve_disease': 'Free-text detail describing the '
                                                                            "patient's tricuspid valve "
                                                                            'disease where recorded.',
                                               'mcm_rdetail': 'Free-text detail of the cardiac magnetic '
                                                              'resonance findings considered at the MDT.',
                                               'md_tchair': 'Clinician recorded as chairing the MDT meeting.',
                                               'md_tdate': 'Date of the multidisciplinary team meeting at '
                                                           'which the case was discussed.',
                                               'md_tguide': 'Indicator recorded in the coronary MDT record '
                                                            'referencing the MDT guidance applied to the '
                                                            'discussion.',
                                               'medical_therapy_only': 'MDT recommendation status for '
                                                                       'medical therapy alone as the '
                                                                       'treatment option.',
                                               'mff_rdetail': 'Free-text detail of the fractional flow '
                                                              'reserve assessment recorded in the coronary '
                                                              'MDT record.',
                                               'mguc_hdetail': 'Free-text detail supporting the grown-up '
                                                               'congenital heart disease item recorded '
                                                               'within the cardiac history.',
                                               'mmps_detail': 'Free-text detail of the myocardial perfusion '
                                                              'scan findings recorded for the MDT '
                                                              'assessment.',
                                               'mrn': 'Local hospital medical record number of the patient '
                                                      'the MDT record relates to, carried through from the '
                                                      'source iWeb MDT registries.',
                                               'new_operative_threshold': 'Revised operative threshold '
                                                                          'agreed at the MDT for this '
                                                                          'patient, populated only for '
                                                                          'REGISTRY_TYPE=CT_AORTIC; no unit '
                                                                          'is stated in source metadata.',
                                               'new_surveillance_interval': 'Revised surveillance imaging '
                                                                            'interval agreed at the MDT, '
                                                                            'populated only for '
                                                                            'REGISTRY_TYPE=CT_AORTIC; no '
                                                                            'unit is stated in source '
                                                                            'metadata.',
                                               'nhs_number': 'NHS number of the patient the MDT record '
                                                             'relates to, carried through from the source '
                                                             'iWeb MDT registries.',
                                               'nhs_number_valid_ind': 'Derived boolean indicating whether '
                                                                       'the NHS number held on the source '
                                                                       'MDT record passed validation, '
                                                                       'without exposing the number itself.',
                                               'open_surgery': 'Indicator that open surgery was the '
                                                               'treatment decided at the CT/aortic MDT.',
                                               'operative_threshold_met': 'Indicates whether the applicable '
                                                                          'operative threshold was met at '
                                                                          'the time of MDT review, populated '
                                                                          'only for REGISTRY_TYPE=CT_AORTIC.',
                                               'other_consultant_and_speciality': 'Free-text record of an '
                                                                                  'additional consultant and '
                                                                                  'their speciality involved '
                                                                                  'in the MDT decision.',
                                               'other_consultants_name': 'Name of another consultant '
                                                                         'involved in the CT/aortic MDT '
                                                                         'discussion, entered as free text.',
                                               'other_coronary_consultant': 'Free-text name of an additional '
                                                                            'coronary consultant recorded '
                                                                            'against the coronary '
                                                                            'revascularisation MDT record.',
                                               'other_patient_preference': 'Free-text detail of the '
                                                                           "patient's stated treatment "
                                                                           'preference where not covered by '
                                                                           'the coded PATIENT_PREFERENCE '
                                                                           'options, populated only for '
                                                                           'REGISTRY_TYPE=CT_AORTIC.',
                                               'other_presenting_clinician': 'Free-text name of the '
                                                                             'presenting clinician where the '
                                                                             'individual is not available in '
                                                                             'the standard clinician '
                                                                             'picklist.',
                                               'other_referring_hospitaltxt': 'Free-text or picklist-derived '
                                                                              'name of the referring '
                                                                              'hospital where the '
                                                                              'organisation is not available '
                                                                              'in the standard referral '
                                                                              'list.',
                                               'other_speciality': 'Free-text specialty of another clinician '
                                                                   'involved in or attending the CT/aortic '
                                                                   'MDT discussion.',
                                               'other_therapy_optionstxt': 'Recorded label describing other '
                                                                           'therapy options considered by '
                                                                           'the MDT.',
                                               'outcome_completed': 'Indicates whether the MDT outcome '
                                                                    'record has been completed, populated '
                                                                    'only for REGISTRY_TYPE=CT_AORTIC.',
                                               'palliative_care_only': 'Indicates whether the MDT treatment '
                                                                       'decision was palliative care only, '
                                                                       'populated only for '
                                                                       'REGISTRY_TYPE=CT_AORTIC.',
                                               'pathway_date': 'Date recorded against the treatment pathway '
                                                               'for the MDT record.',
                                               'pathway_dt': 'Date and time recorded for the CT/aortic MDT '
                                                             'treatment pathway milestone.',
                                               'patient_event_key': 'Deterministic SHA-256 key matching '
                                                                    'clinical_registry_entry.patient_event_key '
                                                                    'for the cardiac_mdt source family and '
                                                                    'native entry_id.',
                                               'patient_preference': 'Recorded treatment preference '
                                                                     'expressed by the patient to inform the '
                                                                     'MDT decision.',
                                               'patient_removed_without_discussion': 'Indicator or reason '
                                                                                     'recorded when the '
                                                                                     'patient was removed '
                                                                                     'from the MDT list '
                                                                                     'without being '
                                                                                     'discussed.',
                                               'pci': 'MDT recommendation status for percutaneous coronary '
                                                      'intervention as a treatment option.',
                                               'person_id': 'Internal Millennium person identifier linked to '
                                                            'this MDT record via the person alias table.',
                                               'pipeline_loaded_at': 'Timestamp when the record was loaded '
                                                                     'into this table by the ingestion '
                                                                     'pipeline.',
                                               'planned_timing': 'Coded urgency or timing category for the '
                                                                 'treatment planned at the MDT.',
                                               'presenting_cliniciantxt': 'Name of the clinician who '
                                                                          'presented the case at the MDT '
                                                                          'meeting.',
                                               'presenting_surgeontxt': 'Named surgeon who presented the '
                                                                        'case at the CT/aortic MDT, held as '
                                                                        'decoded text.',
                                               'primary_operator': 'Clinician recorded as the primary '
                                                                   'operator for the planned procedure, held '
                                                                   'as free text and sometimes containing '
                                                                   'placeholder entries.',
                                               'rationale_for_contradictingtxt': 'Free-text rationale for a '
                                                                                 'decision that contradicts '
                                                                                 'the guideline threshold or '
                                                                                 'recommendation, populated '
                                                                                 'only for '
                                                                                 'REGISTRY_TYPE=CT_AORTIC.',
                                               'referring_consultant_email': 'Email address recorded for the '
                                                                             'consultant who referred the '
                                                                             'patient for MDT discussion, as '
                                                                             'held in the source iWeb MDT '
                                                                             'registries.',
                                               'referring_hospital': 'Hospital or organisation that referred '
                                                                     'the patient for MDT discussion, '
                                                                     'selected from the source picklist.',
                                               'registry_type': 'Source workflow discriminator and part of '
                                                                'the composite key.',
                                               'relevant_medicationtxt': 'Relevant medication recorded at '
                                                                         'the MDT for the CT/aortic '
                                                                         'surveillance workflow, held as '
                                                                         'decoded text.',
                                               'removed_dt': 'Date and time the patient was removed from the '
                                                             'MDT list.',
                                               'removed_without_discussion_other': 'Free-text detail of the '
                                                                                   'other reason the patient '
                                                                                   'was removed from the MDT '
                                                                                   'list without discussion.',
                                               'responsible_consultant_cardiothoracictxt': 'Name of the '
                                                                                           'cardiothoracic '
                                                                                           'surgical '
                                                                                           'consultant '
                                                                                           'recorded as '
                                                                                           'responsible for '
                                                                                           "the patient's "
                                                                                           'planned '
                                                                                           'treatment '
                                                                                           'following the '
                                                                                           'MDT decision, '
                                                                                           'held as decoded '
                                                                                           'picklist text.',
                                               'responsible_consultant_coronarytxt': 'Name of the coronary '
                                                                                     '(cardiology) '
                                                                                     'consultant recorded as '
                                                                                     'responsible for the '
                                                                                     "patient's planned "
                                                                                     'treatment following '
                                                                                     'the MDT decision, held '
                                                                                     'as decoded picklist '
                                                                                     'text.',
                                               'responsible_consultant_speciality': 'Coded speciality of the '
                                                                                    'consultant designated '
                                                                                    'as responsible for the '
                                                                                    "patient's planned "
                                                                                    'treatment.',
                                               'row_hash': "Hash of the source record's business columns "
                                                           'used to detect changes between pipeline loads.',
                                               'significant_non_coronary_cardiac_historytxt': 'Coded list of '
                                                                                              'significant '
                                                                                              'non-coronary '
                                                                                              'cardiac '
                                                                                              'history items '
                                                                                              'recorded at '
                                                                                              'MDT, '
                                                                                              'expressed as '
                                                                                              'resolved '
                                                                                              'picklist '
                                                                                              'labels.',
                                               'significant_valve_diseasetxt': 'Coded list of valves '
                                                                               'affected by significant '
                                                                               'valve disease, expressed as '
                                                                               'resolved picklist labels.',
                                               'smoking_status': 'Coded smoking status recorded as part of '
                                                                 'the cardiac risk factor assessment for the '
                                                                 'MDT.',
                                               'source_absent_detected_ts': 'Timestamp recorded by the '
                                                                            'ingestion pipeline when the '
                                                                            'record was first detected as no '
                                                                            'longer present in the source '
                                                                            'iWeb registry.',
                                               'source_present_ind': 'Canonical boolean mirror of '
                                                                     'IS_CURRENT_IN_SOURCE; false rows are '
                                                                     'retained source tombstones.',
                                               'source_record_key': 'Identifier of the originating source '
                                                                    'registry record, derived from the '
                                                                    'source EntryId and used with '
                                                                    'REGISTRY_TYPE to identify the record '
                                                                    'uniquely.',
                                               'source_table': 'Name of the source iWeb registry table from '
                                                               'which the harmonised record was derived.',
                                               'surname': 'Surname of the patient the MDT record relates to, '
                                                          'carried through from the source iWeb MDT '
                                                          'registries.',
                                               'surveillance': 'Indicator that continued surveillance was '
                                                               'the outcome decided at the CT/aortic MDT.',
                                               'surveillance_pathway_patient': 'Indicates whether the '
                                                                               'patient is managed on the '
                                                                               'aortic surveillance pathway, '
                                                                               'populated only for '
                                                                               'REGISTRY_TYPE=CT_AORTIC.',
                                               'syntax_score1': "SYNTAX score recorded for the patient's "
                                                                'coronary anatomy at the MDT.',
                                               'treatment_deviationtxt': 'Lookup label indicating whether '
                                                                         'the treatment delivered deviated '
                                                                         'from the MDT recommendation.',
                                               'treatment_pathway': 'Coded description of the treatment '
                                                                    'pathway followed relative to the MDT '
                                                                    'recommendation.',
                                               'treatment_planned': 'Coded category describing the treatment '
                                                                    'setting or pathway planned at the MDT.',
                                               'troponin_ng_l': 'Recorded troponin result for the patient in '
                                                                'nanograms per litre.',
                                               'use_guidelines_threshold': 'Indicates whether the standard '
                                                                           'guideline operative threshold '
                                                                           'was applied rather than a '
                                                                           'locally agreed threshold, '
                                                                           'populated only for '
                                                                           'REGISTRY_TYPE=CT_AORTIC.',
                                               'viability_assessmenttxt': 'Coded text describing the '
                                                                          'myocardial viability assessment '
                                                                          'method recorded at the MDT.'},
                                   'table': 'Typed child of clinical_registry_entry for the cardiac_mdt '
                                            'family. Harmonised iWeb cardiac MDT record. '
                                            'REGISTRY_TYPE=CORONARY_REVASC identifies the coronary '
                                            'revascularisation workflow; REGISTRY_TYPE=CT_AORTIC identifies '
                                            'CT/aortic surveillance. Workflow-specific columns are null for '
                                            'the other registry. Every governed bronze column is retained '
                                            'under its lower-case name.'}}
