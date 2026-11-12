"""Silver specialist.registry_coronary_procedure: 1 dataset(s).

- datasets: clinical_registry_coronary_procedure
- moved verbatim from: 3_silver_specialist_care:3704, 3_silver_specialist_care:4510

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


# Clinical / registry coronary procedure

# ==== journey_clinical.registry_coronary_procedure ====
REGISTRY_CORONARY_PROCEDURE_EXPECTED_SCHEMA = [
    ("ENTRY_ID", "bigint"),
    ("PRE_DATE_LAST_CHANGED", "timestamp"),
    ("PRE_ADC_UPDT", "timestamp"),
    ("PRE_GENDER", "string"),
    ("PRE_DATE_OF_DEATH", "date"),
    ("PRE_ETHNIC_ORIGIN", "string"),
    ("PRE_DATEANDTIMEOFOPERATION", "timestamp"),
    ("PRE_AGE", "int"),
    ("PRE_SEX", "string"),
    ("PRE_PATIENT_ETHNIC_GROUP", "string"),
    ("PRE_ADMINISTRATIVE_CATEGORY", "string"),
    ("PRE_CLINICAL_SYNDROMEPCI", "string"),
    ("PRE_INDICATION_FOR_INTERVENTION", "string"),
    ("PRE_PROCEDURE_URGENCY", "string"),
    ("PRE_CARDIOGENIC_SHOCKPRE_PROCEDURE", "string"),
    ("PRE_CCS_ANGINA_STATUSPRE_PROCEDURE_STABLE_ONLY", "string"),
    ("PRE_NYHA_DYSPNOEA_STATUSPRE_PROCEDURE_STABLE_ONLY", "string"),
    ("PRE_DATETIME_ARRIVAL_AT_FIRST_HOSPITALACS_ONLY", "timestamp"),
    ("PRE_ADMISSION_ROUTEACS_ONLY", "string"),
    ("PRE_PRESENTING_ECGACS_ONLY", "string"),
    ("PRE_RECENT_LYSISACS_ONLY", "string"),
    ("PRE_CARDIAC_ENZYMESMARKERS_RAISED", "string"),
    ("PRE_PREVIOUS_MI", "string"),
    ("PRE_PREVIOUS_CABG", "string"),
    ("PRE_PREVIOUS_PCI", "string"),
    ("PRE_DIABETES", "string"),
    ("PRE_HEIGHT", "int"),
    ("PRE_WEIGHT", "double"),
    ("PRE_BODY_SURFACE_AREA", "double"),
    ("PRE_BODY_MASS_INDEX", "double"),
    ("PRE_LV_EJECTION_FRACTION_CATEGORY", "string"),
    ("PRE_LV_EJECTION_FRACTION", "int"),
    ("PRE_CONSULTANT_RESPONSIBLE_FOR_PROCEDURETXT", "string"),
    ("PRE_PRIMARY_OPERATORTXT", "string"),
    ("PRE_PRIMARY_OPERATOR_STATUS", "string"),
    ("PRE_SECOND_OPERATORTXT", "string"),
    ("PRE_SECOND_OPERATOR_STATUS", "string"),
    ("PRE_THIRD_OPERATORTXT", "string"),
    ("PRE_THIRD_OPERATOR_STATUS", "string"),
    ("PRE_GP_IIBIIIA_DRUGS_USED_DURING_PROCEDURETXT", "string"),
    ("PRE_EMBOLI_PROTECTION_DEVICES_USEDTXT", "string"),
    ("PRE_CIRCULATORY_SUPPORTTXT", "string"),
    ("PRE_LOCAL_PROCEDURE_IDENTIFIER", "string"),
    ("PRE_CHOLESTEROL", "double"),
    ("PRE_SMOKING_HISTORY", "string"),
    ("PRE_FAMILY_HISTORY_OF_CAD", "string"),
    ("PRE_MEDICAL_HISTORYTXT", "string"),
    ("PRE_VENTILATED_PREOP", "string"),
    ("PRE_Q_WAVE_ON_ECG", "string"),
    ("PRE_ECG_ISCHAEMIA", "string"),
    ("PRE_FOLLOW_ONAD_HOC_PROCEDURE", "string"),
    ("PRE_TRAINING_PROCEDURE", "string"),
    ("PRE_RESEARCH_PROCEDURE", "string"),
    ("PRE_RESEARCH_TITLE", "string"),
    ("PRE_ARTERIAL_ACCESSTXT", "string"),
    ("PRE_WHY_NO_IIBIIIA_DURING_PROCEDURE", "string"),
    ("PRE_SURGICAL_COVER", "string"),
    ("PRE_DATE_TIME_ARRIVAL_AT_PCI_HOSPITAL", "timestamp"),
    ("PRE_DATE_TIME_OF_CALL_FOR_HELPSTEMI", "timestamp"),
    ("PRE_REFERRING_HOSPITALTXT", "string"),
    ("PRE_DATETIME_OF_ECG_TRIGGERING_PPCI_PATHWAY", "timestamp"),
    ("PRE_PATIENT_LOCATION_AT_TIME_OF_STEMI_ONSET_RAW", "string"),
    ("PRE_PATIENT_LOCATION_AT_TIME_OF_STEMI_ONSET_TS", "timestamp"),
    ("PRE_SERUM_CREATININE", "int"),
    ("PRE_DATETIME_OF_SYMPTOM_ONSETPCI_ACS_ONLY", "timestamp"),
    ("PRE_ECHO_REPORT", "string"),
    ("PRE_ARTERIAL_BLOOD_GAS_ON_ARRIVAL_IN_CATH_LAB_PH", "double"),
    ("PRE_ARTERIAL_BLOOD_GAS_ON_ARRIVAL_IN_CATH_LAB_LACTATE", "double"),
    ("PRE_ARTERIAL_BLOOD_GAS_ON_ARRIVAL_IN_CATH_LAB_BASE_EXCESS", "double"),
    ("PRE_GLASGOW_COMA_SCALE_ON_ARRIVAL_IN_CATH_LAB", "int"),
    ("PRE_OTHER_THERAPEUTIC_HYPOTHERMIA", "string"),
    ("PRE_THERAPEUTIC_HYPOTHERMIATXT", "string"),
    ("PRE_CATH_PROCEDURETXT", "string"),
    ("PRE_OUT_OF_HOSPITAL_CARDIAC_ARREST", "string"),
    ("PRE_PRESUMED_DATE_TIME_OF_ARREST", "timestamp"),
    ("PRE_CORONARY_DOMINANCE", "string"),
    ("PRE_M_PROCEDURE_FINDINGS", "string"),
    ("PRE_ARTERIAL_MANAGEMENTTXT", "string"),
    ("PRE_DRUG_THERAPY_PREPROCEDURETXT", "string"),
    ("PRE_REFERRING_CARDIOLOGISTTXT", "string"),
    ("PRE_HISTORY_OF_RENAL_DISEASE1", "string"),
    ("PRE_TEMPORARY_PACING_WIRE", "string"),
    ("PRE_CATH_DEVICESTXT", "string"),
    ("PRE_CATH_LAB_NUMBER", "string"),
    ("PRE_DATE_AND_TIME_ON_TABLE", "timestamp"),
    ("PRE_VENTILATION", "string"),
    ("PRE_HOSPITAL_IDENTIFIER", "string"),
    ("PRE_DATE_TIME_SHORTCUTS_RAW", "string"),
    ("PRE_DATE_TIME_SHORTCUTS_TS", "timestamp"),
    ("PRE_M_ADMISSION_HISTORY", "string"),
    ("PRE_DIABETES_TYPE", "string"),
    ("PRE_INTERVENTIONS", "string"),
    ("PRE_CONVALESCENT_STEM_IOR_NSTEMI", "string"),
    ("PRE_US_SGUIDED", "string"),
    ("PRE_SCREEN_OF_LEFT_FEMORAL_HEAD", "string"),
    ("PRE_LEFT_FEMORAL_MICROPUNCTURE", "string"),
    ("PRE_LEFT_HEIGHT_OF_PUNCTURE", "string"),
    ("PRE_HEIGHT_OF_PUNCTURE_DIAGRAM", "string"),
    ("PRE_RIGHT_FEMORAL_MICROPUNCTURE", "string"),
    ("PRE_RIGHT_HEIGHT_OF_PUNCTURE", "string"),
    ("PRE_LEFT_FEMORAL_US_SGUIDED", "string"),
    ("PRE_RIGHT_FEMORAL_US_SGUIDED", "string"),
    ("PRE_SCREEN_OF_RIGHT_FEMORAL_HEAD", "string"),
    ("PRE_CLINICAL_FRAILTY_SCALE", "string"),
    ("PRE_CLINICAL_FRAILTY_SCALE_IMAGE", "string"),
    ("POST_DATE_LAST_CHANGED", "timestamp"),
    ("POST_ADC_UPDT", "timestamp"),
    ("POST_DATE_OF_DISCHARGE_RAW", "string"),
    ("POST_DATE_OF_DISCHARGE_TS", "timestamp"),
    ("POST_GENDER", "string"),
    ("POST_DATE_OF_DEATH", "date"),
    ("POST_ETHNIC_ORIGIN", "string"),
    ("POST_VESSELS_ATTEMPTEDTXT", "string"),
    ("POST_NUMBER_OF_VESSELS_ATTEMPTEDNOT_EPICARDIAL_TERRITORIES", "int"),
    ("POST_NUMBER_OF_LESIONS_ATTEMPTED", "int"),
    ("POST_NUMBER_OF_CHRONIC_OCCLUSIONS_ATTEMPTED", "int"),
    ("POST_NUMBER_RESTENOSES_ATTEMPTED", "int"),
    ("POST_NUMBER_INSTENT_STENOSES_ATTEMPTED", "int"),
    ("POST_NUMBER_STENTS_USED", "int"),
    ("POST_NUMBER_OF_DRUG_ELUTING_STENTS_USED", "int"),
    ("POST_DRUGS_ELUTED_BY_STENTSTXT", "string"),
    ("POST_LEFT_MAIN_STEM_STENOSISPOST_PCI", "string"),
    ("POST_LAD_PROXIMAL_STENOSISPOST_PCI", "string"),
    ("POST_LAD_OTHER_STENOSISPOST_PCI", "string"),
    ("POST_RCA_STENOSISPOST_PCI", "string"),
    ("POST_CX_STENOSISPOST_PCI", "string"),
    ("POST_NUMBER_CORONARY_GRAFTS_PATENT_POSTOP", "int"),
    ("POST_FLOW_IN_IRA_POSTOPACS", "string"),
    ("POST_M_OPERATION_REPORTCOMMENT", "string"),
    ("POST_DEVICE_FAILURE", "string"),
    ("POST_PCI_HOSPITAL_OUTCOMETXT", "string"),
    ("POST_ENZYMES_POSTOP", "string"),
    ("POST_STATUS_AT_DISCHARGE", "string"),
    ("POST_LARGEST_BALLON_STENT_DIAMETER", "double"),
    ("POST_LONGEST_STENTED_TREATED_SEGMENT", "int"),
    ("POST_PROCEDURAL_COMPLICATIONTXT", "string"),
    ("POST_ARTERIAL_COMPLICATIONSTXT", "string"),
    ("POST_TIME_TO_BYPASS", "int"),
    ("POST_LEFT_MAIN_STEM_PROTECTED", "string"),
    ("POST_PATIENT_STATUS_DURING_TRANSFER_TO_THEATRE", "string"),
    ("POST_INDICATION_FOR_STENT", "string"),
    ("POST_NUMBER_OF_LESIONS_SUCCESSFUL", "int"),
    ("POST_BLEEDING_UP_TO_DISCHARGE", "string"),
    ("POST_VENTILATION", "string"),
    ("POST_EXTERNAL_COOLING_PADS_BLANKETS_WRAPSTXT", "string"),
    ("POST_CONSULTANT_RESPONSIBLE_FOR_PROCEDURE_GMC_NUMBERTXT", "string"),
    ("POST_PRIMARY_OPERATOR_GMC_NUMBERTXT", "string"),
    ("POST_SECOND_OPERATOR_GMC_NUMBERTXT", "string"),
    ("POST_THIRD_OPERATOR_GMC_NUMBERTXT", "string"),
    ("POST_LEFT_MAIN_STEM_STENOSISPRE_PCI", "string"),
    ("POST_LAD_PROXIMAL_STENOSIS_PRE_PCI", "string"),
    ("POST_LAD_OTHER_STENOSIS_PRE_PCI", "string"),
    ("POST_RCA_STENOSIS_PRE_PCI", "string"),
    ("POST_CX_STENOSIS_PRE_PCI", "string"),
    ("POST_MANAGEMENT_PLAN", "string"),
    ("POST_OTHER_MANAGEMENT_PLAN", "string"),
    ("POST_CATH_PROCEDURETXT", "string"),
    ("POST_M_ANGIOGRAPHY_FINDINGS", "string"),
    ("POST_M_PROCEDURE_FINDINGS_AND_REPORT", "string"),
    ("POST_LEFT_MAIN_STEM_STENOSIS_PROCEDURE", "string"),
    ("POST_LAD_PROXIMAL_STENOSIS_PROCEDURE", "string"),
    ("POST_LAD_OTHER_STENOSIS_PROCEDURE", "string"),
    ("POST_CX_STENOSIS_PROCEDURE", "string"),
    ("POST_FLOW_IN_IRA_AT_ANGIO", "string"),
    ("POST_RCA_STENOSIS_PROCEDURE", "string"),
    ("POST_NUMBER_GRAFTS_PRESENTANGIO", "int"),
    ("POST_NUMBER_GRAFTS_PATENT_PRE_PCI", "int"),
    ("POST_PROCEDURAL_DEVICES_USED_DETAILTXT", "string"),
    ("POST_ATHERO_THROMBUS_REMOVAL_DEVICES_USEDTXT", "string"),
    ("POST_LEFT_MAIN_STEM_STENOSIS_COMMENT", "string"),
    ("POST_LAD_PROXIMAL_STENOSIS_COMMENT", "string"),
    ("POST_LAD_OTHER_STENOSIS_COMMENT", "string"),
    ("POST_RCA_STENOSIS_COMMENT", "string"),
    ("POST_CX_STENOSIS_COMMENT", "string"),
    ("POST_DIAGNOSTIC_DEVICES_USED_DURING_PROCEDURETXT", "string"),
    ("POST_PROCEDURAL_DEVICES_USEDTXT", "string"),
    ("POST_BRACHYTHERAPY_DEVICES_USEDTXT", "string"),
    ("POST_EMBOLI_PROTECTION_DEVICES_USEDTXT", "string"),
    ("POST_DATETIME_OF_FIRST_BALLOON_INFLATIONPCI", "timestamp"),
    ("POST_GP_IIBIIIA_DRUGS_USED_DURING_PROCEDURETXT", "string"),
    ("POST_WHY_NO_IIBIIIA_DURING_PROCEDURE", "string"),
    ("POST_DATASET_COMPLETE", "string"),
    ("POST_PCI_FOR_STENT_THROMBOSIS", "string"),
    ("POST_M_PRE_DISCHARGE_INSTRUCTIONS", "string"),
    ("POST_M_DISCHARGE_MANAGEMENT_PLAN", "string"),
    ("POST_M_INFORMATION_FOR_THE_PATIENT", "string"),
    ("POST_MG_PINFO", "string"),
    ("POST_NURSE_LED_DISCHARGE", "string"),
    ("POST_M_DETAILS_OF_ALL_DEVICES_IMPLANTED", "string"),
    ("POST_DIAGNOSTIC_INTERVENTIONS_UNDERTAKENTXT", "string"),
    ("POST_THERAPEUTIC_INTERVENTIONS_UNDERTAKENTXT", "string"),
    ("POST_CONTRAST_VOLUME", "int"),
    ("POST_INTERESTING_CASE", "string"),
    ("POST_M_CASE_DETAILS", "string"),
    ("POST_SCA_ISTAGE_PRE_PCI", "string"),
    ("POST_SCA_ISTAGE_POST_CCI", "string"),
    ("POST_ECHO_PHYSIOLOGY_PRE_PCI", "string"),
    ("POST_PEAK_LACTATE", "double"),
    ("POST_TROUGHP_H", "double"),
    ("POST_TROUGH_SYS_BP", "int"),
    ("POST_INOPRESSOR_USE_PRE_PCI", "string"),
    ("POST_PEAK_NORADRENALINE_DOSE_DURING_CATH_LAB", "double"),
    ("POST_IC_UADMISSION_POST_PCI", "string"),
    ("POST_CSA_IIMAGE", "string"),
    ("POST_L_VGRAM", "string"),
    ("SOURCE_JOIN_STATUS", "string"),
    ("NHS_NUMBER", "string"),
    ("MRN", "string"),
    ("IDENTIFIER_CONCORDANCE_IND", "boolean"),
    ("DATE_LAST_CHANGED", "timestamp"),
    ("ADC_UPDT", "timestamp"),
    ("NHS_NUMBER_VALID_IND", "boolean"),
    ("LINKAGE_STATUS", "string"),
    ("PERSON_ID", "bigint"),
    ("LINKAGE_METHOD", "string"),
    ("LINKAGE_HISTORICAL_FALLBACK_IND", "boolean"),
    ("LINKAGE_FALLBACK_CONFLICT_IND", "boolean"),
    ("LESION_CHILD_COUNT", "bigint"),
    ("PRE_DATE_OF_DEATH_FUTURE_IND", "boolean"),
    ("PRE_DATE_OF_DEATH_SENTINEL_IND", "boolean"),
    ("PRE_DATE_OF_DEATH_CLEAN", "timestamp"),
    ("PRE_DATEANDTIMEOFOPERATION_FUTURE_IND", "boolean"),
    ("PRE_DATEANDTIMEOFOPERATION_SENTINEL_IND", "boolean"),
    ("PRE_DATEANDTIMEOFOPERATION_CLEAN", "timestamp"),
    ("PRE_DATETIME_ARRIVAL_AT_FIRST_HOSPITALACS_ONLY_FUTURE_IND", "boolean"),
    ("PRE_DATETIME_ARRIVAL_AT_FIRST_HOSPITALACS_ONLY_SENTINEL_IND", "boolean"),
    ("PRE_DATETIME_ARRIVAL_AT_FIRST_HOSPITALACS_ONLY_CLEAN", "timestamp"),
    ("PRE_DATE_TIME_ARRIVAL_AT_PCI_HOSPITAL_FUTURE_IND", "boolean"),
    ("PRE_DATE_TIME_ARRIVAL_AT_PCI_HOSPITAL_SENTINEL_IND", "boolean"),
    ("PRE_DATE_TIME_ARRIVAL_AT_PCI_HOSPITAL_CLEAN", "timestamp"),
    ("PRE_DATE_TIME_OF_CALL_FOR_HELPSTEMI_FUTURE_IND", "boolean"),
    ("PRE_DATE_TIME_OF_CALL_FOR_HELPSTEMI_SENTINEL_IND", "boolean"),
    ("PRE_DATE_TIME_OF_CALL_FOR_HELPSTEMI_CLEAN", "timestamp"),
    ("PRE_DATETIME_OF_ECG_TRIGGERING_PPCI_PATHWAY_FUTURE_IND", "boolean"),
    ("PRE_DATETIME_OF_ECG_TRIGGERING_PPCI_PATHWAY_SENTINEL_IND", "boolean"),
    ("PRE_DATETIME_OF_ECG_TRIGGERING_PPCI_PATHWAY_CLEAN", "timestamp"),
    ("PRE_PATIENT_LOCATION_AT_TIME_OF_STEMI_ONSET_TS_FUTURE_IND", "boolean"),
    ("PRE_PATIENT_LOCATION_AT_TIME_OF_STEMI_ONSET_TS_SENTINEL_IND", "boolean"),
    ("PRE_PATIENT_LOCATION_AT_TIME_OF_STEMI_ONSET_TS_CLEAN", "timestamp"),
    ("PRE_DATETIME_OF_SYMPTOM_ONSETPCI_ACS_ONLY_FUTURE_IND", "boolean"),
    ("PRE_DATETIME_OF_SYMPTOM_ONSETPCI_ACS_ONLY_SENTINEL_IND", "boolean"),
    ("PRE_DATETIME_OF_SYMPTOM_ONSETPCI_ACS_ONLY_CLEAN", "timestamp"),
    ("PRE_PRESUMED_DATE_TIME_OF_ARREST_FUTURE_IND", "boolean"),
    ("PRE_PRESUMED_DATE_TIME_OF_ARREST_SENTINEL_IND", "boolean"),
    ("PRE_PRESUMED_DATE_TIME_OF_ARREST_CLEAN", "timestamp"),
    ("PRE_DATE_AND_TIME_ON_TABLE_FUTURE_IND", "boolean"),
    ("PRE_DATE_AND_TIME_ON_TABLE_SENTINEL_IND", "boolean"),
    ("PRE_DATE_AND_TIME_ON_TABLE_CLEAN", "timestamp"),
    ("PRE_DATE_TIME_SHORTCUTS_TS_FUTURE_IND", "boolean"),
    ("PRE_DATE_TIME_SHORTCUTS_TS_SENTINEL_IND", "boolean"),
    ("PRE_DATE_TIME_SHORTCUTS_TS_CLEAN", "timestamp"),
    ("POST_DATE_OF_DISCHARGE_TS_FUTURE_IND", "boolean"),
    ("POST_DATE_OF_DISCHARGE_TS_SENTINEL_IND", "boolean"),
    ("POST_DATE_OF_DISCHARGE_TS_CLEAN", "timestamp"),
    ("POST_DATE_OF_DEATH_FUTURE_IND", "boolean"),
    ("POST_DATE_OF_DEATH_SENTINEL_IND", "boolean"),
    ("POST_DATE_OF_DEATH_CLEAN", "timestamp"),
    ("POST_DATETIME_OF_FIRST_BALLOON_INFLATIONPCI_FUTURE_IND", "boolean"),
    ("POST_DATETIME_OF_FIRST_BALLOON_INFLATIONPCI_SENTINEL_IND", "boolean"),
    ("POST_DATETIME_OF_FIRST_BALLOON_INFLATIONPCI_CLEAN", "timestamp"),
    ("PIPELINE_UPDT_DT_TM", "timestamp"),
    ("ROW_HASH", "string"),
    ("SOURCE_PRESENT_IND", "boolean"),
]


@materialized_view(
    name=_n("journey_clinical.registry_coronary_procedure"),
    refresh_policy="auto",
)
def registry_coronary_procedure():
    # Build the declared dataset: Typed child of clinical_registry_entry for the
    # coronary_procedure family. One row per unioned Reg_CORONARY_Pre/Reg_CORONARY_PostProcedure
    # EntryId, with PRE_ and POST_ source fields kept separate. NHS number and MRN are
    # deliberately published and column-tagged ig_risk=4/ig_severity=2 under the 2026-08-13
    # identifier doctrine. Person linkage copies the live iWeb current-alias plus consensus-safe
    # historical fallback. DateOfDischarge raw text is retained beside parsed
    # future/sentinel/clean fields; known source maximum 2026-12-10 is a future typo, never
    # silently accepted. DATE_LAST_CHANGED is an edit timestamp, not the procedure date.
    # LESION_CHILD_COUNT counts current iweb_coronary_lesion children; parent procedures with
    # zero lesions are legitimate and retained. Every governed bronze column is retained under
    # its lower-case name.
    s = read_source(IWEB_REGISTRY_SLOTS["coronary_procedure"])
    actual_schema = [(field.name, field.dataType.simpleString()) for field in s.schema.fields]
    if actual_schema != REGISTRY_CORONARY_PROCEDURE_EXPECTED_SCHEMA:
        raise RuntimeError("coronary_procedure: bronze schema drift" + f"; expected={REGISTRY_CORONARY_PROCEDURE_EXPECTED_SCHEMA!r} actual={actual_schema!r}")
    return s.select(
        stable_id("registry_entry:iweb", F.lit("coronary_procedure"), F.lit("~"), s["ENTRY_ID"]).alias("patient_event_key"),
        s["ENTRY_ID"].alias("entry_id"),
        s["PRE_DATE_LAST_CHANGED"].alias("pre_date_last_changed"),
        s["PRE_ADC_UPDT"].alias("pre_adc_updt"),
        s["PRE_GENDER"].alias("pre_gender"),
        s["PRE_DATE_OF_DEATH"].alias("pre_date_of_death"),
        s["PRE_ETHNIC_ORIGIN"].alias("pre_ethnic_origin"),
        s["PRE_DATEANDTIMEOFOPERATION"].alias("pre_dateandtimeofoperation"),
        s["PRE_AGE"].alias("pre_age"),
        s["PRE_SEX"].alias("pre_sex"),
        s["PRE_PATIENT_ETHNIC_GROUP"].alias("pre_patient_ethnic_group"),
        s["PRE_ADMINISTRATIVE_CATEGORY"].alias("pre_administrative_category"),
        s["PRE_CLINICAL_SYNDROMEPCI"].alias("pre_clinical_syndromepci"),
        s["PRE_INDICATION_FOR_INTERVENTION"].alias("pre_indication_for_intervention"),
        s["PRE_PROCEDURE_URGENCY"].alias("pre_procedure_urgency"),
        s["PRE_CARDIOGENIC_SHOCKPRE_PROCEDURE"].alias("pre_cardiogenic_shockpre_procedure"),
        s["PRE_CCS_ANGINA_STATUSPRE_PROCEDURE_STABLE_ONLY"].alias("pre_ccs_angina_statuspre_procedure_stable_only"),
        s["PRE_NYHA_DYSPNOEA_STATUSPRE_PROCEDURE_STABLE_ONLY"].alias("pre_nyha_dyspnoea_statuspre_procedure_stable_only"),
        s["PRE_DATETIME_ARRIVAL_AT_FIRST_HOSPITALACS_ONLY"].alias("pre_datetime_arrival_at_first_hospitalacs_only"),
        s["PRE_ADMISSION_ROUTEACS_ONLY"].alias("pre_admission_routeacs_only"),
        s["PRE_PRESENTING_ECGACS_ONLY"].alias("pre_presenting_ecgacs_only"),
        s["PRE_RECENT_LYSISACS_ONLY"].alias("pre_recent_lysisacs_only"),
        s["PRE_CARDIAC_ENZYMESMARKERS_RAISED"].alias("pre_cardiac_enzymesmarkers_raised"),
        s["PRE_PREVIOUS_MI"].alias("pre_previous_mi"),
        s["PRE_PREVIOUS_CABG"].alias("pre_previous_cabg"),
        s["PRE_PREVIOUS_PCI"].alias("pre_previous_pci"),
        s["PRE_DIABETES"].alias("pre_diabetes"),
        s["PRE_HEIGHT"].alias("pre_height"),
        s["PRE_WEIGHT"].alias("pre_weight"),
        s["PRE_BODY_SURFACE_AREA"].alias("pre_body_surface_area"),
        s["PRE_BODY_MASS_INDEX"].alias("pre_body_mass_index"),
        s["PRE_LV_EJECTION_FRACTION_CATEGORY"].alias("pre_lv_ejection_fraction_category"),
        s["PRE_LV_EJECTION_FRACTION"].alias("pre_lv_ejection_fraction"),
        s["PRE_CONSULTANT_RESPONSIBLE_FOR_PROCEDURETXT"].alias("pre_consultant_responsible_for_proceduretxt"),
        s["PRE_PRIMARY_OPERATORTXT"].alias("pre_primary_operatortxt"),
        s["PRE_PRIMARY_OPERATOR_STATUS"].alias("pre_primary_operator_status"),
        s["PRE_SECOND_OPERATORTXT"].alias("pre_second_operatortxt"),
        s["PRE_SECOND_OPERATOR_STATUS"].alias("pre_second_operator_status"),
        s["PRE_THIRD_OPERATORTXT"].alias("pre_third_operatortxt"),
        s["PRE_THIRD_OPERATOR_STATUS"].alias("pre_third_operator_status"),
        s["PRE_GP_IIBIIIA_DRUGS_USED_DURING_PROCEDURETXT"].alias("pre_gp_iibiiia_drugs_used_during_proceduretxt"),
        s["PRE_EMBOLI_PROTECTION_DEVICES_USEDTXT"].alias("pre_emboli_protection_devices_usedtxt"),
        s["PRE_CIRCULATORY_SUPPORTTXT"].alias("pre_circulatory_supporttxt"),
        s["PRE_LOCAL_PROCEDURE_IDENTIFIER"].alias("pre_local_procedure_identifier"),
        s["PRE_CHOLESTEROL"].alias("pre_cholesterol"),
        s["PRE_SMOKING_HISTORY"].alias("pre_smoking_history"),
        s["PRE_FAMILY_HISTORY_OF_CAD"].alias("pre_family_history_of_cad"),
        s["PRE_MEDICAL_HISTORYTXT"].alias("pre_medical_historytxt"),
        s["PRE_VENTILATED_PREOP"].alias("pre_ventilated_preop"),
        s["PRE_Q_WAVE_ON_ECG"].alias("pre_q_wave_on_ecg"),
        s["PRE_ECG_ISCHAEMIA"].alias("pre_ecg_ischaemia"),
        s["PRE_FOLLOW_ONAD_HOC_PROCEDURE"].alias("pre_follow_onad_hoc_procedure"),
        s["PRE_TRAINING_PROCEDURE"].alias("pre_training_procedure"),
        s["PRE_RESEARCH_PROCEDURE"].alias("pre_research_procedure"),
        s["PRE_RESEARCH_TITLE"].alias("pre_research_title"),
        s["PRE_ARTERIAL_ACCESSTXT"].alias("pre_arterial_accesstxt"),
        s["PRE_WHY_NO_IIBIIIA_DURING_PROCEDURE"].alias("pre_why_no_iibiiia_during_procedure"),
        s["PRE_SURGICAL_COVER"].alias("pre_surgical_cover"),
        s["PRE_DATE_TIME_ARRIVAL_AT_PCI_HOSPITAL"].alias("pre_date_time_arrival_at_pci_hospital"),
        s["PRE_DATE_TIME_OF_CALL_FOR_HELPSTEMI"].alias("pre_date_time_of_call_for_helpstemi"),
        s["PRE_REFERRING_HOSPITALTXT"].alias("pre_referring_hospitaltxt"),
        s["PRE_DATETIME_OF_ECG_TRIGGERING_PPCI_PATHWAY"].alias("pre_datetime_of_ecg_triggering_ppci_pathway"),
        s["PRE_PATIENT_LOCATION_AT_TIME_OF_STEMI_ONSET_RAW"].alias("pre_patient_location_at_time_of_stemi_onset_raw"),
        s["PRE_PATIENT_LOCATION_AT_TIME_OF_STEMI_ONSET_TS"].alias("pre_patient_location_at_time_of_stemi_onset_ts"),
        s["PRE_SERUM_CREATININE"].alias("pre_serum_creatinine"),
        s["PRE_DATETIME_OF_SYMPTOM_ONSETPCI_ACS_ONLY"].alias("pre_datetime_of_symptom_onsetpci_acs_only"),
        s["PRE_ECHO_REPORT"].alias("pre_echo_report"),
        s["PRE_ARTERIAL_BLOOD_GAS_ON_ARRIVAL_IN_CATH_LAB_PH"].alias("pre_arterial_blood_gas_on_arrival_in_cath_lab_ph"),
        s["PRE_ARTERIAL_BLOOD_GAS_ON_ARRIVAL_IN_CATH_LAB_LACTATE"].alias("pre_arterial_blood_gas_on_arrival_in_cath_lab_lactate"),
        s["PRE_ARTERIAL_BLOOD_GAS_ON_ARRIVAL_IN_CATH_LAB_BASE_EXCESS"].alias("pre_arterial_blood_gas_on_arrival_in_cath_lab_base_excess"),
        s["PRE_GLASGOW_COMA_SCALE_ON_ARRIVAL_IN_CATH_LAB"].alias("pre_glasgow_coma_scale_on_arrival_in_cath_lab"),
        s["PRE_OTHER_THERAPEUTIC_HYPOTHERMIA"].alias("pre_other_therapeutic_hypothermia"),
        s["PRE_THERAPEUTIC_HYPOTHERMIATXT"].alias("pre_therapeutic_hypothermiatxt"),
        s["PRE_CATH_PROCEDURETXT"].alias("pre_cath_proceduretxt"),
        s["PRE_OUT_OF_HOSPITAL_CARDIAC_ARREST"].alias("pre_out_of_hospital_cardiac_arrest"),
        s["PRE_PRESUMED_DATE_TIME_OF_ARREST"].alias("pre_presumed_date_time_of_arrest"),
        s["PRE_CORONARY_DOMINANCE"].alias("pre_coronary_dominance"),
        s["PRE_M_PROCEDURE_FINDINGS"].alias("pre_m_procedure_findings"),
        s["PRE_ARTERIAL_MANAGEMENTTXT"].alias("pre_arterial_managementtxt"),
        s["PRE_DRUG_THERAPY_PREPROCEDURETXT"].alias("pre_drug_therapy_preproceduretxt"),
        s["PRE_REFERRING_CARDIOLOGISTTXT"].alias("pre_referring_cardiologisttxt"),
        s["PRE_HISTORY_OF_RENAL_DISEASE1"].alias("pre_history_of_renal_disease1"),
        s["PRE_TEMPORARY_PACING_WIRE"].alias("pre_temporary_pacing_wire"),
        s["PRE_CATH_DEVICESTXT"].alias("pre_cath_devicestxt"),
        s["PRE_CATH_LAB_NUMBER"].alias("pre_cath_lab_number"),
        s["PRE_DATE_AND_TIME_ON_TABLE"].alias("pre_date_and_time_on_table"),
        s["PRE_VENTILATION"].alias("pre_ventilation"),
        s["PRE_HOSPITAL_IDENTIFIER"].alias("pre_hospital_identifier"),
        s["PRE_DATE_TIME_SHORTCUTS_RAW"].alias("pre_date_time_shortcuts_raw"),
        s["PRE_DATE_TIME_SHORTCUTS_TS"].alias("pre_date_time_shortcuts_ts"),
        s["PRE_M_ADMISSION_HISTORY"].alias("pre_m_admission_history"),
        s["PRE_DIABETES_TYPE"].alias("pre_diabetes_type"),
        s["PRE_INTERVENTIONS"].alias("pre_interventions"),
        s["PRE_CONVALESCENT_STEM_IOR_NSTEMI"].alias("pre_convalescent_stem_ior_nstemi"),
        s["PRE_US_SGUIDED"].alias("pre_us_sguided"),
        s["PRE_SCREEN_OF_LEFT_FEMORAL_HEAD"].alias("pre_screen_of_left_femoral_head"),
        s["PRE_LEFT_FEMORAL_MICROPUNCTURE"].alias("pre_left_femoral_micropuncture"),
        s["PRE_LEFT_HEIGHT_OF_PUNCTURE"].alias("pre_left_height_of_puncture"),
        s["PRE_HEIGHT_OF_PUNCTURE_DIAGRAM"].alias("pre_height_of_puncture_diagram"),
        s["PRE_RIGHT_FEMORAL_MICROPUNCTURE"].alias("pre_right_femoral_micropuncture"),
        s["PRE_RIGHT_HEIGHT_OF_PUNCTURE"].alias("pre_right_height_of_puncture"),
        s["PRE_LEFT_FEMORAL_US_SGUIDED"].alias("pre_left_femoral_us_sguided"),
        s["PRE_RIGHT_FEMORAL_US_SGUIDED"].alias("pre_right_femoral_us_sguided"),
        s["PRE_SCREEN_OF_RIGHT_FEMORAL_HEAD"].alias("pre_screen_of_right_femoral_head"),
        s["PRE_CLINICAL_FRAILTY_SCALE"].alias("pre_clinical_frailty_scale"),
        s["PRE_CLINICAL_FRAILTY_SCALE_IMAGE"].alias("pre_clinical_frailty_scale_image"),
        s["POST_DATE_LAST_CHANGED"].alias("post_date_last_changed"),
        s["POST_ADC_UPDT"].alias("post_adc_updt"),
        s["POST_DATE_OF_DISCHARGE_RAW"].alias("post_date_of_discharge_raw"),
        s["POST_DATE_OF_DISCHARGE_TS"].alias("post_date_of_discharge_ts"),
        s["POST_GENDER"].alias("post_gender"),
        s["POST_DATE_OF_DEATH"].alias("post_date_of_death"),
        s["POST_ETHNIC_ORIGIN"].alias("post_ethnic_origin"),
        s["POST_VESSELS_ATTEMPTEDTXT"].alias("post_vessels_attemptedtxt"),
        s["POST_NUMBER_OF_VESSELS_ATTEMPTEDNOT_EPICARDIAL_TERRITORIES"].alias("post_number_of_vessels_attemptednot_epicardial_territories"),
        s["POST_NUMBER_OF_LESIONS_ATTEMPTED"].alias("post_number_of_lesions_attempted"),
        s["POST_NUMBER_OF_CHRONIC_OCCLUSIONS_ATTEMPTED"].alias("post_number_of_chronic_occlusions_attempted"),
        s["POST_NUMBER_RESTENOSES_ATTEMPTED"].alias("post_number_restenoses_attempted"),
        s["POST_NUMBER_INSTENT_STENOSES_ATTEMPTED"].alias("post_number_instent_stenoses_attempted"),
        s["POST_NUMBER_STENTS_USED"].alias("post_number_stents_used"),
        s["POST_NUMBER_OF_DRUG_ELUTING_STENTS_USED"].alias("post_number_of_drug_eluting_stents_used"),
        s["POST_DRUGS_ELUTED_BY_STENTSTXT"].alias("post_drugs_eluted_by_stentstxt"),
        s["POST_LEFT_MAIN_STEM_STENOSISPOST_PCI"].alias("post_left_main_stem_stenosispost_pci"),
        s["POST_LAD_PROXIMAL_STENOSISPOST_PCI"].alias("post_lad_proximal_stenosispost_pci"),
        s["POST_LAD_OTHER_STENOSISPOST_PCI"].alias("post_lad_other_stenosispost_pci"),
        s["POST_RCA_STENOSISPOST_PCI"].alias("post_rca_stenosispost_pci"),
        s["POST_CX_STENOSISPOST_PCI"].alias("post_cx_stenosispost_pci"),
        s["POST_NUMBER_CORONARY_GRAFTS_PATENT_POSTOP"].alias("post_number_coronary_grafts_patent_postop"),
        s["POST_FLOW_IN_IRA_POSTOPACS"].alias("post_flow_in_ira_postopacs"),
        s["POST_M_OPERATION_REPORTCOMMENT"].alias("post_m_operation_reportcomment"),
        s["POST_DEVICE_FAILURE"].alias("post_device_failure"),
        s["POST_PCI_HOSPITAL_OUTCOMETXT"].alias("post_pci_hospital_outcometxt"),
        s["POST_ENZYMES_POSTOP"].alias("post_enzymes_postop"),
        s["POST_STATUS_AT_DISCHARGE"].alias("post_status_at_discharge"),
        s["POST_LARGEST_BALLON_STENT_DIAMETER"].alias("post_largest_ballon_stent_diameter"),
        s["POST_LONGEST_STENTED_TREATED_SEGMENT"].alias("post_longest_stented_treated_segment"),
        s["POST_PROCEDURAL_COMPLICATIONTXT"].alias("post_procedural_complicationtxt"),
        s["POST_ARTERIAL_COMPLICATIONSTXT"].alias("post_arterial_complicationstxt"),
        s["POST_TIME_TO_BYPASS"].alias("post_time_to_bypass"),
        s["POST_LEFT_MAIN_STEM_PROTECTED"].alias("post_left_main_stem_protected"),
        s["POST_PATIENT_STATUS_DURING_TRANSFER_TO_THEATRE"].alias("post_patient_status_during_transfer_to_theatre"),
        s["POST_INDICATION_FOR_STENT"].alias("post_indication_for_stent"),
        s["POST_NUMBER_OF_LESIONS_SUCCESSFUL"].alias("post_number_of_lesions_successful"),
        s["POST_BLEEDING_UP_TO_DISCHARGE"].alias("post_bleeding_up_to_discharge"),
        s["POST_VENTILATION"].alias("post_ventilation"),
        s["POST_EXTERNAL_COOLING_PADS_BLANKETS_WRAPSTXT"].alias("post_external_cooling_pads_blankets_wrapstxt"),
        s["POST_CONSULTANT_RESPONSIBLE_FOR_PROCEDURE_GMC_NUMBERTXT"].alias("post_consultant_responsible_for_procedure_gmc_numbertxt"),
        s["POST_PRIMARY_OPERATOR_GMC_NUMBERTXT"].alias("post_primary_operator_gmc_numbertxt"),
        s["POST_SECOND_OPERATOR_GMC_NUMBERTXT"].alias("post_second_operator_gmc_numbertxt"),
        s["POST_THIRD_OPERATOR_GMC_NUMBERTXT"].alias("post_third_operator_gmc_numbertxt"),
        s["POST_LEFT_MAIN_STEM_STENOSISPRE_PCI"].alias("post_left_main_stem_stenosispre_pci"),
        s["POST_LAD_PROXIMAL_STENOSIS_PRE_PCI"].alias("post_lad_proximal_stenosis_pre_pci"),
        s["POST_LAD_OTHER_STENOSIS_PRE_PCI"].alias("post_lad_other_stenosis_pre_pci"),
        s["POST_RCA_STENOSIS_PRE_PCI"].alias("post_rca_stenosis_pre_pci"),
        s["POST_CX_STENOSIS_PRE_PCI"].alias("post_cx_stenosis_pre_pci"),
        s["POST_MANAGEMENT_PLAN"].alias("post_management_plan"),
        s["POST_OTHER_MANAGEMENT_PLAN"].alias("post_other_management_plan"),
        s["POST_CATH_PROCEDURETXT"].alias("post_cath_proceduretxt"),
        s["POST_M_ANGIOGRAPHY_FINDINGS"].alias("post_m_angiography_findings"),
        s["POST_M_PROCEDURE_FINDINGS_AND_REPORT"].alias("post_m_procedure_findings_and_report"),
        s["POST_LEFT_MAIN_STEM_STENOSIS_PROCEDURE"].alias("post_left_main_stem_stenosis_procedure"),
        s["POST_LAD_PROXIMAL_STENOSIS_PROCEDURE"].alias("post_lad_proximal_stenosis_procedure"),
        s["POST_LAD_OTHER_STENOSIS_PROCEDURE"].alias("post_lad_other_stenosis_procedure"),
        s["POST_CX_STENOSIS_PROCEDURE"].alias("post_cx_stenosis_procedure"),
        s["POST_FLOW_IN_IRA_AT_ANGIO"].alias("post_flow_in_ira_at_angio"),
        s["POST_RCA_STENOSIS_PROCEDURE"].alias("post_rca_stenosis_procedure"),
        s["POST_NUMBER_GRAFTS_PRESENTANGIO"].alias("post_number_grafts_presentangio"),
        s["POST_NUMBER_GRAFTS_PATENT_PRE_PCI"].alias("post_number_grafts_patent_pre_pci"),
        s["POST_PROCEDURAL_DEVICES_USED_DETAILTXT"].alias("post_procedural_devices_used_detailtxt"),
        s["POST_ATHERO_THROMBUS_REMOVAL_DEVICES_USEDTXT"].alias("post_athero_thrombus_removal_devices_usedtxt"),
        s["POST_LEFT_MAIN_STEM_STENOSIS_COMMENT"].alias("post_left_main_stem_stenosis_comment"),
        s["POST_LAD_PROXIMAL_STENOSIS_COMMENT"].alias("post_lad_proximal_stenosis_comment"),
        s["POST_LAD_OTHER_STENOSIS_COMMENT"].alias("post_lad_other_stenosis_comment"),
        s["POST_RCA_STENOSIS_COMMENT"].alias("post_rca_stenosis_comment"),
        s["POST_CX_STENOSIS_COMMENT"].alias("post_cx_stenosis_comment"),
        s["POST_DIAGNOSTIC_DEVICES_USED_DURING_PROCEDURETXT"].alias("post_diagnostic_devices_used_during_proceduretxt"),
        s["POST_PROCEDURAL_DEVICES_USEDTXT"].alias("post_procedural_devices_usedtxt"),
        s["POST_BRACHYTHERAPY_DEVICES_USEDTXT"].alias("post_brachytherapy_devices_usedtxt"),
        s["POST_EMBOLI_PROTECTION_DEVICES_USEDTXT"].alias("post_emboli_protection_devices_usedtxt"),
        s["POST_DATETIME_OF_FIRST_BALLOON_INFLATIONPCI"].alias("post_datetime_of_first_balloon_inflationpci"),
        s["POST_GP_IIBIIIA_DRUGS_USED_DURING_PROCEDURETXT"].alias("post_gp_iibiiia_drugs_used_during_proceduretxt"),
        s["POST_WHY_NO_IIBIIIA_DURING_PROCEDURE"].alias("post_why_no_iibiiia_during_procedure"),
        s["POST_DATASET_COMPLETE"].alias("post_dataset_complete"),
        s["POST_PCI_FOR_STENT_THROMBOSIS"].alias("post_pci_for_stent_thrombosis"),
        s["POST_M_PRE_DISCHARGE_INSTRUCTIONS"].alias("post_m_pre_discharge_instructions"),
        s["POST_M_DISCHARGE_MANAGEMENT_PLAN"].alias("post_m_discharge_management_plan"),
        s["POST_M_INFORMATION_FOR_THE_PATIENT"].alias("post_m_information_for_the_patient"),
        s["POST_MG_PINFO"].alias("post_mg_pinfo"),
        s["POST_NURSE_LED_DISCHARGE"].alias("post_nurse_led_discharge"),
        s["POST_M_DETAILS_OF_ALL_DEVICES_IMPLANTED"].alias("post_m_details_of_all_devices_implanted"),
        s["POST_DIAGNOSTIC_INTERVENTIONS_UNDERTAKENTXT"].alias("post_diagnostic_interventions_undertakentxt"),
        s["POST_THERAPEUTIC_INTERVENTIONS_UNDERTAKENTXT"].alias("post_therapeutic_interventions_undertakentxt"),
        s["POST_CONTRAST_VOLUME"].alias("post_contrast_volume"),
        s["POST_INTERESTING_CASE"].alias("post_interesting_case"),
        s["POST_M_CASE_DETAILS"].alias("post_m_case_details"),
        s["POST_SCA_ISTAGE_PRE_PCI"].alias("post_sca_istage_pre_pci"),
        s["POST_SCA_ISTAGE_POST_CCI"].alias("post_sca_istage_post_cci"),
        s["POST_ECHO_PHYSIOLOGY_PRE_PCI"].alias("post_echo_physiology_pre_pci"),
        s["POST_PEAK_LACTATE"].alias("post_peak_lactate"),
        s["POST_TROUGHP_H"].alias("post_troughp_h"),
        s["POST_TROUGH_SYS_BP"].alias("post_trough_sys_bp"),
        s["POST_INOPRESSOR_USE_PRE_PCI"].alias("post_inopressor_use_pre_pci"),
        s["POST_PEAK_NORADRENALINE_DOSE_DURING_CATH_LAB"].alias("post_peak_noradrenaline_dose_during_cath_lab"),
        s["POST_IC_UADMISSION_POST_PCI"].alias("post_ic_uadmission_post_pci"),
        s["POST_CSA_IIMAGE"].alias("post_csa_iimage"),
        s["POST_L_VGRAM"].alias("post_l_vgram"),
        s["SOURCE_JOIN_STATUS"].alias("source_join_status"),
        s["NHS_NUMBER"].alias("nhs_number"),
        s["MRN"].alias("mrn"),
        s["IDENTIFIER_CONCORDANCE_IND"].alias("identifier_concordance_ind"),
        s["DATE_LAST_CHANGED"].alias("date_last_changed"),
        s["ADC_UPDT"].alias("adc_updt"),
        s["NHS_NUMBER_VALID_IND"].alias("nhs_number_valid_ind"),
        s["LINKAGE_STATUS"].alias("linkage_status"),
        s["PERSON_ID"].alias("person_id"),
        s["LINKAGE_METHOD"].alias("linkage_method"),
        s["LINKAGE_HISTORICAL_FALLBACK_IND"].alias("linkage_historical_fallback_ind"),
        s["LINKAGE_FALLBACK_CONFLICT_IND"].alias("linkage_fallback_conflict_ind"),
        s["LESION_CHILD_COUNT"].alias("lesion_child_count"),
        s["PRE_DATE_OF_DEATH_FUTURE_IND"].alias("pre_date_of_death_future_ind"),
        s["PRE_DATE_OF_DEATH_SENTINEL_IND"].alias("pre_date_of_death_sentinel_ind"),
        s["PRE_DATE_OF_DEATH_CLEAN"].alias("pre_date_of_death_clean"),
        s["PRE_DATEANDTIMEOFOPERATION_FUTURE_IND"].alias("pre_dateandtimeofoperation_future_ind"),
        s["PRE_DATEANDTIMEOFOPERATION_SENTINEL_IND"].alias("pre_dateandtimeofoperation_sentinel_ind"),
        s["PRE_DATEANDTIMEOFOPERATION_CLEAN"].alias("pre_dateandtimeofoperation_clean"),
        s["PRE_DATETIME_ARRIVAL_AT_FIRST_HOSPITALACS_ONLY_FUTURE_IND"].alias("pre_datetime_arrival_at_first_hospitalacs_only_future_ind"),
        s["PRE_DATETIME_ARRIVAL_AT_FIRST_HOSPITALACS_ONLY_SENTINEL_IND"].alias("pre_datetime_arrival_at_first_hospitalacs_only_sentinel_ind"),
        s["PRE_DATETIME_ARRIVAL_AT_FIRST_HOSPITALACS_ONLY_CLEAN"].alias("pre_datetime_arrival_at_first_hospitalacs_only_clean"),
        s["PRE_DATE_TIME_ARRIVAL_AT_PCI_HOSPITAL_FUTURE_IND"].alias("pre_date_time_arrival_at_pci_hospital_future_ind"),
        s["PRE_DATE_TIME_ARRIVAL_AT_PCI_HOSPITAL_SENTINEL_IND"].alias("pre_date_time_arrival_at_pci_hospital_sentinel_ind"),
        s["PRE_DATE_TIME_ARRIVAL_AT_PCI_HOSPITAL_CLEAN"].alias("pre_date_time_arrival_at_pci_hospital_clean"),
        s["PRE_DATE_TIME_OF_CALL_FOR_HELPSTEMI_FUTURE_IND"].alias("pre_date_time_of_call_for_helpstemi_future_ind"),
        s["PRE_DATE_TIME_OF_CALL_FOR_HELPSTEMI_SENTINEL_IND"].alias("pre_date_time_of_call_for_helpstemi_sentinel_ind"),
        s["PRE_DATE_TIME_OF_CALL_FOR_HELPSTEMI_CLEAN"].alias("pre_date_time_of_call_for_helpstemi_clean"),
        s["PRE_DATETIME_OF_ECG_TRIGGERING_PPCI_PATHWAY_FUTURE_IND"].alias("pre_datetime_of_ecg_triggering_ppci_pathway_future_ind"),
        s["PRE_DATETIME_OF_ECG_TRIGGERING_PPCI_PATHWAY_SENTINEL_IND"].alias("pre_datetime_of_ecg_triggering_ppci_pathway_sentinel_ind"),
        s["PRE_DATETIME_OF_ECG_TRIGGERING_PPCI_PATHWAY_CLEAN"].alias("pre_datetime_of_ecg_triggering_ppci_pathway_clean"),
        s["PRE_PATIENT_LOCATION_AT_TIME_OF_STEMI_ONSET_TS_FUTURE_IND"].alias("pre_patient_location_at_time_of_stemi_onset_ts_future_ind"),
        s["PRE_PATIENT_LOCATION_AT_TIME_OF_STEMI_ONSET_TS_SENTINEL_IND"].alias("pre_patient_location_at_time_of_stemi_onset_ts_sentinel_ind"),
        s["PRE_PATIENT_LOCATION_AT_TIME_OF_STEMI_ONSET_TS_CLEAN"].alias("pre_patient_location_at_time_of_stemi_onset_ts_clean"),
        s["PRE_DATETIME_OF_SYMPTOM_ONSETPCI_ACS_ONLY_FUTURE_IND"].alias("pre_datetime_of_symptom_onsetpci_acs_only_future_ind"),
        s["PRE_DATETIME_OF_SYMPTOM_ONSETPCI_ACS_ONLY_SENTINEL_IND"].alias("pre_datetime_of_symptom_onsetpci_acs_only_sentinel_ind"),
        s["PRE_DATETIME_OF_SYMPTOM_ONSETPCI_ACS_ONLY_CLEAN"].alias("pre_datetime_of_symptom_onsetpci_acs_only_clean"),
        s["PRE_PRESUMED_DATE_TIME_OF_ARREST_FUTURE_IND"].alias("pre_presumed_date_time_of_arrest_future_ind"),
        s["PRE_PRESUMED_DATE_TIME_OF_ARREST_SENTINEL_IND"].alias("pre_presumed_date_time_of_arrest_sentinel_ind"),
        s["PRE_PRESUMED_DATE_TIME_OF_ARREST_CLEAN"].alias("pre_presumed_date_time_of_arrest_clean"),
        s["PRE_DATE_AND_TIME_ON_TABLE_FUTURE_IND"].alias("pre_date_and_time_on_table_future_ind"),
        s["PRE_DATE_AND_TIME_ON_TABLE_SENTINEL_IND"].alias("pre_date_and_time_on_table_sentinel_ind"),
        s["PRE_DATE_AND_TIME_ON_TABLE_CLEAN"].alias("pre_date_and_time_on_table_clean"),
        s["PRE_DATE_TIME_SHORTCUTS_TS_FUTURE_IND"].alias("pre_date_time_shortcuts_ts_future_ind"),
        s["PRE_DATE_TIME_SHORTCUTS_TS_SENTINEL_IND"].alias("pre_date_time_shortcuts_ts_sentinel_ind"),
        s["PRE_DATE_TIME_SHORTCUTS_TS_CLEAN"].alias("pre_date_time_shortcuts_ts_clean"),
        s["POST_DATE_OF_DISCHARGE_TS_FUTURE_IND"].alias("post_date_of_discharge_ts_future_ind"),
        s["POST_DATE_OF_DISCHARGE_TS_SENTINEL_IND"].alias("post_date_of_discharge_ts_sentinel_ind"),
        s["POST_DATE_OF_DISCHARGE_TS_CLEAN"].alias("post_date_of_discharge_ts_clean"),
        s["POST_DATE_OF_DEATH_FUTURE_IND"].alias("post_date_of_death_future_ind"),
        s["POST_DATE_OF_DEATH_SENTINEL_IND"].alias("post_date_of_death_sentinel_ind"),
        s["POST_DATE_OF_DEATH_CLEAN"].alias("post_date_of_death_clean"),
        s["POST_DATETIME_OF_FIRST_BALLOON_INFLATIONPCI_FUTURE_IND"].alias("post_datetime_of_first_balloon_inflationpci_future_ind"),
        s["POST_DATETIME_OF_FIRST_BALLOON_INFLATIONPCI_SENTINEL_IND"].alias("post_datetime_of_first_balloon_inflationpci_sentinel_ind"),
        s["POST_DATETIME_OF_FIRST_BALLOON_INFLATIONPCI_CLEAN"].alias("post_datetime_of_first_balloon_inflationpci_clean"),
        s["PIPELINE_UPDT_DT_TM"].alias("pipeline_updt_dt_tm"),
        s["ROW_HASH"].alias("row_hash"),
        s["SOURCE_PRESENT_IND"].alias("source_present_ind"),
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_registry_coronary_procedure': {'columns': {'adc_updt': 'Greatest landing timestamp across the '
                                                                  'pre-procedure and post-procedure source '
                                                                  'records.',
                                                      'date_last_changed': 'Greatest Pre/Post source edit '
                                                                           'timestamp; not the clinical '
                                                                           'procedure date.',
                                                      'entry_id': 'Reg_CORONARY_Pre/PostProcedure EntryId; '
                                                                  'parent key for lesion PARENT_ENTRY_ID.',
                                                      'identifier_concordance_ind': 'True when the '
                                                                                    'pre-procedure and '
                                                                                    'post-procedure NHS '
                                                                                    'number and MRN agree; '
                                                                                    'null when either module '
                                                                                    'lacks a comparison '
                                                                                    'identifier.',
                                                      'lesion_child_count': 'Count of current '
                                                                            'iweb_coronary_lesion rows whose '
                                                                            'PARENT_ENTRY_ID equals this '
                                                                            'EntryId.',
                                                      'linkage_fallback_conflict_ind': 'True when historical '
                                                                                       'alias evidence '
                                                                                       'conflicted and '
                                                                                       'therefore could not '
                                                                                       'safely resolve a '
                                                                                       'person.',
                                                      'linkage_historical_fallback_ind': 'True when person '
                                                                                         'resolution '
                                                                                         'required the '
                                                                                         'consensus-safe '
                                                                                         'historical alias '
                                                                                         'fallback.',
                                                      'linkage_method': 'Identifier route used to resolve '
                                                                        'PERSON_ID, using MRN, NHS number, '
                                                                        'or both.',
                                                      'linkage_status': 'MATCHED_BOTH, MATCHED_MRN, '
                                                                        'MATCHED_NHS, CONFLICT, AMBIGUOUS, '
                                                                        'or UNMATCHED.',
                                                      'mrn': 'Source local medical record number published '
                                                             'under identifier doctrine; ig_risk=4 and '
                                                             'ig_severity=2.',
                                                      'nhs_number': 'Source NHS number published under '
                                                                    'identifier doctrine; ig_risk=4 and '
                                                                    'ig_severity=2.',
                                                      'nhs_number_valid_ind': 'True when the published NHS '
                                                                              'number passes the bronze '
                                                                              'NHS-number validity check.',
                                                      'patient_event_key': 'Deterministic SHA-256 key '
                                                                           'matching '
                                                                           'clinical_registry_entry.patient_event_key '
                                                                           'for the coronary_procedure '
                                                                           'source family and native '
                                                                           'entry_id.',
                                                      'person_id': 'Millennium PERSON_ID from the live iWeb '
                                                                   'consensus-safe MRN/NHS resolver.',
                                                      'pipeline_updt_dt_tm': 'Timestamp when the bronze '
                                                                             'coronary-procedure pipeline '
                                                                             'last processed this row.',
                                                      'post_adc_updt': 'Landing timestamp carried by the '
                                                                       'post-procedure iWeb coronary source '
                                                                       'record.',
                                                      'post_arterial_complicationstxt': 'Free-text arterial '
                                                                                        'complications '
                                                                                        'recorded in the '
                                                                                        'post-procedure iWeb '
                                                                                        'coronary record.',
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
                                                                                       'discharge recorded '
                                                                                       'in the '
                                                                                       'post-procedure iWeb '
                                                                                       'coronary record.',
                                                      'post_brachytherapy_devices_usedtxt': 'Free-text '
                                                                                            'brachytherapy '
                                                                                            'devices used '
                                                                                            'recorded in the '
                                                                                            'post-procedure '
                                                                                            'iWeb coronary '
                                                                                            'record.',
                                                      'post_cath_proceduretxt': 'Free-text cath procedure '
                                                                                'recorded in the '
                                                                                'post-procedure iWeb '
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
                                                      'post_contrast_volume': 'Contrast volume recorded in '
                                                                              'the post-procedure iWeb '
                                                                              'coronary record.',
                                                      'post_csa_iimage': 'SCAI image or image reference '
                                                                         'retained from the post-procedure '
                                                                         'iWeb module.',
                                                      'post_cx_stenosis_comment': 'Circumflex coronary '
                                                                                  'artery stenosis comment '
                                                                                  'recorded in the '
                                                                                  'post-procedure iWeb '
                                                                                  'coronary record.',
                                                      'post_cx_stenosis_pre_pci': 'Circumflex coronary '
                                                                                  'artery stenosis pre '
                                                                                  'percutaneous coronary '
                                                                                  'intervention (PCI) '
                                                                                  'recorded in the '
                                                                                  'post-procedure iWeb '
                                                                                  'coronary record.',
                                                      'post_cx_stenosis_procedure': 'Circumflex coronary '
                                                                                    'artery stenosis '
                                                                                    'procedure recorded in '
                                                                                    'the post-procedure iWeb '
                                                                                    'coronary record.',
                                                      'post_cx_stenosispost_pci': 'Circumflex coronary '
                                                                                  'artery stenosis post '
                                                                                  'percutaneous coronary '
                                                                                  'intervention (PCI) '
                                                                                  'recorded in the '
                                                                                  'post-procedure iWeb '
                                                                                  'coronary record.',
                                                      'post_dataset_complete': 'Dataset complete recorded in '
                                                                               'the post-procedure iWeb '
                                                                               'coronary record.',
                                                      'post_date_last_changed': 'Source edit timestamp from '
                                                                                'the post-procedure iWeb '
                                                                                'coronary record; it is not '
                                                                                'a clinical event time.',
                                                      'post_date_of_death': 'Date of death recorded in the '
                                                                            'post-procedure iWeb coronary '
                                                                            'record.',
                                                      'post_date_of_death_clean': 'Post-procedure date of '
                                                                                  'death after date-quality '
                                                                                  'cleaning; null when '
                                                                                  'missing, future-dated or '
                                                                                  'a configured sentinel.',
                                                      'post_date_of_death_future_ind': 'True when the '
                                                                                       'post-procedure date '
                                                                                       'of death is later '
                                                                                       'than the pipeline '
                                                                                       'evaluation time.',
                                                      'post_date_of_death_sentinel_ind': 'True when the '
                                                                                         'post-procedure '
                                                                                         'date of death '
                                                                                         'matches a '
                                                                                         'configured '
                                                                                         'sentinel date or '
                                                                                         'timestamp.',
                                                      'post_date_of_discharge_raw': 'Verbatim source '
                                                                                    'discharge value; '
                                                                                    'retained even when '
                                                                                    'future-dated.',
                                                      'post_date_of_discharge_ts': 'Parsed discharge '
                                                                                   'timestamp; use its DQ '
                                                                                   'triplet and CLEAN value.',
                                                      'post_date_of_discharge_ts_clean': 'Post-procedure '
                                                                                         'date of discharge '
                                                                                         'ts after '
                                                                                         'date-quality '
                                                                                         'cleaning; null '
                                                                                         'when missing, '
                                                                                         'future-dated or a '
                                                                                         'configured '
                                                                                         'sentinel.',
                                                      'post_date_of_discharge_ts_future_ind': 'True when the '
                                                                                              'post-procedure '
                                                                                              'date of '
                                                                                              'discharge ts '
                                                                                              'is later than '
                                                                                              'the pipeline '
                                                                                              'evaluation '
                                                                                              'time.',
                                                      'post_date_of_discharge_ts_sentinel_ind': 'True when '
                                                                                                'the '
                                                                                                'post-procedure '
                                                                                                'date of '
                                                                                                'discharge '
                                                                                                'ts matches '
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
                                                                                                     'in the '
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
                                                      'post_device_failure': 'Device failure recorded in the '
                                                                             'post-procedure iWeb coronary '
                                                                             'record.',
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
                                                                                                     'in the '
                                                                                                     'post-procedure '
                                                                                                     'iWeb '
                                                                                                     'coronary '
                                                                                                     'record.',
                                                      'post_drugs_eluted_by_stentstxt': 'Free-text drugs '
                                                                                        'eluted by stents '
                                                                                        'recorded in the '
                                                                                        'post-procedure iWeb '
                                                                                        'coronary record.',
                                                      'post_echo_physiology_pre_pci': 'Echocardiographic or '
                                                                                      'physiological '
                                                                                      'assessment recorded '
                                                                                      'before percutaneous '
                                                                                      'coronary '
                                                                                      'intervention.',
                                                      'post_emboli_protection_devices_usedtxt': 'Free-text '
                                                                                                'emboli '
                                                                                                'protection '
                                                                                                'devices '
                                                                                                'used '
                                                                                                'recorded in '
                                                                                                'the '
                                                                                                'post-procedure '
                                                                                                'iWeb '
                                                                                                'coronary '
                                                                                                'record.',
                                                      'post_enzymes_postop': 'Enzymes post-operative '
                                                                             'recorded in the post-procedure '
                                                                             'iWeb coronary record.',
                                                      'post_ethnic_origin': 'Ethnic origin recorded in the '
                                                                            'post-procedure iWeb coronary '
                                                                            'record.',
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
                                                      'post_flow_in_ira_at_angio': 'Flow in infarct-related '
                                                                                   'artery at angio recorded '
                                                                                   'in the post-procedure '
                                                                                   'iWeb coronary record.',
                                                      'post_flow_in_ira_postopacs': 'Flow in infarct-related '
                                                                                    'artery post '
                                                                                    'operativeacs recorded '
                                                                                    'in the post-procedure '
                                                                                    'iWeb coronary record.',
                                                      'post_gender': 'Gender recorded in the post-procedure '
                                                                     'iWeb coronary record.',
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
                                                                                     'admission status after '
                                                                                     'percutaneous coronary '
                                                                                     'intervention; the '
                                                                                     'source field retains '
                                                                                     'its ICU spelling.',
                                                      'post_indication_for_stent': 'Indication for stent '
                                                                                   'recorded in the '
                                                                                   'post-procedure iWeb '
                                                                                   'coronary record.',
                                                      'post_inopressor_use_pre_pci': 'Inopressor use pre '
                                                                                     'percutaneous coronary '
                                                                                     'intervention (PCI) '
                                                                                     'recorded in the '
                                                                                     'post-procedure iWeb '
                                                                                     'coronary record.',
                                                      'post_interesting_case': 'Interesting case recorded in '
                                                                               'the post-procedure iWeb '
                                                                               'coronary record.',
                                                      'post_l_vgram': 'Left ventriculogram finding or status '
                                                                      'retained from the post-procedure iWeb '
                                                                      'module.',
                                                      'post_lad_other_stenosis_comment': 'Left anterior '
                                                                                         'descending artery '
                                                                                         'other stenosis '
                                                                                         'comment recorded '
                                                                                         'in the '
                                                                                         'post-procedure '
                                                                                         'iWeb coronary '
                                                                                         'record.',
                                                      'post_lad_other_stenosis_pre_pci': 'Left anterior '
                                                                                         'descending artery '
                                                                                         'other stenosis pre '
                                                                                         'percutaneous '
                                                                                         'coronary '
                                                                                         'intervention (PCI) '
                                                                                         'recorded in the '
                                                                                         'post-procedure '
                                                                                         'iWeb coronary '
                                                                                         'record.',
                                                      'post_lad_other_stenosis_procedure': 'Left anterior '
                                                                                           'descending '
                                                                                           'artery other '
                                                                                           'stenosis '
                                                                                           'procedure '
                                                                                           'recorded in the '
                                                                                           'post-procedure '
                                                                                           'iWeb coronary '
                                                                                           'record.',
                                                      'post_lad_other_stenosispost_pci': 'Left anterior '
                                                                                         'descending artery '
                                                                                         'other stenosis '
                                                                                         'post percutaneous '
                                                                                         'coronary '
                                                                                         'intervention (PCI) '
                                                                                         'recorded in the '
                                                                                         'post-procedure '
                                                                                         'iWeb coronary '
                                                                                         'record.',
                                                      'post_lad_proximal_stenosis_comment': 'Left anterior '
                                                                                            'descending '
                                                                                            'artery proximal '
                                                                                            'stenosis '
                                                                                            'comment '
                                                                                            'recorded in the '
                                                                                            'post-procedure '
                                                                                            'iWeb coronary '
                                                                                            'record.',
                                                      'post_lad_proximal_stenosis_pre_pci': 'Left anterior '
                                                                                            'descending '
                                                                                            'artery proximal '
                                                                                            'stenosis pre '
                                                                                            'percutaneous '
                                                                                            'coronary '
                                                                                            'intervention '
                                                                                            '(PCI) recorded '
                                                                                            'in the '
                                                                                            'post-procedure '
                                                                                            'iWeb coronary '
                                                                                            'record.',
                                                      'post_lad_proximal_stenosis_procedure': 'Left anterior '
                                                                                              'descending '
                                                                                              'artery '
                                                                                              'proximal '
                                                                                              'stenosis '
                                                                                              'procedure '
                                                                                              'recorded in '
                                                                                              'the '
                                                                                              'post-procedure '
                                                                                              'iWeb coronary '
                                                                                              'record.',
                                                      'post_lad_proximal_stenosispost_pci': 'Left anterior '
                                                                                            'descending '
                                                                                            'artery proximal '
                                                                                            'stenosis post '
                                                                                            'percutaneous '
                                                                                            'coronary '
                                                                                            'intervention '
                                                                                            '(PCI) recorded '
                                                                                            'in the '
                                                                                            'post-procedure '
                                                                                            'iWeb coronary '
                                                                                            'record.',
                                                      'post_largest_ballon_stent_diameter': 'Largest balloon '
                                                                                            'stent diameter '
                                                                                            'recorded in the '
                                                                                            'post-procedure '
                                                                                            'iWeb coronary '
                                                                                            'record.',
                                                      'post_left_main_stem_protected': 'Left main stem '
                                                                                       'protected recorded '
                                                                                       'in the '
                                                                                       'post-procedure iWeb '
                                                                                       'coronary record.',
                                                      'post_left_main_stem_stenosis_comment': 'Left main '
                                                                                              'stem stenosis '
                                                                                              'comment '
                                                                                              'recorded in '
                                                                                              'the '
                                                                                              'post-procedure '
                                                                                              'iWeb coronary '
                                                                                              'record.',
                                                      'post_left_main_stem_stenosis_procedure': 'Left main '
                                                                                                'stem '
                                                                                                'stenosis '
                                                                                                'procedure '
                                                                                                'recorded in '
                                                                                                'the '
                                                                                                'post-procedure '
                                                                                                'iWeb '
                                                                                                'coronary '
                                                                                                'record.',
                                                      'post_left_main_stem_stenosispost_pci': 'Left main '
                                                                                              'stem stenosis '
                                                                                              'post '
                                                                                              'percutaneous '
                                                                                              'coronary '
                                                                                              'intervention '
                                                                                              '(PCI) '
                                                                                              'recorded in '
                                                                                              'the '
                                                                                              'post-procedure '
                                                                                              'iWeb coronary '
                                                                                              'record.',
                                                      'post_left_main_stem_stenosispre_pci': 'Left main stem '
                                                                                             'stenosis pre '
                                                                                             'percutaneous '
                                                                                             'coronary '
                                                                                             'intervention '
                                                                                             '(PCI) recorded '
                                                                                             'in the '
                                                                                             'post-procedure '
                                                                                             'iWeb coronary '
                                                                                             'record.',
                                                      'post_longest_stented_treated_segment': 'Longest '
                                                                                              'stented '
                                                                                              'treated '
                                                                                              'segment '
                                                                                              'recorded in '
                                                                                              'the '
                                                                                              'post-procedure '
                                                                                              'iWeb coronary '
                                                                                              'record.',
                                                      'post_m_angiography_findings': 'Free-text angiography '
                                                                                     'findings recorded in '
                                                                                     'the post-procedure '
                                                                                     'iWeb coronary record.',
                                                      'post_m_case_details': 'Free-text case details '
                                                                             'recorded in the post-procedure '
                                                                             'iWeb coronary record.',
                                                      'post_m_details_of_all_devices_implanted': 'Free-text '
                                                                                                 'details of '
                                                                                                 'all '
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
                                                                                          'management plan '
                                                                                          'recorded in the '
                                                                                          'post-procedure '
                                                                                          'iWeb coronary '
                                                                                          'record.',
                                                      'post_m_information_for_the_patient': 'Free-text '
                                                                                            'information for '
                                                                                            'the patient '
                                                                                            'recorded in the '
                                                                                            'post-procedure '
                                                                                            'iWeb coronary '
                                                                                            'record.',
                                                      'post_m_operation_reportcomment': 'Free-text operation '
                                                                                        'reportcomment '
                                                                                        'recorded in the '
                                                                                        'post-procedure iWeb '
                                                                                        'coronary record.',
                                                      'post_m_pre_discharge_instructions': 'Free-text pre '
                                                                                           'discharge '
                                                                                           'instructions '
                                                                                           'recorded in the '
                                                                                           'post-procedure '
                                                                                           'iWeb coronary '
                                                                                           'record.',
                                                      'post_m_procedure_findings_and_report': 'Free-text '
                                                                                              'procedure '
                                                                                              'findings and '
                                                                                              'report '
                                                                                              'recorded in '
                                                                                              'the '
                                                                                              'post-procedure '
                                                                                              'iWeb coronary '
                                                                                              'record.',
                                                      'post_management_plan': 'Management plan recorded in '
                                                                              'the post-procedure iWeb '
                                                                              'coronary record.',
                                                      'post_mg_pinfo': 'Mg pinfo recorded in the '
                                                                       'post-procedure iWeb coronary record.',
                                                      'post_number_coronary_grafts_patent_postop': 'Number '
                                                                                                   'coronary '
                                                                                                   'grafts '
                                                                                                   'patent '
                                                                                                   'post-operative '
                                                                                                   'recorded '
                                                                                                   'in the '
                                                                                                   'post-procedure '
                                                                                                   'iWeb '
                                                                                                   'coronary '
                                                                                                   'record.',
                                                      'post_number_grafts_patent_pre_pci': 'Number grafts '
                                                                                           'patent pre '
                                                                                           'percutaneous '
                                                                                           'coronary '
                                                                                           'intervention '
                                                                                           '(PCI) recorded '
                                                                                           'in the '
                                                                                           'post-procedure '
                                                                                           'iWeb coronary '
                                                                                           'record.',
                                                      'post_number_grafts_presentangio': 'Number grafts '
                                                                                         'present angio '
                                                                                         'recorded in the '
                                                                                         'post-procedure '
                                                                                         'iWeb coronary '
                                                                                         'record.',
                                                      'post_number_instent_stenoses_attempted': 'Number of '
                                                                                                'in-stent '
                                                                                                'stenoses '
                                                                                                'attempted '
                                                                                                'recorded in '
                                                                                                'the '
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
                                                                                                     'in the '
                                                                                                     'post-procedure '
                                                                                                     'iWeb '
                                                                                                     'coronary '
                                                                                                     'record.',
                                                      'post_number_of_drug_eluting_stents_used': 'Number of '
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
                                                      'post_number_of_lesions_attempted': 'Number of lesions '
                                                                                          'attempted '
                                                                                          'recorded in the '
                                                                                          'post-procedure '
                                                                                          'iWeb coronary '
                                                                                          'record.',
                                                      'post_number_of_lesions_successful': 'Number of '
                                                                                           'lesions '
                                                                                           'successful '
                                                                                           'recorded in the '
                                                                                           'post-procedure '
                                                                                           'iWeb coronary '
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
                                                                                          'recorded in the '
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
                                                      'post_other_management_plan': 'Other management plan '
                                                                                    'recorded in the '
                                                                                    'post-procedure iWeb '
                                                                                    'coronary record.',
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
                                                                                       'intervention (PCI) '
                                                                                       'for stent thrombosis '
                                                                                       'recorded in the '
                                                                                       'post-procedure iWeb '
                                                                                       'coronary record.',
                                                      'post_pci_hospital_outcometxt': 'Free-text '
                                                                                      'percutaneous coronary '
                                                                                      'intervention (PCI) '
                                                                                      'hospital outcome '
                                                                                      'recorded in the '
                                                                                      'post-procedure iWeb '
                                                                                      'coronary record.',
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
                                                                                             'Council number '
                                                                                             'recorded in '
                                                                                             'the '
                                                                                             'post-procedure '
                                                                                             'iWeb coronary '
                                                                                             'record.',
                                                      'post_procedural_complicationtxt': 'Free-text '
                                                                                         'procedural '
                                                                                         'complication '
                                                                                         'recorded in the '
                                                                                         'post-procedure '
                                                                                         'iWeb coronary '
                                                                                         'record.',
                                                      'post_procedural_devices_used_detailtxt': 'Free-text '
                                                                                                'procedural '
                                                                                                'devices '
                                                                                                'used detail '
                                                                                                'recorded in '
                                                                                                'the '
                                                                                                'post-procedure '
                                                                                                'iWeb '
                                                                                                'coronary '
                                                                                                'record.',
                                                      'post_procedural_devices_usedtxt': 'Free-text '
                                                                                         'procedural devices '
                                                                                         'used recorded in '
                                                                                         'the post-procedure '
                                                                                         'iWeb coronary '
                                                                                         'record.',
                                                      'post_rca_stenosis_comment': 'Right coronary artery '
                                                                                   'stenosis comment '
                                                                                   'recorded in the '
                                                                                   'post-procedure iWeb '
                                                                                   'coronary record.',
                                                      'post_rca_stenosis_pre_pci': 'Right coronary artery '
                                                                                   'stenosis pre '
                                                                                   'percutaneous coronary '
                                                                                   'intervention (PCI) '
                                                                                   'recorded in the '
                                                                                   'post-procedure iWeb '
                                                                                   'coronary record.',
                                                      'post_rca_stenosis_procedure': 'Right coronary artery '
                                                                                     'stenosis procedure '
                                                                                     'recorded in the '
                                                                                     'post-procedure iWeb '
                                                                                     'coronary record.',
                                                      'post_rca_stenosispost_pci': 'Right coronary artery '
                                                                                   'stenosis post '
                                                                                   'percutaneous coronary '
                                                                                   'intervention (PCI) '
                                                                                   'recorded in the '
                                                                                   'post-procedure iWeb '
                                                                                   'coronary record.',
                                                      'post_sca_istage_post_cci': 'SCAI cardiogenic-shock '
                                                                                  'stage recorded after '
                                                                                  'coronary intervention; '
                                                                                  'the source field retains '
                                                                                  'its CCI spelling.',
                                                      'post_sca_istage_pre_pci': 'SCAI cardiogenic-shock '
                                                                                 'stage recorded before '
                                                                                 'percutaneous coronary '
                                                                                 'intervention.',
                                                      'post_second_operator_gmc_numbertxt': 'Free-text '
                                                                                            'second operator '
                                                                                            'General Medical '
                                                                                            'Council number '
                                                                                            'recorded in the '
                                                                                            'post-procedure '
                                                                                            'iWeb coronary '
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
                                                      'post_third_operator_gmc_numbertxt': 'Free-text third '
                                                                                           'operator General '
                                                                                           'Medical Council '
                                                                                           'number recorded '
                                                                                           'in the '
                                                                                           'post-procedure '
                                                                                           'iWeb coronary '
                                                                                           'record.',
                                                      'post_time_to_bypass': 'Time to bypass recorded in the '
                                                                             'post-procedure iWeb coronary '
                                                                             'record.',
                                                      'post_trough_sys_bp': 'Trough sys bp recorded in the '
                                                                            'post-procedure iWeb coronary '
                                                                            'record.',
                                                      'post_troughp_h': 'Lowest recorded pH associated with '
                                                                        'the coronary procedure.',
                                                      'post_ventilation': 'Ventilation recorded in the '
                                                                          'post-procedure iWeb coronary '
                                                                          'record.',
                                                      'post_vessels_attemptedtxt': 'Free-text vessels '
                                                                                   'attempted recorded in '
                                                                                   'the post-procedure iWeb '
                                                                                   'coronary record.',
                                                      'post_why_no_iibiiia_during_procedure': 'Why no '
                                                                                              'IIb/IIIa '
                                                                                              'during '
                                                                                              'procedure '
                                                                                              'recorded in '
                                                                                              'the '
                                                                                              'post-procedure '
                                                                                              'iWeb coronary '
                                                                                              'record.',
                                                      'pre_adc_updt': 'Landing timestamp carried by the '
                                                                      'pre-procedure iWeb coronary source '
                                                                      'record.',
                                                      'pre_administrative_category': 'Administrative '
                                                                                     'category recorded in '
                                                                                     'the pre-procedure iWeb '
                                                                                     'coronary assessment.',
                                                      'pre_admission_routeacs_only': 'Admission routeacs '
                                                                                     'only recorded in the '
                                                                                     'pre-procedure iWeb '
                                                                                     'coronary assessment.',
                                                      'pre_age': 'Age recorded in the pre-procedure iWeb '
                                                                 'coronary assessment.',
                                                      'pre_arterial_accesstxt': 'Free-text arterial access '
                                                                                'recorded in the '
                                                                                'pre-procedure iWeb coronary '
                                                                                'assessment.',
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
                                                                                    'management recorded in '
                                                                                    'the pre-procedure iWeb '
                                                                                    'coronary assessment.',
                                                      'pre_body_mass_index': 'Body mass index recorded in '
                                                                             'the pre-procedure iWeb '
                                                                             'coronary assessment.',
                                                      'pre_body_surface_area': 'Body surface area recorded '
                                                                               'in the pre-procedure iWeb '
                                                                               'coronary assessment.',
                                                      'pre_cardiac_enzymesmarkers_raised': 'Cardiac enzymes '
                                                                                           'markers raised '
                                                                                           'recorded in the '
                                                                                           'pre-procedure '
                                                                                           'iWeb coronary '
                                                                                           'assessment.',
                                                      'pre_cardiogenic_shockpre_procedure': 'Cardiogenic '
                                                                                            'shockpre '
                                                                                            'procedure '
                                                                                            'recorded in the '
                                                                                            'pre-procedure '
                                                                                            'iWeb coronary '
                                                                                            'assessment.',
                                                      'pre_cath_devicestxt': 'Free-text cath devices '
                                                                             'recorded in the pre-procedure '
                                                                             'iWeb coronary assessment.',
                                                      'pre_cath_lab_number': 'Cath lab number recorded in '
                                                                             'the pre-procedure iWeb '
                                                                             'coronary assessment.',
                                                      'pre_cath_proceduretxt': 'Free-text cath procedure '
                                                                               'recorded in the '
                                                                               'pre-procedure iWeb coronary '
                                                                               'assessment.',
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
                                                      'pre_circulatory_supporttxt': 'Free-text circulatory '
                                                                                    'support recorded in the '
                                                                                    'pre-procedure iWeb '
                                                                                    'coronary assessment.',
                                                      'pre_clinical_frailty_scale': 'Clinical frailty scale '
                                                                                    'recorded in the '
                                                                                    'pre-procedure iWeb '
                                                                                    'coronary assessment.',
                                                      'pre_clinical_frailty_scale_image': 'Clinical frailty '
                                                                                          'scale image '
                                                                                          'recorded in the '
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
                                                                                                     'in the '
                                                                                                     'pre-procedure '
                                                                                                     'iWeb '
                                                                                                     'coronary '
                                                                                                     'assessment.',
                                                      'pre_convalescent_stem_ior_nstemi': 'Convalescent stem '
                                                                                          'or '
                                                                                          'non-ST-elevation '
                                                                                          'myocardial '
                                                                                          'infarction '
                                                                                          '(NSTEMI) recorded '
                                                                                          'in the '
                                                                                          'pre-procedure '
                                                                                          'iWeb coronary '
                                                                                          'assessment.',
                                                      'pre_coronary_dominance': 'Coronary dominance recorded '
                                                                                'in the pre-procedure iWeb '
                                                                                'coronary assessment.',
                                                      'pre_date_and_time_on_table': 'Date and time on table '
                                                                                    'recorded in the '
                                                                                    'pre-procedure iWeb '
                                                                                    'coronary assessment.',
                                                      'pre_date_and_time_on_table_clean': 'Pre-procedure '
                                                                                          'date and time on '
                                                                                          'table after '
                                                                                          'date-quality '
                                                                                          'cleaning; null '
                                                                                          'when missing, '
                                                                                          'future-dated or a '
                                                                                          'configured '
                                                                                          'sentinel.',
                                                      'pre_date_and_time_on_table_future_ind': 'True when '
                                                                                               'the '
                                                                                               'pre-procedure '
                                                                                               'date and '
                                                                                               'time on '
                                                                                               'table is '
                                                                                               'later than '
                                                                                               'the pipeline '
                                                                                               'evaluation '
                                                                                               'time.',
                                                      'pre_date_and_time_on_table_sentinel_ind': 'True when '
                                                                                                 'the '
                                                                                                 'pre-procedure '
                                                                                                 'date and '
                                                                                                 'time on '
                                                                                                 'table '
                                                                                                 'matches a '
                                                                                                 'configured '
                                                                                                 'sentinel '
                                                                                                 'date or '
                                                                                                 'timestamp.',
                                                      'pre_date_last_changed': 'Source edit timestamp from '
                                                                               'the pre-procedure iWeb '
                                                                               'coronary record; it is not a '
                                                                               'clinical event time.',
                                                      'pre_date_of_death': 'Date of death recorded in the '
                                                                           'pre-procedure iWeb coronary '
                                                                           'assessment.',
                                                      'pre_date_of_death_clean': 'Pre-procedure date of '
                                                                                 'death after date-quality '
                                                                                 'cleaning; null when '
                                                                                 'missing, future-dated or a '
                                                                                 'configured sentinel.',
                                                      'pre_date_of_death_future_ind': 'True when the '
                                                                                      'pre-procedure date of '
                                                                                      'death is later than '
                                                                                      'the pipeline '
                                                                                      'evaluation time.',
                                                      'pre_date_of_death_sentinel_ind': 'True when the '
                                                                                        'pre-procedure date '
                                                                                        'of death matches a '
                                                                                        'configured sentinel '
                                                                                        'date or timestamp.',
                                                      'pre_date_time_arrival_at_pci_hospital': 'Date and '
                                                                                               'time arrival '
                                                                                               'at '
                                                                                               'percutaneous '
                                                                                               'coronary '
                                                                                               'intervention '
                                                                                               '(PCI) '
                                                                                               'hospital '
                                                                                               'recorded in '
                                                                                               'the '
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
                                                                                                     'or a '
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
                                                      'pre_date_time_of_call_for_helpstemi': 'Date and time '
                                                                                             'of call for '
                                                                                             'help '
                                                                                             'ST-elevation '
                                                                                             'myocardial '
                                                                                             'infarction '
                                                                                             '(STEMI) '
                                                                                             'recorded in '
                                                                                             'the '
                                                                                             'pre-procedure '
                                                                                             'iWeb coronary '
                                                                                             'assessment.',
                                                      'pre_date_time_of_call_for_helpstemi_clean': 'Pre-procedure '
                                                                                                   'date and '
                                                                                                   'time of '
                                                                                                   'call for '
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
                                                      'pre_date_time_shortcuts_raw': 'Verbatim pre-procedure '
                                                                                     'date/time shortcut '
                                                                                     'value before timestamp '
                                                                                     'parsing.',
                                                      'pre_date_time_shortcuts_ts': 'Timestamp parsed from '
                                                                                    'the pre-procedure '
                                                                                    'date/time shortcut '
                                                                                    'source value.',
                                                      'pre_date_time_shortcuts_ts_clean': 'Pre-procedure '
                                                                                          'date and time '
                                                                                          'shortcuts ts '
                                                                                          'after '
                                                                                          'date-quality '
                                                                                          'cleaning; null '
                                                                                          'when missing, '
                                                                                          'future-dated or a '
                                                                                          'configured '
                                                                                          'sentinel.',
                                                      'pre_date_time_shortcuts_ts_future_ind': 'True when '
                                                                                               'the '
                                                                                               'pre-procedure '
                                                                                               'date and '
                                                                                               'time '
                                                                                               'shortcuts ts '
                                                                                               'is later '
                                                                                               'than the '
                                                                                               'pipeline '
                                                                                               'evaluation '
                                                                                               'time.',
                                                      'pre_date_time_shortcuts_ts_sentinel_ind': 'True when '
                                                                                                 'the '
                                                                                                 'pre-procedure '
                                                                                                 'date and '
                                                                                                 'time '
                                                                                                 'shortcuts '
                                                                                                 'ts matches '
                                                                                                 'a '
                                                                                                 'configured '
                                                                                                 'sentinel '
                                                                                                 'date or '
                                                                                                 'timestamp.',
                                                      'pre_dateandtimeofoperation': 'Date and '
                                                                                    'timeofoperation '
                                                                                    'recorded in the '
                                                                                    'pre-procedure iWeb '
                                                                                    'coronary assessment.',
                                                      'pre_dateandtimeofoperation_clean': 'Pre-procedure '
                                                                                          'date and '
                                                                                          'timeofoperation '
                                                                                          'after '
                                                                                          'date-quality '
                                                                                          'cleaning; null '
                                                                                          'when missing, '
                                                                                          'future-dated or a '
                                                                                          'configured '
                                                                                          'sentinel.',
                                                      'pre_dateandtimeofoperation_future_ind': 'True when '
                                                                                               'the '
                                                                                               'pre-procedure '
                                                                                               'date and '
                                                                                               'timeofoperation '
                                                                                               'is later '
                                                                                               'than the '
                                                                                               'pipeline '
                                                                                               'evaluation '
                                                                                               'time.',
                                                      'pre_dateandtimeofoperation_sentinel_ind': 'True when '
                                                                                                 'the '
                                                                                                 'pre-procedure '
                                                                                                 'date and '
                                                                                                 'timeofoperation '
                                                                                                 'matches a '
                                                                                                 'configured '
                                                                                                 'sentinel '
                                                                                                 'date or '
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
                                                                                                     'in the '
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
                                                      'pre_datetime_of_symptom_onsetpci_acs_only': 'Date and '
                                                                                                   'time of '
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
                                                                                                   'in the '
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
                                                      'pre_diabetes_type': 'Diabetes type recorded in the '
                                                                           'pre-procedure iWeb coronary '
                                                                           'assessment.',
                                                      'pre_drug_therapy_preproceduretxt': 'Free-text drug '
                                                                                          'therapy before '
                                                                                          'the procedure '
                                                                                          'recorded in the '
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
                                                                                               'devices used '
                                                                                               'recorded in '
                                                                                               'the '
                                                                                               'pre-procedure '
                                                                                               'iWeb '
                                                                                               'coronary '
                                                                                               'assessment.',
                                                      'pre_ethnic_origin': 'Ethnic origin recorded in the '
                                                                           'pre-procedure iWeb coronary '
                                                                           'assessment.',
                                                      'pre_family_history_of_cad': 'Family history of '
                                                                                   'coronary artery disease '
                                                                                   '(CAD) recorded in the '
                                                                                   'pre-procedure iWeb '
                                                                                   'coronary assessment.',
                                                      'pre_follow_onad_hoc_procedure': 'Follow onad hoc '
                                                                                       'procedure recorded '
                                                                                       'in the pre-procedure '
                                                                                       'iWeb coronary '
                                                                                       'assessment.',
                                                      'pre_gender': 'Gender recorded in the pre-procedure '
                                                                    'iWeb coronary assessment.',
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
                                                      'pre_height': 'Height recorded in the pre-procedure '
                                                                    'iWeb coronary assessment.',
                                                      'pre_height_of_puncture_diagram': 'Height of puncture '
                                                                                        'diagram recorded in '
                                                                                        'the pre-procedure '
                                                                                        'iWeb coronary '
                                                                                        'assessment.',
                                                      'pre_history_of_renal_disease1': 'History of renal '
                                                                                       'disease1 recorded in '
                                                                                       'the pre-procedure '
                                                                                       'iWeb coronary '
                                                                                       'assessment.',
                                                      'pre_hospital_identifier': 'Hospital identifier '
                                                                                 'recorded in the '
                                                                                 'pre-procedure iWeb '
                                                                                 'coronary assessment.',
                                                      'pre_indication_for_intervention': 'Indication for '
                                                                                         'intervention '
                                                                                         'recorded in the '
                                                                                         'pre-procedure iWeb '
                                                                                         'coronary '
                                                                                         'assessment.',
                                                      'pre_interventions': 'Interventions recorded in the '
                                                                           'pre-procedure iWeb coronary '
                                                                           'assessment.',
                                                      'pre_left_femoral_micropuncture': 'Left femoral '
                                                                                        'micropuncture '
                                                                                        'recorded in the '
                                                                                        'pre-procedure iWeb '
                                                                                        'coronary '
                                                                                        'assessment.',
                                                      'pre_left_femoral_us_sguided': 'Indicates whether left '
                                                                                     'femoral access was '
                                                                                     'ultrasound-guided.',
                                                      'pre_left_height_of_puncture': 'Left height of '
                                                                                     'puncture recorded in '
                                                                                     'the pre-procedure iWeb '
                                                                                     'coronary assessment.',
                                                      'pre_local_procedure_identifier': 'Local procedure '
                                                                                        'identifier recorded '
                                                                                        'in the '
                                                                                        'pre-procedure iWeb '
                                                                                        'coronary '
                                                                                        'assessment.',
                                                      'pre_lv_ejection_fraction': 'Left-ventricular ejection '
                                                                                  'fraction recorded in the '
                                                                                  'pre-procedure iWeb '
                                                                                  'coronary assessment.',
                                                      'pre_lv_ejection_fraction_category': 'Left-ventricular '
                                                                                           'ejection '
                                                                                           'fraction '
                                                                                           'category '
                                                                                           'recorded in the '
                                                                                           'pre-procedure '
                                                                                           'iWeb coronary '
                                                                                           'assessment.',
                                                      'pre_m_admission_history': 'Free-text admission '
                                                                                 'history recorded in the '
                                                                                 'pre-procedure iWeb '
                                                                                 'coronary assessment.',
                                                      'pre_m_procedure_findings': 'Free-text procedure '
                                                                                  'findings recorded in the '
                                                                                  'pre-procedure iWeb '
                                                                                  'coronary assessment.',
                                                      'pre_medical_historytxt': 'Free-text medical history '
                                                                                'recorded in the '
                                                                                'pre-procedure iWeb coronary '
                                                                                'assessment.',
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
                                                                                           'recorded in the '
                                                                                           'pre-procedure '
                                                                                           'iWeb coronary '
                                                                                           'assessment.',
                                                      'pre_out_of_hospital_cardiac_arrest': 'Out of hospital '
                                                                                            'cardiac arrest '
                                                                                            'recorded in the '
                                                                                            'pre-procedure '
                                                                                            'iWeb coronary '
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
                                                      'pre_presenting_ecgacs_only': 'Presenting ecgacs only '
                                                                                    'recorded in the '
                                                                                    'pre-procedure iWeb '
                                                                                    'coronary assessment.',
                                                      'pre_presumed_date_time_of_arrest': 'Presumed date and '
                                                                                          'time of arrest '
                                                                                          'recorded in the '
                                                                                          'pre-procedure '
                                                                                          'iWeb coronary '
                                                                                          'assessment.',
                                                      'pre_presumed_date_time_of_arrest_clean': 'Pre-procedure '
                                                                                                'presumed '
                                                                                                'date and '
                                                                                                'time of '
                                                                                                'arrest '
                                                                                                'after '
                                                                                                'date-quality '
                                                                                                'cleaning; '
                                                                                                'null when '
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
                                                      'pre_previous_cabg': 'Previous coronary artery bypass '
                                                                           'grafting (CABG) recorded in the '
                                                                           'pre-procedure iWeb coronary '
                                                                           'assessment.',
                                                      'pre_previous_mi': 'Previous mi recorded in the '
                                                                         'pre-procedure iWeb coronary '
                                                                         'assessment.',
                                                      'pre_previous_pci': 'Previous percutaneous coronary '
                                                                          'intervention (PCI) recorded in '
                                                                          'the pre-procedure iWeb coronary '
                                                                          'assessment.',
                                                      'pre_primary_operator_status': 'Primary operator '
                                                                                     'status recorded in the '
                                                                                     'pre-procedure iWeb '
                                                                                     'coronary assessment.',
                                                      'pre_primary_operatortxt': 'Free-text primary operator '
                                                                                 'recorded in the '
                                                                                 'pre-procedure iWeb '
                                                                                 'coronary assessment.',
                                                      'pre_procedure_urgency': 'Procedure urgency recorded '
                                                                               'in the pre-procedure iWeb '
                                                                               'coronary assessment.',
                                                      'pre_q_wave_on_ecg': 'Q wave on electrocardiogram '
                                                                           '(ECG) recorded in the '
                                                                           'pre-procedure iWeb coronary '
                                                                           'assessment.',
                                                      'pre_recent_lysisacs_only': 'Recent lysisacs only '
                                                                                  'recorded in the '
                                                                                  'pre-procedure iWeb '
                                                                                  'coronary assessment.',
                                                      'pre_referring_cardiologisttxt': 'Free-text referring '
                                                                                       'cardiologist '
                                                                                       'recorded in the '
                                                                                       'pre-procedure iWeb '
                                                                                       'coronary assessment.',
                                                      'pre_referring_hospitaltxt': 'Free-text referring '
                                                                                   'hospital recorded in the '
                                                                                   'pre-procedure iWeb '
                                                                                   'coronary assessment.',
                                                      'pre_research_procedure': 'Research procedure recorded '
                                                                                'in the pre-procedure iWeb '
                                                                                'coronary assessment.',
                                                      'pre_research_title': 'Research title recorded in the '
                                                                            'pre-procedure iWeb coronary '
                                                                            'assessment.',
                                                      'pre_right_femoral_micropuncture': 'Right femoral '
                                                                                         'micropuncture '
                                                                                         'recorded in the '
                                                                                         'pre-procedure iWeb '
                                                                                         'coronary '
                                                                                         'assessment.',
                                                      'pre_right_femoral_us_sguided': 'Indicates whether '
                                                                                      'right femoral access '
                                                                                      'was '
                                                                                      'ultrasound-guided.',
                                                      'pre_right_height_of_puncture': 'Right height of '
                                                                                      'puncture recorded in '
                                                                                      'the pre-procedure '
                                                                                      'iWeb coronary '
                                                                                      'assessment.',
                                                      'pre_screen_of_left_femoral_head': 'Screen of left '
                                                                                         'femoral head '
                                                                                         'recorded in the '
                                                                                         'pre-procedure iWeb '
                                                                                         'coronary '
                                                                                         'assessment.',
                                                      'pre_screen_of_right_femoral_head': 'Screen of right '
                                                                                          'femoral head '
                                                                                          'recorded in the '
                                                                                          'pre-procedure '
                                                                                          'iWeb coronary '
                                                                                          'assessment.',
                                                      'pre_second_operator_status': 'Second operator status '
                                                                                    'recorded in the '
                                                                                    'pre-procedure iWeb '
                                                                                    'coronary assessment.',
                                                      'pre_second_operatortxt': 'Free-text second operator '
                                                                                'recorded in the '
                                                                                'pre-procedure iWeb coronary '
                                                                                'assessment.',
                                                      'pre_serum_creatinine': 'Serum creatinine recorded in '
                                                                              'the pre-procedure iWeb '
                                                                              'coronary assessment.',
                                                      'pre_sex': 'Sex recorded in the pre-procedure iWeb '
                                                                 'coronary assessment.',
                                                      'pre_smoking_history': 'Smoking history recorded in '
                                                                             'the pre-procedure iWeb '
                                                                             'coronary assessment.',
                                                      'pre_surgical_cover': 'Surgical cover recorded in the '
                                                                            'pre-procedure iWeb coronary '
                                                                            'assessment.',
                                                      'pre_temporary_pacing_wire': 'Temporary pacing wire '
                                                                                   'recorded in the '
                                                                                   'pre-procedure iWeb '
                                                                                   'coronary assessment.',
                                                      'pre_therapeutic_hypothermiatxt': 'Free-text '
                                                                                        'therapeutic '
                                                                                        'hypothermia '
                                                                                        'recorded in the '
                                                                                        'pre-procedure iWeb '
                                                                                        'coronary '
                                                                                        'assessment.',
                                                      'pre_third_operator_status': 'Third operator status '
                                                                                   'recorded in the '
                                                                                   'pre-procedure iWeb '
                                                                                   'coronary assessment.',
                                                      'pre_third_operatortxt': 'Free-text third operator '
                                                                               'recorded in the '
                                                                               'pre-procedure iWeb coronary '
                                                                               'assessment.',
                                                      'pre_training_procedure': 'Training procedure recorded '
                                                                                'in the pre-procedure iWeb '
                                                                                'coronary assessment.',
                                                      'pre_us_sguided': 'Indicates whether ultrasound '
                                                                        'guidance was recorded for vascular '
                                                                        'access before the procedure.',
                                                      'pre_ventilated_preop': 'Ventilated pre-operative '
                                                                              'recorded in the pre-procedure '
                                                                              'iWeb coronary assessment.',
                                                      'pre_ventilation': 'Ventilation recorded in the '
                                                                         'pre-procedure iWeb coronary '
                                                                         'assessment.',
                                                      'pre_weight': 'Weight recorded in the pre-procedure '
                                                                    'iWeb coronary assessment.',
                                                      'pre_why_no_iibiiia_during_procedure': 'Why no '
                                                                                             'IIb/IIIa '
                                                                                             'during '
                                                                                             'procedure '
                                                                                             'recorded in '
                                                                                             'the '
                                                                                             'pre-procedure '
                                                                                             'iWeb coronary '
                                                                                             'assessment.',
                                                      'row_hash': 'SHA-256 content fingerprint over the '
                                                                  "row's business columns, excluding "
                                                                  'volatile pipeline timestamps.',
                                                      'source_join_status': 'Indicates whether the coronary '
                                                                            'procedure was present in both '
                                                                            'source modules or only the '
                                                                            'pre-procedure or post-procedure '
                                                                            'module.',
                                                      'source_present_ind': 'True while the procedure '
                                                                            'remains present in the latest '
                                                                            'source snapshot; false '
                                                                            'identifies a retained source '
                                                                            'retraction.'},
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
                                                   'its lower-case name.'}}
