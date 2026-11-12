"""Silver specialist.registry_acs_transfer: 1 dataset(s).

- datasets: clinical_registry_acs_transfer
- moved verbatim from: 3_silver_specialist_care:202, 3_silver_specialist_care:795, silver_value_policy.py:344, silver_value_policy.py:345, silver_value_policy.py:348

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


# W2.11 (L3029): ACS troponin in ng/L, with the unit decided per assay (measured in prod 2026-10-30):
#   High sensitivity Troponin T: 8,232 rows, median 0.06, 7,603 below 5 -> recorded in ug/L;
#   Troponin I / Troponin T:     medians 0.16 / 0.07                     -> recorded in ug/L;
#   High sensitivity Troponin I: 1,918 rows, median 101                  -> recorded in ng/L (values < 5 are genuine);
#   Unknown / NULL assay (146 rows): unit cannot be decided              -> NULL, unit_method 'undetermined'.
# ug/L assays: values below 5 are x1000; values >= 5 were already ng/L and are kept. The source value is unchanged.
_TROPONIN_UG_L_ASSAYS = ("high sensitivity troponin t", "troponin t", "troponin i")


_TROPONIN_NG_L_ASSAYS = ("high sensitivity troponin i",)


def _troponin_assay(F):
    """Lower-cased, single-spaced assay name (NULL stays NULL)."""
    return F.lower(F.trim(F.regexp_replace(F.col("troponin_assay"), r"\s+", " ")))


# Clinical / registry acs transfer

# ==== journey_clinical.registry_acs_transfer ====
REGISTRY_ACS_TRANSFER_EXPECTED_SCHEMA = [
    ("ENTRY_ID", "int"),
    ("MRN", "string"),
    ("GENDER", "string"),
    ("DATE_OF_DEATH", "date"),
    ("ETHNIC_ORIGIN", "string"),
    ("ADMISSIONDATE", "timestamp"),
    ("PATIENT_ADMIN_STATUS", "string"),
    ("ADMISSION_METHOD", "string"),
    ("REFERRING_HOSPITALTXT", "string"),
    ("ADMISSION_WARD", "string"),
    ("ADMITTING_CONSULTANT", "string"),
    ("PATHWAY", "string"),
    ("SYMPTOM_ONSET", "timestamp"),
    ("CALL_FOR_HELP", "timestamp"),
    ("FIRST_RESPONDER", "timestamp"),
    ("ARRIVAL_OF_AMBULANCE", "timestamp"),
    ("ARRIVE_DGH", "timestamp"),
    ("ARRIVE_HERE", "timestamp"),
    ("TIME_OF_DIAGNOSTIC_ECG", "timestamp"),
    ("ANGIO_TABLE_TIME", "timestamp"),
    ("REPERFUSION_DATE_TIME", "timestamp"),
    ("LOCAL_INTERVENTION_DATE", "timestamp"),
    ("REFERRAL_FOR_INVESTIGATIONINTERVENTION", "timestamp"),
    ("CRITICAL_TRANSFER_CALL", "timestamp"),
    ("TIME_LEFT_DGH", "timestamp"),
    ("AMBULANCE_TRUST_CODETXT", "string"),
    ("CAD", "string"),
    ("TRANSFER_CAD", "string"),
    ("PREVIOUS_AMI", "string"),
    ("PREVIOUS_ANGINA", "string"),
    ("HYPERTENSION", "string"),
    ("HYPERCHOLESTEROLAEMIA", "string"),
    ("PERIPHERAL_VASCULAR_DISEASE", "string"),
    ("CEREBROVASCULAR_DISEASE", "string"),
    ("ASTHMA_COPD", "string"),
    ("CHRONIC_RENAL_FAILURE", "string"),
    ("HEART_FAILURE", "string"),
    ("PREVIOUS_PCI", "string"),
    ("PREVIOUS_CABG", "string"),
    ("FAMILY_HISTORY_OF_CHD", "string"),
    ("SMOKING_STATUS", "string"),
    ("DIABETES", "string"),
    ("BETA_BLOCKER_USE", "string"),
    ("AC_EOR_AR_BUSE", "string"),
    ("STATIN_USE", "string"),
    ("THIENOPYRIDINE_INHIBITOR_USE", "string"),
    ("INITIAL_DIAGNOSIS", "string"),
    ("HIGH_RISK_NSTEMI", "string"),
    ("KILLIP_CLASS", "string"),
    ("PLACE_FIRST12_LEAD_EC_GPERFORMED", "string"),
    ("EC_GDETERMINING_TREATMENT", "string"),
    ("EC_GSUBCATEGORIESTXT", "string"),
    ("QR_SDURATION", "string"),
    ("WHERE_WAS_ASPIRIN_GIVEN", "string"),
    ("PATIENT_LOCATION_AT_TIME_OF_STEMI", "string"),
    ("PATIENT_VENTILATED", "string"),
    ("CARDIOGENIC_SHOCK", "string"),
    ("GRAC_ESCORE", "string"),
    ("SERUM_GLUCOSE", "double"),
    ("SERUM_CHOLESTEROL", "double"),
    ("CREATININE", "int"),
    ("HAEMOGLOBIN", "int"),
    ("PEAK_CK", "int"),
    ("PEAK_TROPONIN", "double"),
    ("TROPONIN_ASSAY", "string"),
    ("CARDIAC_ENZYMES_RAISED", "string"),
    ("SYSTOLIC_BP", "int"),
    ("HEART_RATE", "int"),
    ("LVEF", "string"),
    ("HEIGHT", "int"),
    ("WEIGHT", "double"),
    ("ASSESSMENT_AT_NON_INTERVENTIONAL_HOSPITAL", "string"),
    ("CARDIOLOGICAL_CARE", "string"),
    ("INITIAL_REPERFUSION", "string"),
    ("WHERE_WAS_INITIAL_REPERFUSION_TREATMENT_GIVEN", "string"),
    ("REASON_REPERFUSION_TREATMENT_NOT_GIVEN", "string"),
    ("DELAY_BEFORE_TREATMENT", "string"),
    ("ADDITIONAL_REPERFUSION", "string"),
    ("ASSESSMENT_AT_INTERVENTIONAL_CENTRE", "string"),
    ("INTENDED_REPERFUSION_PROCEDURE", "string"),
    ("PROCEDURE_PERFORMED", "string"),
    ("WHY_WAS_NO_ANGIOGRAM_PERFORMED", "string"),
    ("WHY_WAS_NO_INTERVENTION_PERFORMED", "string"),
    ("BLEEDING_COMPLICATIONS", "string"),
    ("SITE_OF_INFARCTION", "string"),
    ("CORONARY_ANGIOGRAPHY", "string"),
    ("CORONARY_INTERVENTION", "string"),
    ("DELAY_TO_PERFORMANCE_OF_ANGIOGRAM", "string"),
    ("EXERCISE_TEST", "string"),
    ("ECHOCARDIOGRAPHY", "string"),
    ("RADIONUCLIDE_STUDY", "string"),
    ("STRESS_ECHO", "string"),
    ("CARDIAC_ARREST", "timestamp"),
    ("CARDIAC_ARREST_LOCATION", "string"),
    ("ARREST_PRESENTING_RHYTHM", "string"),
    ("OUTCOME_OF_ARREST", "string"),
    ("UNFRACTIONATED_HEPARIN", "string"),
    ("LOW_MOLECULAR_WEIGHT_HEPARIN", "string"),
    ("THIENOPYRIDINE_PLATELET_INHIBITOR", "string"),
    ("IV2B3A_AGENT", "string"),
    ("IV_BETA_BLOCKER", "string"),
    ("CALCIUM_CHANNEL_BLOCKER", "string"),
    ("I_VNITRATE", "string"),
    ("ORAL_NITRATE", "string"),
    ("POTASSIUM_CHANNEL_MODULATOR", "string"),
    ("WARFARIN", "string"),
    ("AC_EI_ARB", "string"),
    ("THIAZIDE_DIURETIC", "string"),
    ("LOOP_DIURETIC", "string"),
    ("ORAL_BETA_BLOCKER", "string"),
    ("ALDOSTERONE_ANTAGONIST", "string"),
    ("FONDAPARINUX", "string"),
    ("BIVALIRUDIN", "string"),
    ("THROMBOLYTIC_DRUG", "string"),
    ("INPATIENT_MANAGEMENT_OF_HYPERGLYCAEMIA_DIABETES", "string"),
    ("START_OF_INSULIN_INFUSION", "timestamp"),
    ("BETA_BLOCKERDISCHARGE", "string"),
    ("ACE_IOR_AR_BDISCHARGE", "string"),
    ("STATIN_DISCHARGE", "string"),
    ("ASPIRIN_DISCHARGE", "string"),
    ("THIENOPYRIDINE_INHIBITOR_DISCHARGE", "string"),
    ("ALDOSTERONE_ANTAGONIST_DISCHARGE", "string"),
    ("TICAGRELOR_DISCHARGE", "string"),
    ("DIABETIC_THERAPY_AT_DISCHARGE", "string"),
    ("DISCHARGE_DIAGNOSIS", "string"),
    ("OTHER_DISCHARGE_DIAGNOSISTXT", "string"),
    ("DISCHARGE_DESTINATION", "string"),
    ("REINFARCTION", "string"),
    ("DEATH_IN_HOSPITAL", "string"),
    ("FOLLOWED_UP_BY", "string"),
    ("CARDIAC_REHABILITATION", "string"),
    ("SMOKING_CESSATION_ADVICE_GIVEN", "string"),
    ("DIETARY_ADVICE_GIVEN", "string"),
    ("DATE_OF_RETURN_TO_REFERRING_HOSPITAL", "timestamp"),
    ("WHAT_PROCEDURE_WAS_PERFORMED_AT_THE_INTERVENTIONAL_HOSPIT", "string"),
    ("DATE_OF_DISCHARGE", "timestamp"),
    ("STRESS_PRECIPITANTTXT", "string"),
    ("M_STRESS_PRECIPITANT_OTHER", "string"),
    ("DATETIME_OF_STRESSFUL_PRECIPITANT", "timestamp"),
    ("REGIONAL_LV_DYSFUNCTION_DISTRIBUTION", "string"),
    ("M_REGIONAL_LV_DYSFUNCTION_DISTRIBUTION_OTHER", "string"),
    ("PREVIOUS_HISTORY_OF_TAKOTSUBO", "string"),
    ("FAMILY_HISTORY_OF_TAKOTSUBO", "string"),
    ("THYROID_DISEASE", "string"),
    ("PSYCHIATRIC_DISEASE", "string"),
    ("HORMONAL_STATUS", "string"),
    ("HR_TOR_ORAL_CONTRACEPTIVE", "string"),
    ("ANDROGEN_MODIFYING_THERAPY", "string"),
    ("BETA_ADRENERGIC_RECEPTOR_AGONISTS", "string"),
    ("QTC_INTERVAL", "string"),
    ("ATRIAL_TACHYCARDIA_OR_FIBRILLATION", "string"),
    ("VENTRICULAR_TACHYCARDIA_OR_FIBRILLATION", "string"),
    ("LEFT_VENTRICULAR_APICAL_THROMBUS", "string"),
    ("LEFT_VENTRICULAR_OUTFLOW_TRACT_OBSTRUCTION", "string"),
    ("BYSTANDER_CORONARY_ARTERY_DISEASE", "string"),
    ("LVEDP", "string"),
    ("CM_RIMAGING", "string"),
    ("POSITIVE_INOTROPIC_SUPPORTTXT", "string"),
    ("INTRAAORTIC_BALLOON_PUMP_SUPPORT", "string"),
    ("REMINDER", "string"),
    ("LVED_PVALUE", "int"),
    ("DATASET_VERSION", "string"),
    ("AMBULANCE_JOB_NUMBER", "string"),
    ("DAYCASE_TRANSFER_DT", "timestamp"),
    ("REFERRAL_DT_TM", "timestamp"),
    ("INTERVENTIONAL_HOSPITALTXT", "string"),
    ("M_HAC_TEAM_AUDIT_NOTES", "string"),
    ("DATA_COMPLETE", "string"),
    ("CM_RIMAGING_PERFORMED", "string"),
    ("TICAGRELOR_USE", "string"),
    ("DATE_LAST_CHANGED", "timestamp"),
    ("ADC_UPDT", "timestamp"),
    ("NHS_NUMBER_VALID_IND", "boolean"),
    ("LINKAGE_STATUS", "string"),
    ("PERSON_ID", "bigint"),
    ("LINKAGE_METHOD", "string"),
    ("LINKAGE_HISTORICAL_FALLBACK_IND", "boolean"),
    ("LINKAGE_FALLBACK_CONFLICT_IND", "boolean"),
    ("ADMISSION_DATE_QUALITY", "string"),
    ("ADMISSION_DATE_CLEAN", "date"),
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
    ("HOSPITAL_IDENTIFIER", "string"),
]


@materialized_view(
    name=_n("journey_clinical.registry_acs_transfer"),
    refresh_policy="auto",
)
def registry_acs_transfer():
    # Build the declared dataset: Typed child of clinical_registry_entry for the acs_transfer
    # family. MINAP-style ACS transfer pathway: symptom, transfer and reperfusion timestamps,
    # GRACE/Killip, drugs, laboratory results and Takotsubo fields. Diagnoses remain source
    # labels. ADMISSION_DATE_QUALITY identifies the known 2000-01-01 placeholder range. Every
    # governed bronze column is retained under its lower-case name.
    s = read_source(IWEB_REGISTRY_SLOTS["acs_transfer"])
    actual_schema = [(field.name, field.dataType.simpleString()) for field in s.schema.fields]
    if actual_schema != REGISTRY_ACS_TRANSFER_EXPECTED_SCHEMA:
        raise RuntimeError("acs_transfer: bronze schema drift" + f"; expected={REGISTRY_ACS_TRANSFER_EXPECTED_SCHEMA!r} actual={actual_schema!r}")
    return s.select(
        stable_id("registry_entry:iweb", F.lit("acs_transfer"), F.lit("~"), s["ENTRY_ID"]).alias("patient_event_key"),
        s["ENTRY_ID"].alias("entry_id"),
        s["MRN"].alias("mrn"),
        s["GENDER"].alias("gender"),
        s["DATE_OF_DEATH"].alias("date_of_death"),
        s["ETHNIC_ORIGIN"].alias("ethnic_origin"),
        s["ADMISSIONDATE"].alias("admissiondate"),
        s["PATIENT_ADMIN_STATUS"].alias("patient_admin_status"),
        s["ADMISSION_METHOD"].alias("admission_method"),
        s["REFERRING_HOSPITALTXT"].alias("referring_hospitaltxt"),
        s["ADMISSION_WARD"].alias("admission_ward"),
        s["ADMITTING_CONSULTANT"].alias("admitting_consultant"),
        s["PATHWAY"].alias("pathway"),
        s["SYMPTOM_ONSET"].alias("symptom_onset"),
        s["CALL_FOR_HELP"].alias("call_for_help"),
        s["FIRST_RESPONDER"].alias("first_responder"),
        s["ARRIVAL_OF_AMBULANCE"].alias("arrival_of_ambulance"),
        s["ARRIVE_DGH"].alias("arrive_dgh"),
        s["ARRIVE_HERE"].alias("arrive_here"),
        s["TIME_OF_DIAGNOSTIC_ECG"].alias("time_of_diagnostic_ecg"),
        s["ANGIO_TABLE_TIME"].alias("angio_table_time"),
        s["REPERFUSION_DATE_TIME"].alias("reperfusion_date_time"),
        s["LOCAL_INTERVENTION_DATE"].alias("local_intervention_date"),
        s["REFERRAL_FOR_INVESTIGATIONINTERVENTION"].alias("referral_for_investigationintervention"),
        s["CRITICAL_TRANSFER_CALL"].alias("critical_transfer_call"),
        s["TIME_LEFT_DGH"].alias("time_left_dgh"),
        s["AMBULANCE_TRUST_CODETXT"].alias("ambulance_trust_codetxt"),
        s["CAD"].alias("cad"),
        s["TRANSFER_CAD"].alias("transfer_cad"),
        s["PREVIOUS_AMI"].alias("previous_ami"),
        s["PREVIOUS_ANGINA"].alias("previous_angina"),
        s["HYPERTENSION"].alias("hypertension"),
        s["HYPERCHOLESTEROLAEMIA"].alias("hypercholesterolaemia"),
        s["PERIPHERAL_VASCULAR_DISEASE"].alias("peripheral_vascular_disease"),
        s["CEREBROVASCULAR_DISEASE"].alias("cerebrovascular_disease"),
        s["ASTHMA_COPD"].alias("asthma_copd"),
        s["CHRONIC_RENAL_FAILURE"].alias("chronic_renal_failure"),
        s["HEART_FAILURE"].alias("heart_failure"),
        s["PREVIOUS_PCI"].alias("previous_pci"),
        s["PREVIOUS_CABG"].alias("previous_cabg"),
        s["FAMILY_HISTORY_OF_CHD"].alias("family_history_of_chd"),
        s["SMOKING_STATUS"].alias("smoking_status"),
        s["DIABETES"].alias("diabetes"),
        s["BETA_BLOCKER_USE"].alias("beta_blocker_use"),
        s["AC_EOR_AR_BUSE"].alias("ac_eor_ar_buse"),
        s["STATIN_USE"].alias("statin_use"),
        s["THIENOPYRIDINE_INHIBITOR_USE"].alias("thienopyridine_inhibitor_use"),
        s["INITIAL_DIAGNOSIS"].alias("initial_diagnosis"),
        s["HIGH_RISK_NSTEMI"].alias("high_risk_nstemi"),
        s["KILLIP_CLASS"].alias("killip_class"),
        s["PLACE_FIRST12_LEAD_EC_GPERFORMED"].alias("place_first12_lead_ec_gperformed"),
        s["EC_GDETERMINING_TREATMENT"].alias("ec_gdetermining_treatment"),
        s["EC_GSUBCATEGORIESTXT"].alias("ec_gsubcategoriestxt"),
        s["QR_SDURATION"].alias("qr_sduration"),
        s["WHERE_WAS_ASPIRIN_GIVEN"].alias("where_was_aspirin_given"),
        s["PATIENT_LOCATION_AT_TIME_OF_STEMI"].alias("patient_location_at_time_of_stemi"),
        s["PATIENT_VENTILATED"].alias("patient_ventilated"),
        s["CARDIOGENIC_SHOCK"].alias("cardiogenic_shock"),
        s["GRAC_ESCORE"].alias("grac_escore"),
        s["SERUM_GLUCOSE"].alias("serum_glucose"),
        s["SERUM_CHOLESTEROL"].alias("serum_cholesterol"),
        s["CREATININE"].alias("creatinine"),
        s["HAEMOGLOBIN"].alias("haemoglobin"),
        s["PEAK_CK"].alias("peak_ck"),
        s["PEAK_TROPONIN"].alias("peak_troponin"),
        s["TROPONIN_ASSAY"].alias("troponin_assay"),
        s["CARDIAC_ENZYMES_RAISED"].alias("cardiac_enzymes_raised"),
        s["SYSTOLIC_BP"].alias("systolic_bp"),
        s["HEART_RATE"].alias("heart_rate"),
        s["LVEF"].alias("lvef"),
        s["HEIGHT"].alias("height"),
        s["WEIGHT"].alias("weight"),
        s["ASSESSMENT_AT_NON_INTERVENTIONAL_HOSPITAL"].alias("assessment_at_non_interventional_hospital"),
        s["CARDIOLOGICAL_CARE"].alias("cardiological_care"),
        s["INITIAL_REPERFUSION"].alias("initial_reperfusion"),
        s["WHERE_WAS_INITIAL_REPERFUSION_TREATMENT_GIVEN"].alias("where_was_initial_reperfusion_treatment_given"),
        s["REASON_REPERFUSION_TREATMENT_NOT_GIVEN"].alias("reason_reperfusion_treatment_not_given"),
        s["DELAY_BEFORE_TREATMENT"].alias("delay_before_treatment"),
        s["ADDITIONAL_REPERFUSION"].alias("additional_reperfusion"),
        s["ASSESSMENT_AT_INTERVENTIONAL_CENTRE"].alias("assessment_at_interventional_centre"),
        s["INTENDED_REPERFUSION_PROCEDURE"].alias("intended_reperfusion_procedure"),
        s["PROCEDURE_PERFORMED"].alias("procedure_performed"),
        s["WHY_WAS_NO_ANGIOGRAM_PERFORMED"].alias("why_was_no_angiogram_performed"),
        s["WHY_WAS_NO_INTERVENTION_PERFORMED"].alias("why_was_no_intervention_performed"),
        s["BLEEDING_COMPLICATIONS"].alias("bleeding_complications"),
        s["SITE_OF_INFARCTION"].alias("site_of_infarction"),
        s["CORONARY_ANGIOGRAPHY"].alias("coronary_angiography"),
        s["CORONARY_INTERVENTION"].alias("coronary_intervention"),
        s["DELAY_TO_PERFORMANCE_OF_ANGIOGRAM"].alias("delay_to_performance_of_angiogram"),
        s["EXERCISE_TEST"].alias("exercise_test"),
        s["ECHOCARDIOGRAPHY"].alias("echocardiography"),
        s["RADIONUCLIDE_STUDY"].alias("radionuclide_study"),
        s["STRESS_ECHO"].alias("stress_echo"),
        s["CARDIAC_ARREST"].alias("cardiac_arrest"),
        s["CARDIAC_ARREST_LOCATION"].alias("cardiac_arrest_location"),
        s["ARREST_PRESENTING_RHYTHM"].alias("arrest_presenting_rhythm"),
        s["OUTCOME_OF_ARREST"].alias("outcome_of_arrest"),
        s["UNFRACTIONATED_HEPARIN"].alias("unfractionated_heparin"),
        s["LOW_MOLECULAR_WEIGHT_HEPARIN"].alias("low_molecular_weight_heparin"),
        s["THIENOPYRIDINE_PLATELET_INHIBITOR"].alias("thienopyridine_platelet_inhibitor"),
        s["IV2B3A_AGENT"].alias("iv2b3a_agent"),
        s["IV_BETA_BLOCKER"].alias("iv_beta_blocker"),
        s["CALCIUM_CHANNEL_BLOCKER"].alias("calcium_channel_blocker"),
        s["I_VNITRATE"].alias("i_vnitrate"),
        s["ORAL_NITRATE"].alias("oral_nitrate"),
        s["POTASSIUM_CHANNEL_MODULATOR"].alias("potassium_channel_modulator"),
        s["WARFARIN"].alias("warfarin"),
        s["AC_EI_ARB"].alias("ac_ei_arb"),
        s["THIAZIDE_DIURETIC"].alias("thiazide_diuretic"),
        s["LOOP_DIURETIC"].alias("loop_diuretic"),
        s["ORAL_BETA_BLOCKER"].alias("oral_beta_blocker"),
        s["ALDOSTERONE_ANTAGONIST"].alias("aldosterone_antagonist"),
        s["FONDAPARINUX"].alias("fondaparinux"),
        s["BIVALIRUDIN"].alias("bivalirudin"),
        s["THROMBOLYTIC_DRUG"].alias("thrombolytic_drug"),
        s["INPATIENT_MANAGEMENT_OF_HYPERGLYCAEMIA_DIABETES"].alias("inpatient_management_of_hyperglycaemia_diabetes"),
        s["START_OF_INSULIN_INFUSION"].alias("start_of_insulin_infusion"),
        s["BETA_BLOCKERDISCHARGE"].alias("beta_blockerdischarge"),
        s["ACE_IOR_AR_BDISCHARGE"].alias("ace_ior_ar_bdischarge"),
        s["STATIN_DISCHARGE"].alias("statin_discharge"),
        s["ASPIRIN_DISCHARGE"].alias("aspirin_discharge"),
        s["THIENOPYRIDINE_INHIBITOR_DISCHARGE"].alias("thienopyridine_inhibitor_discharge"),
        s["ALDOSTERONE_ANTAGONIST_DISCHARGE"].alias("aldosterone_antagonist_discharge"),
        s["TICAGRELOR_DISCHARGE"].alias("ticagrelor_discharge"),
        s["DIABETIC_THERAPY_AT_DISCHARGE"].alias("diabetic_therapy_at_discharge"),
        s["DISCHARGE_DIAGNOSIS"].alias("discharge_diagnosis"),
        s["OTHER_DISCHARGE_DIAGNOSISTXT"].alias("other_discharge_diagnosistxt"),
        s["DISCHARGE_DESTINATION"].alias("discharge_destination"),
        s["REINFARCTION"].alias("reinfarction"),
        s["DEATH_IN_HOSPITAL"].alias("death_in_hospital"),
        s["FOLLOWED_UP_BY"].alias("followed_up_by"),
        s["CARDIAC_REHABILITATION"].alias("cardiac_rehabilitation"),
        s["SMOKING_CESSATION_ADVICE_GIVEN"].alias("smoking_cessation_advice_given"),
        s["DIETARY_ADVICE_GIVEN"].alias("dietary_advice_given"),
        s["DATE_OF_RETURN_TO_REFERRING_HOSPITAL"].alias("date_of_return_to_referring_hospital"),
        s["WHAT_PROCEDURE_WAS_PERFORMED_AT_THE_INTERVENTIONAL_HOSPIT"].alias("what_procedure_was_performed_at_the_interventional_hospit"),
        s["DATE_OF_DISCHARGE"].alias("date_of_discharge"),
        s["STRESS_PRECIPITANTTXT"].alias("stress_precipitanttxt"),
        s["M_STRESS_PRECIPITANT_OTHER"].alias("m_stress_precipitant_other"),
        s["DATETIME_OF_STRESSFUL_PRECIPITANT"].alias("datetime_of_stressful_precipitant"),
        s["REGIONAL_LV_DYSFUNCTION_DISTRIBUTION"].alias("regional_lv_dysfunction_distribution"),
        s["M_REGIONAL_LV_DYSFUNCTION_DISTRIBUTION_OTHER"].alias("m_regional_lv_dysfunction_distribution_other"),
        s["PREVIOUS_HISTORY_OF_TAKOTSUBO"].alias("previous_history_of_takotsubo"),
        s["FAMILY_HISTORY_OF_TAKOTSUBO"].alias("family_history_of_takotsubo"),
        s["THYROID_DISEASE"].alias("thyroid_disease"),
        s["PSYCHIATRIC_DISEASE"].alias("psychiatric_disease"),
        s["HORMONAL_STATUS"].alias("hormonal_status"),
        s["HR_TOR_ORAL_CONTRACEPTIVE"].alias("hr_tor_oral_contraceptive"),
        s["ANDROGEN_MODIFYING_THERAPY"].alias("androgen_modifying_therapy"),
        s["BETA_ADRENERGIC_RECEPTOR_AGONISTS"].alias("beta_adrenergic_receptor_agonists"),
        s["QTC_INTERVAL"].alias("qtc_interval"),
        s["ATRIAL_TACHYCARDIA_OR_FIBRILLATION"].alias("atrial_tachycardia_or_fibrillation"),
        s["VENTRICULAR_TACHYCARDIA_OR_FIBRILLATION"].alias("ventricular_tachycardia_or_fibrillation"),
        s["LEFT_VENTRICULAR_APICAL_THROMBUS"].alias("left_ventricular_apical_thrombus"),
        s["LEFT_VENTRICULAR_OUTFLOW_TRACT_OBSTRUCTION"].alias("left_ventricular_outflow_tract_obstruction"),
        s["BYSTANDER_CORONARY_ARTERY_DISEASE"].alias("bystander_coronary_artery_disease"),
        s["LVEDP"].alias("lvedp"),
        s["CM_RIMAGING"].alias("cm_rimaging"),
        s["POSITIVE_INOTROPIC_SUPPORTTXT"].alias("positive_inotropic_supporttxt"),
        s["INTRAAORTIC_BALLOON_PUMP_SUPPORT"].alias("intraaortic_balloon_pump_support"),
        s["REMINDER"].alias("reminder"),
        s["LVED_PVALUE"].alias("lved_pvalue"),
        s["DATASET_VERSION"].alias("dataset_version"),
        s["AMBULANCE_JOB_NUMBER"].alias("ambulance_job_number"),
        s["DAYCASE_TRANSFER_DT"].alias("daycase_transfer_dt"),
        s["REFERRAL_DT_TM"].alias("referral_dt_tm"),
        s["INTERVENTIONAL_HOSPITALTXT"].alias("interventional_hospitaltxt"),
        s["M_HAC_TEAM_AUDIT_NOTES"].alias("m_hac_team_audit_notes"),
        s["DATA_COMPLETE"].alias("data_complete"),
        s["CM_RIMAGING_PERFORMED"].alias("cm_rimaging_performed"),
        s["TICAGRELOR_USE"].alias("ticagrelor_use"),
        s["DATE_LAST_CHANGED"].alias("date_last_changed"),
        s["ADC_UPDT"].alias("adc_updt"),
        s["NHS_NUMBER_VALID_IND"].alias("nhs_number_valid_ind"),
        s["LINKAGE_STATUS"].alias("linkage_status"),
        s["PERSON_ID"].alias("person_id"),
        s["LINKAGE_METHOD"].alias("linkage_method"),
        s["LINKAGE_HISTORICAL_FALLBACK_IND"].alias("linkage_historical_fallback_ind"),
        s["LINKAGE_FALLBACK_CONFLICT_IND"].alias("linkage_fallback_conflict_ind"),
        s["ADMISSION_DATE_QUALITY"].alias("admission_date_quality"),
        s["ADMISSION_DATE_CLEAN"].alias("admission_date_clean"),
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
        s["HOSPITAL_IDENTIFIER"].alias("hospital_identifier"),
    )


VALUE_POLICIES = {
    'clinical_registry_acs_transfer': ValuePolicy(table='clinical_registry_acs_transfer', enabled=True),
}


VALUE_POLICIES["clinical_registry_acs_transfer"].register("peak_troponin_ng_l", ["peak_troponin", "troponin_assay"],
         lambda F: F.when(_troponin_assay(F).isin(*_TROPONIN_NG_L_ASSAYS), F.col("peak_troponin"))
                    .when(_troponin_assay(F).isin(*_TROPONIN_UG_L_ASSAYS),
                          F.when(F.col("peak_troponin") < 5, F.col("peak_troponin") * 1000)
                           .otherwise(F.col("peak_troponin"))),
         "Peak troponin in ng/L, unit decided per assay: High sensitivity Troponin I is recorded in ng/L and kept; "
         "High sensitivity Troponin T, Troponin T and Troponin I are recorded in ug/L, so values below 5 are x1000 "
         "(values >= 5 were already ng/L). NULL when the assay is unknown or missing. INSR_W2 W2.11 (L3029).")


VALUE_POLICIES["clinical_registry_acs_transfer"].register("unit_method", ["peak_troponin", "troponin_assay"],
         lambda F: F.when(F.col("peak_troponin").isNull(), F.lit(None).cast("string"))
                    .when(_troponin_assay(F).isin(*_TROPONIN_NG_L_ASSAYS), F.lit("assay_ng_l"))
                    .when(_troponin_assay(F).isin(*_TROPONIN_UG_L_ASSAYS) & (F.col("peak_troponin") < 5),
                          F.lit("assay_ug_l_x1000"))
                    .when(_troponin_assay(F).isin(*_TROPONIN_UG_L_ASSAYS), F.lit("assay_ug_l_value_already_ng_l"))
                    .otherwise(F.lit("undetermined")),
         "How peak_troponin_ng_l was derived: assay_ng_l, assay_ug_l_x1000, assay_ug_l_value_already_ng_l, or "
         "undetermined (unknown/missing assay; peak_troponin_ng_l NULL). INSR_W2 W2.11.")


WRAP_POLICY = {'clinical_registry_acs_transfer'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_registry_acs_transfer': {'columns': {'ac_ei_arb': 'Coded indicator of whether an ACE inhibitor or '
                                                             'angiotensin receptor blocker was given during '
                                                             'the acute admission episode.',
                                                'ac_eor_ar_buse': 'Indicates whether the patient was taking '
                                                                  'an ACE inhibitor or angiotensin receptor '
                                                                  'blocker prior to the ACS admission.',
                                                'ace_ior_ar_bdischarge': 'Coded indicator of whether an ACE '
                                                                         'inhibitor or angiotensin receptor '
                                                                         'blocker was prescribed at '
                                                                         'discharge.',
                                                'adc_updt': 'Source system extract timestamp indicating when '
                                                            'the row was last updated in the upstream feed.',
                                                'additional_reperfusion': 'Coded record of any additional '
                                                                          'reperfusion treatment given after '
                                                                          'the initial reperfusion attempt.',
                                                'admission_date_clean': 'Admission date with sentinel/future '
                                                                        'values nulled; raw ADMISSIONDATE '
                                                                        'retained.',
                                                'admission_date_quality': 'VALID | SENTINEL (year <=2000) | '
                                                                          'FUTURE | MISSING.',
                                                'admission_method': 'Coded method by which the patient was '
                                                                    'admitted for this ACS episode, as '
                                                                    'recorded in the source registry.',
                                                'admission_ward': 'Ward or clinical area to which the '
                                                                  'patient was admitted, as recorded in the '
                                                                  'source registry.',
                                                'admissiondate': 'Raw hospital admission date/time from the '
                                                                 'source record, which may contain sentinel '
                                                                 'or future values flagged by '
                                                                 'ADMISSION_DATE_QUALITY.',
                                                'admitting_consultant': 'Category or identity of the '
                                                                        'consultant responsible for the '
                                                                        'admission, as recorded in the '
                                                                        'source registry.',
                                                'aldosterone_antagonist': 'Coded indicator of whether an '
                                                                          'aldosterone antagonist was given '
                                                                          'during the acute admission '
                                                                          'episode.',
                                                'aldosterone_antagonist_discharge': 'Coded indicator of '
                                                                                    'whether an aldosterone '
                                                                                    'antagonist was '
                                                                                    'prescribed at '
                                                                                    'discharge.',
                                                'ambulance_job_number': 'Ambulance service job or incident '
                                                                        'reference recorded for the '
                                                                        "patient's conveyance.",
                                                'ambulance_trust_codetxt': 'Decoded text label of the '
                                                                           'ambulance trust involved in the '
                                                                           "patient's conveyance, following "
                                                                           'the txt suffix convention for '
                                                                           'lookup descriptions in this '
                                                                           'extract.',
                                                'androgen_modifying_therapy': 'Source-coded indicator of '
                                                                              'androgen-modifying therapy '
                                                                              'use recorded in the Takotsubo '
                                                                              'assessment.',
                                                'angio_table_time': 'Date and time the patient was on the '
                                                                    'angiography table, as recorded in the '
                                                                    'source registry.',
                                                'arrest_presenting_rhythm': 'Coded presenting cardiac rhythm '
                                                                            'at the time of the cardiac '
                                                                            'arrest.',
                                                'arrival_of_ambulance': 'Date and time of ambulance arrival '
                                                                        'at the patient, as recorded in the '
                                                                        'source registry.',
                                                'arrive_dgh': 'Date and time the patient arrived at the '
                                                              'district general hospital, as recorded in the '
                                                              'source registry.',
                                                'arrive_here': 'Date and time the patient arrived at the '
                                                               'reporting hospital, as recorded in the '
                                                               'source registry.',
                                                'aspirin_discharge': 'Coded indicator of whether aspirin was '
                                                                     'prescribed at discharge.',
                                                'assessment_at_interventional_centre': 'Coded outcome of '
                                                                                       'clinical assessment '
                                                                                       'of the patient at '
                                                                                       'the interventional '
                                                                                       '(PCI) centre.',
                                                'assessment_at_non_interventional_hospital': 'Categorical '
                                                                                             'record of the '
                                                                                             'assessment or '
                                                                                             'care received '
                                                                                             'at the '
                                                                                             'referring '
                                                                                             'non-interventional '
                                                                                             'hospital '
                                                                                             'before '
                                                                                             'transfer.',
                                                'asthma_copd': 'Indicates whether the patient has a recorded '
                                                               'history of asthma or chronic obstructive '
                                                               'pulmonary disease.',
                                                'atrial_tachycardia_or_fibrillation': 'Coded source '
                                                                                      'indicator of whether '
                                                                                      'atrial tachycardia or '
                                                                                      'atrial fibrillation '
                                                                                      'was recorded for the '
                                                                                      'episode.',
                                                'beta_adrenergic_receptor_agonists': 'Source-coded indicator '
                                                                                     'of beta-adrenergic '
                                                                                     'receptor agonist use '
                                                                                     'recorded in the '
                                                                                     'Takotsubo assessment.',
                                                'beta_blocker_use': 'Indicates whether the patient was '
                                                                    'taking a beta blocker prior to the ACS '
                                                                    'admission.',
                                                'beta_blockerdischarge': 'Coded indicator of whether a beta '
                                                                         'blocker was prescribed at '
                                                                         'discharge.',
                                                'bivalirudin': 'Coded indicator of whether bivalirudin was '
                                                               'given during the acute admission episode.',
                                                'bleeding_complications': 'Coded bleeding complications '
                                                                          'recorded during the admission or '
                                                                          'after intervention.',
                                                'bystander_coronary_artery_disease': 'Coded source indicator '
                                                                                     'of whether incidental '
                                                                                     '(bystander) coronary '
                                                                                     'artery disease was '
                                                                                     'present alongside the '
                                                                                     'presenting condition.',
                                                'cad': 'Source-recorded indicator of known coronary artery '
                                                       'disease for the patient.',
                                                'calcium_channel_blocker': 'Coded indicator of whether a '
                                                                           'calcium channel blocker was '
                                                                           'given during the acute admission '
                                                                           'episode.',
                                                'call_for_help': 'Date and time the patient or bystander '
                                                                 'called for medical help, as recorded in '
                                                                 'the source registry.',
                                                'cardiac_arrest': 'Date and time of cardiac arrest recorded '
                                                                  'for the episode.',
                                                'cardiac_arrest_location': 'Coded location at which the '
                                                                           'cardiac arrest occurred, '
                                                                           'including a no-arrest category, '
                                                                           'as recorded in the source '
                                                                           'registry.',
                                                'cardiac_enzymes_raised': 'Indicator of whether cardiac '
                                                                          'enzyme or troponin levels were '
                                                                          'raised during the episode.',
                                                'cardiac_rehabilitation': 'Source-coded record of whether '
                                                                          'cardiac rehabilitation was '
                                                                          'arranged or indicated following '
                                                                          'the admission.',
                                                'cardiogenic_shock': 'Indicator of whether cardiogenic shock '
                                                                     'was present during the acute coronary '
                                                                     'syndrome episode.',
                                                'cardiological_care': 'Categorical record of whether the '
                                                                      'patient received cardiological care '
                                                                      'during the admission.',
                                                'cerebrovascular_disease': 'Indicates whether the patient '
                                                                           'has a recorded history of '
                                                                           'cerebrovascular disease.',
                                                'chronic_renal_failure': 'Indicates whether the patient has '
                                                                         'a recorded history of chronic '
                                                                         'renal failure.',
                                                'cm_rimaging': 'Coded source field describing cardiac '
                                                               'magnetic resonance imaging assessment '
                                                               'recorded for the episode.',
                                                'cm_rimaging_performed': 'Source flag indicating whether '
                                                                         'cardiac magnetic resonance imaging '
                                                                         'was performed.',
                                                'coronary_angiography': 'Coded status of coronary '
                                                                        'angiography for the episode, '
                                                                        'including whether and where it was '
                                                                        'performed or arranged.',
                                                'coronary_intervention': 'Coded status of coronary '
                                                                         'intervention for the episode, '
                                                                         'including whether it was performed '
                                                                         'or arranged.',
                                                'creatinine': 'Recorded serum creatinine laboratory result '
                                                              'for the acute episode; source units are not '
                                                              'documented.',
                                                'critical_transfer_call': 'Date and time of the critical '
                                                                          'transfer call for the patient, as '
                                                                          'recorded in the source registry.',
                                                'data_complete': 'Source flag indicating whether the '
                                                                 'registry record has been marked as '
                                                                 'complete.',
                                                'dataset_version': 'Version label of the registry dataset '
                                                                   'specification under which the record was '
                                                                   'collected.',
                                                'date_last_changed': 'Timestamp of the last change made to '
                                                                     'the record in the source registry '
                                                                     'system.',
                                                'date_of_birth': 'Patient date of birth as recorded in the '
                                                                 'source ACS transfer registry record.',
                                                'date_of_death': 'Recorded date of patient death as held in '
                                                                 'the source registry record.',
                                                'date_of_discharge': 'Date and time of discharge from the '
                                                                     'admission recorded in this ACS '
                                                                     'transfer record.',
                                                'date_of_return_to_referring_hospital': 'Date and time the '
                                                                                        'patient was '
                                                                                        'repatriated to the '
                                                                                        'referring district '
                                                                                        'general hospital '
                                                                                        'after intervention.',
                                                'datetime_of_stressful_precipitant': 'Date and time of the '
                                                                                     'stressful precipitant '
                                                                                     'event recorded in the '
                                                                                     'Takotsubo assessment.',
                                                'daycase_transfer_dt': 'Date and time of daycase transfer '
                                                                       'recorded in the source registry '
                                                                       'record.',
                                                'death_in_hospital': 'Source-coded indicator of whether the '
                                                                     'patient died during the hospital '
                                                                     'admission.',
                                                'delay_before_treatment': 'Coded MINAP reason for any delay '
                                                                          'before reperfusion treatment was '
                                                                          'given.',
                                                'delay_to_performance_of_angiogram': 'Coded reason for any '
                                                                                     'delay to performance '
                                                                                     'of the coronary '
                                                                                     'angiogram.',
                                                'diabetes': 'Recorded diabetes status of the patient, '
                                                            'including non-diabetic and treatment-based '
                                                            'categories.',
                                                'diabetic_therapy_at_discharge': 'Coded description of the '
                                                                                 'diabetes therapy in place '
                                                                                 'at discharge.',
                                                'dietary_advice_given': 'Source-coded record of whether '
                                                                        'dietary advice was given before '
                                                                        'discharge.',
                                                'discharge_destination': 'Coded destination category to '
                                                                         'which the patient was discharged '
                                                                         'at the end of the acute coronary '
                                                                         'syndrome admission.',
                                                'discharge_diagnosis': 'Source-coded discharge diagnosis '
                                                                       'label for the acute coronary '
                                                                       'syndrome episode, retained as '
                                                                       'recorded in the source registry.',
                                                'ec_gdetermining_treatment': 'Coded ECG finding that '
                                                                             'determined the treatment given '
                                                                             'for the acute coronary '
                                                                             'syndrome episode.',
                                                'ec_gsubcategoriestxt': 'Text label of the ECG subcategory '
                                                                        'classification recorded alongside '
                                                                        'the determining ECG finding, blank '
                                                                        'where not recorded.',
                                                'echocardiography': 'Coded source record of whether '
                                                                    'echocardiography was carried out, '
                                                                    'arranged for later or judged '
                                                                    'unnecessary during the episode.',
                                                'entry_id': 'Internal source registry record identifier for '
                                                            'the ACS transfer entry, carried through from '
                                                            'the feeder registry table.',
                                                'ethnic_origin': 'Patient ethnic origin code as recorded in '
                                                                 'the source ACS registry.',
                                                'exercise_test': 'Coded source record of whether an exercise '
                                                                 'tolerance test was carried out, planned or '
                                                                 'considered unnecessary during the episode.',
                                                'family_history_of_chd': 'Indicates whether the patient has '
                                                                         'a family history of coronary heart '
                                                                         'disease.',
                                                'family_history_of_takotsubo': 'Source-coded indicator of '
                                                                               'whether there is a family '
                                                                               'history of Takotsubo '
                                                                               'syndrome.',
                                                'first_responder': 'Date and time the first responder '
                                                                   'attended the patient, as recorded in the '
                                                                   'source registry.',
                                                'followed_up_by': 'Coded category describing the clinician '
                                                                  'or service responsible for follow-up '
                                                                  'after discharge.',
                                                'fondaparinux': 'Coded indicator of whether fondaparinux was '
                                                                'given during the acute admission episode.',
                                                'forename': 'Patient forename as recorded in the source ACS '
                                                            'transfer registry record.',
                                                'gender': 'Patient gender as recorded in the source ACS '
                                                          'registry record.',
                                                'grac_escore': 'GRACE risk score recorded for the acute '
                                                               'coronary syndrome episode, stored as a '
                                                               'string and frequently unpopulated.',
                                                'haemoglobin': 'Recorded haemoglobin laboratory result for '
                                                               'the acute episode; source units are not '
                                                               'documented.',
                                                'heart_failure': 'Indicates whether the patient has a '
                                                                 'recorded history of heart failure.',
                                                'heart_rate': 'Heart rate recorded at initial assessment for '
                                                              'the episode; source units are not documented.',
                                                'height': 'Patient height recorded for the episode; source '
                                                          'units are not documented.',
                                                'high_risk_nstemi': 'Indicates whether the patient was '
                                                                    'assessed as having high-risk '
                                                                    'non-ST-elevation myocardial infarction.',
                                                'hormonal_status': 'Source-coded hormonal status recorded as '
                                                                   'part of the Takotsubo assessment.',
                                                'hospital_identifier': 'Hospital identifier recorded against '
                                                                       'the patient in the source registry '
                                                                       'record.',
                                                'hr_tor_oral_contraceptive': 'Source-coded indicator of '
                                                                             'hormone replacement therapy or '
                                                                             'oral contraceptive use, '
                                                                             'recorded in the Takotsubo '
                                                                             'assessment.',
                                                'hypercholesterolaemia': 'Indicates whether the patient has '
                                                                         'a recorded history of '
                                                                         'hypercholesterolaemia.',
                                                'hypertension': 'Indicates whether the patient has a '
                                                                'recorded history of hypertension.',
                                                'i_vnitrate': 'Coded indicator of whether an intravenous '
                                                              'nitrate was given during the acute admission '
                                                              'episode.',
                                                'initial_diagnosis': 'Initial working diagnosis recorded at '
                                                                     'presentation, retained as the source '
                                                                     'label.',
                                                'initial_reperfusion': 'Categorical record of the initial '
                                                                       'reperfusion treatment given for the '
                                                                       'acute coronary syndrome episode.',
                                                'inpatient_management_of_hyperglycaemia_diabetes': 'Coded '
                                                                                                   'description '
                                                                                                   'of how '
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
                                                                                  'intended for the patient '
                                                                                  'at the interventional '
                                                                                  'centre.',
                                                'interventional_hospitaltxt': 'Decoded name of the '
                                                                              'interventional hospital '
                                                                              'involved in the transfer '
                                                                              'pathway.',
                                                'intraaortic_balloon_pump_support': 'Coded source indicator '
                                                                                    'of whether intra-aortic '
                                                                                    'balloon pump support '
                                                                                    'was used during the '
                                                                                    'episode.',
                                                'is_current_in_source': 'Boolean flag indicating whether the '
                                                                        'record was still present as current '
                                                                        'in the source system at the last '
                                                                        'load.',
                                                'iv2b3a_agent': 'Coded indication of whether an intravenous '
                                                                'glycoprotein IIb/IIIa agent was given '
                                                                'during the admission.',
                                                'iv_beta_blocker': 'Coded indicator of whether an '
                                                                   'intravenous beta blocker was given '
                                                                   'during the acute admission episode.',
                                                'killip_class': 'Killip classification of heart failure '
                                                                'severity recorded at presentation.',
                                                'left_ventricular_apical_thrombus': 'Coded source indicator '
                                                                                    'of whether left '
                                                                                    'ventricular apical '
                                                                                    'thrombus was '
                                                                                    'identified.',
                                                'left_ventricular_outflow_tract_obstruction': 'Coded source '
                                                                                              'indicator of '
                                                                                              'whether left '
                                                                                              'ventricular '
                                                                                              'outflow tract '
                                                                                              'obstruction '
                                                                                              'was '
                                                                                              'identified.',
                                                'linkage_fallback_conflict_ind': 'Boolean flag indicating '
                                                                                 'that the fallback linkage '
                                                                                 'produced a conflicting '
                                                                                 'person match during '
                                                                                 'processing.',
                                                'linkage_historical_fallback_ind': 'Boolean flag indicating '
                                                                                   'that the person linkage '
                                                                                   'was resolved using a '
                                                                                   'historical fallback '
                                                                                   'identifier rather than a '
                                                                                   'current match.',
                                                'linkage_method': 'Code describing which identifier '
                                                                  'combination was used to link this record '
                                                                  'to the master person identifier during '
                                                                  'pipeline processing.',
                                                'linkage_status': 'Derived code describing the outcome of '
                                                                  'linking the registry record to a master '
                                                                  'person record.',
                                                'local_intervention_date': 'Date and time of coronary '
                                                                           'intervention performed at the '
                                                                           'local hospital, as recorded in '
                                                                           'the source registry.',
                                                'loop_diuretic': 'Coded indicator of whether a loop diuretic '
                                                                 'was given during the acute admission '
                                                                 'episode.',
                                                'low_molecular_weight_heparin': 'Coded indication of whether '
                                                                                'low molecular weight '
                                                                                'heparin was given during '
                                                                                'the admission.',
                                                'lved_pvalue': 'Recorded numeric left ventricular '
                                                               'end-diastolic pressure value as held in the '
                                                               'source registry, with no unit asserted in '
                                                               'source metadata.',
                                                'lvedp': 'Coded source field recording left ventricular '
                                                         'end-diastolic pressure assessment status, with any '
                                                         'measured figure held in LVED_PVALUE.',
                                                'lvef': 'Categorical banding of left ventricular ejection '
                                                        'fraction assessed during the episode, including a '
                                                        'not-assessed option.',
                                                'm_hac_team_audit_notes': 'Free-text audit notes entered by '
                                                                          'the heart attack centre team '
                                                                          'about the registry record.',
                                                'm_regional_lv_dysfunction_distribution_other': 'Free-text '
                                                                                                'description '
                                                                                                'of a '
                                                                                                'regional '
                                                                                                'left '
                                                                                                'ventricular '
                                                                                                'dysfunction '
                                                                                                'distribution '
                                                                                                'not covered '
                                                                                                'by the '
                                                                                                'coded '
                                                                                                'categories.',
                                                'm_stress_precipitant_other': 'Free-text description of a '
                                                                              'stressful precipitant not '
                                                                              'covered by the coded '
                                                                              'Takotsubo precipitant '
                                                                              'categories.',
                                                'mrn': 'Local hospital medical record number identifying the '
                                                       'patient for this registry record.',
                                                'nhs_number': 'Patient NHS number as recorded in the source '
                                                              'registry, with validity indicated by '
                                                              'NHS_NUMBER_VALID_IND.',
                                                'nhs_number_valid_ind': 'Derived boolean flag indicating '
                                                                        'whether the source NHS number '
                                                                        'passed validity checking; the NHS '
                                                                        'number itself is not retained in '
                                                                        'this column.',
                                                'oral_beta_blocker': 'Coded indicator of whether an oral '
                                                                     'beta blocker was given during the '
                                                                     'acute admission episode.',
                                                'oral_nitrate': 'Coded indicator of whether an oral nitrate '
                                                                'was given during the acute admission '
                                                                'episode.',
                                                'other_discharge_diagnosistxt': 'Decoded text label of an '
                                                                                'additional or other '
                                                                                'discharge diagnosis '
                                                                                'category, retained as '
                                                                                'recorded in the source '
                                                                                'registry.',
                                                'outcome_of_arrest': 'Coded outcome of the cardiac arrest.',
                                                'pathway': 'Care pathway classification for the acute '
                                                           'coronary syndrome episode, as recorded in the '
                                                           'source registry.',
                                                'patient_admin_status': 'Administrative status of the '
                                                                        'patient for the admission, such as '
                                                                        'NHS or private funding category, as '
                                                                        'recorded in the source registry.',
                                                'patient_event_key': 'Deterministic SHA-256 key matching '
                                                                     'clinical_registry_entry.patient_event_key '
                                                                     'for the acs_transfer source family and '
                                                                     'native entry_id.',
                                                'patient_location_at_time_of_stemi': 'Categorical record of '
                                                                                     "the patient's care "
                                                                                     'location when the '
                                                                                     'STEMI occurred, '
                                                                                     'including a '
                                                                                     'not-applicable option '
                                                                                     'for non-STEMI '
                                                                                     'presentations.',
                                                'patient_ventilated': 'Indicator of whether the patient '
                                                                      'required mechanical ventilation '
                                                                      'during the acute coronary syndrome '
                                                                      'episode.',
                                                'peak_ck': 'Peak creatine kinase laboratory result recorded '
                                                           'for the episode; source units are not '
                                                           'documented.',
                                                'peak_troponin': 'Peak troponin laboratory result recorded '
                                                                 'for the episode, interpreted with '
                                                                 'reference to the assay recorded in '
                                                                 'TROPONIN_ASSAY; source units are not '
                                                                 'documented.',
                                                'peak_troponin_ng_l': 'Peak troponin in ng/L, unit decided '
                                                                      'per assay: High sensitivity Troponin '
                                                                      'I is recorded in ng/L and kept; High '
                                                                      'sensitivity Troponin T, Troponin T '
                                                                      'and Troponin I are recorded in ug/L, '
                                                                      'so values below 5 are x1000 (values '
                                                                      '>= 5 were already ng/L). NULL when '
                                                                      'the assay is unknown or missing. '
                                                                      'INSR_W2 W2.11 (L3029).',
                                                'peripheral_vascular_disease': 'Indicates whether the '
                                                                               'patient has a recorded '
                                                                               'history of peripheral '
                                                                               'vascular disease.',
                                                'person_id': 'Internal surrogate person identifier from the '
                                                             'source patient administration system used to '
                                                             'link the record to the master person record.',
                                                'pipeline_loaded_at': 'Timestamp recording when the pipeline '
                                                                      'loaded this row into the bronze '
                                                                      'table.',
                                                'place_first12_lead_ec_gperformed': 'Location where the '
                                                                                    'first 12-lead ECG was '
                                                                                    'performed for the '
                                                                                    'patient.',
                                                'positive_inotropic_supporttxt': 'Decoded source text '
                                                                                 'describing positive '
                                                                                 'inotropic support given, '
                                                                                 'following the txt suffix '
                                                                                 'convention for lookup '
                                                                                 'descriptions in this '
                                                                                 'extract.',
                                                'potassium_channel_modulator': 'Coded indicator of whether a '
                                                                               'potassium channel modulator '
                                                                               '(for example nicorandil) was '
                                                                               'given during the acute '
                                                                               'admission episode.',
                                                'previous_ami': 'Source-coded indicator of whether the '
                                                                'patient has a history of previous acute '
                                                                'myocardial infarction.',
                                                'previous_angina': 'Source-coded indicator of whether the '
                                                                   'patient has a history of previous '
                                                                   'angina.',
                                                'previous_cabg': 'Indicates whether the patient has '
                                                                 'undergone previous coronary artery bypass '
                                                                 'graft surgery.',
                                                'previous_history_of_takotsubo': 'Source-coded indicator of '
                                                                                 'whether the patient has a '
                                                                                 'previous history of '
                                                                                 'Takotsubo syndrome.',
                                                'previous_pci': 'Indicates whether the patient has undergone '
                                                                'previous percutaneous coronary '
                                                                'intervention.',
                                                'procedure_performed': 'Coded procedure actually performed '
                                                                       'at the interventional centre for '
                                                                       'this ACS episode.',
                                                'psychiatric_disease': 'Source-coded indicator of '
                                                                       'psychiatric disease recorded as part '
                                                                       'of the Takotsubo assessment '
                                                                       'comorbidities.',
                                                'qr_sduration': 'Recorded QRS duration category from the ECG '
                                                                'assessment, held as source-formatted text.',
                                                'qtc_interval': 'Corrected QT interval recorded from the ECG '
                                                                'in the Takotsubo assessment, held as '
                                                                'source-formatted text.',
                                                'radionuclide_study': 'Coded source record of whether a '
                                                                      'radionuclide myocardial study was '
                                                                      'carried out, arranged or judged '
                                                                      'unnecessary during the episode.',
                                                'reason_reperfusion_treatment_not_given': 'Coded MINAP '
                                                                                          'reason why '
                                                                                          'reperfusion '
                                                                                          'treatment was not '
                                                                                          'given for the '
                                                                                          'acute coronary '
                                                                                          'syndrome episode.',
                                                'referral_dt_tm': 'Date and time the referral was made '
                                                                  'within the acute coronary syndrome '
                                                                  'transfer pathway.',
                                                'referral_for_investigationintervention': 'Date and time of '
                                                                                          'referral for '
                                                                                          'further '
                                                                                          'investigation or '
                                                                                          'intervention, as '
                                                                                          'recorded in the '
                                                                                          'source registry.',
                                                'referring_hospitaltxt': 'Text label of the hospital that '
                                                                         'referred or transferred the '
                                                                         'patient, as held in the source '
                                                                         'registry.',
                                                'regional_lv_dysfunction_distribution': 'Source-coded '
                                                                                        'distribution '
                                                                                        'pattern of regional '
                                                                                        'left ventricular '
                                                                                        'dysfunction '
                                                                                        'recorded in the '
                                                                                        'Takotsubo '
                                                                                        'assessment.',
                                                'reinfarction': 'Source-coded indicator of whether '
                                                                'reinfarction occurred during the admission.',
                                                'reminder': 'Free-text reminder or follow-up note entered by '
                                                            'data collectors against the registry record.',
                                                'reperfusion_date_time': 'Date and time reperfusion '
                                                                         'treatment was delivered, as '
                                                                         'recorded in the source registry.',
                                                'row_hash': "Hash of the source record's field values used "
                                                            'to detect row-level changes between pipeline '
                                                            'loads.',
                                                'serum_cholesterol': 'Recorded serum cholesterol laboratory '
                                                                     'result for the acute episode; source '
                                                                     'units are not documented.',
                                                'serum_glucose': 'Recorded serum glucose laboratory result '
                                                                 'for the acute episode; source units are '
                                                                 'not documented.',
                                                'site_of_infarction': 'Coded anatomical site of the '
                                                                      'myocardial infarction.',
                                                'smoking_cessation_advice_given': 'Source-coded record of '
                                                                                  'whether smoking cessation '
                                                                                  'advice was given before '
                                                                                  'discharge.',
                                                'smoking_status': 'Recorded smoking status of the patient at '
                                                                  'the time of the ACS episode.',
                                                'source_absent_detected_ts': 'Timestamp recording when the '
                                                                             'pipeline first detected that '
                                                                             'the record was no longer '
                                                                             'present in the source system.',
                                                'source_present_ind': 'Canonical boolean mirror of '
                                                                      'IS_CURRENT_IN_SOURCE; false rows are '
                                                                      'retained source tombstones.',
                                                'source_record_key': 'Source system record identifier for '
                                                                     'this row, taken from the feeder '
                                                                     'registry entry key.',
                                                'source_table': 'Name of the source system table from which '
                                                                'this record was ingested.',
                                                'start_of_insulin_infusion': 'Date and time that an insulin '
                                                                             'infusion was started during '
                                                                             'the inpatient stay.',
                                                'statin_discharge': 'Coded indicator of whether a statin was '
                                                                    'prescribed at discharge.',
                                                'statin_use': 'Indicates whether the patient was taking a '
                                                              'statin prior to the ACS admission.',
                                                'stress_echo': 'Coded source record of whether stress '
                                                               'echocardiography was carried out, arranged '
                                                               'or judged unnecessary during the episode.',
                                                'stress_precipitanttxt': 'Source text label for the '
                                                                         'stressful precipitant category '
                                                                         'recorded in the Takotsubo '
                                                                         'assessment.',
                                                'surname': 'Patient surname as recorded in the source ACS '
                                                           'transfer registry record.',
                                                'symptom_onset': "Date and time of onset of the patient's "
                                                                 'cardiac symptoms, as recorded in the '
                                                                 'source registry.',
                                                'systolic_bp': 'Systolic blood pressure recorded at initial '
                                                               'assessment for the episode; source units are '
                                                               'not documented.',
                                                'thiazide_diuretic': 'Coded indicator of whether a thiazide '
                                                                     'diuretic was given during the acute '
                                                                     'admission episode.',
                                                'thienopyridine_inhibitor_discharge': 'Coded indicator of '
                                                                                      'whether a '
                                                                                      'thienopyridine '
                                                                                      'platelet inhibitor '
                                                                                      'was prescribed at '
                                                                                      'discharge.',
                                                'thienopyridine_inhibitor_use': 'Indicates whether the '
                                                                                'patient was taking a '
                                                                                'thienopyridine platelet '
                                                                                'inhibitor prior to the ACS '
                                                                                'admission.',
                                                'thienopyridine_platelet_inhibitor': 'Coded indication of '
                                                                                     'whether a '
                                                                                     'thienopyridine '
                                                                                     'platelet inhibitor was '
                                                                                     'given during the '
                                                                                     'admission.',
                                                'thrombolytic_drug': 'Coded record of the thrombolytic drug '
                                                                     'used, where thrombolysis was given for '
                                                                     'the acute coronary event.',
                                                'thyroid_disease': 'Source-coded indicator of thyroid '
                                                                   'disease recorded as part of the '
                                                                   'Takotsubo assessment comorbidities.',
                                                'ticagrelor_discharge': 'Coded indicator of whether '
                                                                        'ticagrelor was prescribed at '
                                                                        'discharge.',
                                                'ticagrelor_use': 'Source field recording ticagrelor use for '
                                                                  'the patient as captured in the registry '
                                                                  'medication section.',
                                                'time_left_dgh': 'Date and time the patient left the '
                                                                 'district general hospital for transfer, as '
                                                                 'recorded in the source registry.',
                                                'time_of_diagnostic_ecg': 'Date and time of the ECG that '
                                                                          'determined the diagnosis, as '
                                                                          'recorded in the source registry.',
                                                'transfer_cad': 'Source-recorded coronary artery disease '
                                                                'status captured in the transfer section of '
                                                                'the record.',
                                                'troponin_assay': 'Categorical record of the troponin assay '
                                                                  'type used for the recorded troponin '
                                                                  'result.',
                                                'unfractionated_heparin': 'Coded indicator of whether '
                                                                          'unfractionated heparin was given '
                                                                          'during the acute admission '
                                                                          'episode.',
                                                'unit_method': 'How peak_troponin_ng_l was derived: '
                                                               'assay_ng_l, assay_ug_l_x1000, '
                                                               'assay_ug_l_value_already_ng_l, or '
                                                               'undetermined (unknown/missing assay; '
                                                               'peak_troponin_ng_l NULL). INSR_W2 W2.11.',
                                                'ventricular_tachycardia_or_fibrillation': 'Coded source '
                                                                                           'indicator of '
                                                                                           'whether '
                                                                                           'ventricular '
                                                                                           'tachycardia or '
                                                                                           'ventricular '
                                                                                           'fibrillation was '
                                                                                           'recorded for the '
                                                                                           'episode.',
                                                'warfarin': 'Coded indicator of whether warfarin was given '
                                                            'during the acute admission episode.',
                                                'weight': 'Patient weight recorded for the episode; source '
                                                          'units are not documented.',
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
                                                                           'first administered during the '
                                                                           'acute episode.',
                                                'where_was_initial_reperfusion_treatment_given': 'Categorical '
                                                                                                 'record of '
                                                                                                 'the '
                                                                                                 'setting in '
                                                                                                 'which '
                                                                                                 'initial '
                                                                                                 'reperfusion '
                                                                                                 'treatment '
                                                                                                 'was '
                                                                                                 'delivered.',
                                                'why_was_no_angiogram_performed': 'Coded reason recorded '
                                                                                  'when no coronary '
                                                                                  'angiogram was performed.',
                                                'why_was_no_intervention_performed': 'Coded reason recorded '
                                                                                     'when no coronary '
                                                                                     'intervention was '
                                                                                     'performed.'},
                                    'table': 'Typed child of clinical_registry_entry for the acs_transfer '
                                             'family. MINAP-style ACS transfer pathway: symptom, transfer '
                                             'and reperfusion timestamps, GRACE/Killip, drugs, laboratory '
                                             'results and Takotsubo fields. Diagnoses remain source labels. '
                                             'ADMISSION_DATE_QUALITY identifies the known 2000-01-01 '
                                             'placeholder range. Every governed bronze column is retained '
                                             'under its lower-case name.'}}
